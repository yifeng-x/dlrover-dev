# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
"""Pre-checkpoint benefit evaluation using a normalized HTTP snapshot.

The endpoint is an adapter contract, not a SwanLab API implementation. It
combines training observations with candidate profiling and scheduler evidence.
No candidate throughput is inferred from GPU utilization or GPU count.
"""

import math
import os
import time
import uuid
from dataclasses import asdict, dataclass, field
from urllib.parse import urlparse

import requests

from dlrover.python.master.resource.benefit_log import BenefitLogger
from dlrover.python.master.resource.swanlab import (
    SwanLabTrainingMetrics,
    TrainingMetricsUnavailable,
)

class InvalidSnapshot(ValueError):
    pass


def number(data, key, minimum=0, positive=False):
    value = data.get(key)
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < minimum
        or (positive and value == 0)
    ):
        raise InvalidSnapshot(key)
    return value


@dataclass(frozen=True)
class BenefitResult:
    status: str
    reason: str
    estimates: dict = field(default_factory=dict)

    def to_dict(self):
        return asdict(self)


def evaluate(snapshot, request, now, max_age=60, min_window=30,
             objective="time", min_gain_ratio=0.05):
    """Return positive, negative or unknown; durations are seconds.

    Bounds are supplied conservative scenarios, not statistical confidence
    intervals. A missing observation never means zero cost or zero queue impact.
    """
    try:
        if objective not in ("time", "gpu_hours", "cluster_jct"):
            raise InvalidSnapshot("objective")
        if snapshot.get("schema_version") != 1:
            raise InvalidSnapshot("schema_version")
        for key in ("request_id", "job", "current", "target"):
            if snapshot.get(key) != request[key]:
                raise InvalidSnapshot("identity_or_resource_mismatch")
        observed = number(snapshot, "observed_at", positive=True)
        if not 0 <= now - observed <= max_age:
            raise InvalidSnapshot("stale_snapshot")
        for resource in (request["current"], request["target"]):
            for key in ("workers", "gpus_per_worker"):
                value = number(resource, key, positive=True)
                if int(value) != value:
                    raise InvalidSnapshot("resource_count")
        if (
            request["current"]["workers"] != request["target"]["workers"]
            or request["current"]["gpu_type"] != request["target"]["gpu_type"]
        ):
            raise InvalidSnapshot("not_homogeneous_vertical")

        training = snapshot["training"]
        if training.get("unit") not in ("steps", "samples", "tokens"):
            raise InvalidSnapshot("work_unit")
        if training.get("comparable") is not True:
            raise InvalidSnapshot("incomparable_training")
        start, end = training["start"], training["end"]
        if not start.get("generation") or start["generation"] != end.get("generation"):
            raise InvalidSnapshot("generation_mismatch")
        if "generation" in request and (
            not request["generation"] or end["generation"] != request["generation"]
        ):
            raise InvalidSnapshot("unexpected_training_generation")
        t0 = number(start, "observed_at", positive=True)
        t1 = number(end, "observed_at", positive=True)
        if t1 - t0 < min_window or not 0 <= now - t1 <= max_age:
            raise InvalidSnapshot("invalid_training_window")
        if training.get("steady") is not True:
            raise InvalidSnapshot("not_steady")
        progress = number(end, "work") - number(start, "work")
        if progress <= 0:
            raise InvalidSnapshot("no_progress")
        remaining = number(training, "target_work") - end["work"]
        if remaining <= 0:
            return BenefitResult("negative", "no_remaining_work")
        q0 = progress / (t1 - t0)

        transition = snapshot["transition"]
        delay = number(transition, "lost_seconds_upper")
        replay = number(transition, "replay_work_upper")
        overhead = number(transition, "gpu_seconds_upper")
        scheduling = snapshot["scheduling"]
        if scheduling.get("target_feasible") is False:
            return BenefitResult("negative", "target_infeasible")
        if scheduling.get("target_feasible") is not True:
            raise InvalidSnapshot("feasibility_unknown")
        # Explicit evidence is required even when the scheduler sees no impact.
        if scheduling.get("constraints_satisfied") is False:
            return BenefitResult("negative", "scheduler_constraints")
        if scheduling.get("constraints_satisfied") is not True:
            raise InvalidSnapshot("scheduler_constraints_unknown")

        limits = snapshot["limits"]
        number(limits, "max_delay_seconds")
        if limits.get("deadline_at") is not None:
            number(limits, "deadline_at", positive=True)
        candidate = snapshot["candidate"]
        if candidate == {"status": "unavailable"}:
            return BenefitResult("unknown", "candidate_unavailable", {
                "current_throughput": q0, "remaining_work": remaining,
                "baseline_seconds": remaining / q0,
            })
        if candidate.get("evidence") not in ("trial", "profile", "history"):
            raise InvalidSnapshot("candidate_evidence_missing")
        if not candidate.get("evidence_id"):
            raise InvalidSnapshot("candidate_evidence_missing")
        q1 = number(candidate, "throughput_lower", positive=True)

        g0 = request["current"]["workers"] * request["current"]["gpus_per_worker"]
        g1 = request["target"]["workers"] * request["target"]["gpus_per_worker"]
        baseline = remaining / q0
        runtime = (remaining + replay) / q1
        predicted = delay + runtime
        h0, h1 = g0 * baseline / 3600, (overhead + g1 * runtime) / 3600
        values = dict(
            current_throughput=q0, candidate_throughput=q1,
            remaining_work=remaining, baseline_seconds=baseline,
            candidate_seconds=predicted, time_gain_seconds=baseline - predicted,
            baseline_gpu_hours=h0, candidate_gpu_hours=h1,
            gpu_hours_saved=h0 - h1,
        )
        limits = snapshot["limits"]
        max_delay = number(limits, "max_delay_seconds")
        if predicted - baseline > max_delay:
            return BenefitResult("negative", "delay_budget", values)
        if limits.get("deadline_at") is not None:
            deadline = number(limits, "deadline_at", positive=True)
            if now + predicted > deadline:
                return BenefitResult("negative", "deadline", values)
        if objective == "gpu_hours":
            gain, denominator = h0 - h1, h0
        else:
            gain, denominator = baseline - predicted, baseline
        if objective == "cluster_jct":
            if scheduling.get("impacts_complete") is not True:
                raise InvalidSnapshot("queue_impact_unknown")
            weight = number(scheduling, "job_weight", positive=True)
            gain, denominator = weight * gain, weight * baseline
            seen = set()
            for impact in scheduling["other_jobs"]:
                job_id = impact["job_id"]
                if not isinstance(job_id, str) or not job_id or job_id in seen:
                    raise InvalidSnapshot("duplicate_queue_job")
                if job_id == request["job"]["job_uid"]:
                    raise InvalidSnapshot("queue_contains_current_job")
                seen.add(job_id)
                before = number(impact, "baseline_remaining_seconds")
                after = number(impact, "candidate_remaining_seconds_upper")
                weight = number(impact, "weight", positive=True)
                gain += weight * (before - after)
                denominator += weight * before
            values["cluster_jct_gain_seconds"] = gain
        if not all(math.isfinite(v) for v in values.values()) or not math.isfinite(gain):
            raise InvalidSnapshot("nonfinite_estimate")
        if not math.isfinite(denominator) or denominator <= 0:
            raise InvalidSnapshot("invalid_denominator")
        positive = gain > 0 and gain >= min_gain_ratio * denominator
        return BenefitResult(
            "positive" if positive else "negative",
            "gain_above_threshold" if positive else "insufficient_gain", values,
        )
    except (KeyError, TypeError, AttributeError, ValueError, ArithmeticError):
        # Do not include arbitrary provider text in logs/metric labels.
        return BenefitResult("unknown", "invalid_or_missing_evidence")


class BenefitGuard:
    """Opt-in HTTP gate: off preserves behavior; shadow observes; enforce gates."""

    def __init__(self, job=None, environ=None, clock=time.time, post=None,
                 generation_provider=None, platform_task_id=None, training_context_provider=None):
        env = dict(os.environ if environ is None else environ)
        label_task_id = isinstance(platform_task_id, str) and bool(platform_task_id.strip())
        # Only the task label supplies identity; never reuse a process env ID.
        env["LOW_CODE_PLATFORM_TASK_ID"] = platform_task_id.strip() if label_task_id else ""
        self._log = BenefitLogger("guard")
        self.job = dict(job or {})
        self.clock = clock
        self.post = post or requests.post
        self.generation_provider = generation_provider
        self.training_context_provider = training_context_provider
        self.recovery_provider = None
        self.training_metrics = (
            SwanLabTrainingMetrics(env, post=self.post)
            if (env.get("DLROVER_SWANLAB_HTTP_URL")
                or env.get("DLROVER_SWANLAB_MOCK", "false").strip().lower() == "true") else None
        )
        if self.training_metrics is not None:
            self._log.emit(
                "training_metrics_configured",
                job_uid=self.job.get("job_uid"),
                task_id=self.training_metrics.task_id,
                task_id_source="job_label" if label_task_id else "missing_job_label",
                valid=self.training_metrics.valid,
                data_source="mock" if self.training_metrics.mock else "http"
        )
        self.mode = env.get("DLROVER_BENEFIT_MODE", "off").lower()
        self.url = env.get("DLROVER_BENEFIT_HTTP_URL", "")
        self.token = env.get("DLROVER_BENEFIT_HTTP_TOKEN", "")
        self.objective = env.get("DLROVER_BENEFIT_OBJECTIVE", "time")
        self.valid = self.mode in ("off", "shadow", "enforce")
        try:
            self.timeout = float(env.get("DLROVER_BENEFIT_TIMEOUT_SECONDS", "3"))
            self.max_age = float(env.get("DLROVER_BENEFIT_MAX_AGE_SECONDS", "60"))
            self.min_window = float(env.get("DLROVER_BENEFIT_MIN_WINDOW_SECONDS", "30"))
            self.min_gain = float(env.get("DLROVER_BENEFIT_MIN_GAIN_RATIO", "0.05"))
            self.valid &= all(math.isfinite(x) and x > 0 for x in (
                self.timeout, self.max_age, self.min_window,
            )) and math.isfinite(self.min_gain) and 0 <= self.min_gain <= 1
            self.valid &= self.objective in ("time", "gpu_hours", "cluster_jct")
        except ValueError:
            self.valid = False
        self.last_request = None
        self.last_snapshot = None
        self.last_result = BenefitResult("unknown", "not_evaluated")

    @staticmethod
    def resource(group):
        return dict(workers=group.count,
                    gpus_per_worker=group.node_resource.gpu_num,
                    gpu_type=group.node_resource.gpu_type)

    def check(self, current, target, plan_id):
        if self.mode == "off":
            return True
        self.last_result = self._check(current, target, plan_id)
        return (self.valid and self.mode == "shadow") or (
            self.valid and self.last_result.status == "positive"
        )

    def _check(self, current, target, plan_id, exploration=None):
        started = time.monotonic()
        self.last_request = None
        result = self._evaluate_request(current, target, plan_id, exploration)
        request = self.last_request or {}
        operation = ("authorize_mutation" if (exploration or {}).get("authorize_mutation")
                     else "release_reservation" if (exploration or {}).get("release_reservation")
                     else "observation" if exploration else "evaluation")
        self._log.emit(
            "evaluation_finished", 
            repeat_key=(plan_id, operation, result.status, result.reason),
            plan_id=plan_id, 
            request_id=request.get("request_id"),
            job_uid=self.job.get("job_uid"), 
            generation=request.get("generation"),
            operation=operation, 
            mode=self.mode, 
            objective=self.objective,
            status=result.status, 
            reason=result.reason, 
            estimates=result.estimates,
            min_gain_ratio=getattr(self, "min_gain", None),
            duration_ms=round((time.monotonic() - started) * 1000, 3),
        )
        return result

    def _training_context(self):
        context = dict(self.training_context_provider() if self.training_context_provider else {})
        if self.recovery_provider:
            context.update(self.recovery_provider())
        else:
            context.setdefault("recovery_ready", True)
            context.setdefault("recovered_at", 0)
        return context

    def _evaluate_request(self, current, target, plan_id, exploration=None):
        self.last_snapshot = None
        try:
            parsed = urlparse(self.url)
        except ValueError:
            return BenefitResult("unknown", "invalid_configuration")
        if (not self.valid or parsed.scheme not in ("http", "https")
                or not parsed.hostname or parsed.username or parsed.password
                or not self.job.get("job_uid")):
            return BenefitResult("unknown", "invalid_configuration")
        try:
            request = dict(
                schema_version=1, request_id=uuid.uuid4().hex,
                job=self.job, plan_id=plan_id, requested_at=self.clock(),
                objective=self.objective,
                current=self.resource(current), target=self.resource(target),
            )
            if exploration is not None:
                request["exploration"] = exploration
            if self.generation_provider is not None:
                request["generation"] = self.generation_provider()
            self.last_request = request
            self._log.emit(
                "evaluation_started", 
                level="debug",
                plan_id=plan_id, 
                request_id=request["request_id"],
                generation=request.get("generation"),
                current=request["current"], 
                target=request["target"],
                mode=self.mode, 
                objective=self.objective
            )
            control_request = exploration and (
                exploration.get("authorize_mutation") or exploration.get("release_reservation")
            )
            training = None
            training_context = None
            if self.training_metrics is not None and not control_request:
                training_context = self._training_context()
                if training_context.get("generation") != request.get("generation"):
                    raise TrainingMetricsUnavailable("missing_or_changed_training_context")
                after = max((exploration or {}).get("observation_after", 0),
                            (exploration or {}).get("last_window_end", 0))
                training = self.training_metrics.fetch(
                    generation=request.get("generation"), 
                    clock=self.clock, 
                    timeout=self.timeout,
                    max_age=self.max_age, 
                    min_window=self.min_window, 
                    after=after,
                    plan_id=plan_id, 
                    request_id=request["request_id"],
                    training_context=training_context,
                )
                request["training"] = training
            headers = {"Accept": "application/json"}
            if self.token:
                headers["Authorization"] = "Bearer " + self.token
            http_started = time.monotonic()
            with self.post(self.url, json=request, headers=headers,
                           timeout=self.timeout, allow_redirects=False) as response:
                self._log.emit(
                    "evidence_response", 
                    level="debug" if response.status_code == 200 else "warning",
                    repeat_key=(plan_id, response.status_code),
                    plan_id=plan_id, 
                    request_id=request["request_id"],
                    http_status=response.status_code,
                    duration_ms=round((time.monotonic() - http_started) * 1000, 3),
                )
                if response.status_code != 200:
                    return BenefitResult("unknown", "http_error")
                snapshot = response.json()
            if training is not None:
                if not isinstance(snapshot, dict):
                    raise ValueError("invalid_benefit_snapshot")
                # Model/data/parallelism comparability remains the evidence
                # service's responsibility; scalar progress cannot prove it.
                training = {**training, "comparable": (
                    snapshot.get("training", {}).get("comparable") is True
                )}
                snapshot["training"] = training
            self.last_snapshot = snapshot
            if training_context is not None and training_context != self._training_context():
                return BenefitResult("unknown", "training_context_changed_during_fetch")
            if not control_request and self.generation_provider is not None:
                if request["generation"] != self.generation_provider():
                    return BenefitResult("unknown", "generation_changed_during_fetch")
            return evaluate(snapshot, request, self.clock(), self.max_age,
                            self.min_window, self.objective, self.min_gain)
        except TrainingMetricsUnavailable as error:
            return BenefitResult("unknown", str(error))
        except (requests.RequestException, ValueError, TypeError, AttributeError) as error:
            self._log.emit(
                "evidence_failed", level="warning",
                repeat_key=(plan_id, type(error).__name__),
                plan_id=plan_id, 
                request_id=(self.last_request or {}).get("request_id"),
                error_type=type(error).__name__
            )
            return BenefitResult("unknown", "http_or_payload_error")
