# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
"""Best-effort structured events, separate from controller and recovery state."""

import copy
import functools
import json
import math
import threading
import time
import uuid

from dlrover.python.common.log import default_logger as logger


def best_effort(method):
    @functools.wraps(method)
    def call(self, *args, **kwargs):
        try:
            with self._lock:
                return method(self, *args, **kwargs)
        except Exception:
            logger.warning("Vertical event reporting failed: %s", method.__name__)
    return call


def resource_snapshot(group):
    if group is None:
        return None
    resource = group.node_resource
    return dict(workers=group.count, gpus_per_worker=resource.gpu_num,
                total_gpus=group.count * resource.gpu_num,
                gpu_resource_type=resource.gpu_type,
                cpu_per_worker=resource.cpu, memory_mb_per_worker=resource.memory)


def finite_values(value):
    """Invalid measurements must not suppress the event explaining rejection."""
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {key: finite_values(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [finite_values(item) for item in value]
    return value


class VerticalElasticEvents:
    def __init__(self, job=None, task_id=None, sink=None, clock=time.time,
                 observation_provider=None):
        self.job = copy.deepcopy(job or {})
        self.task_id = task_id
        self.clock = clock
        self.sink = sink or self._log
        self.observation_provider = observation_provider
        self._lock = threading.RLock()
        self.context = None

    @staticmethod
    def _log(event):
        logger.info("VerticalElasticEvent %s", json.dumps(
            event, ensure_ascii=False, allow_nan=False, sort_keys=True))

    def _emit(self, context, event_type, **fields):
        event = {k: v for k, v in context.items() if not k.startswith("_")}
        event.update(schema_version=1, event_id=uuid.uuid4().hex,
                     event_type=event_type, occurred_at=self.clock(),
                     source="dlrover-master", job=self.job,
                     swanlab_task_id=self.task_id, **fields)
        # Snapshot before dispatch: later updates never rewrite an earlier event.
        event = finite_values(copy.deepcopy(event))
        json.dumps(event, allow_nan=False)
        self.sink(event)

    @best_effort
    def request(self, request_id, action, result, reason, trigger_source):
        self._emit(dict(request_id=request_id or uuid.uuid4().hex, plan_id=None, action=action,
                        trigger_source=trigger_source), "requested",
                   request_result=result, reason_code=reason)

    @best_effort
    def start(self, plan_id, action, request_id=None, trigger_source="unknown",
              root_plan_id=None, parent_plan_id=None, resumed=False, exploring=False):
        self.context = dict(
            plan_id=plan_id, root_plan_id=root_plan_id or plan_id,
            parent_plan_id=parent_plan_id, request_id=request_id,
            action=action, trigger_source=trigger_source,
            started_at=self.clock(),
            resources=dict(before=None, target=None, actual_after=None, final=None),
            execution_status="completed" if resumed else "pending", recovery_status="pending",
            benefit=None, _finished=False, _exploring=exploring,
        )
        self._emit(self.context, "phase_changed", phase="resumed" if resumed else "preparing")

    @best_effort
    def resources(self, before, target, generation=None):
        if self.context is not None:
            self.context["resources"].update(
                before=resource_snapshot(before), target=resource_snapshot(target))
            self.context["generation_before"] = generation or None

    @best_effort
    def evaluated(self, guard, allowed, exploring):
        if self.context is None:
            return
        result = guard.last_result
        snapshot = (guard.last_snapshot or {}) if guard.mode != "off" else {}
        snapshot = snapshot if isinstance(snapshot, dict) else {}
        # Do not emit credentials, arbitrary provider fields or entire histories.
        fields = {
            "training": ("unit", "target_work", "comparable", "steady", "global_batch_size", "data_source", "steady_source", "time_source", "steady_windows", "batch_size_source", "global_batch_size_fixed", "reported_train_speed", "speed_relative_difference"),
            "transition": ("lost_seconds_upper", "replay_work_upper", "gpu_seconds_upper"),
            "limits": ("max_delay_seconds", "deadline_at"),
            "candidate": ("status", "throughput_lower", "evidence", "evidence_id"),
        }
        evidence = {name: {key: snapshot[name][key] for key in keys if key in snapshot[name]}
                    for name, keys in fields.items() if isinstance(snapshot.get(name), dict)}
        for endpoint in ("start", "end"):
            training = snapshot.get("training")
            point = training.get(endpoint) if isinstance(training, dict) else None
            if isinstance(point, dict):
                evidence["training"][endpoint] = {k: point[k] for k in (
                    "generation", "observed_at", "work") if k in point}
        scheduling = snapshot.get("scheduling", {})
        scheduling = scheduling if isinstance(scheduling, dict) else {}
        evidence["scheduling"] = {k: scheduling[k] for k in (
            "target_feasible", "constraints_satisfied", "impacts_complete",
            "job_weight"
        ) if k in scheduling}
        if isinstance(scheduling.get("other_jobs"), list):
            evidence["scheduling"]["other_jobs"] = [
                {key: job[key] for key in ("job_id", "weight", "baseline_remaining_seconds",
                                          "candidate_remaining_seconds_upper") if key in job}
                for job in scheduling["other_jobs"] if isinstance(job, dict)
            ]
        self.context["_exploring"] = exploring
        self.context["benefit"] = dict(
            mode=guard.mode, objective=guard.objective, rule_version=1,
            evaluation_status="not_evaluated" if guard.mode == "off" else result.status,
            decision="explore" if exploring else "allow" if allowed else "reject",
            reason_code="disabled" if guard.mode == "off" else result.reason,
            min_gain_ratio=guard.min_gain,
            estimates={} if guard.mode == "off" else result.estimates,
            evidence=evidence, evidence_request_id=(guard.last_request or {}).get("request_id"),
            estimate_kind="prediction",
        )
        self._emit(self.context, "evaluated")

    @best_effort
    def phase(self, phase):
        if self.context is not None and not self.context["_finished"]:
            now = self.clock()
            previous_at = self.context.get("phase_started_at", self.context["started_at"])
            self.context["previous_phase_duration_seconds"] = max(0, now - previous_at)
            self.context["phase_started_at"] = now
            self.context["phase"] = phase
            self._emit(self.context, "phase_changed")

    @best_effort
    def control_finished(self, result, reason):
        context = self.context
        if context is None or context["_finished"]:
            return
        context["execution_status"] = result
        if result == "skipped":
            context["phase"] = "skipped"
        if result == "completed":
            self._observe(context)
            self._emit(context, "control_completed", reason_code=reason)
        elif context["_exploring"]:
            self._emit(context, "phase_changed", phase="failed", reason_code=reason)
        else:
            self.finish(result, reason, context)

    @best_effort
    def checkpoint(self, step, path, targets):
        if self.context is not None:
            self.context.update(checkpoint_step=step, checkpoint_ref=path,
                                target_node_ids=sorted(targets))

    def _observe(self, context):
        if self.observation_provider:
            try:
                observed = self.observation_provider(context.get("target_node_ids", []))
            except Exception:
                observed = {"status": "observation_unavailable", "resources": None,
                            "generation": None}
            context["resource_observation"] = observed
            if context["resources"]["actual_after"] is None:
                context["resources"]["actual_after"] = observed.get("resources")
            context["generation_after"] = observed.get("generation")
            observed["matches_target"] = (
                observed.get("resources") == context["resources"]["target"]
                if observed.get("resources") is not None else None
            )

    @best_effort
    def recovery_finished(self, context, result, elapsed, progress=None):
        if context is None or context["_finished"]:
            return
        context.update(recovery_status=result, recovery_seconds=elapsed)
        if result == "recovered":
            self._observe(context)
            self._emit(context, "training_recovered", recovery_evidence=progress,
                       recovery_basis="replacement_worker_progress")
        if not context["_exploring"]:
            self.finish("completed" if result == "recovered" else "unverified",
                        "training_" + result, context)

    @best_effort
    def exploration_decided(self, state, decision):
        context = self.context
        if (context is None or context["root_plan_id"] != state["id"]
                or context.get("_exploration_decision") == decision):
            return
        context["_exploration_decision"] = decision
        self._observe(context)
        context["exploration"] = dict(
            status=decision, reason_code=state.get("reason"),
            evaluation_status=("positive" if decision == "ACCEPTED" else
                               "unknown" if decision in ("INCONCLUSIVE", "FAILED") else
                               "not_accepted"),
            estimates=copy.deepcopy(state.get("result")),
            gain_unit="seconds" if state["objective"] == "time" else "gpu_seconds",
            estimate_kind="observed_throughput_projection",
            spent_gpu_seconds=state.get("spent_gpu_seconds"),
            candidate_generation=state.get("candidate_generation"),
        )
        if decision in ("ACCEPTED", "ROLLED_BACK"):
            # This is scalar-window verification, distinct from native progress reports.
            context["recovery_status"] = "verified_by_exploration"
        self._emit(context, "exploration_decided")
        self.finish(decision.lower(), state.get("reason", decision.lower()), context)

    @best_effort
    def finish(self, result, reason, context=None):
        context = self.context if context is None else context
        if context is None or context["_finished"]:
            return
        # Re-read known worker state; never copy the requested target as fact.
        self._observe(context)
        context["resources"]["final"] = (
            None if result == "rollback_requested"
            else context.get("resource_observation", {}).get("resources")
        )
        context["_finished"] = True
        self._emit(context, "finished", result=result, reason_code=reason,
                   elapsed_seconds=max(0, self.clock() - context["started_at"]))
