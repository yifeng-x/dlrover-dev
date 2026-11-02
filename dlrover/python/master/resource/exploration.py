# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
"""One bounded, durable observation-only trial per job. Never auto-rollback.

State directory must be a durable shared POSIX volume with working flock.
Interrupted mutations are fenced for reconciliation, never blindly replayed.
"""

import copy
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
import time

from dlrover.python.common.log import default_logger as logger
from dlrover.python.common.node import NodeGroupResource, NodeResource
from dlrover.python.master.resource.benefit import number
from dlrover.python.master.resource.benefit_log import BenefitLogger

POLICY = "observe_only"
# ROLLED_BACK is accepted only for completed legacy journals.
TERMINAL = {"ACCEPTED", "RETAINED", "ROLLED_BACK"}
FINISHED = TERMINAL | {"FAILED", "INCONCLUSIVE"}


def pack(group):
    r = group.node_resource
    return dict(count=group.count, cpu=r.cpu, memory=r.memory,
                gpu_type=r.gpu_type, gpu_num=r.gpu_num, priority=r.priority,
                image=r.image, kwargs=copy.deepcopy(r.kwargs))


def unpack(data):
    r = NodeResource(data["cpu"], data["memory"], data["gpu_type"],
                     data["gpu_num"], priority=data["priority"], **data["kwargs"])
    r.image = data["image"]
    return NodeGroupResource(data["count"], r)


class TrialStore:
    def __init__(self, directory, job):
        if not directory or not job.get("job_uid"):
            raise ValueError("Durable state directory and job identity required")
        self.directory = Path(directory)
        if not self.directory.is_absolute():
            raise ValueError("State directory must be an absolute durable path")
        key = hashlib.sha256(json.dumps(job, sort_keys=True).encode()).hexdigest()
        self.path = self.directory / (key + ".json")
        self.lock = None

    def acquire(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        self.lock = open(str(self.path) + ".lock", "a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.release()
            return False
        return True

    def release(self):
        if self.lock is not None:
            self.lock.close()
            self.lock = None

    def load(self):
        if not self.path.exists():
            return None
        with self.path.open() as stream:
            data = json.load(stream)
        if not isinstance(data, dict) or data.get("version") != 1:
            raise ValueError("Invalid exploration journal")
        return data

    def save(self, data):
        if self.lock is None:
            raise RuntimeError("Exploration journal is not locked")
        fd, name = tempfile.mkstemp(dir=self.directory, prefix="trial-")
        try:
            with os.fdopen(fd, "w") as stream:
                json.dump(data, stream, allow_nan=False)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, self.path)
            directory_fd = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if os.path.exists(name):
                os.unlink(name)


class Exploration:
    def __init__(self, guard, env=None, clock=time.time, metrics=None):
        env = os.environ if env is None else env
        self._log = BenefitLogger("exploration")
        self.guard = guard
        self.metrics = metrics
        self.clock = clock
        # Startup switch: only explicit true enables trials. Keep journal
        # recovery fences active even when new exploration is disabled.
        self.enabled = (
            env.get("DLROVER_BENEFIT_EXPLORE", "false").strip().lower() == "true"
        )
        directory = env.get("DLROVER_BENEFIT_STATE_DIR", "")
        self.store = None
        self.state = None
        self.valid = True
        try:
            self.duration = float(env.get("DLROVER_BENEFIT_TRIAL_SECONDS", "600"))
            self.poll = float(env.get("DLROVER_BENEFIT_TRIAL_POLL_SECONDS", "10"))
            self.cooldown = float(env.get("DLROVER_BENEFIT_TRIAL_COOLDOWN_SECONDS", "1800"))
            self.max_gpu_hours = float(env.get("DLROVER_BENEFIT_TRIAL_GPU_HOURS", "0"))
            self.valid = all(math.isfinite(v) and v > 0 for v in (
                self.duration, self.poll, self.cooldown, self.max_gpu_hours
            ))
            if directory:
                self.store = TrialStore(directory, guard.job)
        except (TypeError, ValueError):
            self.valid = False
        self.active = False

    def startup_fence(self):
        """Return (block_initial_launch, require_manual_reconciliation)."""
        if self.store is None:
            return False, False
        try:
            state = self.store.load()
            if not state or state["status"] in TERMINAL:
                return False, False
            return True, not (state.get("policy") == POLICY and state["status"] == "OBSERVING")
        except (OSError, ValueError, KeyError, TypeError):
            return True, True

    def enter(self, recovering=False):
        allowed = self._enter(recovering)
        self._log.emit(
            "entry", 
            recovering=recovering, 
            allowed=allowed,
            enabled=self.enabled, valid=self.valid,
            plan_id=(self.state or {}).get("id"),
            status=(self.state or {}).get("status")
        )
        return allowed

    def _enter(self, recovering=False):
        """Called with scaler mutex held, also fences other Masters via PVC."""
        if not self.enabled and self.store is None:
            return not recovering
        try:
            if self.store is None or not self.store.acquire():
                return False
            self.state = self.store.load()
            self._publish_metrics(restored=True)
            if self.state:
                status = self.state["status"]
                if status in FINISHED and not self.state.get("reported", False):
                    self.report_result()
                if status in TERMINAL:
                    return (not recovering and
                            self.clock() >= self.state["cooldown_until"])
                if status == "OBSERVING" and self.state.get("policy") == POLICY and recovering:
                    self.active = True
                    return self.enabled and self.guard.mode == "enforce" and self.valid
                # Partial mutations and all unfinished legacy trials need manual
                # reconciliation. Never resume an old automatic rollback policy.
                return False
            return not recovering and (not self.enabled or self.valid)
        except (OSError, ValueError, KeyError, TypeError):
            logger.error("Exploration journal unavailable or invalid; scaling blocked")
            return False

    def leave(self):
        if self.store:
            self.store.release()
        self.active = False

    def _publish_metrics(self, restored=False):
        if self.metrics is not None:
            try:
                self.metrics.record_exploration(self.state, restored=restored)
            except Exception:
                # Observability must never change the resource decision.
                logger.exception("Failed to publish exploration metrics")

    def save(self, status=None, **changes):
        next_state = copy.deepcopy(self.state)
        next_state.update(changes)
        if status:
            next_state["status"] = status
        self.store.save(next_state)
        self.state = next_state
        self._publish_metrics()
        self._log.emit(
            "state_saved", 
            repeat_key=(
                self.state["id"], 
                self.state["status"],
                self.state.get("windows"), 
                self.state.get("reported")
            ),
            plan_id=self.state["id"], 
            status=self.state["status"], 
            reason=self.state.get("reason"),
            windows=self.state.get("windows"), 
            measured_lower=self.state.get("measured_lower"),
            spent_gpu_seconds=self.state.get("spent_gpu_seconds"),
            gpu_budget=self.state.get("gpu_budget"), 
            reported=self.state.get("reported", False),
        )

    def finish(self, status, reason):
        self.save(status, reason=reason, cooldown_until=self.clock() + self.cooldown)
        self.active = False
        self._log.emit(
            "finished", 
            plan_id=self.state["id"], 
            status=status, 
            reason=reason,
            windows=self.state.get("windows"),
            spent_gpu_seconds=self.state.get("spent_gpu_seconds")
        )

    def prepare(self, current, target, plan_id):
        """Only explicit missing candidate evidence can enter exploration."""
        def reject(reason):
            self._log.emit("admission", plan_id=plan_id, allowed=False, reason=reason)
            return False

        if (not self.enabled or not self.valid or self.state is not None
                or self.guard.mode != "enforce" or self.guard.objective not in ("time", "gpu_hours")
                or self.guard.last_result.reason != "candidate_unavailable"
                or self.store is None or self.store.lock is None):
            return reject("ineligible_or_disabled")
        snapshot = self.guard.last_snapshot
        try:
            admission = snapshot["exploration"]
            if admission.get("policy") != POLICY:
                return reject("observation_policy_required")
            if not all(admission.get(k) is True for k in (
                "allowed", "checkpoint_compatible", "quiescent_checkpoint"
            )):
                return reject("admission_conditions")
            tiers = admission["gpu_tiers"]
            if (not isinstance(tiers, list) or len(tiers) < 2 or
                    any(type(t) is not int or t <= 0 for t in tiers) or
                    tiers != sorted(set(tiers))):
                return reject("invalid_gpu_tiers")
            a, b = pack(current), pack(target)
            if abs(tiers.index(a["gpu_num"]) - tiers.index(b["gpu_num"])) != 1:
                return reject("nonadjacent_gpu_tiers")
            if {k: v for k, v in a.items() if k != "gpu_num"} != {
                k: v for k, v in b.items() if k != "gpu_num"
            }:
                return reject("resource_shape_changed")
            limits = snapshot["limits"]
            baseline = self.guard.last_result.estimates["baseline_seconds"]
            # The configured duration is a ceiling, not a requirement for the
            # task to outlive an entire trial plus a hypothetical rollback.
            duration = min(self.duration, baseline)
            if duration <= 2 * self.guard.min_window:
                return reject("insufficient_remaining_time")
            if limits["max_delay_seconds"] < duration:
                return reject("delay_budget")
            deadline = limits.get("deadline_at")
            if deadline is not None and self.clock() + baseline + duration > deadline:
                return reject("deadline")
            gpu_budget = self.max_gpu_hours * 3600
            trial_gpu = max(a["gpu_num"], b["gpu_num"]) * a["count"] * duration
            if trial_gpu > gpu_budget:
                return reject("gpu_budget")
            self.state = dict(
                version=1, policy=POLICY, id=plan_id, status="PREPARING", original=a, target=b,
                started_at=self.clock(), expires_at=self.clock() + duration,
                gpu_budget=gpu_budget,
                objective=self.guard.objective, baseline=copy.deepcopy(snapshot),
                q0=self.guard.last_result.estimates["current_throughput"],
                baseline_seconds=baseline, last_window_end=0, windows=0,
                spent_gpu_seconds=0,
            )
            self.store.save(self.state)
            self._publish_metrics()
            self.active = True
            self._log.emit(
                "admission", 
                plan_id=plan_id, 
                allowed=True,
                reason="eligible", 
                baseline_throughput=self.state["q0"],
                expires_at=self.state["expires_at"], 
                gpu_budget=gpu_budget,
                current_gpus=a["gpu_num"], 
                target_gpus=b["gpu_num"]
            )
            return True
        except (KeyError, TypeError, ValueError, OSError):
            # No resource mutation may follow an uncertain journal write.
            self.active = False
            return reject("invalid_evidence_or_journal")

    def authorize_mutation(self):
        allowed = self._authorize_mutation()
        self._log.emit(
            "mutation_authorized", 
            plan_id=self.state["id"],
            allowed=allowed,
            request_id=(self.guard.last_request or {}).get("request_id")
        )
        return allowed

    def _authorize_mutation(self):
        """Refresh target feasibility and explicit admission before replacement."""
        state = self.state
        if state.get("policy") != POLICY or state["status"] != "SWITCHING":
            return False
        current = unpack(state["original"])
        target = unpack(state["target"])
        context = dict(
            id=state["id"], 
            status=state["status"],
            policy=POLICY,
            checkpoint_path=state["checkpoint_path"],
            authorize_mutation=True
        )
        self.guard._check(current, target, state["id"], context)
        data, request = self.guard.last_snapshot, self.guard.last_request
        try:
            if data.get("schema_version") != 1:
                return False
            if any(data.get(key) != request[key] for key in ("request_id", "job", "current", "target")):
                return False
            if not 0 <= self.clock() - number(data, "observed_at") <= self.guard.max_age:
                return False
            if data.get("exploration_id") != state["id"]:
                return False
            scheduling = data["scheduling"]
            if scheduling.get("target_feasible") is not True or scheduling.get("constraints_satisfied") is not True:
                return False
            admission = data["exploration"]
            return (admission.get("policy") == POLICY
                    and admission.get("allowed") is True)
        except (KeyError, TypeError, AttributeError, ValueError):
            return False

    def report_result(self):
        """Idempotently upload durable outcomes, including incomplete observations."""
        state = self.state
        try:
            context = dict(id=state["id"], status=state["status"],
                           reason=state.get("reason"), policy=state.get("policy"),
                           objective=state.get("objective"),
                           result=state.get("result"), windows=state.get("windows", 0),
                           candidate_observed_at=state.get("candidate_observed_at"),
                           candidate_generation=state.get("candidate_generation"))
            if state.get("policy") == POLICY:
                context["report_result"] = True
            else:
                # Completed legacy journals may still need to release capacity.
                # This is a notification only; it cannot execute a reverse plan.
                context.update(release_reservation=True,
                               reservation_id=state.get("reservation_id"))
            self.guard._check(unpack(state["original"]), unpack(state["target"]),
                              state["id"], context)
            ack = self.guard.last_snapshot
            if (isinstance(ack, dict) and ack.get("recorded") is True
                    and ack.get("exploration_id") == state["id"]
                    and ack.get("request_id") == self.guard.last_request["request_id"]):
                self.save(reported=True)
            self._log.emit(
                "result_reported", 
                plan_id=state["id"],
                acknowledged=self.state.get("reported", False),
                request_id=(self.guard.last_request or {}).get("request_id")
            )
        except Exception:
            logger.warning("Exploration result notification failed")

    def switching(self, checkpoint_step, checkpoint_path):
        if not checkpoint_path:
            raise ValueError("Exploration requires a persistent checkpoint path")
        self.save("SWITCHING",
                  checkpoint_step=checkpoint_step, checkpoint_path=checkpoint_path)

    def observing(self):
        self.save("OBSERVING",
                  observation_after=self.clock(), last_window_end=0, windows=0,
                  observation_generation=None, measured_lower=None)

    def _wait(self, reason):
        self._log.emit(
            "observation_wait", 
            repeat_key=(self.state["id"], self.state["status"], reason),
            plan_id=self.state["id"], 
            status=self.state["status"], 
            reason=reason,
            request_id=(self.guard.last_request or {}).get("request_id"),
            windows=self.state.get("windows"),
            remaining_seconds=max(0, self.state["expires_at"] - self.clock())
        )
        return "wait"

    def inspect(self):
        """Poll candidate performance; never issue a resource mutation."""
        state = self.state
        now = self.clock()
        if state.get("policy") != POLICY or state["status"] != "OBSERVING":
            self.finish("INCONCLUSIVE", "state_requires_reconciliation")
            return "INCONCLUSIVE"
        if now >= state["expires_at"]:
            self.finish("INCONCLUSIVE", "observation_budget_exhausted")
            return "INCONCLUSIVE"
        # Both sides describe the running candidate. The original resource is
        # carried separately as the comparison baseline, not a reverse plan.
        current = unpack(state["target"])
        target = unpack(state["target"])
        context = dict(id=state["id"], status=state["status"],
                       policy=POLICY, original=self.guard.resource(unpack(state["original"])),
                       checkpoint_path=state["checkpoint_path"],
                       applied_plan_id=state["id"],
                       observation_after=state["observation_after"],
                       last_window_end=state["last_window_end"],
                       started_at=state["started_at"])
        result = self.guard._check(current, target, context["applied_plan_id"], context)
        snapshot, request = self.guard.last_snapshot, self.guard.last_request
        # This also validates identities, freshness, window and constraints.
        if result.status == "unknown" and result.reason != "candidate_unavailable":
            return self._wait(result.reason)
        try:
            execution = snapshot["execution"]
            training = snapshot["training"]
            start, end = training["start"], training["end"]
            if (snapshot.get("exploration_id") != state["id"]
                    or execution.get("applied_plan_id") != context["applied_plan_id"]
                    or execution.get("all_workers_ready") is not True
                    or execution.get("restored_checkpoint") != state["checkpoint_path"]
                    or execution.get("generation") != start["generation"]
                    or training["unit"] != state["baseline"]["training"]["unit"]
                    or training["target_work"] != state["baseline"]["training"]["target_work"]
                    or training.get("global_batch_size") != state["baseline"]["training"].get("global_batch_size")
                    or training.get("global_batch_size_fixed") != state["baseline"]["training"].get("global_batch_size_fixed")
                    or start["observed_at"] < state["observation_after"]
                    or start["observed_at"] < state["last_window_end"]):
                return self._wait("execution_or_window_mismatch")
            if state.get("observation_generation") not in (None, start["generation"]):
                return self._wait("observation_generation_changed")
            if start["generation"] == state["baseline"]["training"]["end"]["generation"]:
                return self._wait("original_generation")
            if training["unit"] == "steps" and end["work"] <= state["checkpoint_step"]:
                return self._wait("checkpoint_not_exceeded")
            if snapshot["scheduling"].get("constraints_satisfied") is not True:
                return self._wait("scheduler_constraints")
            spent = number(snapshot["execution"], "trial_gpu_seconds")
            if spent < state["spent_gpu_seconds"]:
                return self._wait("gpu_accounting_regressed")
            if spent > state["gpu_budget"]:
                self.finish("INCONCLUSIVE", "gpu_budget_exhausted")
                return "INCONCLUSIVE"
            q1 = (end["work"] - start["work"]) / (end["observed_at"] - start["observed_at"])
            if not math.isfinite(q1) or q1 <= 0:
                return self._wait("invalid_throughput")
            q1 = min(q1, state.get("measured_lower") or q1)
            self.save(last_window_end=end["observed_at"], windows=state["windows"] + 1,
                      spent_gpu_seconds=spent, observation_generation=start["generation"],
                      measured_lower=q1)
            state = self.state
            remaining = training["target_work"] - end["work"]
            if state["windows"] < 2:
                if remaining <= 0:
                    self.finish("INCONCLUSIVE", "training_finished_before_observation")
                    return "INCONCLUSIVE"
                return self._wait("more_windows_required")
            elapsed = now - state["started_at"]
            keep = max(0, remaining) / q1
            g0 = state["original"]["count"] * state["original"]["gpu_num"]
            g1 = state["target"]["count"] * state["target"]["gpu_num"]
            limits = copy.deepcopy(state["baseline"]["limits"])
            limits["max_delay_seconds"] = min(limits["max_delay_seconds"], number(snapshot["limits"], "max_delay_seconds"))
            current_deadline = snapshot["limits"].get("deadline_at")
            if current_deadline is not None:
                limits["deadline_at"] = min(limits.get("deadline_at") or current_deadline,
                                            number(snapshot["limits"], "deadline_at", positive=True))
            keep_allowed = elapsed + keep <= state["baseline_seconds"] + limits["max_delay_seconds"]
            if limits.get("deadline_at") is not None:
                keep_allowed &= now + keep <= limits["deadline_at"]
            if state["objective"] == "time":
                total_gain = state["baseline_seconds"] - elapsed - keep
                denominator = state["baseline_seconds"]
            else:
                total_gain = g0 * state["baseline_seconds"] - spent - g1 * keep
                denominator = g0 * state["baseline_seconds"]
            if not all(math.isfinite(v) for v in (elapsed, keep, total_gain, denominator)):
                return self._wait("invalid_observation")
            positive = total_gain > 0 and total_gain >= self.guard.min_gain * denominator
            values = dict(candidate_throughput=q1, baseline_throughput=state["q0"],
                          gain_unit="seconds" if state["objective"] == "time" else "gpu_seconds",
                          overall_gain=total_gain, gain_ratio=total_gain / denominator,
                          time_gain_seconds=state["baseline_seconds"] - elapsed - keep,
                          gpu_seconds_saved=g0 * state["baseline_seconds"] - spent - g1 * keep,
                          keep_seconds=keep, elapsed_seconds=elapsed,
                          remaining_work=max(0, remaining), trial_gpu_seconds=spent,
                          evaluation_status="positive" if positive and keep_allowed else "negative",
                          constraints_satisfied=bool(keep_allowed),
                          data_source=training.get("data_source", "http"),
                          unit=training["unit"], windows=state["windows"],
                          candidate_resource=self.guard.resource(unpack(state["target"])))
            self.save(result=values, candidate_generation=start["generation"],
                      candidate_observed_at=end["observed_at"])
            self._log.emit("observed_benefit", plan_id=state["id"],
                           objective=state["objective"], keep_allowed=keep_allowed,
                           min_gain_ratio=self.guard.min_gain, **values)
            if not keep_allowed:
                self.finish("INCONCLUSIVE", "constraint_violation")
                return "INCONCLUSIVE"
            if positive:
                self.finish("ACCEPTED", "positive_observed_gain")
                return "ACCEPTED"
            self.finish("RETAINED", "insufficient_observed_gain")
            return "RETAINED"
        except (KeyError, TypeError, ValueError, ArithmeticError):
            return self._wait("invalid_observation")
