# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
"""One bounded, durable cold-start trial per job. No scheduler emulation.

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

TERMINAL = {"ACCEPTED", "RETAINED", "ROLLED_BACK"}


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
    def __init__(self, guard, env=None, clock=time.time):
        env = os.environ if env is None else env
        self.guard = guard
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
            return True, state["status"] not in ("OBSERVING", "VERIFYING_ROLLBACK")
        except (OSError, ValueError, KeyError, TypeError):
            return True, True

    def enter(self, recovering=False):
        """Called with scaler mutex held, also fences other Masters via PVC."""
        if not self.enabled and self.store is None:
            return not recovering
        try:
            if self.store is None or not self.store.acquire():
                return False
            self.state = self.store.load()
            if self.state:
                status = self.state["status"]
                if status in TERMINAL:
                    if not self.state.get("reported", False):
                        self.release_reservation()
                    return (not recovering and
                            self.clock() >= self.state["cooldown_until"])
                if status in ("OBSERVING", "VERIFYING_ROLLBACK") and recovering:
                    self.active = True
                    return self.enabled and self.guard.mode == "enforce" and self.valid
                # PREPARING/SWITCHING/ROLLING_BACK may have partially mutated
                # Kubernetes. Reconcile manually rather than repeat deletion.
                return False
            return not recovering and (not self.enabled or self.valid)
        except (OSError, ValueError, KeyError, TypeError):
            logger.error("Exploration journal unavailable or invalid; scaling blocked")
            return False

    def leave(self):
        if self.store:
            self.store.release()
        self.active = False

    def save(self, status=None, **changes):
        next_state = copy.deepcopy(self.state)
        next_state.update(changes)
        if status:
            next_state["status"] = status
        self.store.save(next_state)
        self.state = next_state
        logger.info("Vertical exploration id=%s status=%s reason=%s",
                    self.state["id"], self.state["status"], self.state.get("reason", ""))

    def finish(self, status, reason):
        self.save(status, reason=reason, cooldown_until=self.clock() + self.cooldown)
        self.active = False

    def prepare(self, current, target, plan_id):
        """Only explicit missing candidate evidence can enter exploration."""
        if (not self.enabled or not self.valid or self.state is not None
                or self.guard.mode != "enforce" or self.guard.objective not in ("time", "gpu_hours")
                or self.guard.last_result.reason != "candidate_unavailable"
                or self.store is None or self.store.lock is None):
            return False
        snapshot = self.guard.last_snapshot
        try:
            admission = snapshot["exploration"]
            if not all(admission.get(k) is True for k in (
                "allowed", "checkpoint_compatible", "rollback_supported", "quiescent_checkpoint"
            )):
                return False
            tiers = admission["gpu_tiers"]
            if (not isinstance(tiers, list) or len(tiers) < 2 or
                    any(type(t) is not int or t <= 0 for t in tiers) or
                    tiers != sorted(set(tiers))):
                return False
            a, b = pack(current), pack(target)
            if abs(tiers.index(a["gpu_num"]) - tiers.index(b["gpu_num"])) != 1:
                return False
            if {k: v for k, v in a.items() if k != "gpu_num"} != {
                k: v for k, v in b.items() if k != "gpu_num"
            }:
                return False
            rollback_seconds = number(admission, "rollback_seconds_upper", positive=True)
            rollback_gpu = number(admission, "rollback_gpu_seconds_upper", positive=True)
            limits = snapshot["limits"]
            baseline = self.guard.last_result.estimates["baseline_seconds"]
            if baseline <= self.duration + rollback_seconds:
                return False
            if limits["max_delay_seconds"] < self.duration + rollback_seconds:
                return False
            deadline = limits.get("deadline_at")
            if deadline is not None and self.clock() + baseline + self.duration + rollback_seconds > deadline:
                return False
            gpu_budget = self.max_gpu_hours * 3600
            # Conservative budget includes reserved rollback capacity, not just
            # the GPUs doing useful work during the trial.
            trial_gpu = max(a["gpu_num"], b["gpu_num"]) * a["count"] * self.duration
            if trial_gpu + rollback_gpu > gpu_budget:
                return False
            reservation = admission["rollback_reservation"]
            if (not isinstance(reservation.get("id"), str) or not reservation["id"]
                    or reservation.get("confirmed") is not True
                    or number(reservation, "expires_at") <= self.clock() + self.duration + rollback_seconds):
                return False
            self.state = dict(
                version=1, id=plan_id, status="PREPARING", original=a, target=b,
                started_at=self.clock(), expires_at=self.clock() + self.duration,
                rollback_seconds=rollback_seconds, rollback_gpu_seconds=rollback_gpu,
                gpu_budget=gpu_budget, reservation_id=reservation["id"],
                objective=self.guard.objective, baseline=copy.deepcopy(snapshot),
                q0=self.guard.last_result.estimates["current_throughput"],
                baseline_seconds=baseline, last_window_end=0, windows=0,
                rollback_attempted=False, spent_gpu_seconds=0,
            )
            self.store.save(self.state)
            self.active = True
            return True
        except (KeyError, TypeError, ValueError, OSError):
            # No resource mutation may follow an uncertain journal write.
            self.active = False
            return False

    def authorize_mutation(self, rollback=False):
        """Refresh scheduler reservation immediately before replacing workers."""
        state = self.state
        current = unpack(state["target"] if rollback else state["original"])
        target = unpack(state["original"] if rollback else state["target"])
        context = dict(id=state["id"], status=state["status"],
                       reservation_id=state["reservation_id"],
                       checkpoint_path=state["checkpoint_path"],
                       authorize_mutation=True)
        self.guard._check(current, target, state.get("rollback_plan_id", state["id"]), context)
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
            reservation = data["exploration"]["rollback_reservation"]
            until = state["expires_at"] + (0 if rollback else state["rollback_seconds"])
            return (reservation.get("id") == state["reservation_id"]
                    and reservation.get("confirmed") is True
                    and number(reservation, "expires_at") > until)
        except (KeyError, TypeError, AttributeError, ValueError):
            return False

    def release_reservation(self):
        # Idempotent notification. If unavailable, the external reservation TTL
        # releases capacity; the terminal outcome is already durable.
        state = self.state
        try:
            self.guard._check(unpack(state["original"]), unpack(state["target"]),
                              state["id"], dict(id=state["id"], status=state["status"],
                              reservation_id=state["reservation_id"], release_reservation=True,
                              result=state.get("result"),
                              candidate_observed_at=state.get("candidate_observed_at"),
                              candidate_generation=state.get("candidate_generation")))
            ack = self.guard.last_snapshot
            if (isinstance(ack, dict) and ack.get("recorded") is True
                    and ack.get("exploration_id") == state["id"]
                    and ack.get("request_id") == self.guard.last_request["request_id"]):
                self.save(reported=True)
        except Exception:
            logger.warning("Exploration reservation release notification failed")

    def switching(self, checkpoint_step, checkpoint_path, rollback=False):
        if not checkpoint_path:
            raise ValueError("Exploration requires a persistent checkpoint path")
        self.save("ROLLING_BACK" if rollback else "SWITCHING",
                  checkpoint_step=checkpoint_step, checkpoint_path=checkpoint_path)

    def observing(self, rollback=False):
        self.save("VERIFYING_ROLLBACK" if rollback else "OBSERVING",
                  observation_after=self.clock(), last_window_end=0, windows=0,
                  observation_generation=None, measured_lower=None)

    def inspect(self):
        """Poll one observation. Return wait, rollback, or terminal status."""
        state = self.state
        now = self.clock()
        if now >= state["expires_at"]:
            self.finish("INCONCLUSIVE", "observation_budget_exhausted")
            return "INCONCLUSIVE"
        rollback = state["status"] == "VERIFYING_ROLLBACK"
        current = unpack(state["original"] if rollback else state["target"])
        target = unpack(state["target"] if rollback else state["original"])
        context = dict(id=state["id"], status=state["status"],
                       reservation_id=state["reservation_id"],
                       checkpoint_path=state["checkpoint_path"],
                       applied_plan_id=state.get("rollback_plan_id", state["id"]),
                       observation_after=state["observation_after"],
                       last_window_end=state["last_window_end"],
                       started_at=state["started_at"])
        result = self.guard._check(current, target, context["applied_plan_id"], context)
        snapshot, request = self.guard.last_snapshot, self.guard.last_request
        # This also validates identities, freshness, window and constraints.
        if result.status == "unknown" and result.reason != "candidate_unavailable":
            return "wait"
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
                    or start["observed_at"] < state["observation_after"]
                    or start["observed_at"] < state["last_window_end"]):
                return "wait"
            if state.get("observation_generation") not in (None, start["generation"]):
                return "wait"
            if not rollback and start["generation"] == state["baseline"]["training"]["end"]["generation"]:
                return "wait"
            if rollback and start["generation"] == state.get("candidate_generation"):
                return "wait"
            if training["unit"] == "steps" and end["work"] <= state["checkpoint_step"]:
                return "wait"
            if snapshot["scheduling"].get("constraints_satisfied") is not True:
                return "wait"
            reservation = snapshot["exploration"]["rollback_reservation"]
            if (reservation.get("id") != state["reservation_id"] or
                    reservation.get("confirmed") is not True or
                    number(reservation, "expires_at") <= now + state["rollback_seconds"]):
                self.finish("INCONCLUSIVE", "rollback_reservation_lost")
                return "INCONCLUSIVE"
            spent = number(snapshot["execution"], "trial_gpu_seconds")
            if spent < state["spent_gpu_seconds"]:
                return "wait"
            required = spent + (0 if rollback else state["rollback_gpu_seconds"])
            if required > state["gpu_budget"]:
                self.finish("INCONCLUSIVE", "gpu_budget_exhausted")
                return "INCONCLUSIVE"
            q1 = (end["work"] - start["work"]) / (end["observed_at"] - start["observed_at"])
            if not math.isfinite(q1) or q1 <= 0:
                return "wait"
            q1 = min(q1, state.get("measured_lower") or q1)
            self.save(last_window_end=end["observed_at"], windows=state["windows"] + 1,
                      spent_gpu_seconds=spent, observation_generation=start["generation"],
                      measured_lower=q1)
            state = self.state
            if state["windows"] < 2:
                return "wait"
            if rollback:
                self.finish("ROLLED_BACK", "rollback_progress_verified")
                return "ROLLED_BACK"
            remaining = training["target_work"] - end["work"]
            if remaining <= 0:
                self.finish("RETAINED", "training_finished")
                return "RETAINED"
            elapsed = now - state["started_at"]
            keep = remaining / q1
            back = state["rollback_seconds"] + remaining / state["q0"]
            g0 = state["original"]["count"] * state["original"]["gpu_num"]
            g1 = state["target"]["count"] * state["target"]["gpu_num"]
            limits = copy.deepcopy(state["baseline"]["limits"])
            limits["max_delay_seconds"] = min(limits["max_delay_seconds"], number(snapshot["limits"], "max_delay_seconds"))
            current_deadline = snapshot["limits"].get("deadline_at")
            if current_deadline is not None:
                current_deadline = number(snapshot["limits"], "deadline_at", positive=True)
                limits["deadline_at"] = min(limits.get("deadline_at") or current_deadline, current_deadline)
            keep_allowed = elapsed + keep <= state["baseline_seconds"] + limits["max_delay_seconds"]
            back_allowed = elapsed + back <= state["baseline_seconds"] + limits["max_delay_seconds"]
            if limits.get("deadline_at") is not None:
                keep_allowed &= now + keep <= limits["deadline_at"]
                back_allowed &= now + back <= limits["deadline_at"]
            if state["objective"] == "time":
                total_gain = state["baseline_seconds"] - elapsed - keep
                denominator = state["baseline_seconds"]
                future_gain = keep - back
            else:
                total_gain = g0 * state["baseline_seconds"] - spent - g1 * keep
                denominator = g0 * state["baseline_seconds"]
                future_gain = g1 * keep - state["rollback_gpu_seconds"] - g0 * remaining / state["q0"]
            values = dict(candidate_throughput=q1, overall_gain=total_gain,
                          keep_seconds=keep, rollback_seconds=back,
                          future_rollback_gain=future_gain)
            self.save(result=values, candidate_generation=start["generation"],
                      candidate_observed_at=end["observed_at"])
            budget_available = spent + state["rollback_gpu_seconds"] <= state["gpu_budget"]
            if keep_allowed and total_gain > 0 and total_gain >= self.guard.min_gain * denominator and budget_available:
                self.finish("ACCEPTED", "positive_observed_gain")
                return "ACCEPTED"
            if (back_allowed and budget_available and not state["rollback_attempted"]
                    and (not keep_allowed or future_gain > 0)):
                # Current reverse plan must still be schedulable. Evidence is
                # supplied by the reservation service for this exact request.
                if snapshot["scheduling"].get("target_feasible") is not True:
                    return "wait"
                self.save("ROLLING_BACK", rollback_attempted=True,
                          rollback_plan_id=state["id"] + "-rollback",
                          expires_at=now + state["rollback_seconds"])
                return "rollback"
            if keep_allowed and budget_available:
                self.finish("RETAINED", "rollback_not_beneficial")
                return "RETAINED"
            self.finish("INCONCLUSIVE", "constraint_or_budget_violation")
            return "INCONCLUSIVE"
        except (KeyError, TypeError, ValueError, ArithmeticError):
            return "wait"
