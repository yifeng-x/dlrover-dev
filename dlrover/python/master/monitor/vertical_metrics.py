# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
"""Process-local vertical execution metrics; scraping only reads snapshots."""

import math
import threading
import time

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from dlrover.python.common.constants import VerticalElasticPhaseType as Phase


class VerticalExecutionMetrics:
    def __init__(self, clock=time.monotonic, wall_clock=time.time):
        self._clock = clock
        self._wall_clock = wall_clock
        self._observation_ready = True
        self._recovered_at = 0
        self.events = None
        self._lock = threading.Lock()
        self._registry = CollectorRegistry()
        prefix = "dlrover_master_vertical_"

        def metric(cls, name, help, labels=(), **kwargs):
            return cls(
                prefix + name, help, labels, registry=self._registry, **kwargs
            )

        self.requests = metric(
            Counter,
            "requests_total",
            "Scaling request outcomes.",
            ("action", "result", "reason"),
        )
        self.explorations = metric(
            Counter, "explorations_total", "Terminal exploration outcomes.",
            ("result",),
        )
        self.started = metric(
            Counter,
            "plans_started_total",
            "Plans created.",
            ("action",),
        )
        self.benefit_evaluations = metric(
            Counter, "benefit_evaluations_total",
            "Pre-checkpoint benefit outcomes.",
            ("mode", "result", "reason"),
        )
        # Latest evidence per stage. No plan/generation labels: histories live
        # in Prometheus timestamps and durable exploration journals.
        self._benefit_labels = {}
        self._exploration_counted = set()
        self._benefit_gauges = {}
        for name, help_text in (
            ("baseline_throughput", "Baseline work units per second."),
            ("candidate_throughput", "Candidate work units per second."),
            ("gain_ratio", "Gain relative to the selected objective baseline; may be negative."),
            ("time_gain_seconds", "Projected training time saved, including transition costs."),
            ("gpu_seconds_saved", "Projected GPU seconds saved; may be negative."),
            ("valid", "One if a complete numeric benefit result is available, not a positive-gain flag."),
            ("observed_timestamp_seconds", "Training evidence endpoint Unix time; zero if unavailable."),
        ):
            self._benefit_gauges[name] = metric(
                Gauge, "benefit_" + name, help_text,
                ("stage", "objective", "work_unit", "data_source"),
            )
        self.exploration_windows = metric(
            Gauge, "exploration_windows", "Accepted independent observation windows.")
        self.exploration_gpu_seconds = metric(
            Gauge, "exploration_gpu_seconds", "Last accepted cumulative trial GPU seconds; not lifetime consumption.")
        self.exploration_reported = metric(
            Gauge, "exploration_result_reported", "One only after the adapter acknowledges the durable outcome.")
        self.exploration_state = metric(
            Gauge, "exploration_state", "Latest durable exploration state (one hot).", ("state",))
        self.finished = metric(
            Counter,
            "plans_finished_total",
            "Terminal plan outcomes.",
            ("action", "result", "reason"),
        )
        buckets = (1, 5, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600)
        self.duration = metric(
            Histogram,
            "plan_duration_seconds",
            "Time from plan creation to termination.",
            ("action", "result"),
            buckets=buckets,
        )
        # Expose a zero baseline before the first plan finishes. Calling
        # labels() creates children without recording a duration observation.
        for action in ("add_gpus", "reduce_gpus", "manual"):
            for result in ("completed", "failed", "skipped", "cancelled"):
                self.duration.labels(action, result)
            # Successful plans have a fixed reason; other reasons are created
            # when observed rather than exporting fictitious reason="none".
            self.finished.labels(action, "completed", "none")

        self.phase_duration = metric(
            Histogram,
            "phase_duration_seconds",
            "Time in an execution phase, including waits.",
            ("action", "phase", "outcome"),
            buckets=buckets,
        )
        self.phase = metric(
            Gauge,
            "phase",
            "Current controller phase (one hot).",
            ("phase",),
        )
        self.inflight = metric(
            Gauge,
            "inflight",
            "Execution thread owns the scaling lock.",
        )
        self.phase_elapsed = metric(
            Gauge,
            "phase_elapsed_seconds",
            "Elapsed time in the active phase.",
        )
        self.plan_elapsed = metric(
            Gauge,
            "plan_elapsed_seconds",
            "Elapsed time in the active plan.",
        )
        self.workers = metric(
            Gauge,
            "workers",
            "Current plan target and ready workers.",
            ("state",),
        )
        self.gpus = metric(
            Gauge,
            "gpus_per_worker",
            "Requested GPUs per worker for the current plan.",
            ("value_type",),
        )
        self.reports = metric(
            Counter,
            "reports_total",
            "Checkpoint and tuning report outcomes.",
            ("report_type", "result"),
        )
        self.worker_duration = metric(
            Histogram, "worker_operation_duration_seconds",
            "Completed replacement worker scheduling and startup duration.",
            ("operation", "outcome"), buckets=buckets,
        )
        self.recovery = metric(
            Counter, "recovery_total", "Training recovery confirmation outcomes.",
            ("action", "result"),
        )
        self.recovery_duration = metric(
            Histogram, "recovery_duration_seconds",
            "Control completion to confirmed training progress.",
            ("action",), buckets=buckets,
        )
        self.resume_duration = metric(
            Histogram, "resume_duration_seconds",
            "Checkpoint phase entry to confirmed training progress, not downtime.",
            ("action",), buckets=buckets,
        )
        self.recovery_pending = metric(
            Gauge, "recovery_pending", "Waiting for training recovery confirmation.",
        )
        self.recovery_elapsed = metric(
            Gauge, "recovery_elapsed_seconds", "Time since control completion.",
        )
        self._replacement_workers = {}
        self._checkpoint_at = None
        self._recovery = None
        self._active = False
        self._phase = Phase.IDLE
        self._action = "unknown"
        self._set_phase(Phase.IDLE)
        self.workers.labels("target").set(0)
        self.workers.labels("param_ready").set(0)

    @staticmethod
    def _finite(value):
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)

    def _benefit_snapshot(self, stage, objective, training, values, valid, observed_at):
        """Caller holds the metrics lock; replace stale label combinations."""
        objective = objective if objective in ("time", "gpu_hours", "cluster_jct") else "unknown"
        unit = training.get("unit")
        unit = unit if unit in ("steps", "samples", "tokens") else "unknown"
        source = training.get("data_source", "http" if training else "unknown")
        source = source if source in ("mock", "http") else "unknown"
        labels = (stage, objective, unit, source)
        previous = self._benefit_labels.get(stage)
        if previous and previous != labels:
            for metric in self._benefit_gauges.values():
                metric.remove(*previous)
        self._benefit_labels[stage] = labels
        required = ("baseline_throughput", "candidate_throughput", "gain_ratio",
                    "time_gain_seconds", "gpu_seconds_saved")
        valid = bool(valid and all(self._finite(values.get(k)) for k in required))
        for key in required:
            value = values.get(key)
            self._benefit_gauges[key].labels(*labels).set(
                value if self._finite(value) else float("nan"))
        self._benefit_gauges["valid"].labels(*labels).set(int(valid))
        self._benefit_gauges["observed_timestamp_seconds"].labels(*labels).set(
            observed_at if self._finite(observed_at) else 0)

    def record_benefit(self, guard):
        """Record one pre-checkpoint evaluation, never a control HTTP request."""
        result = guard.last_result
        estimates = result.estimates if result is not None and guard.mode != "off" else {}
        training = (guard.last_snapshot or {}).get("training", {}) if estimates else {}
        values = dict(
            baseline_throughput=estimates.get("current_throughput"),
            candidate_throughput=estimates.get("candidate_throughput"),
            time_gain_seconds=estimates.get("time_gain_seconds"),
            gain_ratio=estimates.get("gain_ratio"),
        )
        hours = estimates.get("gpu_hours_saved")
        if self._finite(hours):
            values["gpu_seconds_saved"] = hours * 3600
        if guard.objective in ("time", "gpu_hours"):
            gain = estimates.get("time_gain_seconds" if guard.objective == "time" else "gpu_hours_saved")
            baseline = estimates.get("baseline_seconds" if guard.objective == "time" else "baseline_gpu_hours")
            if self._finite(gain) and self._finite(baseline) and baseline > 0:
                values["gain_ratio"] = gain / baseline
        with self._lock:
            self._benefit_snapshot(
                "prediction", guard.objective, training, values,
                result is not None and result.status in ("positive", "negative") and guard.mode != "off",
                training.get("end", {}).get("observed_at"),
            )

    def record_exploration(self, state, restored=False):
        """Publish durable state; count each new outcome once per process.

        Restoring a completed journal restores gauges without counting a new
        event. Repeated saves (including report acknowledgements) never count.
        """
        if not state:
            return
        status = state.get("status")
        states = ("PREPARING", "SWITCHING", "OBSERVING", "ACCEPTED", "RETAINED",
                  "INCONCLUSIVE", "FAILED", "ROLLED_BACK", "ROLLING_BACK", "VERIFYING_ROLLBACK")
        terminal = status in ("ACCEPTED", "RETAINED", "INCONCLUSIVE", "FAILED", "ROLLED_BACK")
        result = state.get("result") or {}
        training = state.get("baseline", {}).get("training", {})
        training = {**training, **{k: result[k] for k in ("unit", "data_source") if k in result}}
        with self._lock:
            if terminal and state.get("id") not in self._exploration_counted:
                self._exploration_counted.add(state.get("id"))
                if not restored:
                    self.explorations.labels(status.lower()).inc()
            for item in states:
                self.exploration_state.labels(item.lower()).set(int(status == item))
            self.exploration_windows.set(state.get("windows", 0))
            self.exploration_gpu_seconds.set(state.get("spent_gpu_seconds", 0))
            self.exploration_reported.set(int(state.get("reported", False)))
            self._benefit_snapshot(
                "exploration", state.get("objective"), training, result,
                terminal and state.get("windows", 0) >= 2,
                state.get("candidate_observed_at"),
            )

    def request(self, action, result, reason="none", event_request_id=None,
                trigger_source="unknown"):
        if self.events:
            self.events.request(event_request_id, action, result, reason, trigger_source)
        if action not in ("add_gpus", "reduce_gpus", "manual"):
            action = "unknown"
        self.requests.labels(action, result, reason).inc()

    def _set_phase(self, phase):
        self._phase = phase
        for value in Phase._VALID_PHASES:
            self.phase.labels(value).set(int(value == phase))

    def start(self, action):
        with self._lock:
            # A newer plan supersedes an unconfirmed generation.
            self._expire_recovery()
            self._finish_recovery("unverified")
            self._replacement_workers = {}
            self._checkpoint_at = None
            self._active = True
            self._action = action
            self._started_at = self._phase_at = self._clock()
            self._set_phase(Phase.PREPARING)
            self.started.labels(action).inc()

    def transition(self, phase):
        with self._lock:
            if not self._active or phase == self._phase:
                return
            if phase in (Phase.SAVE_CHECKPOINT, Phase.REPLACING_WORKERS):
                self._observation_ready = False
            now = self._clock()
            self.phase_duration.labels(
                self._action, self._phase, "advanced"
            ).observe(now - self._phase_at)
            self._phase_at = now
            if phase == Phase.SAVE_CHECKPOINT:
                self._checkpoint_at = now
            self._set_phase(phase)
            if self.events:
                self.events.phase(phase)

    def finish(self, result, reason="none"):
        with self._lock:
            if not self._active:
                return
            now = self._clock()
            outcome = "advanced" if result == "completed" else result
            self.phase_duration.labels(
                self._action, self._phase, outcome
            ).observe(now - self._phase_at)
            self.duration.labels(self._action, result).observe(
                now - self._started_at
            )
            self.finished.labels(self._action, result, reason).inc()
            self._active = False
            self._set_phase(
                Phase.COMPLETED if result == "completed" else
                Phase.SKIPPED if result == "skipped" else Phase.FAILED
            )
            self.workers.labels("target").set(0)
            self.workers.labels("param_ready").set(0)
            self.gpus.clear()
            if result != "completed":
                self._replacement_workers = {}
            if self.events:
                self.events.control_finished(result, reason)

    def set_workers(self, targets, ready):
        with self._lock:
            if self._active:
                self.workers.labels("target").set(len(targets))
                self.workers.labels("param_ready").set(len(targets & ready))

    def track_replacement_workers(self, node_ids):
        with self._lock:
            self._replacement_workers = {
                node_id: set() for node_id in node_ids
            }

    def worker_event(self, node):
        """Use Kubernetes timestamps, not watch arrival time, for Pod waits."""
        with self._lock:
            observed = self._replacement_workers.get(node.id)
            if node.type != "worker" or observed is None:
                return
            scheduled = node.scheduled_time
            created = node.create_time
            started = node.start_time
            for operation, start, end in (
                ("schedule_wait", created, scheduled),
                ("startup_wait", scheduled, started),
            ):
                if (
                    operation in observed
                    or start is None or end is None
                ):
                    continue
                elapsed = (end - start).total_seconds()
                if elapsed >= 0:
                    self.worker_duration.labels(
                        operation, "success"
                    ).observe(elapsed)
                    observed.add(operation)

    def training_observation_status(self):
        with self._lock:
            return dict(recovery_ready=self._observation_ready, recovered_at=self._recovered_at)

    def await_recovery(self, targets, checkpoint_step, timeout):
        with self._lock:
            self._observation_ready = False
            self._recovery = {
                "action": self._action, "targets": set(targets),
                "baseline": checkpoint_step, "reports": {},
                "completed_at": self._clock(), "timeout": timeout,
                "checkpoint_at": self._checkpoint_at,
                "event_context": self.events.context if self.events else None,
            }
            self.recovery_pending.set(1)
            if not targets or self._checkpoint_at is None:
                self._finish_recovery("unverified")

    def _finish_recovery(self, result, progress=None):
        recovery = self._recovery
        if recovery is None:
            return
        self._observation_ready = result == "recovered"
        if self._observation_ready:
            self._recovered_at = self._wall_clock()
        action = recovery["action"]
        self.recovery.labels(action, result).inc()
        if result == "recovered":
            now = self._clock()
            self.recovery_duration.labels(action).observe(
                now - recovery["completed_at"]
            )
            self.resume_duration.labels(action).observe(
                now - recovery["checkpoint_at"]
            )
        self._recovery = None
        self.recovery_pending.set(0)
        self.recovery_elapsed.set(0)
        if self.events:
            self.events.recovery_finished(
                recovery.get("event_context"), result,
                self._clock() - recovery["completed_at"], progress,
            )

    def _expire_recovery(self):
        recovery = self._recovery
        if (
            recovery
            and self._clock() - recovery["completed_at"] >= recovery["timeout"]
        ):
            result = "timeout" if recovery["reports"] else "unverified"
            self._finish_recovery(result)

    def check_recovery(self):
        # Called by the existing heartbeat loop, never by a Prometheus scrape.
        with self._lock:
            self._expire_recovery()

    def stop_recovery(self):
        with self._lock:
            self._expire_recovery()
            self._finish_recovery("unverified")

    def training_progress(self, node_type, node_id, step, timestamp):
        with self._lock:
            self._expire_recovery()
            recovery = self._recovery
            if (
                recovery is None or node_type != "worker"
                or node_id not in recovery["targets"]
            ):
                return
            if not (
                math.isfinite(step) and math.isfinite(timestamp)
                and step >= 0 and timestamp > 0
            ):
                return
            previous = recovery["reports"].get(node_id)
            # Two ordered reports prevent restored/stale metric files from
            # being mistaken for fresh training progress.
            if previous is not None:
                old_step, old_timestamp = previous
                if timestamp <= old_timestamp:
                    return
                if step > max(old_step, recovery["baseline"]):
                    self._finish_recovery("recovered", {
                        "node_id": node_id, "step": step, "timestamp": timestamp,
                    })
                    return
            recovery["reports"][node_id] = (step, timestamp)

    def collect(self, labels):
        with self._lock:
            now = self._clock()
            self.phase_elapsed.set(now - self._phase_at if self._active else 0)
            self.plan_elapsed.set(now - self._started_at if self._active else 0)
            if self._recovery is not None:
                self.recovery_elapsed.set(now - self._recovery["completed_at"])
            families = list(self._registry.collect())
        for family in families:
            family.samples = [
                sample._replace(labels={**labels, **sample.labels})
                for sample in family.samples
            ]
        return families
