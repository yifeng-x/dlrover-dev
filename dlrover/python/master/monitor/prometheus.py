# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
"""Prometheus snapshot collector for a running DLRover Master."""

import time
from collections import Counter
from typing import Iterable

from dlrover.python.common.metric.prometheus import MetricSample


class MasterMetricsCollector:
    def __init__(
        self, job_args, job_context, perf_monitor, job_manager,
        brain_heartbeat=None,
    ):
        self._brain_heartbeat = brain_heartbeat
        self._job_args = job_args
        self._job_context = job_context
        self._perf_monitor = perf_monitor
        self._job_manager = job_manager
        self._start_time = time.time()

    def _labels(self):
        return {
            "cluster": self._job_args.cluster or "default",
            "namespace": self._job_args.namespace or "default",
            "job_uid": self._job_args.job_uuid or "unknown",
            "strategy": self._job_args.distribution_strategy or "unknown",
        }

    def collect(self) -> Iterable[MetricSample]:
        labels = self._labels()
        vertical_metrics = getattr(self._job_manager, "vertical_metrics", None)
        if vertical_metrics is not None:
            yield from vertical_metrics.collect(labels)
        if self._brain_heartbeat is not None:
            state, failures, last_success = self._brain_heartbeat.snapshot
            for value in ("disabled", "healthy", "backoff", "failed", "stopped"):
                yield MetricSample(
                    "dlrover_master_brain_connection_state",
                    int(state == value), {**labels, "state": value},
                )
            yield MetricSample(
                "dlrover_master_brain_consecutive_failures", failures, labels,
            )
            yield MetricSample(
                "dlrover_master_brain_last_success_timestamp_seconds",
                last_success, labels,
            )
        perf = self._perf_monitor.get_metrics_snapshot()
        yield MetricSample(
            "dlrover_component_ready",
            1,
            {**labels, "component": "master"},
        )
        yield MetricSample(
            "dlrover_process_start_time_seconds",
            self._start_time,
            {**labels, "component": "master"},
        )
        yield MetricSample(
            "dlrover_training_global_step",
            perf.completed_global_step,
            labels,
            help="Latest global training step observed by Master.",
        )
        yield MetricSample(
            "dlrover_job_last_progress_timestamp_seconds",
            perf.last_progress_timestamp,
            labels,
            help=(
                "Unix timestamp when Master last observed global step "
                "progress."
            ),
        )
        # yield MetricSample(
        #     "dlrover_training_speed_steps_per_second",
        #     perf.running_speed,
        #     labels,
        #     help="Training speed calculated from the latest two step reports.",
        # )
        yield MetricSample(
            "dlrover_training_running_workers",
            perf.running_worker_count,
            labels,
            help="Workers currently tracked by the Master performance monitor.",
        )
        yield MetricSample(
            "dlrover_training_target_workers",
            perf.target_worker_count,
            labels,
            help="Target worker count maintained by Master.",
        )
        stage = self._job_context.get_job_stage()
        for value in ("init", "running", "stopping", "stopped", "suspended"):
            yield MetricSample(
                "dlrover_job_state",
                int(stage == value),
                {**labels, "state": value},
            )

        nodes = self._job_context.dup_job_nodes()
        status_counts = Counter()
        max_heartbeat_age = 0.0
        now = time.time()
        for role, role_nodes in nodes.items():
            for node in role_nodes.values():
                status_counts[(role, node.status)] += 1
                if node.heartbeat_time:
                    max_heartbeat_age = max(
                        max_heartbeat_age, now - node.heartbeat_time
                    )
        for (role, status), count in status_counts.items():
            yield MetricSample(
                "dlrover_master_workers",
                count,
                {**labels, "role": role, "status": status},
            )
        yield MetricSample(
            "dlrover_master_heartbeat_age_seconds",
            max_heartbeat_age,
            labels,
        )
        yield MetricSample(
            "dlrover_master_pending_nodes",
            status_counts.get(("worker", "Pending"), 0),
            labels,
        )
