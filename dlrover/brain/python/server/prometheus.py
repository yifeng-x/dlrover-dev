"""Single production Prometheus endpoint for one Brain process."""

import threading
import time
from typing import Callable, Dict, Optional

from prometheus_client import (
    CollectorRegistry,
    GCCollector,
    PlatformCollector,
    ProcessCollector,
    generate_latest,
    start_http_server,
)
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

from dlrover.brain.python.jobmanagement.elastic.persist_manager import (
    PrometheusMetricsPersister,
)
from dlrover.python.common.log import default_logger as logger


class _BrainRuntimeCollector:
    def __init__(
        self, snapshot_provider: Callable[[], Dict], start_time: float
    ):
        self._snapshot_provider = snapshot_provider
        self._start_time = start_time
        self._lock = threading.Lock()
        self._collection_errors = 0

    @staticmethod
    def _gauge(name, documentation, value, labels=None):
        labels = labels or {}
        metric = GaugeMetricFamily(
            name, documentation, labels=list(labels.keys())
        )
        metric.add_metric(list(labels.values()), value)
        return metric

    @staticmethod
    def _counter(name, documentation, value, labels=None):
        labels = labels or {}
        metric = CounterMetricFamily(
            name, documentation, labels=list(labels.keys())
        )
        metric.add_metric(list(labels.values()), value)
        return metric

    def collect(self):
        try:
            snapshot = self._snapshot_provider()
            ready = 1
        except Exception:
            logger.exception("Failed to collect Brain Prometheus metrics")
            snapshot = None
            ready = 0
            with self._lock:
                self._collection_errors += 1

        with self._lock:
            errors = self._collection_errors
        yield self._counter(
            "dlrover_metrics_collection_errors_total",
            "Number of failures while collecting DLRover metrics.",
            errors,
        )
        yield self._gauge(
            "dlrover_metrics_collector_ready",
            "Whether the latest DLRover metric collection succeeded.",
            ready,
        )
        if snapshot is None:
            return

        yield self._gauge(
            "dlrover_component_ready",
            "Whether the DLRover component metrics collector is ready.",
            1,
            {"component": "brain"},
        )
        yield self._gauge(
            "dlrover_process_start_time_seconds",
            "Unix timestamp when the DLRover process started.",
            self._start_time,
            {"component": "brain"},
        )
        reports = CounterMetricFamily(
            "dlrover_brain_runtime_reports_total",
            "Number of runtime reports received by Brain.",
            labels=["result"],
        )
        reports.add_metric(["accepted"], snapshot["reports_total"])
        reports.add_metric(
            ["rejected"], snapshot["reports_rejected_total"]
        )
        yield reports
        yield self._counter(
            "dlrover_brain_runtime_reports_processed_total",
            "Number of runtime reports processed by Brain.",
            snapshot["reports_processed_total"],
        )
        yield self._counter(
            "dlrover_brain_runtime_report_processing_errors_total",
            "Number of Brain runtime report processing errors.",
            snapshot["processing_errors_total"],
        )
        gauges = (
            (
                "dlrover_brain_report_queue_size",
                "queue_size",
                "Current report queue size.",
            ),
            (
                "dlrover_brain_report_queue_capacity",
                "queue_capacity",
                "Report queue capacity.",
            ),
            (
                "dlrover_brain_redis_available",
                "redis_available",
                "Whether Redis is available.",
            ),
            (
                "dlrover_brain_redis_last_check_timestamp_seconds",
                "redis_last_check_timestamp",
                "Unix timestamp of the latest Redis check.",
            ),
            (
                "dlrover_brain_runtime_processor_ready",
                "processor_ready",
                "Whether the runtime report processor is ready.",
            ),
            (
                "dlrover_brain_owned_jobs",
                "owned_jobs",
                "Jobs owned by this Brain instance.",
            ),
            (
                "dlrover_brain_owner_allocator_instance_registered",
                "owner_allocator_instance_registered",
                "Whether this Brain is registered in the owner allocator.",
            ),
            (
                "dlrover_brain_owner_allocator_active_instances",
                "owner_allocator_active_instances",
                "Active Brain instances visible to the owner allocator.",
            ),
            (
                "dlrover_brain_owner_allocator_quarantined_instances",
                "owner_allocator_quarantined_instances",
                "Quarantined Brain instances visible to the owner allocator.",
            ),
        )
        for name, key, help_text in gauges:
            yield self._gauge(name, help_text, snapshot[key])

        redis_checks = CounterMetricFamily(
            "dlrover_brain_redis_checks_total",
            "Number of Redis health checks.",
            labels=["result"],
        )
        for result in ("success", "failed"):
            redis_checks.add_metric(
                [result], snapshot["redis_checks"].get(result, 0)
            )
        yield redis_checks
        groups = (
            (
                "dlrover_brain_distributed_lock_total",
                "distributed_locks",
                ("acquired", "contended", "error", "unavailable", "lost"),
            ),
            (
                "dlrover_brain_elastic_decisions_total",
                "elastic_decisions",
                (
                    "triggered",
                    "no_action",
                    "trigger_failed",
                    "redis_unavailable",
                    "lock_contended",
                    "lock_error",
                    "lock_unavailable",
                    "lock_lost",
                ),
            ),
            (
                "dlrover_brain_elastic_triggers_total",
                "elastic_triggers",
                (
                    "success",
                    "retry_success",
                    "failed",
                    "master_unavailable",
                    "redis_unavailable",
                ),
            ),
            (
                "dlrover_brain_job_dispatch_total",
                "job_dispatches",
                (
                    "local",
                    "forwarded",
                    "forward_failed",
                    "owner_mismatch",
                    "owner_lost",
                    "duplicate",
                    "redis_error",
                ),
            ),
        )
        for name, key, results in groups:
            metric = CounterMetricFamily(
                name,
                "Number of Brain operations by result.",
                labels=["result"],
            )
            for result in results:
                metric.add_metric(
                    [result], snapshot[key].get(result, 0)
                )
            yield metric


class BrainMetricsServer:
    """Own the only CollectorRegistry and metrics HTTP server in Brain."""

    def __init__(self):
        self.registry = CollectorRegistry()
        ProcessCollector(registry=self.registry)
        PlatformCollector(registry=self.registry)
        GCCollector(registry=self.registry)
        self.vertical_metrics = PrometheusMetricsPersister(self.registry)
        self._snapshot_provider: Optional[Callable[[], Dict]] = None
        self._runtime_collector = None
        self._httpd = None
        self._thread = None
        self._start_time = time.time()

    def set_snapshot_provider(self, provider: Callable[[], Dict]) -> None:
        if self._runtime_collector is not None:
            raise RuntimeError("Brain metrics snapshot provider is already set")
        self._snapshot_provider = provider
        self._runtime_collector = _BrainRuntimeCollector(
            provider, self._start_time
        )
        self.registry.register(self._runtime_collector)

    def collect(self):
        if self._snapshot_provider is None:
            raise RuntimeError("Brain metrics snapshot provider is not set")
        body = generate_latest(self.registry).decode("utf-8")
        ready = int("dlrover_metrics_collector_ready 1.0" in body)
        return body, ready

    def start(self, port: int, host: str = "0.0.0.0") -> None:
        if port <= 0 or self._httpd is not None:
            return
        if self._snapshot_provider is None:
            raise RuntimeError("Brain metrics snapshot provider is not set")
        try:
            result = start_http_server(
                port, addr=host, registry=self.registry
            )
        except OSError as error:
            raise RuntimeError(
                f"Failed to bind Brain metrics endpoint {host}:{port}"
            ) from error
        if isinstance(result, tuple):
            self._httpd, self._thread = result
        logger.info(
            "Brain Prometheus metrics exposed at %s:%s/metrics", host, port
        )

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
