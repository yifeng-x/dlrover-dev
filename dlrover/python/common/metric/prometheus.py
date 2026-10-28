# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
"""Official Prometheus client integration for DLRover Python processes."""

import threading
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, Tuple

from prometheus_client import (
    CollectorRegistry,
    GCCollector,
    PlatformCollector,
    ProcessCollector,
    generate_latest,
    start_http_server,
)
from prometheus_client.core import Metric

from dlrover.python.common.log import default_logger as logger


@dataclass(frozen=True)
class MetricSample:
    """A snapshot value converted to an official client metric family."""

    name: str
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    metric_type: str = "gauge"
    help: str = "DLRover metric."


def _family_name(sample: MetricSample) -> str:
    if sample.metric_type == "counter" and sample.name.endswith("_total"):
        return sample.name[:-6]
    return sample.name


def _build_metric_families(samples: Iterable[MetricSample]):
    families = {}
    native_families = []
    for sample in samples:
        if isinstance(sample, Metric):
            native_families.append(sample)
            continue
        family_name = _family_name(sample)
        existing = families.get(sample.name)
        if existing is None:
            metric = Metric(
                family_name,
                sample.help or "DLRover metric.",
                sample.metric_type,
            )
            families[sample.name] = (sample.metric_type, metric)
        else:
            metric_type, metric = existing
            if metric_type != sample.metric_type:
                raise ValueError(
                    f"Metric {sample.name} has conflicting types "
                    f"{metric_type} and {sample.metric_type}"
                )
        metric.add_sample(
            sample.name,
            {str(key): str(value) for key, value in sample.labels.items()},
            float(sample.value),
        )
    return [metric for _, metric in families.values()] + native_families


class _SnapshotCollector:
    def __init__(self, collector: Callable[[], Iterable[MetricSample]]):
        self._collector = collector
        self._collection_errors = 0
        self._ready = 0
        self._lock = threading.Lock()
        self._collect_lock = threading.Lock()

    @property
    def ready(self) -> int:
        with self._lock:
            return self._ready

    def collect(self):
        # Prometheus may overlap scrapes. Snapshot callbacks often read
        # several related fields, so serialize collection within one process.
        with self._collect_lock:
            try:
                families = _build_metric_families(list(self._collector()))
                ready = 1
            except Exception:
                logger.exception("Failed to collect Prometheus metrics")
                families = []
                ready = 0
                with self._lock:
                    self._collection_errors += 1

            with self._lock:
                self._ready = ready
                errors = self._collection_errors
            families.extend(
                _build_metric_families(
                    [
                        MetricSample(
                            "dlrover_metrics_collection_errors_total",
                            errors,
                            metric_type="counter",
                            help=(
                                "Number of failures while collecting DLRover "
                                "metrics."
                            ),
                        ),
                        MetricSample(
                            "dlrover_metrics_collector_ready",
                            ready,
                            help=(
                                "Whether the latest DLRover metric collection "
                                "succeeded."
                            ),
                        ),
                    ]
                )
            )
        yield from families


def render_prometheus(samples: Iterable[MetricSample]) -> str:
    """Render snapshot samples using the official Prometheus encoder."""
    registry = CollectorRegistry()

    class StaticCollector:
        def collect(self):
            yield from _build_metric_families(samples)

    registry.register(StaticCollector())
    return generate_latest(registry).decode("utf-8")


class MetricsServer:
    """Own one explicit Registry and one HTTP endpoint for a process."""

    def __init__(
        self,
        collector: Callable[[], Iterable[MetricSample]],
        include_runtime_metrics: bool = True,
    ):
        self.registry = CollectorRegistry()
        if include_runtime_metrics:
            ProcessCollector(registry=self.registry)
            PlatformCollector(registry=self.registry)
            GCCollector(registry=self.registry)
        self._snapshot_collector = _SnapshotCollector(collector)
        self.registry.register(self._snapshot_collector)
        self._httpd = None
        self._thread = None

    def collect(self) -> Tuple[str, int]:
        body = generate_latest(self.registry).decode("utf-8")
        return body, self._snapshot_collector.ready

    def start(self, port: int, host: str = "0.0.0.0") -> None:
        if port <= 0 or self._httpd is not None:
            return
        _, ready = self.collect()
        if not ready:
            raise RuntimeError("DLRover Prometheus collector is not ready")
        try:
            result = start_http_server(
                port, addr=host, registry=self.registry
            )
        except OSError as error:
            raise RuntimeError(
                f"Failed to bind DLRover metrics endpoint {host}:{port}"
            ) from error
        if not isinstance(result, tuple) or len(result) != 2:
            raise RuntimeError(
                "Unsupported prometheus_client HTTP server lifecycle API"
            )
        self._httpd, self._thread = result
        logger.info(
            "DLRover Prometheus metrics exposed at %s:%s/metrics", host, port
        )

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
