# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
import pytest

from dlrover.dashboard.elastic.aggregate import DashboardService
from dlrover.dashboard.elastic.jct import durations, statistics
from dlrover.dashboard.elastic.store import EventStore


@pytest.mark.parametrize("status,start,end,actual,elapsed", [
    ("completed", 100, 250, 150, 150),
    ("completed", 0, 0, 0, 0),
    ("completed", None, 250, None, None),
    ("completed", 250, 100, None, None),
    ("completed", 100, 400, None, None),
    ("completed", 100, None, None, None),
    ("running", 100, None, None, 200),
    ("running", 400, None, None, None),
    ("failed", 100, None, None, None),
    ("unknown", 100, None, None, None),
])
def test_jct_measurement_boundaries(status, start, end, actual, elapsed):
    result = durations(dict(status=status, submitted_at=start, completed_at=end), 300)
    assert result["actual_seconds"] == actual
    assert result["elapsed_seconds"] == elapsed


def test_jct_statistics_exclude_incomplete_and_use_nearest_rank():
    jobs = [dict(status="completed", submitted_at=0, completed_at=i) for i in range(1, 21)]
    jobs += [dict(status="completed", submitted_at=None, completed_at=10),
             dict(status="failed"), dict(status="running"), dict(status="unknown")]
    result = statistics(jobs, 100)
    assert result["total_jobs"] == 24
    assert result["measured_jobs"] == 20
    assert result["excluded_jobs"] == 4
    assert result["completed_jobs"] == 21
    assert result["mean_seconds"] == 10.5
    assert result["p50_seconds"] == 10
    assert result["p95_seconds"] == 19
    assert result["min_seconds"] == 1
    assert result["max_seconds"] == 20
    assert statistics([], 100)["mean_seconds"] is None
    assert statistics([], 100)["p95_seconds"] is None


def test_jct_persists_completion_and_ignores_stale_snapshots(tmp_path):
    path = tmp_path / "jct.sqlite3"
    store = EventStore(path)
    job = dict(job_uid="job", cluster="c", namespace="n", submitted_at=100)
    store.register(dict(job, status="running", lifecycle_observed_at=110))
    service = DashboardService(store, clock=lambda: 200)
    assert service.dashboard("job")["jct"]["elapsed_seconds"] == 100
    assert service.jct_statistics()["measured_jobs"] == 0
    store.register(dict(job, status="completed", completed_at=180, lifecycle_observed_at=181))
    assert not store.register(dict(job, status="running", lifecycle_observed_at=120))
    service = DashboardService(EventStore(path), clock=lambda: 300)
    jct = service.dashboard("job")["jct"]
    assert jct["actual_seconds"] == jct["elapsed_seconds"] == 80
    assert jct["baseline_seconds"] is None
    assert service.jct_statistics(cluster="c", namespace="n")["mean_seconds"] == 80
    assert service.jct_statistics(namespace="other")["total_jobs"] == 0
