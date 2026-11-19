# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest

from dlrover.dashboard.elastic.aggregate import DashboardService
from dlrover.dashboard.elastic.jct import durations, statistics
from dlrover.dashboard.elastic.store import EventStore


@pytest.mark.parametrize(
    "status,start,end,actual,elapsed",
    [
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
    ],
)
def test_jct_measurement_boundaries(status, start, end, actual, elapsed):
    result = durations(
        dict(status=status, submitted_at=start, completed_at=end), 300
    )
    assert result["actual_seconds"] == actual
    assert result["elapsed_seconds"] == elapsed


def test_jct_statistics_exclude_incomplete_and_use_nearest_rank():
    jobs = [
        dict(status="completed", submitted_at=0, completed_at=i)
        for i in range(1, 21)
    ]
    jobs += [
        dict(status="completed", submitted_at=None, completed_at=10),
        dict(status="failed"),
        dict(status="running"),
        dict(status="unknown"),
    ]
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
    store.register(
        dict(
            job,
            status="completed",
            completed_at=180,
            lifecycle_observed_at=181,
        )
    )
    assert not store.register(
        dict(job, status="running", lifecycle_observed_at=120)
    )
    service = DashboardService(EventStore(path), clock=lambda: 300)
    jct = service.dashboard("job")["jct"]
    assert jct["actual_seconds"] == jct["elapsed_seconds"] == 80
    assert jct["baseline_seconds"] is None
    assert (
        service.jct_statistics(cluster="c", namespace="n")["mean_seconds"]
        == 80
    )
    assert service.jct_statistics(namespace="other")["total_jobs"] == 0


def test_submission_window_includes_later_completion_and_missing(tmp_path):
    store = EventStore(tmp_path / "window.db")
    for uid, start, status, end in [
        ("before", 99, "completed", 150),
        ("left", 100, "completed", 400),
        ("running", 150, "running", None),
        ("failed", 160, "failed", None),
        ("right", 200, "completed", 250),
        ("missing", None, "completed", 300),
    ]:
        store.register(dict(job_uid=uid, cluster="c", namespace="n",
                            submitted_at=start, completed_at=end, status=status,
                            lifecycle_observed_at=500))
    service = DashboardService(store, clock=lambda: 600)
    result = service.jct_statistics(cluster="c", submitted_from=100, submitted_to=200)
    assert result["total_jobs"] == 3
    assert result["measured_jobs"] == 1
    assert result["mean_seconds"] == 300
    assert result["running_jobs"] == result["failed_jobs"] == 1
    assert result["unassigned_jobs"] == 1
    for start, end in [(100, None), (200, 100), (100, 100), (-1, 100),
                       (float("nan"), 100), (0, float("inf"))]:
        with pytest.raises(ValueError):
            service.jct_statistics(submitted_from=start, submitted_to=end)


def test_pre_adjustment_baseline_uses_original_steps_and_queue(tmp_path):
    store = EventStore(tmp_path / "baseline.db")
    job = dict(job_uid="job", submitted_at=100, status="running", lifecycle_observed_at=900)
    store.register(job)
    training = dict(
        unit="steps", steady=True, comparable=True, target_work=6600, data_source="mock",
        start=dict(generation="original", observed_at=750, work=50),
        end=dict(generation="original", observed_at=810, work=110),
    )
    event = dict(schema_version=1, event_id="prediction", event_type="evaluated",
                 job=job, occurred_at=820, benefit=dict(evidence=dict(
                     training=training, workers_started_at=700)))
    store.append(event)
    store.append(dict(event, event_id="checkpoint", event_type="phase_changed",
                      phase="save_checkpoint", occurred_at=821))
    training["target_work"] = 20000
    store.append(dict(event, event_id="later", occurred_at=950))
    result = DashboardService(store, clock=lambda: 1000).dashboard("job")["jct"]
    assert result["baseline_seconds"] == 7200
    assert result["baseline_frozen_at"] == 820
    assert result["baseline_basis"] == dict(total_steps=6600, average_steps_per_second=1,
                                          queue_seconds=600, data_source="mock")
    from dlrover.dashboard.elastic.jct import estimate_baseline
    del event["benefit"]["evidence"]["workers_started_at"]
    partial = estimate_baseline(job, [event])
    assert partial["baseline_basis"]["queue_seconds"] is None
    assert partial["baseline_seconds"] is None
