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

from types import SimpleNamespace as Obj

import pytest

from dlrover.dashboard.elastic.aggregate import DashboardService
from dlrover.dashboard.elastic.store import EventStore
from dlrover.python.common.constants import ElasticJobLabel
from dlrover.python.master.monitor.gpu_sampler import (
    RunningGPUReporter,
    running_gpu_sample,
)


def pod(
    uid, gpus=4, phase="Running", deleting=False, running=True, owner="job"
):
    return Obj(
        metadata=Obj(
            uid=uid,
            labels={ElasticJobLabel.REPLICA_TYPE_KEY: "worker"},
            deletion_timestamp=1 if deleting else None,
            owner_references=[Obj(uid=owner)],
        ),
        spec=Obj(
            containers=[
                Obj(
                    name="main",
                    resources=Obj(
                        requests={"nvidia.com/gpu": str(gpus)},
                        limits={"nvidia.com/gpu": str(gpus)},
                    ),
                )
            ]
        ),
        status=Obj(
            phase=phase,
            container_statuses=[
                Obj(name="main", state=Obj(running=Obj() if running else None))
            ],
        ),
    )


def test_running_worker_allocations_sum_mixed_pods_and_ignore_other_states():
    first = pod("one", 4)
    result = running_gpu_sample(
        [
            first,
            first,
            pod("two", 8),
            pod("pending", phase="Pending"),
            pod("failed", phase="Failed"),
            pod("deleted", deleting=True),
            pod("waiting", running=False),
            pod("foreign", owner="other"),
        ],
        "job",
    )
    assert result["gpus"] == 12
    assert result["workers"] == 2
    assert result["resources"] == {"nvidia.com/gpu": 12}
    assert running_gpu_sample([], "job")["gpus"] == 0
    with pytest.raises(ValueError):
        running_gpu_sample(None, "job")


def test_reporter_marks_failure_unknown_and_recovers_without_waiting():
    reporter = RunningGPUReporter(dict(job_uid="job"), lambda: None)
    reporter.collect()
    assert not reporter._snapshot[1]["complete"]
    reporter.list_pods = lambda: Obj(items=[pod("one")])
    reporter.collect()
    assert reporter._snapshot[1]["gpus"] == 4
    values = {m.name: m.value for m in reporter.metrics({})}
    assert values["dlrover_master_running_worker_allocated_gpus"] == 4
    reporter.stop()
    reporter.collect()
    assert all("allocated_gpus" not in m.name for m in reporter.metrics({}))


def test_gpu_sample_upload_is_rejected_without_writes(tmp_path):
    store = EventStore(tmp_path / "events.db")
    with pytest.raises(ValueError, match="Prometheus"):
        store.append(
            dict(
                schema_version=1,
                event_id="gpu",
                event_type="gpu_sample",
                occurred_at=100,
                job=dict(job_uid="job"),
            )
        )
    assert store.jobs() == []


def test_missing_container_state_is_unknown():
    value = pod("one")
    value.status.container_statuses = []
    with pytest.raises(ValueError):
        running_gpu_sample([value], "job")


def test_sampler_stop_does_not_wait_for_blocked_kubernetes_request():
    import threading

    entered = threading.Event()
    release = threading.Event()

    def blocked_list():
        entered.set()
        release.wait(5)
        return Obj(items=[])

    reporter = RunningGPUReporter(dict(job_uid="job"), blocked_list)
    reporter.start()
    try:
        assert entered.wait(1)
        worker = reporter._thread
        reporter.start()
        assert reporter._thread is worker
        assert worker.daemon
        stopped = threading.Event()

        def stop():
            reporter.stop()
            stopped.set()

        caller = threading.Thread(target=stop, daemon=True)
        caller.start()
        assert stopped.wait(1)
    finally:
        release.set()
        reporter.stop()
        reporter._thread.join(timeout=2)
    assert not reporter._thread.is_alive()
    assert reporter._snapshot == (0, {})


def matrix(target="a", counts=None, observed=None, valid=None):
    fields = {
        "running_worker_allocated_gpus": counts or [[100, "4"], [110, "0"]],
        "worker_gpu_observed_timestamp_seconds": observed
        or [[100, "100"], [110, "110"]],
        "worker_gpu_observation_valid": valid or [[100, "1"], [110, "1"]],
    }
    return [
        dict(
            metric={
                "__name__": "dlrover_master_" + name,
                "job_uid": "job",
                "instance": target,
            },
            values=values,
        )
        for name, values in fields.items()
    ]


def parse_series(series, end=150):
    from dlrover.dashboard.elastic.prometheus import PrometheusBenefits

    return PrometheusBenefits.parse_gpu(
        dict(status="success", data=dict(resultType="matrix", result=series)),
        {"job_uid": "job"},
        100,
        end,
        10,
    )


def test_prometheus_gpu_zero_missing_stale_and_invalid():
    points = parse_series(matrix())
    assert [p["gpus"] for p in points] == [4, 0, None, None, None, None]
    assert (
        parse_series(matrix(observed=[[100, "60"], [110, "120"]]))[0]["gpus"]
        is None
    )
    assert parse_series(matrix(valid=[[100, "0"]]))[0]["gpus"] is None
    assert parse_series(matrix(counts=[[100, "NaN"]]))[0]["gpus"] is None


def test_prometheus_gpu_replicas_are_not_summed():
    assert parse_series(matrix() + matrix("b"))[0]["gpus"] == 4
    assert (
        parse_series(matrix() + matrix("b", counts=[[100, "8"]]))[0]["gpus"]
        is None
    )
    assert (
        parse_series(
            matrix(observed=[[100, "95"]]) + matrix("b", counts=[[100, "8"]])
        )[0]["gpus"]
        == 8
    )


def test_dashboard_prometheus_failure_preserves_events_and_jct(tmp_path):
    store = EventStore(tmp_path / "gpu.db")
    store.register(dict(job_uid="job", lifecycle_observed_at=100))

    class Unavailable:
        def gpu_history(self, *args):
            raise TimeoutError()

        def latest(self, *args):
            raise TimeoutError()

    result = DashboardService(
        store, Unavailable(), clock=lambda: 170
    ).dashboard("job")
    assert result["series"]["points"] == []
    assert "Prometheus GPU 时序查询暂不可用" in result["missing_data"]
    assert "jct" in result


def test_dashboard_reads_prometheus_gpu_history(tmp_path):
    store = EventStore(tmp_path / "gpu.db")
    store.register(dict(job_uid="job", lifecycle_observed_at=100))

    class Prometheus:
        def gpu_history(self, job, start, end):
            assert job["uid"] == "job"
            assert end == 170
            return [dict(at=100, gpus=8, throughput=None)]

        def latest(self, job):
            return None

    result = DashboardService(
        store, Prometheus(), clock=lambda: 170
    ).dashboard("job")
    assert result["series"]["points"][0]["gpus"] == 8


def test_master_collector_exports_gpu_without_network():
    from itertools import islice

    from dlrover.python.master.monitor.prometheus import MasterMetricsCollector

    reporter = RunningGPUReporter(dict(job_uid="job"), lambda: Obj(items=[]))
    reporter.collect()
    reporter.list_pods = lambda: pytest.fail(
        "Scrape must not access Kubernetes"
    )
    args = Obj(
        cluster="c",
        namespace="ns",
        job_uuid="job",
        distribution_strategy="allreduce",
    )
    collector = MasterMetricsCollector(
        args, None, None, Obj(_gpu_reporter=reporter)
    )
    values = {m.name: m.value for m in islice(collector.collect(), 4)}
    assert values["dlrover_master_running_worker_allocated_gpus"] == 0
    assert values["dlrover_master_worker_gpu_observation_valid"] == 1


def test_gpu_range_request_scope_resolution_and_auth(monkeypatch):
    import io
    import json
    from urllib.parse import parse_qs, urlparse

    from dlrover.dashboard.elastic.prometheus import PrometheusBenefits

    def request(req, timeout):
        query = parse_qs(urlparse(req.full_url).query)
        assert urlparse(req.full_url).path == "/api/v1/query_range"
        assert 'job_uid="job"' in query["query"][0]
        assert 'namespace="ns"' in query["query"][0]
        assert float(query["step"][0]) >= 50
        assert req.get_header("Authorization") == "Bearer token"
        assert timeout == 3
        return io.BytesIO(
            json.dumps(
                dict(
                    status="success", data=dict(resultType="matrix", result=[])
                )
            ).encode()
        )

    monkeypatch.setattr(
        "dlrover.dashboard.elastic.prometheus.urlopen", request
    )
    points = PrometheusBenefits("http://prometheus", "token").gpu_history(
        dict(uid="job", cluster="c", namespace="ns"), 100, 100100
    )
    assert len(points) <= 2000
    assert all(p["gpus"] is None for p in points)
