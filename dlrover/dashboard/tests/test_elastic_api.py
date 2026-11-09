# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
import copy
import io
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from dlrover.dashboard.elastic.__main__ import export_events, import_events
from dlrover.dashboard.elastic.aggregate import DashboardService
from dlrover.dashboard.elastic.client import HTTPEventSink
from dlrover.dashboard.elastic.prometheus import PREFIX, PrometheusBenefits
from dlrover.dashboard.elastic.server import ElasticAPI
from dlrover.dashboard.elastic.store import EventStore
from dlrover.dashboard.elastic.store import StoreUnavailable
from dlrover.python.master.monitor.vertical_events import VerticalElasticEvents


JOB = dict(job_uid="uid-1", job_name="train-001", cluster="c", namespace="ns")


def event(key, at, kind="phase_changed", **fields):
    return dict(schema_version=1, event_id=key, occurred_at=at, event_type=kind,
                job=JOB, plan_id="p1", request_id="r1", action="add_gpus", **fields)


def benefit():
    return dict(mode="enforce", objective="time", evaluation_status="negative",
                decision="allow", min_gain_ratio=.05, max_age_seconds=60,
                evidence={"training": {"unit": "steps", "data_source": "http", "end": {"observed_at": 100}}},
                estimates=dict(current_throughput=2, candidate_throughput=3,
                               time_gain_seconds=-120, gpu_hours_saved=-.5,
                               gain_ratio=-.1))


@pytest.fixture
def store(tmp_path):
    return EventStore(tmp_path / "events.sqlite3")


def test_replay_conflict_restart_and_job_identity(store):
    value = event("e1", 100, phase="preparing")
    assert store.append(value)
    assert not store.append(copy.deepcopy(value))
    restored = EventStore(store.path)
    assert restored.read("uid-1")[1] == [value]
    with pytest.raises(ValueError, match="Conflicting"):
        store.append({**value, "occurred_at": 101})
    with pytest.raises(ValueError, match="collision"):
        store.append({**value, "event_id": "e2", "job": {**JOB, "namespace": "other"}})


def test_concurrent_identical_ingestion_is_idempotent(store):
    value = event("same", 100)
    with ThreadPoolExecutor(max_workers=4) as pool:
        assert sum(pool.map(store.append, [value] * 8)) == 1
    assert len(store.read("uid-1")[1]) == 1


def test_out_of_order_negative_gain_and_actual_not_target(store):
    store.append(event("finish", 150, "finished", execution_status="failed",
                       recovery_status="unknown", resources={"before": {"total_gpus": 2}, "target": {"total_gpus": 4}, "final": None}))
    store.append(event("eval", 110, "evaluated", benefit=benefit()))
    store.append(event("start", 105, phase="save_checkpoint"))
    body = DashboardService(store, clock=lambda: 200).dashboard("uid-1")
    item = body["events"][0]
    assert item["occurred_at"] == 105
    assert item["actual_gpus"] is None
    assert item["target_gpus"] == 4
    assert item["interruption_seconds"] is None
    assert item["evaluation"]["valid"]
    assert item["evaluation"]["gpu_seconds_saved"] == -1800
    assert body["summary"] == dict(attempted=1, succeeded=0, failed=1, complete=False)
    assert body["job"]["status"] == "unknown"
    assert body["jct"]["baseline_seconds"] is None
    assert body["series"]["points"] == []


def test_request_is_attached_once_and_rejection_is_not_execution(store):
    request = event("request", 100, "requested", request_result="accepted")
    request["plan_id"] = None
    store.append(request)
    rejected = benefit()
    rejected["decision"] = "reject"
    store.append(event("eval", 101, "evaluated", benefit=rejected))
    store.append(event("end", 102, "finished", execution_status="skipped"))
    body = DashboardService(store).dashboard("uid-1")
    assert len(body["events"]) == 1
    assert body["events"][0]["conclusion"] == "rejected"
    assert body["summary"]["attempted"] == 0


def test_unrelated_requests_remain_separate(store):
    for i in range(2):
        value = event(str(i), 100 + i, "requested", request_result="rejected")
        value.update(plan_id=None, request_id="r" + str(i))
        store.append(value)
    assert len(DashboardService(store).dashboard("uid-1")["events"]) == 2


def test_latest_invalid_result_replaces_valid_and_off_not_evaluated(store):
    store.append(event("eval", 100, "evaluated", benefit=benefit()))
    store.append(event("trial", 200, "exploration_decided", benefit=benefit(),
                       exploration=dict(status="INCONCLUSIVE", estimates=None, windows=1, observed_at=None)))
    body = DashboardService(store).dashboard("uid-1")
    assert not body["latest_evaluation"]["valid"]
    assert body["latest_evaluation"]["time_gain_seconds"] is None
    disabled = benefit()
    disabled["mode"] = "off"
    store.append(event("off", 300, "evaluated", benefit=disabled))
    assert DashboardService(store).dashboard("uid-1")["latest_evaluation"]["gain_ratio"] is None


def test_exploration_ack_does_not_duplicate_adjustment(store):
    store.append(event("eval", 100, "evaluated", benefit=benefit()))
    store.append(event("execute", 110, "control_completed", execution_status="completed"))
    trial = dict(status="RETAINED", observed_at=180, windows=2, reported=False,
                 estimates=dict(baseline_throughput=2, candidate_throughput=1, gain_ratio=-.5,
                                time_gain_seconds=-200, gpu_seconds_saved=-400, unit="steps", data_source="http"))
    store.append(event("trial", 200, "exploration_decided", exploration=trial, execution_status="completed"))
    store.append(event("ack", 210, "exploration_updated", exploration={**trial, "reported": True}))
    result = DashboardService(store).dashboard("uid-1")
    assert result["summary"]["attempted"] == 1
    assert result["events"][0]["execution_status"] == "completed"
    assert result["events"][0]["conclusion"] == "retained"
    assert result["latest_evaluation"]["reported"] is True
    assert result["latest_evaluation"]["observed_at"] == 180
    assert result["latest_evaluation"]["valid"]


def test_old_plan_ack_does_not_replace_new_plan_evaluation(store):
    trial = dict(status="ACCEPTED", observed_at=100, windows=2, reported=False,
                 estimates=dict(baseline_throughput=2, candidate_throughput=3, gain_ratio=.1,
                                time_gain_seconds=20, gpu_seconds_saved=40, unit="steps"))
    store.append(event("old-trial", 110, "exploration_decided", exploration=trial))
    newer = event("new-eval", 200, "evaluated", benefit=benefit())
    newer.update(plan_id="new-plan", request_id="new-request")
    store.append(newer)
    store.append(event("late-ack", 300, "exploration_updated", exploration={**trial, "reported": True}))
    body = DashboardService(store).dashboard("uid-1")
    assert body["latest_evaluation"]["stage"] == "prediction"
    assert body["latest_evaluation"]["time_gain_seconds"] == -120


def test_lifecycle_only_from_platform_and_rejects_stale_updates(store):
    lifecycle = dict(JOB, status="completed", submitted_at=100, completed_at=200, lifecycle_observed_at=210)
    assert store.register(lifecycle)
    assert not store.register({**lifecycle, "lifecycle_observed_at": 150, "status": "running", "completed_at": None})
    assert not store.register(lifecycle)
    body = DashboardService(store).dashboard("uid-1")
    assert body["job"]["status"] == "completed"
    assert body["jct"]["completed_at"] == 200
    assert body["summary"]["attempted"] is None
    with pytest.raises(ValueError):
        store.register({**lifecycle, "status": "running"})


def test_real_emitter_persists_checkpoint_and_evaluation(tmp_path, monkeypatch):
    path = tmp_path / "real.sqlite3"
    monkeypatch.setenv("DLROVER_ELASTIC_EVENT_DB", str(path))
    emitter = VerticalElasticEvents(job=JOB, clock=lambda: 123)
    emitter.start("plan-real", "add_gpus")
    value = benefit()
    gate = SimpleNamespace(mode="enforce", objective="time", min_gain=.05, max_age=60,
                           last_result=SimpleNamespace(status="negative", reason="insufficient_gain", estimates=value["estimates"]),
                           last_snapshot=value["evidence"], last_request={})
    emitter.evaluated(gate, True, False)
    emitter.checkpoint(10, "private/checkpoint", {1})
    emitter.control_finished("completed", "ready")
    body = DashboardService(EventStore(path)).dashboard(JOB["job_uid"])
    assert body["latest_evaluation"]["valid"]
    assert any(s["name"] == "checkpoint 完成" for s in body["events"][0]["stages"])
    assert "private/checkpoint" not in json.dumps(body)


def test_persistence_failure_keeps_log_and_never_breaks_execution(tmp_path, monkeypatch):
    blocker = tmp_path / "file"
    blocker.write_text("not a directory")
    monkeypatch.setenv("DLROVER_ELASTIC_EVENT_DB", str(blocker / "db"))
    emitter = VerticalElasticEvents(job=JOB)
    seen = []
    emitter._log = seen.append
    emitter.start("p", "add_gpus")
    assert seen[0]["plan_id"] == "p"
    assert emitter.context["plan_id"] == "p"


def test_log_import_replay_and_corruption(store):
    line = json.dumps(event("log", 100))
    log = "unrelated log\nINFO VerticalElasticEvent " + line + "\n"
    assert import_events(store, io.StringIO(log))["inserted"] == 1
    assert import_events(store, io.StringIO(log))["inserted"] == 0
    with pytest.raises(ValueError, match="line 1"):
        import_events(store, io.StringIO("VerticalElasticEvent {broken}\n"))


def test_sqlite_export_preserves_raw_ids_for_mysql_migration(store):
    value = event("migration-event", 100)
    store.append(value)
    stream = io.StringIO()
    assert export_events(store.path, stream) == 1
    assert json.loads(stream.getvalue()) == value
    assert import_events(store, io.StringIO(stream.getvalue()))["inserted"] == 0


def test_prometheus_mapping_rejects_cross_job_and_nonfinite():
    labels = dict(cluster="c", namespace="ns", job_uid="uid-1", stage="prediction", objective="cluster_jct", work_unit="steps", data_source="http")
    values = dict(valid=1, gain_ratio=.2, time_gain_seconds=-20, gpu_seconds_saved=-300,
                  baseline_throughput=2, candidate_throughput=1, observed_timestamp_seconds=90)
    samples = [{"metric": dict(labels, __name__=PREFIX + k), "value": [100, str(v)]} for k, v in values.items()]
    samples.append({"metric": dict(labels, job_uid="other", __name__=PREFIX + "gain_ratio"), "value": [100, "10"]})
    expected = {k: labels[k] for k in ("cluster", "namespace", "job_uid")}
    body = dict(status="success", data=dict(resultType="vector", result=samples))
    result = PrometheusBenefits.parse(body, expected)
    assert result["gain_ratio"] == .2
    assert result["time_gain_seconds"] == -20
    assert result["max_age_seconds"] is None
    samples[1]["value"][1] = "NaN"
    assert not PrometheusBenefits.parse(body, expected)["valid"]
    samples.append(samples[0])
    with pytest.raises(ValueError, match="Ambiguous"):
        PrometheusBenefits.parse(body, expected)


def test_http_end_to_end_internal_ingestion(store):
    server = ElasticAPI(("0.0.0.0", 0), DashboardService(store))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = "http://127.0.0.1:%d" % server.server_port

    def call(path, data=None):
        headers = {"Content-Type": "application/json"}
        request = Request(base + path, data=json.dumps(data).encode() if data is not None else None, headers=headers)
        with urlopen(request, timeout=3) as response:
            return json.load(response)

    try:
        assert call("/healthz")["status"] == "alive"
        assert call("/readyz")["status"] == "ready"
        original = store.health
        store.health = lambda: (_ for _ in ()).throw(StoreUnavailable("offline"))
        with pytest.raises(HTTPError) as unavailable:
            call("/readyz")
        assert unavailable.value.code == 503
        assert call("/healthz")["status"] == "alive"
        store.health = original
        assert call("/api/v1/elastic/jobs")["items"] == []
        assert call("/api/v1/elastic/events", event("http", 100))["inserted"]
        assert not call("/api/v1/elastic/events", event("http", 100))["inserted"]
        HTTPEventSink(base + "/api/v1/elastic/events")(event("http-sink", 101))
        assert len(store.read("uid-1")[1]) == 2
        assert call("/api/v1/elastic/jobs")["items"][0]["uid"] == "uid-1"
        assert call("/api/v1/elastic/jobs/uid-1/dashboard")["job"]["status"] == "unknown"
        with pytest.raises(HTTPError) as error:
            call("/api/v1/elastic/jobs/missing/dashboard")
        assert error.value.code == 404
        with pytest.raises(HTTPError) as error:
            call("/api/v1/elastic/events", {})
        assert error.value.code == 422
        assert call("/api/v1/elastic/job-snapshots", {
            **JOB, "status": "completed",
            "submitted_at": 50, "completed_at": 200, "lifecycle_observed_at": 201,
        })["inserted"]
        dashboard = call("/api/v1/elastic/jobs/uid-1/dashboard")
        assert dashboard["job"]["status"] == "completed"
        assert dashboard["jct"]["actual_seconds"] == 150
        assert call("/api/v1/elastic/jct-statistics?cluster=c&namespace=ns")["mean_seconds"] == 150
        assert call("/api/v1/elastic/jct-statistics?namespace=missing")["measured_jobs"] == 0
        with pytest.raises(HTTPError) as error:
            call("/api/v1/elastic/jct-statistics?namespace=")
        assert error.value.code == 422
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
