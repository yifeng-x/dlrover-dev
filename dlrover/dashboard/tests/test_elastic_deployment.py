# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
import io
import json
import threading
from pathlib import Path
from urllib.error import HTTPError
from unittest.mock import MagicMock

import pytest

from dlrover.dashboard.elastic.client import AsyncHTTPEventSink, HTTPEventSink
from dlrover.python.master.monitor.vertical_events import VerticalElasticEvents


def test_http_sink_retries_same_id_after_transient_failure(monkeypatch):
    response = MagicMock()
    response.__enter__.return_value.read.return_value = b'{"recorded":true,"inserted":false}'
    send = MagicMock(side_effect=[HTTPError("url", 503, "unavailable", {}, io.BytesIO()), response])
    monkeypatch.setattr("dlrover.dashboard.elastic.client.urlopen", send)
    sink = HTTPEventSink("http://api:8080/api/v1/elastic/events")
    sink({"event_id": "same-id"})
    assert send.call_count == 2
    assert all(not call.args[0].has_header("Authorization") for call in send.call_args_list)
    assert all(json.loads(call.args[0].data)["event_id"] == "same-id" for call in send.call_args_list)


def test_http_sink_does_not_retry_invalid_record(monkeypatch):
    send = MagicMock(side_effect=HTTPError("private-url", 422, "sensitive", {}, io.BytesIO()))
    monkeypatch.setattr("dlrover.dashboard.elastic.client.urlopen", send)
    with pytest.raises(RuntimeError, match="rejected"):
        HTTPEventSink("http://api/api/v1/elastic/events")({"event_id": "id"})
    assert send.call_count == 1


def test_master_http_sink_keeps_existing_logs(monkeypatch):
    monkeypatch.setenv("DLROVER_ELASTIC_EVENT_URL", "http://api/api/v1/elastic/events")
    emitter = VerticalElasticEvents(job={"job_uid": "job"})
    seen = []
    emitter._log = seen.append
    emitter._http_sink = MagicMock(side_effect=RuntimeError("unavailable"))
    emitter.start("plan", "add_gpus")
    assert seen[0]["plan_id"] == "plan"
    assert emitter.context["plan_id"] == "plan"


def test_manifest_internal_service_and_precreated_schema():
    yaml = pytest.importorskip("yaml")
    root = Path(__file__).resolve().parents[3] / "deploy" / "elastic-dashboard"
    assert not (root / "kustomization.yaml").exists()
    assert not (root / "schema-job.yaml").exists()
    assert not (root / "namespace.yaml").exists()
    assert not (root / "pdb.yaml").exists()
    deployment = yaml.safe_load((root / "deployment.yaml").read_text())
    pod = deployment["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert deployment["spec"]["replicas"] == 2
    assert "mysql" in container["args"]
    assert container["livenessProbe"]["httpGet"]["path"] == "/healthz"
    assert container["readinessProbe"]["httpGet"]["path"] == "/readyz"
    assert container["securityContext"]["readOnlyRootFilesystem"]
    assert not pod.get("volumes")  # No local event DB/PVC in stateless API Pods.
    assert container["envFrom"] == [{"configMapRef": {"name": "elastic-dashboard-config"}}]
    config = yaml.safe_load((root / "configmap.yaml").read_text())["data"]
    assert "DLROVER_MYSQL_USER" in config and "DLROVER_MYSQL_PASSWORD" in config
    assert not any("TOKEN" in key for key in config)
    service = yaml.safe_load((root / "service.yaml").read_text())
    assert service["spec"]["type"] == "ClusterIP"


def test_master_does_not_wait_for_stalled_http_and_queue_is_bounded(monkeypatch):
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    drained = threading.Event()
    delivered = []

    def stalled_send(self, payload):
        entered.set()
        release.wait(5)
        delivered.append(payload)
        if len(delivered) == 33:
            drained.set()

    monkeypatch.setattr(HTTPEventSink, "send_payload", stalled_send)
    monkeypatch.setenv("DLROVER_ELASTIC_EVENT_URL", "http://api/api/v1/elastic/events")
    emitter = VerticalElasticEvents(job={"job_uid": "job"})
    logged = []
    emitter._log = logged.append
    emitter.start("plan", "add_gpus")
    sink = emitter._http_sink
    try:
        assert entered.wait(1)

        def control_work():
            for i in range(100):
                emitter.request(str(i), "add_gpus", "accepted", "test", "manual")
            finished.set()

        caller = threading.Thread(target=control_work, daemon=True)
        caller.start()
        assert finished.wait(1), "Master waited for the blocked HTTP worker"
        assert sink._queue.qsize() == 32
        assert sink.dropped == 68
        assert len(logged) == 101
        assert emitter.context["plan_id"] == "plan"
        assert sink._worker.daemon
    finally:
        release.set()
    assert drained.wait(2)


def test_async_delivery_continues_after_failure_and_snapshots_payload(monkeypatch):
    failed, recovered = threading.Event(), threading.Event()
    received = []

    def send(self, payload):
        received.append(json.loads(payload))
        if len(received) == 1:
            failed.set()
            raise RuntimeError("service offline")
        recovered.set()

    monkeypatch.setattr(HTTPEventSink, "send_payload", send)
    sink = AsyncHTTPEventSink("http://api/api/v1/elastic/events")
    sink({"event_id": "failed"})
    assert failed.wait(1)
    event = {"event_id": "recovered"}
    sink(event)
    event["event_id"] = "mutated"
    assert recovered.wait(1)
    assert sink.failed == 1
    assert received[1]["event_id"] == "recovered"
    sink({"event_id": "oversized", "data": "x" * (1024 * 1024)})
    assert sink.dropped == 1
