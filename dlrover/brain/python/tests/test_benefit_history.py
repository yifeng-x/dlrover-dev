# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
from copy import deepcopy
import time
from unittest.mock import Mock

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dlrover.brain.python.server import benefit_history as history


@pytest.fixture
def service(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path}/history.db")
    history.ExplorationHistory.__table__.create(engine)
    monkeypatch.setattr(history, "get_db_session", sessionmaker(engine))
    monkeypatch.setenv("DLROVER_BENEFIT_UPSTREAM_URL", "http://adapter/snapshot")
    monkeypatch.delenv("DLROVER_BENEFIT_HTTP_TOKEN", raising=False)
    upstream = Mock(status_code=200)
    monkeypatch.setattr(history.requests, "post", Mock(return_value=upstream))
    yield upstream
    engine.dispose()


@pytest.fixture
def report():
    target = dict(workers=1, gpus_per_worker=8, gpu_type="A100")
    return dict(
        request_id="r1", job=dict(job_uid="job", cluster="cluster", namespace="team"),
        current={**target, "gpus_per_worker": 4}, target=target,
        exploration=dict(
            id="trial", report_result=True, policy="observe_only", status="RETAINED",
            baseline_context=dict(template_id="recipe", config_revision="recipe-v1", phase="finetune"),
            history_context=dict(template_id="recipe", config_revision="recipe-v1", phase="finetune"),
            baseline_training=dict(comparable=True, unit="steps", global_batch_size=64),
            candidate_observed_at=time.time(), baseline_throughput=2,
            result=dict(candidate_throughput=1.5, windows=2, data_source="http",
                        unit="steps", candidate_resource=target),
        ),
    )


def prediction(report):
    trial = report["exploration"]
    return dict(candidate={"status": "unavailable"}, history_context=trial["history_context"],
                training={**trial["baseline_training"], "start": dict(work=0, observed_at=100),
                          "end": dict(work=16, observed_at=110)})


def test_retry_then_reuse_negative_gain(service, report):
    assert history.snapshot(report, "")["recorded"]
    report["request_id"] = "retry"
    assert history.snapshot(report, "") == dict(recorded=True, request_id="retry", exploration_id="trial")
    with history.get_db_session() as db:
        row = db.query(history.ExplorationHistory).one()
        assert (row.baseline_throughput, row.candidate_throughput) == (2, 1.5)
    service.json.return_value = prediction(report)
    request = {k: v for k, v in report.items() if k != "exploration"}
    request["job"] = {**report["job"], "job_uid": "new-run"}
    assert history.snapshot(request, "")["candidate"] == dict(
        throughput_lower=pytest.approx(1.2), evidence="history", evidence_id="trial",
        source_job_uid="job", source_cluster="cluster")
    report["exploration"]["status"] = "ACCEPTED"
    with pytest.raises(HTTPException) as exc:
        history.snapshot(report, "")
    assert exc.value.status_code == 409


@pytest.mark.parametrize("status", ["FAILED", "INCONCLUSIVE"])
def test_incomplete_outcome_is_archived(service, report, status):
    report["exploration"].update(status=status, result=None)
    assert history.snapshot(report, "")["recorded"]
    with history.get_db_session() as db:
        row = db.query(history.ExplorationHistory).one()
        assert row.status == status and row.candidate_throughput is None and row.reuse_key is None


@pytest.mark.parametrize("change", ["namespace", "cluster", "current", "target", "phase", "template_id", "batch", "mock",
                                    "missing_window", "bad_baseline", "missing_template", "changed_recipe",
                                    "expired", "unknown_config", "control", "profile"])
def test_prediction_is_scoped(service, report, change):
    if change == "mock":
        report["exploration"]["result"]["data_source"] = "mock"
    if change == "expired":
        report["exploration"]["candidate_observed_at"] -= 86401
    if change == "bad_baseline":
        report["exploration"]["baseline_throughput"] = 0
    if change == "changed_recipe":
        report["exploration"]["baseline_context"]["config_revision"] = "previous"
    history.record(report)
    data = prediction(report)
    request = {k: v for k, v in report.items() if k != "exploration"}
    if change in ("namespace", "cluster"):
        request["job"][change] = "other"
    elif change == "current":
        request["current"]["gpus_per_worker"] = 2
    elif change == "missing_window":
        del data["training"]["start"]
    elif change == "missing_template":
        del data["history_context"]["template_id"]
    elif change == "target":
        request["target"]["workers"] = 2
    elif change in ("phase", "template_id"):
        data["history_context"][change] = "other"
    elif change == "batch":
        request["training"] = {**data["training"], "global_batch_size": 128}
    elif change == "unknown_config":
        del data["history_context"]
    elif change == "control":
        request["exploration"] = {"authorize_mutation": True}
    elif change == "profile":
        data["candidate"] = {"throughput_lower": 5, "evidence": "profile", "evidence_id": "p"}
    expected = data["candidate"].copy()
    service.json.return_value = data
    assert history.snapshot(request, "")["candidate"] == expected


def test_brain_route_commits_before_ack(service, report, monkeypatch):
    from fastapi.testclient import TestClient

    from dlrover.brain.python.server.server import BrainServer

    monkeypatch.setenv("DLROVER_BENEFIT_HTTP_TOKEN", "history-token")
    client = TestClient(BrainServer()._server)
    url = "/benefit/snapshot"
    headers = {"Authorization": "Bearer history-token"}
    assert client.post(url, json=report).status_code == 401
    with history.get_db_session() as db:
        table = history.ExplorationHistory.__table__
        table.drop(db.get_bind())
        response = client.post(url, json=report, headers=headers)
        assert response.status_code == 503
        assert "recorded" not in response.json()
        table.create(db.get_bind())
    assert client.post(url, json=report, headers=headers).json()["recorded"]
    service.json.return_value = prediction(report)
    request = {k: v for k, v in report.items() if k != "exploration"}
    response = client.post(url, json=request, headers=headers)
    assert response.json()["candidate"]["evidence_id"] == "trial"

def test_lowest_ratio_selected_without_uid_preference(service, report):
    history.record(report)
    other = deepcopy(report)
    other["job"]["job_uid"] = "earlier-run"
    other["exploration"].update(id="another-trial", baseline_throughput=4)
    other["exploration"]["result"]["candidate_throughput"] = 2
    history.record(other)
    data = prediction(report)
    request = {k: v for k, v in report.items() if k != "exploration"}
    history.fill_candidate(request, data)
    assert data["candidate"]["throughput_lower"] == pytest.approx(0.8)
    assert data["candidate"]["source_job_uid"] == "earlier-run"
