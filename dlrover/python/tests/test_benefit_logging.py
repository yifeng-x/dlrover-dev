# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from dlrover.python.master.resource.benefit_log import BenefitLogger
from dlrover.python.master.resource.swanlab import (
    SwanLabTrainingMetrics,
    TrainingMetricsUnavailable,
)
from dlrover.python.tests.test_swanlab_metrics import ENV, response, rows
from dlrover.python.tests.test_vertical_benefit import group, guard, payload
from dlrover.python.tests.test_vertical_exploration import (
    admission,
    begin,
    observe,
    trial,  # noqa: F401
)


@pytest.fixture
def logs():
    records = []
    with patch("dlrover.python.master.resource.benefit_log.logger") as logger:
        for method in (logger.debug, logger.info, logger.warning):
            method.side_effect = lambda fmt, data: records.append(json.loads(data))
        yield records


def test_swanlab_logs_window_and_correlation_without_secrets(logs):
    client = SwanLabTrainingMetrics({**ENV, "DLROVER_SWANLAB_HTTP_URL":
                                    "https://example.test/scalars?token=URL_SECRET"})
    client.post = MagicMock(return_value=response(rows(client, [940, 970, 1000])))
    client.fetch("101", lambda: 1000, 3, 120, 60, plan_id="plan", request_id="request")
    event = next(record for record in logs if record["event"] == "window_selected")
    assert event["plan_id"] == "plan"
    assert event["request_id"] == "request"
    assert event["task_id"] == ENV["DLROVER_SWANLAB_TASK_ID"]
    assert event["throughput"] == 2
    assert event["window_start"] == 940
    assert event["window_end"] == 1000
    assert event["sample_count"] == 3 * event["series_count"]
    text = json.dumps(logs)
    assert "URL_SECRET" not in text
    assert ENV["DLROVER_SWANLAB_HTTP_TOKEN"] not in text
    assert "scalarMetricsList" not in text


@pytest.mark.parametrize("failure,stage,status", [
    ("timeout", "http", None), ("http", "http", 503),
    ("decode", "decode", 200), ("window", "window", 200),
])
def test_swanlab_failure_stage_and_no_exception_body(logs, failure, stage, status):
    client = SwanLabTrainingMetrics(ENV)
    reply = response([], 503 if failure == "http" else 200)
    if failure == "decode":
        reply.json.side_effect = ValueError("PRIVATE_RESPONSE_BODY")
    client.post = MagicMock(return_value=reply)
    if failure == "timeout":
        client.post.side_effect = requests.Timeout("PRIVATE_RESPONSE_BODY")
    with pytest.raises(TrainingMetricsUnavailable):
        client.fetch("101", lambda: 1000, 3, 120, 60)
    event = next(record for record in logs if record["event"] == "fetch_failed")
    assert event["stage"] == stage
    assert event["http_status"] == status
    assert "PRIVATE_RESPONSE_BODY" not in json.dumps(logs)


def test_guard_propagates_request_identity_and_excludes_provider_fields(logs):
    gate = guard()
    gate.generation_provider = lambda: "101"
    client = SwanLabTrainingMetrics(ENV)
    client.post = MagicMock(return_value=response(rows(client, [940, 970, 1000])))
    gate.training_metrics = client

    def post(url, **kwargs):
        data = payload(kwargs["json"])
        data["candidate"]["throughput_lower"] = 4
        data["private"] = "PROVIDER_SECRET"
        return response(data)

    gate.post = post
    assert gate.check(group(4), group(8), "plan")
    selected = next(r for r in logs if r["event"] == "window_selected")
    evaluated = next(r for r in logs if r["event"] == "evaluation_finished")
    assert selected["request_id"] == evaluated["request_id"] == gate.last_request["request_id"]
    assert evaluated["status"] == "positive"
    assert "PROVIDER_SECRET" not in json.dumps(logs)


def test_poll_logs_are_bounded_and_reason_changes_are_visible(logs):
    now = [0]
    diagnostic = BenefitLogger("test", clock=lambda: now[0])
    for _ in range(10):
        diagnostic.emit("wait", repeat_key="stale", reason="stale")
    assert len(logs) == 1
    now[0] = 60
    diagnostic.emit("wait", repeat_key="stale", reason="stale")
    assert logs[-1]["suppressed"] == 9
    diagnostic.emit("wait", repeat_key="missing", reason="missing")
    assert len(logs) == 3
    assert len(diagnostic._last) == 1


def test_logging_failure_cannot_change_fetch_result():
    client = SwanLabTrainingMetrics(ENV)
    client.post = MagicMock(return_value=response(rows(client, [940, 1000])))
    with patch("dlrover.python.master.resource.benefit_log.logger") as logger:
        for method in (logger.debug, logger.info, logger.warning):
            method.side_effect = RuntimeError("log sink unavailable")
        assert client.fetch("101", lambda: 1000, 3, 120, 60)["end"]["work"] == 280


def test_exploration_admission_and_wait_reasons(trial, logs):
    admission(trial)["exploration"]["allowed"] = False
    assert not trial.prepare(group(4), group(8), "trial-1")
    assert logs[-1]["reason"] == "admission_conditions"
    begin(trial)
    assert observe(trial, generation="g1") == "wait"
    waiting = [r for r in logs if r["event"] == "observation_wait"]
    assert waiting[-1]["reason"] == "original_generation"
    assert observe(trial) == "wait"
    assert observe(trial, start=1180, end=1240, work=400) == "ACCEPTED"
    assert any(r["event"] == "observed_benefit" for r in logs)
    assert logs[-1]["event"] == "finished"
    assert logs[-1]["status"] == "ACCEPTED"


@pytest.mark.parametrize("level", ["debug", "info", "warning"])
def test_uses_dlrover_logger_level_methods(level):
    with patch("dlrover.python.master.resource.benefit_log.logger") as logger:
        BenefitLogger("test").emit("event", level=level)
    getattr(logger, level).assert_called_once()
    logger.log.assert_not_called()
