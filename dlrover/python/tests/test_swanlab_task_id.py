# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dlrover.python.master.node.dist_job_manager import DistributedJobManager
from dlrover.python.master.resource.benefit import BenefitGuard
from dlrover.python.master.resource.job import JobResource
from dlrover.python.master.resource.swanlab import TrainingMetricsUnavailable
from dlrover.python.tests.test_elasticity_policy import initialize, job
from dlrover.python.tests.test_swanlab_metrics import ENV, response, rows


@pytest.mark.parametrize("labels,annotations,env_id,expected", [
    ({"low_code_platform_task_id": "label-task"}, {}, "old-env", "label-task"),
    ({}, {"low_code_platform_task_id": "annotation-task"}, "old-env", ""),
    ({"low_code_platform_task_id": "label-task"},
     {"low_code_platform_task_id": "annotation-task"}, "old-env", "label-task"),
    (None, None, "legacy-env", ""),
    ({"low_code_platform_task_id": " "},
     {"low_code_platform_task_id": "annotation-task"}, "", ""),
    ({"low_code_platform_task_id": 123}, {}, "legacy-env", ""),
    ({"low_code_platform_task_id": "label-task"}, {}, "", "label-task"),
    ({}, {}, "", ""),
])
@pytest.mark.parametrize("requested", ["auto", "vertical"])
def test_label_task_id_reaches_request_and_events_ignoring_annotations(
    monkeypatch, labels, annotations, env_id, expected, requested
):
    data = job()
    data["metadata"].update(labels=labels, annotations=annotations)
    args, _ = initialize(data, requested=requested)
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("LOW_CODE_PLATFORM_TASK_ID", env_id)
    manager = SimpleNamespace(
        _job_args=args, _job_resource=JobResource(), _job_optimizer=MagicMock(),
        _perf_monitor=MagicMock(), _worker_manager=MagicMock(), _scaler=MagicMock(),
        vertical_metrics=None, training_generation_provider=lambda: "101",
    )
    DistributedJobManager._init_job_resource_scaler(manager)
    scaler = manager._job_resource_scaler
    client = scaler.benefit_guard.training_metrics
    assert client.task_id == expected
    assert scaler.events.task_id == expected
    client.post = MagicMock(return_value=response(rows(client, [940, 1000])))
    if not expected:
        with pytest.raises(TrainingMetricsUnavailable):
            client.fetch("101", lambda: 1000, 3, 120, 60)
        client.post.assert_not_called()
    else:
        for generation in ("101", "202"):
            client.post.return_value = response(rows(client, [940, 1000], generation))
            client.fetch(generation, lambda: 1000, 3, 120, 60)
            assert client.post.call_args.kwargs["json"]["taskId"] == expected


def test_task_identity_is_per_job_without_mutating_environment():
    env = dict(ENV)
    first = BenefitGuard(environ=env, platform_task_id="label-one")
    env["LOW_CODE_PLATFORM_TASK_ID"] = "job-two"
    second = BenefitGuard(environ=env, platform_task_id="label-two")
    assert first.training_metrics.task_id == "label-one"
    assert second.training_metrics.task_id == "label-two"
    assert env["LOW_CODE_PLATFORM_TASK_ID"] == "job-two"


@pytest.mark.parametrize("old_name", ["DLROVER_SWANLAB_TASK_ID", "low_code_platform_task_id", "LOW_CODE_PLATFORM_TASK_ID"])
def test_environment_cannot_supply_missing_label(monkeypatch, old_name):
    monkeypatch.delenv("LOW_CODE_PLATFORM_TASK_ID", raising=False)
    monkeypatch.setenv(old_name, "old-task")
    monkeypatch.setenv("DLROVER_SWANLAB_HTTP_URL", ENV["DLROVER_SWANLAB_HTTP_URL"])
    client = BenefitGuard().training_metrics
    assert client.task_id == ""
    assert not client.valid


def test_training_generation_changes_keep_platform_task_id(monkeypatch):
    monkeypatch.setenv("LOW_CODE_PLATFORM_TASK_ID", "platform-task")
    monkeypatch.setenv("DLROVER_SWANLAB_HTTP_URL", ENV["DLROVER_SWANLAB_HTTP_URL"])
    client = BenefitGuard(platform_task_id="platform-task").training_metrics
    client.post = MagicMock()
    for generation in ("101", "202"):
        client.post.return_value = response(rows(client, [940, 1000], generation))
        window = client.fetch(generation, lambda: 1000, 3, 120, 60)
        assert window["end"]["generation"] == generation
    assert len(client.post.call_args_list) == 2
    assert all(call.kwargs["json"]["taskId"] == "platform-task"
               for call in client.post.call_args_list)


def test_label_overrides_only_job_local_environment():
    env = dict(ENV)
    first = BenefitGuard(environ=env, platform_task_id="label-one")
    second = BenefitGuard(environ=env, platform_task_id="label-two")
    assert first.training_metrics.task_id == "label-one"
    assert second.training_metrics.task_id == "label-two"
    assert env == ENV
