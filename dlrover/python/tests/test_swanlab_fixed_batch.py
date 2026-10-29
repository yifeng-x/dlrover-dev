import json
from unittest.mock import MagicMock

import pytest

from dlrover.python.master.resource.benefit import BenefitGuard
from dlrover.python.master.resource.swanlab import SwanLabTrainingMetrics, TrainingMetricsUnavailable
from dlrover.python.tests.test_swanlab_metrics import ENV, response, rows
from dlrover.python.tests.test_vertical_benefit import group, payload


def fixed_env():
    return {**ENV, "DLROVER_SWANLAB_ASSUME_FIXED_BATCH": "true"}


def fixed_rows(client):
    # Construct the platform's three series without a numeric batch-size value.
    base = SwanLabTrainingMetrics(ENV)
    data = rows(base, [940, 970, 1000])
    data[0]["key"] = client.keys["work"]
    data[1]["key"] = client.keys["target_work"]
    data[2]["key"] = client.keys["train_speed"]
    for point in data[2]["scalarMetricsList"]:
        point["data"] = "2"
    return data


def context():
    return dict(generation="101", started_at=800, recovery_ready=True, recovered_at=800)


def test_fixed_batch_uses_platform_metrics_without_inventing_batch_size():
    client = SwanLabTrainingMetrics(fixed_env(), MagicMock())
    client.post.return_value = response(fixed_rows(client))
    result = client.fetch("101", lambda: 1000, 3, 120, 60, training_context=context())
    assert client.post.call_args.kwargs["json"]["taskScalarList"] == [
        {"key": "global_step"}, {"key": "max_steps"}, {"key": "train_speed"},
    ]
    assert "global_batch_size" not in result
    assert result["global_batch_size_fixed"] is True
    assert result["batch_size_source"] == "assumed_fixed"
    assert result["reported_train_speed"] == 2
    assert result["speed_relative_difference"] == 0


@pytest.mark.parametrize("speed", ["0", "-1", "NaN", "Infinity"])
def test_invalid_reported_speed_is_rejected(speed):
    client = SwanLabTrainingMetrics(fixed_env())
    data = fixed_rows(client)
    data[2]["scalarMetricsList"][-1]["data"] = speed
    with pytest.raises(TrainingMetricsUnavailable):
        client.window(data, "101", 1000, 120, 60, training_context=context())


def test_no_fixed_assumption_still_requires_batch_metrics():
    client = SwanLabTrainingMetrics(ENV)
    data = rows(client, [940, 970, 1000])[:-1]
    with pytest.raises(TrainingMetricsUnavailable, match="missing_training_keys"):
        client.window(data, "101", 1000, 120, 60, training_context=context())


def test_fixed_batch_keys_can_be_mapped():
    client = SwanLabTrainingMetrics({**fixed_env(), "DLROVER_SWANLAB_KEYS": json.dumps(
        dict(work="train/global_step", target_work="train/max_steps", train_speed="train/speed"))})
    assert client.valid
    assert client.keys["target_work"] == "train/max_steps"


@pytest.mark.parametrize("candidate,allowed", [(4, True), (1, False)])
def test_fixed_batch_reaches_benefit_decision(candidate, allowed):
    def evidence(url, **kwargs):
        data = payload(kwargs["json"])
        data["candidate"]["throughput_lower"] = candidate
        return response(data)
    guard = BenefitGuard(
        job={"job_uid": "job"}, environ={**fixed_env(),
            "DLROVER_BENEFIT_MODE": "enforce", "DLROVER_BENEFIT_HTTP_URL": "http://benefit.test"},
        clock=lambda: 1000, generation_provider=lambda: "101",
        training_context_provider=context, platform_task_id="test-task", post=evidence,
    )
    client = guard.training_metrics
    client.post = MagicMock(return_value=response(fixed_rows(client)))
    assert guard.check(group(4), group(8), "plan") is allowed
    assert guard.last_result.estimates["current_throughput"] == 2
    assert guard.last_request["training"]["batch_size_source"] == "assumed_fixed"


def test_reported_speed_cannot_replace_observed_progress():
    client = SwanLabTrainingMetrics(fixed_env())
    data = fixed_rows(client)
    for point in data[0]["scalarMetricsList"]:
        point["data"] = "160"
    with pytest.raises(TrainingMetricsUnavailable, match="no_progress"):
        client.window(data, "101", 1000, 120, 60, training_context=context())
