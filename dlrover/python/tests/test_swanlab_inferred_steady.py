from unittest.mock import MagicMock

import pytest

from dlrover.python.common.constants import VerticalElasticPhaseType as Phase
from dlrover.python.master.monitor.vertical_metrics import VerticalExecutionMetrics
from dlrover.python.master.resource.swanlab import SwanLabTrainingMetrics, TrainingMetricsUnavailable
from dlrover.python.tests.test_swanlab_metrics import ENV, response, rows
from dlrover.python.tests.test_vertical_benefit import guard, group, payload


def context(**changes):
    return dict(dict(generation="101", started_at=800, recovery_ready=True, recovered_at=800), **changes)


def window(client, data, state=None, now=1000):
    return client.window(data, "101", now, 120, 60, training_context=context() if state is None else state)


def test_requests_only_real_counters_and_derives_internal_fields():
    client = SwanLabTrainingMetrics(ENV, MagicMock())
    client.post.return_value = response(rows(client, [940, 970, 1000]))
    result = client.fetch("101", lambda: 1000, 3, 120, 60, training_context=context())
    assert client.post.call_args.kwargs["json"]["taskScalarList"] == [
        {"key": "train/global_step"}, {"key": "train/total_steps"},
        {"key": "train/global_batch_size"},
    ]
    assert result["steady"] is True
    assert result["steady_source"] == "inferred"
    assert result["start"]["generation"] == result["end"]["generation"] == "101"


def test_legacy_scalar_mappings_are_not_requested():
    client = SwanLabTrainingMetrics({**ENV, "DLROVER_SWANLAB_KEYS":
        '{"generation":"legacy/generation","steady":"legacy/steady",'
        '"observed_at":"legacy/time","work":"trainer/step"}'})
    assert client.valid
    assert client.keys == dict(work="trainer/step", target_work="train/total_steps",
                               global_batch_size="train/global_batch_size")


@pytest.mark.parametrize("key,value", [
    ("DLROVER_SWANLAB_WARMUP_SECONDS", "-1"),
    ("DLROVER_SWANLAB_WARMUP_SECONDS", "NaN"),
    ("DLROVER_SWANLAB_STEADY_WINDOWS", "1"),
    ("DLROVER_SWANLAB_MAX_THROUGHPUT_VARIATION", "Infinity"),
    ("DLROVER_SWANLAB_MAX_THROUGHPUT_VARIATION", "-0.1"),
])
def test_invalid_inference_settings_fail_closed(key, value):
    client = SwanLabTrainingMetrics({**ENV, key: value})
    assert not client.valid


def test_duplicate_points_do_not_supply_extra_stability_windows():
    client = SwanLabTrainingMetrics(ENV)
    data = rows(client, [940, 970, 1000])
    for series in data:
        series["scalarMetricsList"][1] = dict(series["scalarMetricsList"][0])
    with pytest.raises(TrainingMetricsUnavailable, match="insufficient_steady_samples"):
        window(client, data)


@pytest.mark.parametrize("source", ["unknown", "ingestion", "request"])
def test_unconfirmed_timestamps_never_use_fetch_time(source):
    env = dict(ENV)
    env.pop("DLROVER_SWANLAB_TIME_SOURCE")
    if source:
        env["DLROVER_SWANLAB_TIME_SOURCE"] = source
    client = SwanLabTrainingMetrics(env)
    with pytest.raises(TrainingMetricsUnavailable, match="untrusted_sample_time"):
        window(client, rows(client, [940, 970, 1000]))


@pytest.mark.parametrize("state,reason", [
    ({}, "missing_or_changed_training_context"),
    (context(generation="202"), "missing_or_changed_training_context"),
    (context(started_at=0), "invalid_generation_boundary"),
    (context(started_at=1001), "invalid_generation_boundary"),
    (context(recovery_ready=False), "training_recovery_pending"),
    (context(recovered_at=950), "stale_or_missing_generation"),
    (context(started_at=950), "stale_or_missing_generation"),
])
def test_master_boundary_and_recovery_are_required(state, reason):
    client = SwanLabTrainingMetrics(ENV)
    with pytest.raises(TrainingMetricsUnavailable, match=reason):
        window(client, rows(client, [940, 970, 1000]), state)


def test_history_before_new_generation_is_excluded_before_counter_validation():
    client = SwanLabTrainingMetrics(ENV)
    data = rows(client, [700, 730, 760, 940, 970, 1000])
    for series in data:
        for point in series["scalarMetricsList"][:3]:
            point["data"] = "old-invalid-data"
    result = window(client, data)
    assert result["start"]["observed_at"] == 940
    with pytest.raises(TrainingMetricsUnavailable):
        window(client, data, context(started_at=980))


@pytest.mark.parametrize("steps", [[100, 100, 160], [100, 101, 160], [100, 200, 160]])
def test_available_data_is_not_automatically_steady(steps):
    client = SwanLabTrainingMetrics(ENV)
    data = rows(client, [940, 970, 1000])
    for point, work in zip(data[0]["scalarMetricsList"], steps):
        point["data"] = str(work)
    with pytest.raises(TrainingMetricsUnavailable):
        window(client, data)


def test_new_batch_size_needs_fresh_stability_windows():
    client = SwanLabTrainingMetrics(ENV)
    data = rows(client, [910, 940, 970, 1000])
    batch = next(series for series in data if series["key"] == client.keys["global_batch_size"])
    batch["scalarMetricsList"][-1]["data"] = "128"
    with pytest.raises(TrainingMetricsUnavailable, match="insufficient_steady_samples"):
        window(client, data)


@pytest.mark.parametrize("change", ["generation", "recovery"])
def test_guard_rejects_context_change_during_http(change):
    gate = guard()
    state = context()
    gate.generation_provider = lambda: state["generation"]
    gate.training_context_provider = lambda: state.copy()
    gate.training_metrics = SwanLabTrainingMetrics(ENV, MagicMock())
    gate.training_metrics.post.return_value = response(rows(gate.training_metrics, [940, 970, 1000]))
    def post(url, **kwargs):
        data = payload(kwargs["json"])
        if change == "generation":
            state.update(generation="202", started_at=1000)
        else:
            state["recovery_ready"] = False
        return response(data)
    gate.post = post
    assert not gate.check(group(4), group(8), "plan")
    assert gate.last_result.reason == "training_context_changed_during_fetch"


def test_recovery_status_blocks_observations_until_progress_confirmed():
    now = [100]
    metrics = VerticalExecutionMetrics(clock=lambda: now[0], wall_clock=lambda: 1000 + now[0])
    assert metrics.training_observation_status()["recovery_ready"]
    metrics.start("add_gpus")
    metrics.transition(Phase.SAVE_CHECKPOINT)
    assert not metrics.training_observation_status()["recovery_ready"]
    metrics.finish("completed")
    metrics.await_recovery({2}, 100, 60)
    metrics.training_progress("worker", 2, 101, 1000)
    assert not metrics.training_observation_status()["recovery_ready"]
    now[0] = 110
    metrics.training_progress("worker", 2, 102, 1001)
    assert metrics.training_observation_status() == dict(recovery_ready=True, recovered_at=1110)
    metrics.start("reduce_gpus")
    metrics.transition(Phase.SAVE_CHECKPOINT)
    metrics.finish("completed")
    metrics.await_recovery({3}, 102, 10)
    now[0] = 130
    metrics.check_recovery()
    assert not metrics.training_observation_status()["recovery_ready"]


def test_default_uses_confirmed_sampling_time():
    env = dict(ENV)
    env.pop("DLROVER_SWANLAB_TIME_SOURCE")
    client = SwanLabTrainingMetrics(env)
    result = window(client, rows(client, [940, 970, 1000]))
    assert result["time_source"] == "sample"
    assert result["start"]["observed_at"] == 940
    assert result["end"]["observed_at"] == 1000
