# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
import copy
import math
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from prometheus_client.parser import text_string_to_metric_families

from dlrover.python.common.metric.prometheus import MetricsServer
from dlrover.python.master.monitor.perf_monitor import PerfMonitor
from dlrover.python.master.monitor.prometheus import MasterMetricsCollector
from dlrover.python.master.monitor.vertical_metrics import VerticalExecutionMetrics
from dlrover.python.master.node.job_context import get_job_context
from dlrover.python.master.resource.benefit import BenefitResult, evaluate
from dlrover.python.master.resource.exploration import Exploration
from dlrover.python.tests.test_vertical_benefit import guard, payload, request_for
from dlrover.python.tests.test_vertical_exploration import begin, observe, trial  # noqa: F401


def samples(metrics):
    return { (sample.name[len("dlrover_master_vertical_"): ], tuple(sorted(sample.labels.items()))): sample.value
             for family in metrics.collect({}) for sample in family.samples }


def value(metrics, name, **labels):
    found = [v for (n, items), v in samples(metrics).items()
             if n == name and all(dict(items).get(k) == v for k, v in labels.items())]
    assert len(found) == 1, (name, found)
    return found[0]


def prediction(objective="time"):
    gate = guard()
    gate.objective = objective
    req = request_for()
    gate.last_snapshot = payload(req)
    gate.last_snapshot["training"]["data_source"] = "mock"
    gate.last_result = evaluate(gate.last_snapshot, req, 1000, objective=objective)
    return gate


@pytest.mark.parametrize("objective,ratio", [("time", 1680/3600), ("gpu_hours", -960/14400)])
def test_prediction_exposes_both_gain_units_and_objective_ratio(objective, ratio):
    metrics = VerticalExecutionMetrics()
    metrics.record_benefit(prediction(objective))
    assert value(metrics, "benefit_baseline_throughput") == 1
    assert value(metrics, "benefit_candidate_throughput") == 2
    assert value(metrics, "benefit_time_gain_seconds") == 1680
    assert value(metrics, "benefit_gpu_seconds_saved") == pytest.approx(-960)
    assert value(metrics, "benefit_gain_ratio") == pytest.approx(ratio)
    assert value(metrics, "benefit_valid", data_source="mock", work_unit="steps") == 1
    assert value(metrics, "benefit_observed_timestamp_seconds") == 1000


def test_unknown_off_and_label_changes_do_not_leave_stale_positive_values():
    metrics = VerticalExecutionMetrics()
    gate = prediction()
    metrics.record_benefit(gate)
    gate.last_result = BenefitResult("unknown", "http_error")
    gate.last_snapshot = None
    metrics.record_benefit(gate)
    assert value(metrics, "benefit_valid") == 0
    assert math.isnan(value(metrics, "benefit_gain_ratio"))
    assert value(metrics, "benefit_observed_timestamp_seconds") == 0
    assert not any(dict(labels).get("data_source") == "mock" for _, labels in samples(metrics))
    gate = prediction()
    gate.mode = "off"
    metrics.record_benefit(gate)
    assert value(metrics, "benefit_valid") == 0
    assert math.isnan(value(metrics, "benefit_time_gain_seconds"))


@pytest.mark.parametrize("speed,work,status", [(2, 400, "ACCEPTED"), (.8, 256, "RETAINED"), (.5, 220, "INCONCLUSIVE")])
def test_durable_observation_updates_and_counts_only_once(trial, speed, work, status):
    metrics = VerticalExecutionMetrics()
    trial.metrics = metrics
    begin(trial)
    assert value(metrics, "benefit_valid") == 0
    assert observe(trial, speed=speed, work=160 + 60*speed) == "wait"
    assert value(metrics, "exploration_windows") == 1
    assert value(metrics, "benefit_valid") == 0
    assert observe(trial, speed=speed, start=1180, end=1240, work=work) == status
    assert value(metrics, "benefit_valid") == 1  # Includes valid negative/constraint-violating evidence.
    assert value(metrics, "benefit_time_gain_seconds") == trial.state["result"]["time_gain_seconds"]
    assert value(metrics, "benefit_gpu_seconds_saved") == trial.state["result"]["gpu_seconds_saved"]
    assert value(metrics, "exploration_state", state=status.lower()) == 1
    assert value(metrics, "explorations_total", result=status.lower()) == 1
    assert value(metrics, "exploration_result_reported") == 0
    trial.save(reported=True)
    assert value(metrics, "exploration_result_reported") == 1
    assert value(metrics, "explorations_total", result=status.lower()) == 1
    assert value(metrics, "exploration_gpu_seconds") == 1920
    assert value(metrics, "benefit_observed_timestamp_seconds") == 1240


def test_switch_failure_counts_without_entering_observation_and_restores_without_recount(trial):
    metrics = VerticalExecutionMetrics()
    trial.metrics = metrics
    begin(trial)
    trial.switching(160, "checkpoint-original")
    trial.finish("FAILED", "switch_failed")
    assert value(metrics, "explorations_total", result="failed") == 1
    assert value(metrics, "benefit_valid") == 0
    restored = VerticalExecutionMetrics()
    trial.leave()
    other = Exploration(trial.guard, trial.test_env, clock=trial.clock, metrics=restored)
    try:
        assert not other.enter(recovering=True)
        assert value(restored, "exploration_state", state="failed") == 1
        assert not any(n == "explorations_total" for n, _ in samples(restored))
        other.save(reported=True)
        assert not any(n == "explorations_total" for n, _ in samples(restored))
    finally:
        other.leave()


def test_metrics_error_cannot_abort_durable_trial(trial):
    trial.metrics = MagicMock()
    trial.metrics.record_exploration.side_effect = RuntimeError("metrics unavailable")
    begin(trial)
    trial.finish("FAILED", "test")
    assert trial.store.load()["status"] == "FAILED"


def test_master_endpoint_exports_labels_without_scrape_mutations(trial):
    metrics = VerticalExecutionMetrics()
    trial.metrics = metrics
    metrics.record_benefit(prediction())
    begin(trial)
    observe(trial)
    observe(trial, start=1180, end=1240, work=400)
    state = copy.deepcopy(trial.store.load())
    args = SimpleNamespace(cluster="c", namespace="elastic-training", job_uuid="job-uid", distribution_strategy="allreduce")
    collector = MasterMetricsCollector(args, get_job_context(), PerfMonitor(), SimpleNamespace(vertical_metrics=metrics))
    server = MetricsServer(collector.collect, include_runtime_metrics=False)
    for _ in range(2):
        body, ready = server.collect()
        assert ready == 1
        exported = [s for family in text_string_to_metric_families(body) for s in family.samples
                    if s.name == "dlrover_master_vertical_benefit_valid"]
        assert len(exported) == 2
        assert {s.labels["stage"] for s in exported} == {"prediction", "exploration"}
        assert all(s.labels["namespace"] == "elastic-training" and s.labels["job_uid"] == "job-uid" for s in exported)
        assert all("plan_id" not in s.labels for s in exported)
        assert value(metrics, "explorations_total", result="accepted") == 1
    assert trial.store.load() == state


def test_cluster_ratio_uses_weighted_cluster_baseline():
    metrics = VerticalExecutionMetrics()
    gate = prediction("cluster_jct")
    req = request_for()
    gate.last_snapshot["scheduling"].update(
        impacts_complete=True, job_weight=2,
        other_jobs=[dict(job_id="waiting", weight=1,
                        baseline_remaining_seconds=1000,
                        candidate_remaining_seconds_upper=500)],
    )
    gate.last_result = evaluate(gate.last_snapshot, req, 1000, objective="cluster_jct")
    metrics.record_benefit(gate)
    assert value(metrics, "benefit_gain_ratio") == pytest.approx((2*1680+500)/(2*3600+1000))
    assert value(metrics, "benefit_valid") == 1
    # Time/GPU savings remain this job's values, not weighted queue totals.
    assert value(metrics, "benefit_time_gain_seconds") == 1680


def test_partial_observation_timeout_is_not_zero_or_valid_gain(trial):
    metrics = VerticalExecutionMetrics()
    trial.metrics = metrics
    begin(trial)
    observe(trial)
    trial.test_clock[0] = trial.state["expires_at"]
    assert trial.inspect() == "INCONCLUSIVE"
    assert value(metrics, "benefit_valid") == 0
    assert math.isnan(value(metrics, "benefit_gain_ratio"))
    assert value(metrics, "exploration_windows") == 1
    assert value(metrics, "explorations_total", result="inconclusive") == 1


def test_restored_complete_result_repopulates_gauges_not_counter(trial):
    begin(trial)
    observe(trial)
    observe(trial, start=1180, end=1240, work=400)
    trial.save(reported=True)
    metrics = VerticalExecutionMetrics()
    trial.leave()
    other = Exploration(trial.guard, trial.test_env, clock=trial.clock, metrics=metrics)
    try:
        assert not other.enter(recovering=True)
        assert value(metrics, "benefit_valid") == 1
        assert value(metrics, "benefit_candidate_throughput") == 2
        assert value(metrics, "exploration_result_reported") == 1
        assert not any(n == "explorations_total" for n, _ in samples(metrics))
    finally:
        other.leave()
