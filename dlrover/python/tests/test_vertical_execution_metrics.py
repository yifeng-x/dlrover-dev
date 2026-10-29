# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from prometheus_client.parser import text_string_to_metric_families

from dlrover.python.common.constants import VerticalElasticPhaseType as Phase
from dlrover.python.common.global_context import Context
from dlrover.python.common.metric.prometheus import MetricsServer
from dlrover.python.common.node import Node, NodeGroupResource, NodeResource
from dlrover.python.master.monitor.perf_monitor import PerfMonitor
from dlrover.python.master.monitor.prometheus import MasterMetricsCollector
from dlrover.python.master.monitor.vertical_metrics import VerticalExecutionMetrics
from dlrover.python.master.node.job_context import get_job_context
from dlrover.python.master.node.job_resource_scaler import AllreduceTrainingResourceScaler
from dlrover.python.master.resource.job import JobResource
from dlrover.python.master.resource.optimizer import ResourcePlan


def value(metrics, suffix, **labels):
    name = "dlrover_master_vertical_" + suffix
    samples = [
        sample.value for family in metrics.collect({})
        for sample in family.samples
        if sample.name == name and all(sample.labels.get(k) == v for k, v in labels.items())
    ]
    return sum(samples)


@pytest.mark.parametrize("action", ["add_gpus", "reduce_gpus", "manual"])
@pytest.mark.parametrize("result", ["completed", "failed", "skipped", "cancelled"])
def test_first_plan_has_exported_zero_baseline(action, result):
    now = [100.0]
    metrics = VerticalExecutionMetrics(clock=lambda: now[0])
    server = MetricsServer(
        lambda: metrics.collect({}), include_runtime_metrics=False
    )

    def snapshot():
        body, ready = server.collect()
        assert ready == 1
        return {
            (sample.name, tuple(sorted(sample.labels.items()))): sample.value
            for family in text_string_to_metric_families(body)
            for sample in family.samples
        }

    prefix = "dlrover_master_vertical_"
    labels = {"action": action, "result": result}
    count = (prefix + "plan_duration_seconds_count", tuple(sorted(labels.items())))
    total = (prefix + "plan_duration_seconds_sum", tuple(sorted(labels.items())))
    before = snapshot()
    assert before[count] == before[total] == 0
    buckets = [
        key for key in before
        if key[0] == prefix + "plan_duration_seconds_bucket"
        and dict(key[1])["action"] == action
        and dict(key[1])["result"] == result
    ]
    assert buckets
    assert all(before[key] == 0 for key in buckets)
    completed = (
        prefix + "plans_finished_total",
        tuple(sorted({"action": action, "result": "completed", "reason": "none"}.items())),
    )
    assert before[completed] == 0

    metrics.start(action)
    now[0] += 7
    metrics.finish(result)
    after = snapshot()
    assert after[count] - before[count] == 1
    assert after[total] - before[total] == 7
    for key in buckets:
        assert after[key] == int(float(dict(key[1])["le"]) >= 7)
    assert after[completed] == int(result == "completed")
    assert snapshot() == after  # Scraping does not add observations.


@pytest.fixture
def scaler():
    resource = JobResource()
    resource.node_group_resources["worker"] = NodeGroupResource(
        1, NodeResource(4, 8192, "nvidia.com/gpu", 1)
    )
    scaler = AllreduceTrainingResourceScaler(
        resource, MagicMock(), MagicMock(), MagicMock(), MagicMock()
    )
    scaler.scale_action = "add_gpus"
    get_job_context().clear_job_nodes()
    context = Context.singleton_instance()
    with patch.dict(context.__dict__, {
        **context.__dict__, "runtime_elastic_dimension": "vertical",
        "enable_elastic_resource": True, "auto_elasticity": False,
        "configmap_manual_scale_switch": "off",
        "vertical_elastic_opt_interval": 0,
        "vertical_elastic_max_opt_num": 10,
        "ckpt_save_status_check_max_wait_time": 1,
        "ckpt_save_status_check_interval": 0.01,
        "log_interval_check_nums": 5,
    }), patch.object(
        scaler, "_get_replaceable_workers",
        return_value=[Node("worker", 0, resource.node_group_resources["worker"].node_resource)],
    ), patch.object(scaler, "_wait_for_replacement_exits"):
        yield scaler
    get_job_context().clear_job_nodes()


def test_phase_clock_deduplication_and_first_failure():
    now = [100.0]
    metrics = VerticalExecutionMetrics(clock=lambda: now[0])
    metrics.start("add_gpus")
    now[0] += 5
    metrics.transition(Phase.SAVE_CHECKPOINT)
    now[0] += 10
    metrics.transition(Phase.SAVE_CHECKPOINT)
    assert value(metrics, "phase_elapsed_seconds") == 10
    metrics.finish("failed", "checkpoint_failed")
    metrics.finish("failed", "checkpoint_timeout")
    assert value(metrics, "plans_finished_total") == 1
    assert value(metrics, "plans_finished_total", reason="checkpoint_failed") == 1
    assert value(metrics, "phase_duration_seconds_sum", phase=Phase.SAVE_CHECKPOINT) == 10
    assert value(metrics, "plan_duration_seconds_sum") == 15
    assert value(metrics, "phase_elapsed_seconds") == 0
    assert value(metrics, "phase", phase=Phase.FAILED) == 1


def test_histogram_is_exported_through_master_endpoint_without_scrape_counting():
    metrics = VerticalExecutionMetrics()
    metrics.start("reduce_gpus")
    metrics.finish("completed")
    args = SimpleNamespace(cluster="c", namespace="ns", job_uuid="job", distribution_strategy="allreduce")
    collector = MasterMetricsCollector(
        args, get_job_context(), PerfMonitor(), SimpleNamespace(vertical_metrics=metrics)
    )
    server = MetricsServer(collector.collect, include_runtime_metrics=False)
    for _ in range(2):
        body, ready = server.collect()
        assert ready == 1
        families = {family.name: family for family in text_string_to_metric_families(body)}
        histogram = families["dlrover_master_vertical_plan_duration_seconds"]
        assert histogram.type == "histogram"
        assert all(sample.labels["job_uid"] == "job" for sample in histogram.samples)
        assert any(sample.name.endswith("_bucket") and sample.labels["le"] == "+Inf" and sample.value == 1 for sample in histogram.samples)
        assert any(sample.name.endswith("_count") and sample.value == 1 for sample in histogram.samples)
    assert value(metrics, "plans_finished_total") == 1


@pytest.mark.parametrize("reason", ["cooldown", "max_attempts", "empty_plan", "same_resource"])
def test_skipped_plans_are_not_failures(scaler, reason):
    if reason == "cooldown":
        scaler.last_plan_time = time.time() + 100
    elif reason == "max_attempts":
        scaler.valid_opt_num = 11
    else:
        plan = ResourcePlan()
        if reason == "same_resource":
            plan.node_group_resources["worker"] = NodeGroupResource(1, NodeResource(4, 8192, "nvidia.com/gpu", 1))
        scaler._job_optimizer.get_job_resource_plan.return_value = plan
    scaler._scaling_lock.acquire()
    scaler._run_resource_scaling()
    metrics = scaler.vertical_metrics
    assert value(metrics, "plans_started_total") == 1
    assert value(metrics, "plans_finished_total", result="skipped", reason=reason) == 1
    assert value(metrics, "plans_finished_total", result="failed") == 0
    assert value(metrics, "inflight") == 0
    assert not scaler._scaling_lock.locked()


def test_checkpoint_failure_is_preserved_and_wait_exits(scaler):
    scaler.vertical_elastic_phase_manager.create("p")
    scaler.vertical_metrics.start("add_gpus")
    scaler.update_vertical_elastic_phase(2, Phase.SAVE_CHECKPOINT, Phase.REPLACING_WORKERS)
    scaler.set_save_ckpt_status_by_vertical_elastic(False, "disk full", "p")
    assert not scaler._wait_for_ckpt_ready(10, 1, 1)
    scaler._fail_vertical_plan("checkpoint_timeout", "timeout")
    assert value(scaler.vertical_metrics, "plans_finished_total", reason="checkpoint_failed") == 1
    assert value(scaler.vertical_metrics, "plans_finished_total") == 1
    assert "disk full" in scaler.get_phase_info()["metadata"]["reason"]


def test_reports_ignore_stale_and_count_only_target_ready_workers(scaler):
    scaler.vertical_elastic_phase_manager.create("p")
    scaler.vertical_metrics.start("add_gpus")
    scaler.update_vertical_elastic_phase(2, Phase.TUNE_PARAM, Phase.RENDEZVOUS_READY)
    scaler._target_tunning_nodes = {1, 2}
    scaler.set_param_tunning_ready(True, 1, "old")
    scaler.set_param_tunning_ready(True, 99, "p")
    scaler.set_param_tunning_ready(True, 1, "p")
    scaler.set_param_tunning_ready(True, 1, "p")
    metrics = scaler.vertical_metrics
    assert value(metrics, "workers", state="param_ready") == 1
    assert value(metrics, "workers", state="target") == 2
    assert value(metrics, "reports_total", result="stale_plan") == 1
    assert value(metrics, "reports_total", result="non_target") == 1
    assert value(metrics, "reports_total", result="duplicate") == 1
    metrics.finish("completed")
    assert value(metrics, "workers") == 0


@pytest.mark.parametrize("action", ["add_gpus", "reduce_gpus"])
def test_successful_execution_has_one_terminal_result(scaler, action):
    scaler.scale_action = action
    plan = ResourcePlan()
    plan.node_group_resources["worker"] = NodeGroupResource(1, NodeResource(4, 8192, "nvidia.com/gpu", 2))
    scaler._job_optimizer.get_job_resource_plan.return_value = plan

    def execute(_):
        scaler.update_vertical_elastic_phase(2, Phase.TUNE_PARAM, Phase.RENDEZVOUS_READY)
        scaler._target_tunning_nodes = {1}
        scaler.set_param_tunning_ready(True, 1, scaler.get_phase_info()["plan_id"])

    with patch.object(scaler, "_wait_for_ckpt_ready", return_value=True), patch.object(scaler, "execute_job_optimization_plan", side_effect=execute):
        scaler._scaling_lock.acquire()
        scaler._run_resource_scaling()
    assert value(scaler.vertical_metrics, "plans_finished_total", action=action, result="completed") == 1
    assert value(scaler.vertical_metrics, "plans_finished_total") == 1
    assert value(scaler.vertical_metrics, "phase_duration_seconds_count") == 5
    assert value(scaler.vertical_metrics, "recovery_pending") == 1
    assert value(scaler.vertical_metrics, "recovery_total") == 0


def test_busy_request_and_thread_start_failure(scaler):
    scaler._scaling_lock.acquire()
    assert not scaler.start_resource_scaling("add_gpus")
    scaler._scaling_lock.release()
    with patch.object(threading.Thread, "start", side_effect=RuntimeError("start failed")):
        with pytest.raises(RuntimeError):
            scaler.start_resource_scaling("add_gpus")
    metrics = scaler.vertical_metrics
    assert value(metrics, "requests_total", result="rejected", reason="busy") == 1
    assert value(metrics, "requests_total", result="error") == 1
    assert value(metrics, "plans_started_total") == 0
    assert value(metrics, "inflight") == 0
    assert not scaler._scaling_lock.locked()


@pytest.mark.parametrize("reason", ["checkpoint_timeout", "param_timeout", "scale_failed", "invalid_plan"])
def test_execution_failures_are_classified(scaler, reason):
    plan = ResourcePlan()
    target = 0 if reason == "invalid_plan" else 2
    plan.node_group_resources["worker"] = NodeGroupResource(
        1, NodeResource(4, 8192, "nvidia.com/gpu", target)
    )
    scaler._job_optimizer.get_job_resource_plan.return_value = plan

    def execute(_):
        if reason == "scale_failed":
            raise RuntimeError("API unavailable")
        scaler.update_vertical_elastic_phase(2, Phase.TUNE_PARAM, Phase.RENDEZVOUS_READY)

    with patch.object(scaler, "_wait_for_ckpt_ready", return_value=reason != "checkpoint_timeout"), patch.object(scaler, "execute_job_optimization_plan", side_effect=execute), patch.object(scaler, "_wait_for_param_tunning_ready", return_value=False):
        scaler._scaling_lock.acquire()
        scaler._run_resource_scaling()
    assert value(scaler.vertical_metrics, "plans_finished_total", result="failed", reason=reason) == 1
    assert value(scaler.vertical_metrics, "plans_finished_total") == 1
    assert value(scaler.vertical_metrics, "phase", phase=Phase.FAILED) == 1


def test_concurrent_finishes_only_count_once():
    metrics = VerticalExecutionMetrics()
    metrics.start("add_gpus")
    threads = [threading.Thread(target=metrics.finish, args=("failed", "checkpoint_failed")) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert value(metrics, "plans_finished_total") == 1
    assert value(metrics, "plan_duration_seconds_count") == 1
