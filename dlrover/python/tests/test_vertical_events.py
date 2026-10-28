# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
import copy
from unittest.mock import MagicMock, patch

import pytest

from dlrover.python.common.constants import NodeStatus, VerticalElasticPhaseType as Phase
from dlrover.python.common.node import Node
from dlrover.python.master.monitor.vertical_events import VerticalElasticEvents, resource_snapshot
from dlrover.python.master.monitor.vertical_metrics import VerticalExecutionMetrics
from dlrover.python.master.resource.benefit import BenefitResult
from dlrover.python.master.resource.optimizer import ResourcePlan
from dlrover.python.tests.test_vertical_benefit import group, guard
from dlrover.python.tests.test_vertical_execution_metrics import scaler  # noqa: F401
from dlrover.python.tests.test_vertical_exploration import trial  # noqa: F401


def recorder():
    messages = []
    events = VerticalElasticEvents(job={"job_uid": "job"}, task_id="task", sink=messages.append)
    events.start("p", "add_gpus", request_id="request")
    events.resources(group(4), group(8), "g0")
    return events, messages


@pytest.mark.parametrize("mode,status,allowed,exploring,decision", [
    ("enforce", "negative", False, False, "reject"),
    ("shadow", "negative", True, False, "allow"),
    ("enforce", "unknown", True, True, "explore"),
    ("off", "unknown", True, False, "allow"),
])
def test_evaluation_is_separate_from_action(mode, status, allowed, exploring, decision):
    events, messages = recorder()
    gate = guard(mode)
    gate.last_result = BenefitResult(status, "reason", {"time_gain_seconds": -12})
    events.evaluated(gate, allowed, exploring)
    value = messages[-1]
    assert value["benefit"]["decision"] == decision
    assert value["benefit"]["evaluation_status"] == ("not_evaluated" if mode == "off" else status)
    assert value["job"]["job_uid"] == "job"
    assert value["swanlab_task_id"] == "task"
    events.resources(group(8), group(4))
    assert value["resources"]["before"]["total_gpus"] == 4
    assert value["resources"]["actual_after"] is None
    assert len({v["event_id"] for v in messages}) == len(messages)


def test_control_completion_waits_for_recovery_and_deduplicates_final():
    events, messages = recorder()
    now = [10.0]
    metrics = VerticalExecutionMetrics(clock=lambda: now[0])
    metrics.events = events
    metrics.start("add_gpus")
    metrics.transition(Phase.SAVE_CHECKPOINT)
    events.checkpoint(100, "ckpt", {7})
    metrics.finish("completed")
    assert messages[-1]["event_type"] == "control_completed"
    assert messages[-1]["recovery_status"] == "pending"
    assert not any(e["event_type"] == "finished" for e in messages)
    metrics.await_recovery({7}, 100, 60)
    metrics.training_progress("worker", 7, 100, 1000)
    now[0] = 12
    metrics.training_progress("worker", 7, 101, 1001)
    metrics.training_progress("worker", 7, 102, 1002)
    assert [e["event_type"] for e in messages][-2:] == ["training_recovered", "finished"]
    assert messages[-1]["recovery_seconds"] == 2
    assert messages[-1]["resources"]["final"] is None
    events.finish("failed", "late")
    assert messages[-1]["result"] == "completed"


def test_delayed_recovery_keeps_original_plan_context():
    events, messages = recorder()
    metrics = VerticalExecutionMetrics()
    metrics.events = events
    metrics.start("add_gpus")
    metrics.transition(Phase.SAVE_CHECKPOINT)
    metrics.finish("completed")
    metrics.await_recovery({7}, 100, 60)
    events.start("new", "reduce_gpus")
    metrics.stop_recovery()
    assert messages[-1]["plan_id"] == "p"
    assert messages[-1]["reason_code"] == "training_unverified"
    assert events.context["plan_id"] == "new"
    assert not events.context["_finished"]


def test_rollback_links_and_distinguishes_observed_projection():
    events, messages = recorder()
    observed = resource_snapshot(group(8))
    events.observation_provider = lambda ids: {"resources": copy.deepcopy(observed), "generation": "g1"}
    state = dict(id="p", objective="gpu_hours", result={"overall_gain": -100}, reason="negative")
    events.exploration_decided(state, "ROLLBACK_REQUESTED")
    assert messages[-1]["resources"]["actual_after"]["total_gpus"] == 8
    assert messages[-1]["resources"]["final"] is None
    events.start("p-rollback", "reduce_gpus", root_plan_id="p", parent_plan_id="p")
    events.resources(group(8), group(4))
    observed = resource_snapshot(group(4))
    events.exploration_decided(state, "ROLLED_BACK")
    assert messages[-1]["parent_plan_id"] == "p"
    assert messages[-1]["root_plan_id"] == "p"
    assert messages[-1]["resources"]["final"]["total_gpus"] == 4
    assert messages[-1]["exploration"]["gain_unit"] == "gpu_seconds"
    count = len(messages)
    events.exploration_decided(state, "ROLLED_BACK")
    assert len(messages) == count


def test_reporting_failure_does_not_break_metrics_or_controller(scaler):
    scaler.events.sink = MagicMock(side_effect=RuntimeError("sink unavailable"))
    scaler._job_optimizer.get_job_resource_plan.return_value = ResourcePlan()
    scaler._scaling_lock.acquire()
    scaler._run_resource_scaling()
    assert not scaler._scaling_lock.locked()
    assert scaler.events.sink.called


def test_rejected_request_does_not_attach_to_an_active_plan(scaler):
    messages = []
    scaler.events.sink = messages.append
    scaler.events.start("active", "add_gpus")
    scaler._scaling_lock.acquire()
    try:
        assert not scaler.start_resource_scaling("reduce_gpus")
    finally:
        scaler._scaling_lock.release()
    assert messages[-1]["event_type"] == "requested"
    assert messages[-1]["plan_id"] is None
    assert messages[-1]["reason_code"] == "busy"
    assert scaler.events.context["plan_id"] == "active"


def test_real_scaler_emits_evaluation_control_and_recovery(scaler):
    messages = []
    scaler.events.sink = messages.append
    plan = ResourcePlan()
    plan.node_group_resources["worker"] = group(8)
    scaler._job_optimizer.get_job_resource_plan.return_value = plan
    def execute(_):
        scaler.update_vertical_elastic_phase(8, Phase.TUNE_PARAM, Phase.RENDEZVOUS_READY)
        scaler._target_tunning_nodes = {7}
        node = Node("worker", 7, group(8).node_resource, status=NodeStatus.RUNNING)
        scaler._job_context.update_job_node(node)
        scaler.set_param_tunning_ready(True, 7, scaler.get_phase_info()["plan_id"])
    with patch.object(scaler, "_wait_for_ckpt_ready", return_value=True), \
         patch.object(scaler, "execute_job_optimization_plan", side_effect=execute):
        scaler._scaling_lock.acquire()
        scaler._run_resource_scaling()
    assert messages[-1]["event_type"] == "control_completed"
    assert messages[-1]["resources"]["actual_after"]["total_gpus"] == 8
    evaluated = next(e for e in messages if e["event_type"] == "evaluated")
    assert evaluated["benefit"]["evaluation_status"] == "not_evaluated"
    assert evaluated["resources"]["before"]["total_gpus"] == 1
    scaler.vertical_metrics.training_progress("worker", 7, 10, 1000)
    scaler.vertical_metrics.training_progress("worker", 7, 11, 1001)
    assert messages[-1]["event_type"] == "finished"
    assert messages[-1]["resources"]["final"]["total_gpus"] == 8


def test_partial_workers_never_report_target_as_actual(scaler):
    events = scaler.events
    messages = []
    events.sink = messages.append
    events.start("p", "add_gpus")
    events.resources(group(4), group(8))
    events.checkpoint(10, "ckpt", {7, 8})
    scaler._job_context.update_job_node(Node("worker", 7, group(8).node_resource, status=NodeStatus.RUNNING))
    events.control_finished("failed", "param_timeout")
    assert messages[-1]["resources"]["actual_after"] is None
    assert messages[-1]["resources"]["final"] is None
    assert messages[-1]["resource_observation"]["status"] == "unknown_or_partial"


def test_plan_ids_do_not_repeat_when_sequence_restarts(scaler):
    messages = []
    scaler.events.sink = messages.append
    scaler._plan_id_generator.generate_id = lambda: "vertical_elastic_plan_date_001"
    scaler._job_optimizer.get_job_resource_plan.return_value = ResourcePlan()
    for _ in range(2):
        scaler._scaling_lock.acquire()
        scaler._run_resource_scaling()
    finished = [e for e in messages if e["event_type"] == "finished"]
    assert len(finished) == 2
    assert finished[0]["plan_id"] != finished[1]["plan_id"]
    assert all(e["reason_code"] == "empty_plan" for e in finished)


def test_benefit_rejection_is_recorded_before_checkpoint(scaler):
    messages = []
    scaler.events.sink = messages.append
    plan = ResourcePlan()
    plan.node_group_resources["worker"] = group(8)
    scaler._job_optimizer.get_job_resource_plan.return_value = plan
    scaler.benefit_guard.mode = "enforce"
    scaler.benefit_guard.last_result = BenefitResult("negative", "insufficient_gain")
    with patch.object(scaler.benefit_guard, "check", return_value=False), \
         patch.object(scaler, "_wait_for_ckpt_ready") as checkpoint:
        scaler._scaling_lock.acquire()
        scaler._run_resource_scaling()
    checkpoint.assert_not_called()
    evaluated = next(e for e in messages if e["event_type"] == "evaluated")
    assert evaluated["benefit"]["decision"] == "reject"
    assert evaluated["benefit"]["evaluation_status"] == "negative"
    assert messages[-1]["reason_code"] == "benefit_negative"
    assert messages[-1]["resources"]["actual_after"] is None


def test_evidence_uses_whitelisted_fields():
    events, messages = recorder()
    gate = guard()
    gate.last_snapshot = {"training": {"unit": "steps", "secret": "do-not-log",
                                      "start": {"work": 1, "secret": "do-not-log"}},
                          "http_token": "do-not-log"}
    events.evaluated(gate, False, False)
    assert "do-not-log" not in str(messages)


def test_recovery_timeout_produces_unverified_terminal():
    events, messages = recorder()
    now = [10]
    metrics = VerticalExecutionMetrics(clock=lambda: now[0])
    metrics.events = events
    metrics.start("add_gpus")
    metrics.transition(Phase.SAVE_CHECKPOINT)
    metrics.finish("completed")
    metrics.await_recovery({7}, 100, 5)
    now[0] = 16
    metrics.check_recovery()
    assert messages[-1]["result"] == "unverified"
    assert not any(e["event_type"] == "training_recovered" for e in messages)


def test_invalid_measurement_still_emits_rejection_event():
    events, messages = recorder()
    gate = guard()
    gate.last_snapshot = {"candidate": {"throughput_lower": float("nan")}}
    events.evaluated(gate, False, False)
    assert messages[-1]["event_type"] == "evaluated"
    assert messages[-1]["benefit"]["evidence"]["candidate"]["throughput_lower"] is None


def test_exploration_failure_finishes_after_failure_reason_is_available():
    events, messages = recorder()
    gate = guard()
    events.evaluated(gate, True, True)
    events.control_finished("failed", "param_timeout")
    assert not any(e["event_type"] == "finished" for e in messages)
    state = dict(id="p", objective="time", reason="switch_failed")
    events.exploration_decided(state, "FAILED")
    assert messages[-1]["event_type"] == "finished"
    assert messages[-1]["execution_status"] == "failed"
    assert messages[-1]["exploration"]["reason_code"] == "switch_failed"


def test_manager_propagates_trigger_source():
    from types import SimpleNamespace
    from dlrover.python.master.node.dist_job_manager import DistributedJobManager
    manager = object.__new__(DistributedJobManager)
    manager._job_args = SimpleNamespace(runtime_elastic_dimension="vertical")
    manager._workspace_training_state = SimpleNamespace(releasing=False)
    manager._job_resource_scaler = MagicMock()
    manager._job_resource_scaler.start_resource_scaling.return_value = True
    assert manager.start_resource_scaling("add_gpus", trigger_source="rpc")
    manager._job_resource_scaler.start_resource_scaling.assert_called_once_with(
        "add_gpus", trigger_source="rpc")


@pytest.mark.parametrize("snapshot", [["invalid"], {"training": None, "scheduling": None}])
def test_bad_provider_payload_does_not_hide_evaluation(snapshot):
    events, messages = recorder()
    gate = guard()
    gate.last_snapshot = snapshot
    events.evaluated(gate, False, False)
    assert messages[-1]["event_type"] == "evaluated"
    assert messages[-1]["benefit"]["decision"] == "reject"


def test_resource_lookup_error_still_emits_terminal():
    events, messages = recorder()
    events.observation_provider = MagicMock(side_effect=RuntimeError("unavailable"))
    events.control_finished("failed", "scale_failed")
    assert messages[-1]["event_type"] == "finished"
    assert messages[-1]["resources"]["final"] is None


def test_scaler_rollback_emits_linked_terminal_events(trial):
    from dlrover.python.tests.test_vertical_exploration import begin, make_scaler, observe
    begin(trial)
    obj = make_scaler(trial)
    messages = []
    actual = [resource_snapshot(group(8))]
    obj.events.sink = messages.append
    obj.events.observation_provider = lambda ids: {"resources": copy.deepcopy(actual[0])}
    obj.events.start("trial-1", "add_gpus", exploring=True)
    obj.events.resources(group(4), group(8))
    observe(trial, speed=.5, work=190)
    assert observe(trial, speed=.5, start=1180, end=1240, work=220) == "rollback"
    def execute(*args, **kwargs):
        actual[0] = resource_snapshot(group(4))
        obj.vertical_metrics.finish("completed")
        trial.switching(230, "latest-return-checkpoint", rollback=True)
        trial.observing(rollback=True)
        return True
    with patch.object(trial, "inspect", side_effect=["rollback", "ROLLED_BACK"]), \
         patch.object(obj, "_execute_resource_plan", side_effect=execute), \
         patch.object(obj._benefit_stop, "wait"), patch.object(trial, "release_reservation"):
        assert obj._observe_exploration()
    finished = [e for e in messages if e["event_type"] == "finished"]
    assert [e["plan_id"] for e in finished] == ["trial-1", "trial-1-rollback"]
    assert finished[0]["resources"]["final"] is None
    assert finished[1]["parent_plan_id"] == "trial-1"
    assert finished[1]["resources"]["final"]["total_gpus"] == 4
    assert finished[1]["execution_status"] == "completed"
