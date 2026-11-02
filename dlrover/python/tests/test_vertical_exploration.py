# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
import copy
from unittest.mock import MagicMock, patch

import pytest

from dlrover.python.common.global_context import Context
from dlrover.python.master.resource.benefit import evaluate
from dlrover.python.master.resource.exploration import Exploration, TrialStore, unpack
from dlrover.python.tests.test_vertical_benefit import group, guard, payload, request_for


@pytest.fixture
def trial(tmp_path):
    clock = [1000.0]
    gate = guard()
    gate.clock = lambda: clock[0]
    env = {"DLROVER_BENEFIT_EXPLORE": "true", "DLROVER_BENEFIT_STATE_DIR": str(tmp_path),
           "DLROVER_BENEFIT_TRIAL_SECONDS": "600", "DLROVER_BENEFIT_TRIAL_GPU_HOURS": "10"}
    obj = Exploration(gate, env, clock=lambda: clock[0])
    obj.test_clock = clock
    obj.test_env = env
    assert obj.enter()
    yield obj
    obj.leave()
    from dlrover.python.master.node.job_context import get_job_context
    get_job_context().clear_job_nodes()


def admission(obj, before=4, after=8):
    req = request_for(before, after)
    data = payload(req)
    data["candidate"] = {"status": "unavailable"}
    data["exploration"] = {
        "policy": "observe_only", "allowed": True, "checkpoint_compatible": True,
        "quiescent_checkpoint": True, "gpu_tiers": [1, 2, 4, 8],
    }
    obj.guard.last_request = req
    obj.guard.last_snapshot = data
    obj.guard.last_result = evaluate(data, req, 1000)
    return data


def begin(obj, before=4, after=8):
    admission(obj, before, after)
    assert obj.prepare(group(before), group(after), "trial-1")
    obj.switching(160, "checkpoint-original")
    obj.test_clock[0] = 1120
    obj.observing()


def observe(obj, speed=2, generation="g2", start=1120, end=1180, work=280):
    def http(url, **kwargs):
        req = kwargs["json"]
        data = payload(req, now=obj.test_clock[0])
        data["candidate"] = {"status": "unavailable"}
        data["training"]["start"] = dict(generation=generation, observed_at=start, work=work-speed*(end-start))
        data["training"]["end"] = dict(generation=generation, observed_at=end, work=work)
        data["exploration_id"] = "trial-1"
        data["execution"] = dict(
            applied_plan_id="trial-1",
            restored_checkpoint=obj.state["checkpoint_path"], all_workers_ready=True,
            generation=generation, trial_gpu_seconds=(end-1000)*8,
        )
        data["exploration"] = {"policy": "observe_only", "allowed": True}
        response = MagicMock(status_code=200)
        response.__enter__.return_value = response
        response.json.return_value = data
        return response
    obj.guard.post = http
    obj.test_clock[0] = end
    return obj.inspect()


@pytest.mark.parametrize("value,enabled", [
    (None, False), ("false", False), ("", False), ("invalid", False),
    ("true", True), (" TRUE ", True),
])
def test_startup_switch_controls_trial_admission(trial, value, enabled):
    trial.leave()
    env = dict(trial.test_env)
    if value is None:
        env.pop("DLROVER_BENEFIT_EXPLORE")
    else:
        env["DLROVER_BENEFIT_EXPLORE"] = value
    obj = Exploration(trial.guard, env, clock=lambda: 1000)
    try:
        assert obj.enter()
        admission(obj)
        assert obj.prepare(group(4), group(8), "p") is enabled
        assert (obj.store.load() is not None) is enabled
    finally:
        obj.leave()


@pytest.mark.parametrize("mode", ["off", "shadow"])
def test_enabled_switch_requires_enforce_mode(trial, mode):
    admission(trial)
    trial.guard.mode = mode
    assert not trial.prepare(group(4), group(8), "p")
    assert trial.store.load() is None


def test_default_disabled_does_not_require_exploration_configuration():
    obj = Exploration(guard(), env={})
    assert not obj.enabled
    assert obj.enter()
    assert not obj.enter(recovering=True)
    assert obj.startup_fence() == (False, False)
    obj.leave()


def test_disabling_on_restart_preserves_unfinished_trial_fence(trial):
    begin(trial)
    trial.leave()
    env = {**trial.test_env, "DLROVER_BENEFIT_EXPLORE": "false"}
    obj = Exploration(trial.guard, env, clock=lambda: 1120)
    try:
        assert obj.startup_fence()[0]
        assert not obj.enter(recovering=True)
        assert obj.store.load()["status"] == "OBSERVING"
        assert not obj.enter()
    finally:
        obj.leave()


def test_only_explicit_unavailable_enters(trial):
    data = admission(trial)
    assert trial.guard.last_result.reason == "candidate_unavailable"
    data["training"]["end"]["generation"] = "other"
    trial.guard.last_result = evaluate(data, trial.guard.last_request, 1000)
    assert not trial.prepare(group(4), group(8), "p")
    data = admission(trial)
    del data["transition"]["gpu_seconds_upper"]
    trial.guard.last_result = evaluate(data, trial.guard.last_request, 1000)
    assert not trial.prepare(group(4), group(8), "p")


@pytest.mark.parametrize("field,value", [("allowed", False), ("policy", "automatic_rollback"),
    ("checkpoint_compatible", False), ("quiescent_checkpoint", False),
    ("gpu_tiers", [1, 4, 6, 8])])
def test_admission_fails_closed(trial, field, value):
    data = admission(trial)
    data["exploration"][field] = value
    assert not trial.prepare(group(4), group(8), "p")


def test_budget_and_deadline_prevent_trial(trial):
    data = admission(trial)
    trial.max_gpu_hours = .1
    assert not trial.prepare(group(4), group(8), "p")
    trial.max_gpu_hours = 10
    data["limits"]["deadline_at"] = 5000
    assert not trial.prepare(group(4), group(8), "p")
    data["limits"]["deadline_at"] = None
    data["limits"]["max_delay_seconds"] = 599
    assert not trial.prepare(group(4), group(8), "p")


def test_positive_measurement_accepts_and_preserves_journal(trial):
    begin(trial)
    assert observe(trial) == "wait"
    assert observe(trial) == "wait"  # duplicate window cannot satisfy two windows
    assert observe(trial, start=1180, end=1240, work=400) == "ACCEPTED"
    assert trial.state["result"]["candidate_throughput"] == 2
    assert trial.store.load()["status"] == "ACCEPTED"
    trial.leave()
    assert not trial.enter()  # cooldown survives releasing process lock
    trial.test_clock[0] += trial.cooldown
    assert trial.enter()
    admission(trial)
    assert not trial.prepare(group(4), group(8), "second")  # at most one trial per job


def test_exploration_observes_real_scalar_contract(trial):
    from dlrover.python.master.resource.swanlab import SwanLabTrainingMetrics
    from dlrover.python.tests.test_swanlab_metrics import ENV, response, rows

    data = admission(trial)
    data["training"].update(target_work=10000, global_batch_size=64)
    for endpoint in ("start", "end"):
        data["training"][endpoint]["generation"] = "101"
    trial.guard.last_result = evaluate(data, trial.guard.last_request, 1000)
    assert trial.prepare(group(4), group(8), "trial-1")
    trial.switching(160, "checkpoint-original")
    trial.test_clock[0] = 1120
    trial.observing()
    trial.guard.generation_provider = lambda: "202"
    trial.guard.training_context_provider = lambda: dict(generation="202", started_at=1100)

    def post(url, **kwargs):
        now = trial.test_clock[0]
        if url == ENV["DLROVER_SWANLAB_HTTP_URL"]:
            data = rows(client, [now - 60, now - 30, now], generation="202")
            for series in data:
                if series["key"] == "train/global_step":
                    for point in series["scalarMetricsList"]:
                        point["data"] = str(float(point["data"]) + 2 * (now - 1240))
            return response(data)
        req = kwargs["json"]
        data = payload(req, now=now)
        data["candidate"] = {"status": "unavailable"}
        data["exploration_id"] = "trial-1"
        data["execution"] = dict(
            applied_plan_id="trial-1", restored_checkpoint="checkpoint-original",
            all_workers_ready=True, generation="202", trial_gpu_seconds=(now - 1000) * 8,
        )
        return response(data)

    client = SwanLabTrainingMetrics(ENV, post)
    trial.guard.training_metrics = client
    trial.guard.post = post
    trial.test_clock[0] = 1240
    assert trial.inspect() == "wait"
    assert trial.inspect() == "wait"  # repeated HTTP history is not a new window
    trial.test_clock[0] = 1300
    assert trial.inspect() == "ACCEPTED"
    assert trial.state["result"]["candidate_throughput"] == 2


def test_negative_measurement_retains_candidate_without_rollback(trial):
    begin(trial)
    assert observe(trial, speed=.8, work=208) == "wait"
    assert observe(trial, speed=.8, start=1180, end=1240, work=256) == "RETAINED"
    assert trial.state["result"]["evaluation_status"] == "negative"
    assert trial.state["result"]["overall_gain"] < 0
    assert "rollback_plan_id" not in trial.state
    assert "reservation_id" not in trial.state
    request = trial.guard.last_request
    assert request["current"] == request["target"]
    assert request["exploration"]["original"] != request["current"]


def test_sunk_cost_does_not_force_harmful_rollback(trial):
    data = admission(trial)
    data["training"]["target_work"] = 1160
    trial.guard.last_result = evaluate(data, trial.guard.last_request, 1000)
    assert trial.prepare(group(4), group(8), "trial-1")
    trial.switching(160, "checkpoint-original")
    trial.test_clock[0] = 1120
    trial.observing()
    # Provider must retain the same target across the trial.
    original_payload = payload
    def shorter_payload(request, now=1000):
        value = original_payload(request, now)
        value["training"]["target_work"] = 1160
        return value
    with patch(__name__ + ".payload", side_effect=shorter_payload):
        assert observe(trial, speed=.8, work=208) == "wait"
        assert observe(trial, speed=.8, start=1180, end=1240, work=256) == "RETAINED"
    assert trial.state["result"]["overall_gain"] < 0
    assert trial.state["result"]["evaluation_status"] == "negative"


def test_old_generation_and_mixed_generations_not_accepted(trial):
    begin(trial)
    assert observe(trial, generation="g1") == "wait"
    assert trial.state["windows"] == 0
    assert observe(trial, generation="g2") == "wait"
    assert observe(trial, generation="g3", start=1180, end=1240, work=400) == "wait"
    assert trial.state["windows"] == 1


def test_timeout_and_restart_fence(trial):
    begin(trial)
    trial.test_clock[0] = trial.state["expires_at"]
    assert trial.inspect() == "INCONCLUSIVE"
    trial.leave()
    other = Exploration(trial.guard, trial.test_env, clock=trial.clock)
    try:
        assert not other.enter()
        assert other.state["status"] == "INCONCLUSIVE"
    finally:
        other.leave()


def test_shared_lock_and_resume_observing(trial):
    begin(trial)
    other = Exploration(trial.guard, trial.test_env, clock=trial.clock)
    assert not other.enter(recovering=True)
    other.leave()
    trial.leave()
    try:
        assert other.enter(recovering=True)
        assert other.state["status"] == "OBSERVING"
        assert other.state["original"]["gpu_num"] == 4
    finally:
        other.leave()


def test_restart_during_mutation_never_replays(trial):
    admission(trial)
    assert trial.prepare(group(4), group(8), "p")
    trial.switching(160, "checkpoint-original")
    trial.leave()
    try:
        assert not trial.enter(recovering=True)
        assert trial.state["status"] == "SWITCHING"
    finally:
        trial.leave()


def test_missing_checkpoint_or_journal_failure_prevents_switch(trial):
    admission(trial)
    with patch.object(trial.store, "save", side_effect=OSError("disk full")):
        assert not trial.prepare(group(4), group(8), "p")
    assert not trial.active
    with pytest.raises(ValueError):
        trial.switching(10, "")


def test_target_feasibility_is_refreshed_before_mutation(trial):
    begin(trial)
    observe(trial)  # Installs a provider that echoes the request identity.
    trial.switching(160, "checkpoint-original")
    assert trial.authorize_mutation() is True
    trial.state["policy"] = "automatic_rollback"
    assert trial.authorize_mutation() is False


def test_resource_roundtrip_preserves_original_configuration():
    from dlrover.python.master.resource.exploration import pack
    original = group(4)
    original.node_resource.image = "training-image"
    original.node_resource.kwargs = {"ephemeral-storage": "20Gi"}
    restored = unpack(pack(original))
    assert restored.node_resource.__dict__ == original.node_resource.__dict__


def make_scaler(trial):
    from dlrover.python.master.node.job_resource_scaler import AllreduceTrainingResourceScaler
    from dlrover.python.master.resource.job import JobResource
    from dlrover.python.master.node.job_context import get_job_context
    resource = JobResource()
    resource.node_group_resources["worker"] = group(4)
    obj = AllreduceTrainingResourceScaler(resource, MagicMock(), MagicMock(), MagicMock(), MagicMock(), benefit_guard=trial.guard)
    obj.exploration = trial
    obj.scale_action = "add_gpus"
    obj.vertical_elastic_phase_manager.create("trial-1")
    obj.vertical_metrics.start("add_gpus")
    get_job_context().clear_job_nodes()
    from dlrover.python.common.node import Node
    from dlrover.python.common.constants import NodeStatus
    node = Node("worker", 0, group(4).node_resource, status=NodeStatus.RUNNING)
    get_job_context().update_job_node(node)
    return obj


def test_observation_holds_existing_scaling_mutex(trial):
    begin(trial)
    scaler = make_scaler(trial)
    scaler._scaling_lock.acquire()
    try:
        with patch.dict(Context.singleton_instance().__dict__, {"runtime_elastic_dimension": "vertical"}):
            assert not scaler.start_resource_scaling("reduce_gpus")
        assert scaler._scaling_lock.locked()
    finally:
        scaler._scaling_lock.release()


def test_full_switch_keeps_latest_checkpoint_and_waits_for_observation(trial):
    from dlrover.python.master.resource.optimizer import ResourcePlan
    admission(trial)
    scaler = make_scaler(trial)
    response_data = copy.deepcopy(trial.guard.last_snapshot)
    def http(url, **kwargs):
        data = {**response_data, **{k: kwargs["json"][k] for k in ("request_id", "job", "current", "target")}}
        data["exploration_id"] = "trial-1"
        response = MagicMock(status_code=200)
        response.__enter__.return_value = response
        response.json.return_value = data
        return response
    trial.guard.post = http
    plan = ResourcePlan()
    plan.node_group_resources["worker"] = group(8)
    scaler._job_optimizer.get_job_resource_plan.return_value = plan
    def checkpoint(*args):
        scaler._save_ckpt_step = 160
        scaler._save_ckpt_path = "new-checkpoint"
        return True
    with patch.dict(Context.singleton_instance().__dict__, {"enable_elastic_resource": True, "auto_elasticity": False}), \
         patch.object(scaler, "_wait_for_ckpt_ready", side_effect=checkpoint), \
         patch.object(scaler, "_wait_for_param_tunning_ready", return_value=True), \
         patch.object(scaler, "execute_job_optimization_plan") as execute, \
         patch.object(scaler, "_observe_exploration", return_value=True) as observe_loop, \
         patch.object(scaler, "_wait_for_replacement_exits"):
        assert scaler._try_execute_optimization(100, .01, 1)
    execute.assert_called_once_with(plan)
    observe_loop.assert_called_once()
    assert trial.state["status"] == "OBSERVING"
    assert trial.state["checkpoint_path"] == "new-checkpoint"


@pytest.mark.parametrize("speed,work,outcome", [(2, 400, "ACCEPTED"), (.8, 256, "RETAINED")])
def test_observation_completion_never_executes_reverse_plan(trial, speed, work, outcome):
    begin(trial)
    scaler = make_scaler(trial)
    observe(trial, speed=speed, work=160 + 60 * speed)
    assert observe(trial, speed=speed, start=1180, end=1240, work=work) == outcome
    with patch.object(trial, "inspect", return_value=outcome), \
         patch.object(scaler, "_execute_resource_plan") as mutation, \
         patch.object(trial, "report_result") as report:
        assert scaler._observe_exploration()
    mutation.assert_not_called()
    report.assert_called_once()
    assert scaler._job_resource.node_group_resources["worker"].node_resource.gpu_num == 8
    assert scaler.get_phase_info()["plan_id"] == "trial-1"


def test_stale_untagged_framework_reports_rejected_during_trial(trial):
    from dlrover.python.common.constants import VerticalElasticPhaseType as Phase
    begin(trial)
    scaler = make_scaler(trial)
    scaler.update_vertical_elastic_phase(8, Phase.SAVE_CHECKPOINT, Phase.REPLACING_WORKERS)
    scaler.set_save_ckpt_status_by_vertical_elastic(True, step=100, checkpoint_path="stale")
    assert not scaler._save_ckpt_status
    with patch.object(scaler._ucp_task_manager, "get_checkpoint_task", return_value=MagicMock(task_id="checkpoint-200")) as lookup:
        scaler.set_save_ckpt_status_by_vertical_elastic(True, plan_id="trial-1", step=200, checkpoint_path="fresh")
    lookup.assert_called_once_with(200, "fresh")
    assert scaler._save_ckpt_step == 200


def test_legacy_rollback_action_is_fenced_without_mutation(trial):
    begin(trial)
    scaler = make_scaler(trial)
    observe(trial, speed=.5, work=190)
    observe(trial, speed=.5, start=1180, end=1240, work=220)
    with patch.object(trial, "inspect", return_value="rollback"), \
         patch.object(scaler, "_execute_resource_plan", return_value=False) as mutation:
        assert not scaler._observe_exploration()
    mutation.assert_not_called()
    assert trial.store.load()["status"] == "FAILED"


def test_corrupt_journal_blocks_even_when_exploration_disabled(trial):
    trial.store.path.write_text('{broken')
    trial.leave()
    trial.enabled = False
    assert not trial.enter()


def test_startup_fence_blocks_initial_launch_on_partial_mutation(trial):
    assert trial.startup_fence() == (False, False)
    admission(trial)
    assert trial.prepare(group(4), group(8), "trial-1")
    assert trial.startup_fence() == (True, True)
    trial.switching(160, "checkpoint")
    assert trial.startup_fence() == (True, True)
    trial.observing()
    assert trial.startup_fence() == (True, False)
    trial.finish("ACCEPTED", "test")
    assert trial.startup_fence() == (False, False)


def test_result_upload_ack_is_durable_and_idempotent(trial):
    begin(trial)
    observe(trial)
    assert observe(trial, start=1180, end=1240, work=400) == "ACCEPTED"
    requests = []
    def http(url, **kwargs):
        request = kwargs["json"]
        requests.append(request)
        assert request["exploration"]["result"]["candidate_throughput"] == 2
        assert request["exploration"]["report_result"] is True
        response = MagicMock(status_code=200)
        response.__enter__.return_value = response
        response.json.return_value = dict(recorded=True, request_id=request["request_id"], exploration_id="trial-1")
        return response
    trial.guard.post = http
    trial.report_result()
    assert trial.store.load()["reported"] is True
    trial.leave()
    trial.enter(recovering=True)
    assert len(requests) == 1


def test_resource_budget_exhaustion_does_not_wait_for_second_window(trial):
    begin(trial)
    trial.state["gpu_budget"] = 1
    assert observe(trial) == "INCONCLUSIVE"
    assert trial.state["reason"] == "gpu_budget_exhausted"


@pytest.mark.parametrize("status", ["OBSERVING", "ROLLING_BACK", "VERIFYING_ROLLBACK"])
def test_unfinished_legacy_policy_cannot_resume(trial, status):
    begin(trial)
    trial.save(status, policy="automatic_rollback")
    assert trial.startup_fence() == (True, True)
    trial.leave()
    assert not trial.enter(recovering=True)


def test_short_task_caps_observation_budget_without_rollback_reserve(trial):
    data = admission(trial, before=1, after=2)
    data["training"]["target_work"] = 1044
    trial.duration = 1800
    trial.guard.last_result = evaluate(data, trial.guard.last_request, 1000)
    assert trial.prepare(group(1), group(2), "trial-1")
    assert trial.state["expires_at"] == 1884


def test_constraint_violation_records_negative_without_rollback(trial):
    begin(trial)
    assert observe(trial, speed=.5, work=190) == "wait"
    assert observe(trial, speed=.5, start=1180, end=1240, work=220) == "INCONCLUSIVE"
    assert trial.state["reason"] == "constraint_violation"
    assert trial.state["result"]["evaluation_status"] == "negative"
    assert trial.state["result"]["constraints_satisfied"] is False
    assert "rollback_plan_id" not in trial.state


@pytest.mark.parametrize("status", ["ACCEPTED", "RETAINED", "INCONCLUSIVE", "FAILED"])
def test_all_outcomes_retry_reporting_without_swanlab_dependency(trial, status):
    begin(trial)
    trial.finish(status, "test")
    metrics = MagicMock()
    metrics.fetch.side_effect = AssertionError("Result upload must not fetch training metrics")
    trial.guard.training_metrics = metrics
    requests = []

    def http(url, **kwargs):
        request = kwargs["json"]
        requests.append(request)
        assert request["exploration"]["report_result"] is True
        assert request["exploration"]["status"] == status
        response = MagicMock(status_code=200)
        response.__enter__.return_value = response
        response.json.return_value = dict(recorded=len(requests) > 1,
            request_id=request["request_id"], exploration_id="trial-1")
        return response

    trial.guard.post = http
    trial.report_result()
    assert not trial.state.get("reported")
    trial.leave()
    trial.enter(recovering=True)
    assert trial.store.load()["reported"] is True
    assert len(requests) == 2
    metrics.fetch.assert_not_called()
    if status in ("FAILED", "INCONCLUSIVE"):
        assert trial.startup_fence() == (True, True)


def test_two_windows_insufficient_time_rejects_before_mutation(trial):
    admission(trial)
    trial.duration = 2 * trial.guard.min_window
    assert not trial.prepare(group(4), group(8), "trial-1")
    assert trial.store.load() is None


def test_gpu_objective_records_negative_when_faster_but_more_expensive(trial):
    begin(trial)
    trial.state["objective"] = "gpu_hours"
    assert observe(trial) == "wait"
    assert observe(trial, start=1180, end=1240, work=400) == "RETAINED"
    assert trial.state["result"]["overall_gain"] == -960
    assert trial.state["result"]["evaluation_status"] == "negative"


def test_completed_legacy_journal_only_sends_release_notification(trial):
    begin(trial)
    trial.save("ROLLED_BACK", policy="automatic_rollback", cooldown_until=0, reservation_id="old-lease")
    requests = []
    def http(url, **kwargs):
        request = kwargs["json"]
        requests.append(request)
        assert request["exploration"]["release_reservation"] is True
        assert request["exploration"]["reservation_id"] == "old-lease"
        response = MagicMock(status_code=200)
        response.__enter__.return_value = response
        response.json.return_value = dict(recorded=True,
            request_id=request["request_id"], exploration_id="trial-1")
        return response
    trial.guard.post = http
    trial.leave()
    assert trial.enter()
    assert trial.state["reported"] is True
    assert len(requests) == 1
