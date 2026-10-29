import json
from unittest.mock import MagicMock, patch

import pytest

from dlrover.python.common.constants import NodeStatus
from dlrover.python.common.global_context import Context
from dlrover.python.common.node import Node
from dlrover.python.master.node.job_context import get_job_context
from dlrover.python.master.node.job_resource_scaler import AllreduceTrainingResourceScaler
from dlrover.python.master.resource.benefit import BenefitGuard, evaluate
from dlrover.python.master.resource.job import JobResource
from dlrover.python.master.resource.optimizer import ResourcePlan
from dlrover.python.master.resource.swanlab import SwanLabTrainingMetrics, TrainingMetricsUnavailable
from dlrover.python.tests.test_swanlab_metrics import ENV, response, rows
from dlrover.python.tests.test_vertical_benefit import group, payload
from dlrover.python.tests.test_vertical_exploration import admission, trial  # noqa: F401


def mock_env(path):
    return {**ENV, "DLROVER_SWANLAB_HTTP_URL": "", "DLROVER_SWANLAB_MOCK": "true",
            "DLROVER_SWANLAB_MOCK_FILE": str(path)}


def write_rows(path, client, times, generation="101", work_start=160, speed=2):
    data = rows(client, times, generation)
    for series in data:
        if series["key"] == client.keys["work"]:
            for point, stamp in zip(series["scalarMetricsList"], times):
                point["data"] = str(work_start + speed * (stamp - times[0]))
    path.write_text(json.dumps(data))


@pytest.mark.parametrize("switch", [None, "false", "invalid", "1", "true", " TRUE "])
def test_mock_requires_explicit_switch(tmp_path, switch):
    path = tmp_path / "rows.json"
    env = {**ENV, "DLROVER_SWANLAB_MOCK_FILE": str(path)}
    if switch is not None:
        env["DLROVER_SWANLAB_MOCK"] = switch
    post = MagicMock()
    client = SwanLabTrainingMetrics(env, post)
    write_rows(path, client, [940, 970, 1000])
    post.return_value = response(rows(client, [940, 970, 1000]))
    result = client.fetch("101", lambda: 1000, 3, 120, 60)
    enabled = switch in ("true", " TRUE ")
    assert post.called is not enabled
    assert (result.get("data_source") == "mock") is enabled


@pytest.mark.parametrize("failure", ["missing_file", "invalid_json", "wrong_task", "stale", "wrong_generation"])
def test_mock_errors_never_fall_back_to_http(tmp_path, failure):
    path = tmp_path / "rows.json"
    post = MagicMock()
    client = SwanLabTrainingMetrics(mock_env(path), post)
    if failure != "missing_file":
        write_rows(path, client, [940, 970, 1000])
    if failure == "invalid_json":
        path.write_text("{")
    if failure == "wrong_task":
        data = json.loads(path.read_text())
        data[0]["taskId"] = "another-task"
        path.write_text(json.dumps(data))
    with pytest.raises(TrainingMetricsUnavailable):
        client.fetch("202" if failure == "wrong_generation" else "101",
                     lambda: 2000 if failure == "stale" else 1000, 3, 120, 60)
    post.assert_not_called()


def test_mock_still_requires_task_label(tmp_path):
    gate = BenefitGuard(environ=mock_env(tmp_path / "rows.json"))
    assert gate.training_metrics is not None
    assert not gate.training_metrics.valid
    with pytest.raises(TrainingMetricsUnavailable):
        gate.training_metrics.fetch("101", lambda: 1000, 3, 120, 60)


@pytest.mark.parametrize("candidate,allowed", [(4, True), (1, False)])
def test_mock_drives_real_benefit_gate_before_execution(tmp_path, candidate, allowed):
    path = tmp_path / "rows.json"
    def evidence(url, **kwargs):
        assert url == "http://benefit.test/snapshot"
        data = payload(kwargs["json"])
        data["candidate"]["throughput_lower"] = candidate
        return response(data)
    gate = BenefitGuard(
        job={"job_uid": "job"}, platform_task_id="test-task", clock=lambda: 1000,
        generation_provider=lambda: "101", post=evidence,
        environ={**mock_env(path), "DLROVER_BENEFIT_MODE": "enforce",
                 "DLROVER_BENEFIT_HTTP_URL": "http://benefit.test/snapshot"},
    )
    write_rows(path, gate.training_metrics, [940, 970, 1000])
    resource = JobResource()
    resource.node_group_resources["worker"] = group(4)
    scaler = AllreduceTrainingResourceScaler(
        resource, MagicMock(), MagicMock(), MagicMock(), MagicMock(), benefit_guard=gate)
    plan = ResourcePlan()
    plan.node_group_resources["worker"] = group(8)
    scaler._job_optimizer.get_job_resource_plan.return_value = plan
    scaler.vertical_elastic_phase_manager.create("plan")
    scaler.vertical_metrics.start("add_gpus")
    scaler.events.start("plan", "add_gpus")
    messages = []
    scaler.events.sink = messages.append
    context = get_job_context()
    context.clear_job_nodes()
    context.update_job_node(Node("worker", 0, group(4).node_resource, status=NodeStatus.RUNNING))
    try:
        with patch.dict(Context.singleton_instance().__dict__, {
            "enable_elastic_resource": True, "auto_elasticity": False,
        }), patch.object(scaler, "_wait_for_ckpt_ready", return_value=True) as checkpoint, \
             patch.object(scaler, "_wait_for_param_tunning_ready", return_value=True), \
             patch.object(scaler, "_wait_for_replacement_exits"), \
             patch.object(scaler, "execute_job_optimization_plan") as execute:
            assert scaler._try_execute_optimization(1, .01, 1) is allowed
        assert checkpoint.called is allowed
        assert execute.called is allowed
        assert gate.last_request["training"]["data_source"] == "mock"
        assert any('"data_source": "mock"' in json.dumps(message) for message in messages)
    finally:
        context.clear_job_nodes()


@pytest.mark.parametrize("speed,outcome", [(2, "ACCEPTED"), (0.5, "rollback")])
def test_mock_exploration_requires_new_windows_and_verifies_rollback(trial, tmp_path, speed, outcome):
    path = tmp_path / "rows.json"
    data = admission(trial)
    data["training"].update(target_work=10000, global_batch_size=64)
    for point in ("start", "end"):
        data["training"][point]["generation"] = "101"
    trial.guard.last_result = evaluate(data, trial.guard.last_request, 1000)
    assert trial.prepare(group(4), group(8), "trial-1")
    trial.switching(160, "checkpoint-original")
    trial.test_clock[0] = 1120
    trial.observing()
    generation = ["202"]
    trial.guard.generation_provider = lambda: generation[0]
    client = SwanLabTrainingMetrics(mock_env(path), MagicMock())
    trial.guard.training_metrics = client
    def evidence(url, **kwargs):
        now = trial.test_clock[0]
        data = payload(kwargs["json"], now=now)
        data["candidate"] = {"status": "unavailable"}
        data["exploration_id"] = "trial-1"
        data["execution"] = dict(applied_plan_id=trial.state.get("rollback_plan_id", "trial-1"),
                                 restored_checkpoint=trial.state["checkpoint_path"],
                                 all_workers_ready=True, generation=generation[0], trial_gpu_seconds=(now - 1000) * 8)
        data["exploration"] = {"rollback_reservation": {
            "id": "lease", "confirmed": True, "expires_at": 10000}}
        return response(data)
    trial.guard.post = evidence
    trial.test_clock[0] = 1240
    write_rows(path, client, [1180, 1210, 1240], generation="202", speed=speed)
    assert trial.inspect() == "wait"
    assert trial.inspect() == "wait"
    assert trial.state["windows"] == 1
    trial.test_clock[0] = 1300
    write_rows(path, client, [1240, 1270, 1300], generation="202", work_start=160 + 60 * speed, speed=speed)
    assert trial.inspect() == outcome
    assert trial.state["result"]["candidate_throughput"] == speed
    if outcome == "rollback":
        trial.switching(400, "return-checkpoint", rollback=True)
        trial.observing(rollback=True)
        generation[0] = "303"
        for start, end, work in [(1300, 1360, 400), (1360, 1420, 460)]:
            trial.test_clock[0] = end
            write_rows(path, client, [start, (start + end) / 2, end],
                       generation="303", work_start=work, speed=1)
            assert trial.inspect() == ("wait" if end == 1360 else "ROLLED_BACK")
    client.post.assert_not_called()
