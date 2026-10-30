# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import copy
from unittest.mock import MagicMock, patch

import pytest
import requests

from dlrover.python.common.constants import NodeStatus, VerticalElasticPhaseType as Phase
from dlrover.python.common.global_context import Context
from dlrover.python.common.node import Node, NodeGroupResource, NodeResource
from dlrover.python.master.node.job_context import get_job_context
from dlrover.python.master.node.job_resource_scaler import AllreduceTrainingResourceScaler
from dlrover.python.master.resource.benefit import BenefitGuard, evaluate
from dlrover.python.master.resource.job import JobResource
from dlrover.python.master.resource.optimizer import ResourcePlan


def group(gpus):
    return NodeGroupResource(1, NodeResource(4, 8192, "nvidia.com/gpu", gpus))


def payload(request, now=1000):
    return {
        **copy.deepcopy(request), "observed_at": now,
        "training": {
            "unit": "steps", "comparable": True, "steady": True,
            "start": {"generation": "g1", "observed_at": now - 60, "work": 100},
            "end": {"generation": "g1", "observed_at": now, "work": 160},
            "target_work": 3760,
        },
        "candidate": {"throughput_lower": 2, "evidence": "trial", "evidence_id": "trial-1"},
        "transition": {"lost_seconds_upper": 120, "replay_work_upper": 0, "gpu_seconds_upper": 960},
        "scheduling": {"target_feasible": True, "constraints_satisfied": True},
        "limits": {"max_delay_seconds": 1800, "deadline_at": None},
    }


def request_for(before=4, after=8):
    return dict(schema_version=1, request_id="r1", job={"job_uid": "job1"},
                current=BenefitGuard.resource(group(before)),
                target=BenefitGuard.resource(group(after)))


def test_expansion_can_save_time_but_cost_more_gpu_hours():
    request = request_for()
    result = evaluate(payload(request), request, 1000)
    assert result.status == "positive"
    assert result.estimates["baseline_seconds"] == 3600
    assert result.estimates["candidate_seconds"] == 1920
    assert result.estimates["gpu_hours_saved"] < 0
    assert evaluate(payload(request), request, 1000, objective="gpu_hours").status == "negative"


def test_cluster_shrink_benefits_queue_despite_job_delay():
    request = request_for(8, 4)
    data = payload(request)
    data["candidate"]["throughput_lower"] = 0.75
    data["scheduling"].update(impacts_complete=True, job_weight=1, other_jobs=[
        dict(job_id="waiting", weight=1, baseline_remaining_seconds=6000,
             candidate_remaining_seconds_upper=2520)
    ])
    assert evaluate(data, request, 1000).status == "negative"
    result = evaluate(data, request, 1000, objective="cluster_jct")
    assert result.status == "positive"
    assert result.estimates["cluster_jct_gain_seconds"] == 2160
    data["scheduling"]["other_jobs"].append(data["scheduling"]["other_jobs"][0])
    assert evaluate(data, request, 1000, objective="cluster_jct").status == "unknown"


@pytest.mark.parametrize("mutate", [
    lambda p: p.pop("candidate"),
    lambda p: p.update(observed_at=900),
    lambda p: p.update(observed_at=1001),
    lambda p: p.update(request_id="other"),
    lambda p: p["target"].update(gpus_per_worker=16),
    lambda p: p["training"]["end"].update(generation="g2"),
    lambda p: p["training"]["end"].update(work=90),
    lambda p: p["training"].update(comparable=False),
    lambda p: p["candidate"].update(throughput_lower=float("nan")),
    lambda p: p["candidate"].update(throughput_lower=True),
    lambda p: p["candidate"].update(evidence="linear_gpu_ratio"),
    lambda p: p["transition"].pop("gpu_seconds_upper"),
    lambda p: p["scheduling"].pop("constraints_satisfied"),
])
def test_missing_invalid_or_stale_evidence_is_unknown(mutate):
    request = request_for()
    data = payload(request)
    mutate(data)
    assert evaluate(data, request, 1000).status == "unknown"


def test_deadline_and_replay_are_included():
    request = request_for()
    data = payload(request)
    data["transition"]["replay_work_upper"] = 100
    result = evaluate(data, request, 1000)
    assert result.estimates["candidate_seconds"] == 1970
    data["limits"]["deadline_at"] = 2900
    assert evaluate(data, request, 1000).reason == "deadline"
    data["limits"]["deadline_at"] = None
    data["scheduling"]["target_feasible"] = False
    assert evaluate(data, request, 1000).reason == "target_infeasible"


def test_cluster_requires_explicit_queue_evidence():
    request = request_for()
    assert evaluate(payload(request), request, 1000, objective="cluster_jct").status == "unknown"


def guard(mode="enforce", post=None):
    return BenefitGuard(
        job={"job_uid": "job1"}, clock=lambda: 1000, post=post,
        environ={"DLROVER_BENEFIT_MODE": mode,
                 "DLROVER_BENEFIT_HTTP_URL": "http://adapter/benefit",
                 "DLROVER_BENEFIT_HTTP_TOKEN": "test-token"},
    )


def test_http_contract_auth_identity_and_no_redirect():
    post = MagicMock()
    def respond(url, **kwargs):
        assert url == "http://adapter/benefit"
        assert kwargs["timeout"] == 3
        assert kwargs["allow_redirects"] is False
        assert kwargs["headers"]["Authorization"] == "Bearer test-token"
        response = MagicMock(status_code=200)
        response.json.return_value = payload(kwargs["json"])
        response.__enter__.return_value = response
        return response
    post.side_effect = respond
    instance = guard(post=post)
    assert instance.check(group(4), group(8), "p1")
    assert instance.last_request["plan_id"] == "p1"
    assert instance.last_result.status == "positive"


@pytest.mark.parametrize("mode,allowed", [("enforce", False), ("shadow", True), ("off", True)])
def test_http_failure_modes(mode, allowed):
    post = MagicMock(side_effect=requests.Timeout())
    instance = guard(mode, post)
    assert instance.check(group(4), group(8), "p1") is allowed
    assert instance.last_result.status == "unknown"
    assert post.call_count == (0 if mode == "off" else 1)


def test_invalid_mode_and_bad_timeout_never_enable_scaling():
    instance = guard("typo", MagicMock())
    assert not instance.check(group(4), group(8), "p1")
    instance.post.assert_not_called()
    instance = BenefitGuard(environ={"DLROVER_BENEFIT_MODE": "enforce",
                                   "DLROVER_BENEFIT_TIMEOUT_SECONDS": "nan"})
    assert not instance.check(group(4), group(8), "p1")


@pytest.mark.parametrize("mode,checkpoint_called", [("enforce", False), ("shadow", True), ("off", True)])
def test_gate_is_before_checkpoint_and_worker_replacement(mode, checkpoint_called):
    resource = JobResource()
    resource.node_group_resources["worker"] = group(4)
    instance = AllreduceTrainingResourceScaler(
        resource, MagicMock(), MagicMock(), MagicMock(), MagicMock(),
        benefit_guard=guard(mode, MagicMock(side_effect=requests.Timeout())),
    )
    plan = ResourcePlan()
    plan.node_group_resources["worker"] = group(8)
    instance._job_optimizer.get_job_resource_plan.return_value = plan
    instance.scale_action = "add_gpus"
    instance.vertical_elastic_phase_manager.create("p1")
    instance.vertical_metrics.start("add_gpus")
    get_job_context().clear_job_nodes()
    get_job_context().update_job_node(Node("worker", 0, group(4).node_resource, status=NodeStatus.RUNNING))
    context = Context.singleton_instance()
    try:
        with patch.dict(context.__dict__, {"enable_elastic_resource": True,
                                          "auto_elasticity": False}), \
             patch.object(instance, "_wait_for_ckpt_ready", return_value=False) as checkpoint:
            assert not instance._try_execute_optimization(1, .01, 1)
        assert checkpoint.called is checkpoint_called
        instance._scaler.scale.assert_not_called()
        if mode == "enforce":
            assert instance.get_phase_info()["phase"] == Phase.SKIPPED
            assert not instance.get_phase_info()["ready"]
            assert instance._old_worker_ids is None
            assert not instance._replacement_started
            assert instance.benefit_guard.last_result.status == "unknown"
    finally:
        get_job_context().clear_job_nodes()


@pytest.mark.parametrize("throughput,allowed", [(2, True), (0.5, False)])
def test_http_decision_controls_real_execution_path(throughput, allowed):
    def respond(url, **kwargs):
        data = payload(kwargs["json"])
        data["candidate"]["throughput_lower"] = throughput
        response = MagicMock(status_code=200)
        response.json.return_value = data
        response.__enter__.return_value = response
        return response
    resource = JobResource()
    resource.node_group_resources["worker"] = group(4)
    instance = AllreduceTrainingResourceScaler(
        resource, MagicMock(), MagicMock(), MagicMock(), MagicMock(),
        benefit_guard=guard(post=respond),
    )
    plan = ResourcePlan()
    plan.node_group_resources["worker"] = group(8)
    instance._job_optimizer.get_job_resource_plan.return_value = plan
    instance.vertical_elastic_phase_manager.create("p1")
    instance.vertical_metrics.start("add_gpus")
    get_job_context().clear_job_nodes()
    get_job_context().update_job_node(Node("worker", 0, group(4).node_resource, status=NodeStatus.RUNNING))
    try:
        with patch.dict(Context.singleton_instance().__dict__, {
            "enable_elastic_resource": True, "auto_elasticity": False,
        }), patch.object(instance, "_wait_for_ckpt_ready", return_value=True) as checkpoint, \
             patch.object(instance, "_wait_for_param_tunning_ready", return_value=True), \
             patch.object(instance, "execute_job_optimization_plan") as execute, \
             patch.object(instance, "_wait_for_replacement_exits"):
            assert instance._try_execute_optimization(1, .01, 1) is allowed
        assert checkpoint.called is allowed
        assert execute.called is allowed
        if allowed:
            assert instance.get_phase_info()["phase"] == Phase.COMPLETED
    finally:
        get_job_context().clear_job_nodes()


@pytest.mark.parametrize("status,payload_value", [(503, {}), (302, {}), (200, []), (200, {"schema_version": 1})])
def test_bad_http_response_does_not_allow_enforced_execution(status, payload_value):
    response = MagicMock(status_code=status)
    response.__enter__.return_value = response
    response.json.return_value = payload_value
    instance = guard(post=MagicMock(return_value=response))
    assert not instance.check(group(4), group(8), "p1")
    assert instance.last_result.status == "unknown"


def test_malformed_url_is_unknown_without_request():
    instance = guard(post=MagicMock())
    instance.url = "http://[invalid"
    assert not instance.check(group(4), group(8), "p1")
    assert instance.last_result.reason == "invalid_configuration"
    instance.post.assert_not_called()
