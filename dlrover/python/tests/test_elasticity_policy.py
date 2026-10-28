import copy
import json
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client.rest import ApiException

from dlrover.python.common.constants import ElasticDimension, NodeType
from dlrover.python.common.global_context import Context
from dlrover.python.common.node import NodeGroupResource, NodeResource
from dlrover.python.master.args import parse_master_args
from dlrover.python.master.elasticity_policy import (
    POLICY_ANNOTATION,
    policy_record,
    resolve_policy,
    validate_resource_plan,
    validate_worker_launch,
)
from dlrover.python.master.node.job_auto_scaler import (
    AllreduceTrainingAutoScaler,
)
from dlrover.python.master.node.job_resource_scaler import (
    AllreduceTrainingResourceScaler,
)
from dlrover.python.master.resource.job import JobResource
from dlrover.python.master.resource.optimizer import ResourcePlan
from dlrover.python.scheduler.kubernetes import K8sJobArgs, k8sClient


def job(replicas=1, gpu=4):
    return {
        "metadata": {"uid": "job-uid", "resourceVersion": "1"},
        "spec": {
            "distributionStrategy": "AllreduceStrategy",
            "optimizeMode": "cluster",
            "replicaSpecs": {
                "worker": {
                    "replicas": replicas,
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "main",
                                    "resources": {
                                        "requests": {
                                            "nvidia.com/gpu": gpu,
                                            "cpu": "2",
                                        },
                                        "limits": {"nvidia.com/gpu": gpu},
                                    },
                                }
                            ]
                        }
                    },
                },
            },
        },
    }


def worker(data):
    return data["spec"]["replicaSpecs"]["worker"]


def resources(data):
    return worker(data)["template"]["spec"]["containers"][0]["resources"]


@pytest.mark.parametrize("count,gpu", [(1, 1), (1, 4), (1, 8), (2, 2), (2, 4), (2, 8)])
def test_decision_matrix(count, gpu):
    policy = resolve_policy(job(count, gpu))
    assert policy["elastic_dimension"] == ("vertical" if count == 1 else "horizontal")
    assert policy["resource_monitor_switch"] == ("on" if count == 1 else "off")
    assert policy["configmap_manual_scale_switch"] == "off"


@pytest.mark.parametrize("gpu", [0, -1, 9, "1.5", True, "NaN", "Infinity", None])
def test_invalid_gpu(gpu):
    with pytest.raises(ValueError):
        resolve_policy(job(gpu=gpu))


@pytest.mark.parametrize("count", [0, -1, "1.5", True])
def test_invalid_replicas(count):
    with pytest.raises(ValueError):
        resolve_policy(job(replicas=count))


def test_limits_fallback_and_main_selection():
    data = job()
    del resources(data)["requests"]["nvidia.com/gpu"]
    containers = worker(data)["template"]["spec"]["containers"]
    containers.insert(0, {"name": "sidecar"})
    result = resolve_policy(data)
    assert result["gpu_resource_source"] == "limits"
    assert result["initial_gpu_per_worker"] == 4
    containers[1]["name"] = "ambiguous"
    with pytest.raises(ValueError, match="main container"):
        resolve_policy(data)


def test_invalid_resource_shapes():
    data = job()
    resources(data)["limits"]["nvidia.com/gpu"] = 8
    with pytest.raises(ValueError, match="requests and limits differ"):
        resolve_policy(data)
    data = job()
    resources(data)["requests"]["huawei.com/Ascend910B3"] = 1
    with pytest.raises(ValueError, match="exactly one"):
        resolve_policy(data)
    data = job()
    resources(data)["requests"] = resources(data)["limits"] = {
        "nvidia.com/mig-1g.5gb": 1
    }
    with pytest.raises(ValueError, match="supported"):
        resolve_policy(data)
    data = job()
    data["spec"]["replicaSpecs"]["chief"] = copy.deepcopy(worker(data))
    with pytest.raises(ValueError, match="training role"):
        resolve_policy(data)


def test_accelerator_and_explicit_conflicts():
    data = job()
    with pytest.raises(ValueError, match="xpu_type"):
        resolve_policy(data, xpu="ascend")
    for resource in resources(data).values():
        resource["huawei.com/Ascend910B3"] = resource.pop("nvidia.com/gpu")
    assert resolve_policy(data, xpu="ascend")["elastic_dimension"] == "vertical"
    with pytest.raises(ValueError, match="EXPLICIT_CONFIG_CONFLICT"):
        resolve_policy(job(), resource_monitor="off")
    with pytest.raises(ValueError, match="EXPLICIT_CONFIG_CONFLICT"):
        resolve_policy(job(), manual_scale="on")
    assert resolve_policy(job(), resource_monitor="on", manual_scale="off")
    data = job()
    worker(data)["autoScale"] = False
    with pytest.raises(ValueError, match="autoScale"):
        resolve_policy(data)


def test_cli_keeps_defaults_and_remembers_explicit_off():
    args = parse_master_args(["--job_name", "test"])
    assert args.elastic_dimension == "auto"
    assert args.resource_monitor_switch == "off"
    assert args.elastic_explicit_switches == {}
    args = parse_master_args(
        [
            "--job_name",
            "test",
            "--elastic_dimension",
            "AUTO",
            "--resource_monitor_switch=off",
        ]
    )
    assert args.elastic_dimension == "auto"
    assert args.elastic_explicit_switches == {"resource_monitor_switch": "off"}


def initialize(data, client=None, requested="auto"):
    client = client or MagicMock()
    client.get_custom_resource.return_value = data
    client.client.list_namespaced_pod.return_value = SimpleNamespace(items=[])
    args = K8sJobArgs("pyk8s", "default", "test")
    args.requested_elastic_dimension = requested
    with patch.object(k8sClient, "singleton_instance", return_value=client):
        args.initilize()
    return args, client


def test_initialization_persists_before_worker_start_and_normalizes_limits():
    data = job()
    del resources(data)["requests"]["nvidia.com/gpu"]
    args, client = initialize(data)
    assert args.runtime_elastic_dimension == "vertical"
    assert args.node_args[NodeType.WORKER].group_resource.node_resource.gpu_num == 4
    body = client.api_instance.patch_namespaced_custom_object.call_args.kwargs["body"]
    assert body["metadata"]["resourceVersion"] == "1"
    assert body["metadata"]["uid"] == "job-uid"
    assert json.loads(
        body["metadata"]["annotations"][POLICY_ANNOTATION]
    ) == policy_record(resolve_policy(data))


def test_restore_is_stable_and_rejects_configuration_changes():
    data = job(2)
    data["metadata"]["annotations"] = {
        POLICY_ANNOTATION: json.dumps(resolve_policy(data))
    }
    args, client = initialize(data)
    assert args.runtime_elastic_dimension == "horizontal"
    client.api_instance.patch_namespaced_custom_object.assert_not_called()
    client.client.list_namespaced_pod.assert_not_called()
    worker(data)["replicas"] = 1
    with pytest.raises(ValueError, match="POLICY_RECOVERY_CONFLICT"):
        initialize(data)


def test_existing_workers_without_annotation_and_api_failure_are_not_ignored():
    data = job()
    args = K8sJobArgs("pyk8s", "default", "test")
    args.job_uuid = "job-uid"
    client = MagicMock()
    client.client.list_namespaced_pod.return_value = SimpleNamespace(items=[object()])
    with pytest.raises(ValueError, match="workers exist"):
        args._save_startup_elastic_policy(client, data, resolve_policy(data))
    client.api_instance.patch_namespaced_custom_object.assert_not_called()
    client.client.list_namespaced_pod.return_value = SimpleNamespace(items=[])
    client.api_instance.patch_namespaced_custom_object.side_effect = ApiException(
        status=403
    )
    with pytest.raises(ApiException):
        args._save_startup_elastic_policy(client, data, resolve_policy(data))


def test_conflict_reloads_and_accepts_same_policy():
    data = job()
    latest = copy.deepcopy(data)
    latest["metadata"]["annotations"] = {
        POLICY_ANNOTATION: json.dumps(resolve_policy(data))
    }
    client = MagicMock()
    client.api_instance.patch_namespaced_custom_object.side_effect = ApiException(
        status=409
    )
    client.api_instance.get_namespaced_custom_object.return_value = latest
    args, client = initialize(data, client)
    assert args.runtime_elastic_dimension == "vertical"
    assert client.api_instance.patch_namespaced_custom_object.call_count == 1


def test_cross_dimension_plans_rejected():
    current = NodeGroupResource(1, NodeResource(2, 4096, "nvidia.com/gpu", 4))
    plan = ResourcePlan()
    plan.node_group_resources[NodeType.WORKER] = NodeGroupResource(
        1, NodeResource(2, 4096, "nvidia.com/gpu", 8)
    )
    validate_resource_plan(plan, current, "vertical")
    with pytest.raises(ValueError, match="horizontal plan changes"):
        validate_resource_plan(plan, current, "horizontal")
    plan.node_group_resources[NodeType.WORKER].count = 2
    with pytest.raises(ValueError, match="vertical plan changes"):
        validate_resource_plan(plan, current, "vertical")
    plan.node_group_resources[NodeType.WORKER].count = 1
    plan.node_group_resources[NodeType.WORKER].node_resource.gpu_num = 9
    with pytest.raises(ValueError, match="CAPACITY_EXCEEDED"):
        validate_resource_plan(plan, current, "vertical")


def test_vertical_requests_are_serialized_and_action_is_not_overwritten():
    context = Context.singleton_instance()
    scaler = AllreduceTrainingResourceScaler(
        JobResource(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    )
    with patch.object(
        context, "runtime_elastic_dimension", ElasticDimension.VERTICAL
    ), patch.object(context, "enable_elastic_resource", False):
        started, release = threading.Event(), threading.Event()

        def adjust():
            started.set()
            release.wait(5)

        with patch.object(
            scaler, "_adjust_running_worker_resource", side_effect=adjust
        ):
            try:
                assert scaler.start_resource_scaling("add_gpus")
                assert started.wait(2)
                assert not scaler.start_resource_scaling("reduce_gpus")
                assert scaler.scale_action == "add_gpus"
            finally:
                release.set()
                assert scaler._scaling_lock.acquire(timeout=2)
                scaler._scaling_lock.release()
            assert not context.enable_elastic_resource


def test_horizontal_thread_does_not_start_in_vertical_mode():
    scaler = AllreduceTrainingAutoScaler(
        JobResource(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
    )
    with patch.object(
        Context.singleton_instance(), "runtime_elastic_dimension", "vertical"
    ), patch("threading.Thread") as thread:
        scaler.start_auto_scaling()
        thread.assert_not_called()
        assert not scaler._autoscaling_started


def test_launch_configuration_matches_allocation():
    config = SimpleNamespace(min_nodes=1, max_nodes=1, nproc_per_node=1)
    validate_worker_launch(config, "vertical", "4", "1")
    assert config.nproc_per_node == 4
    with pytest.raises(ValueError, match="nnodes"):
        validate_worker_launch(config, "horizontal", "4", "2")
    config.max_nodes = 2
    validate_worker_launch(config, "horizontal", "4", "2")
    with pytest.raises(ValueError, match="nproc_per_node"):
        validate_worker_launch(config, "horizontal", "2", "2")


@pytest.mark.parametrize(
    "dimension,action,uid,accepted",
    [
        ("vertical", "add_gpus", "job-uid", True),
        ("", "reduce_gpus", "job-uid", True),
        ("horizontal", "add_gpus", "job-uid", False),
        ("vertical", "unknown", "job-uid", False),
        ("vertical", "add_gpus", "another-job", False),
    ],
)
def test_rpc_checks_before_execution(dimension, action, uid, accepted):
    from dlrover.python.common.comm import TriggerElastic
    from dlrover.python.master.servicer import MasterServicer

    manager = MagicMock()
    manager._job_args = SimpleNamespace(
        job_uuid="job-uid", job_name="test", namespace="default"
    )
    manager.start_resource_scaling.return_value = True
    service = SimpleNamespace(_job_manager=manager)
    request = TriggerElastic(
        elastic_dimension=dimension, scale_action=action, job_uuid=uid
    )
    context = Context.singleton_instance()
    with patch.object(context, "runtime_elastic_dimension", "vertical"), patch.object(
        context, "enable_elastic_resource", False
    ):
        assert MasterServicer._trigger_elastic(service, request) is accepted
        assert manager.start_resource_scaling.called is accepted
        # The handler itself never enables execution; acceptance belongs to
        # the controller which owns the lock.
        assert not context.enable_elastic_resource
    manager.reset_mock()
    with patch.object(context, "runtime_elastic_dimension", "horizontal"):
        assert not MasterServicer._trigger_elastic(service, request)
        manager.start_resource_scaling.assert_not_called()


@pytest.mark.parametrize("mode", [None, "single-job", "cluster"])
@pytest.mark.parametrize("replicas", [1, 2])
def test_auto_optimizer_mode_applies_on_startup_and_restore(mode, replicas):
    data = job(replicas)
    if mode is None:
        data["spec"].pop("optimizeMode")
    else:
        data["spec"]["optimizeMode"] = mode
    original_spec = copy.deepcopy(data["spec"])
    expected = "cluster" if replicas == 1 else mode or "single-job"
    assert resolve_policy(data)["optimize_mode"] == expected
    args, client = initialize(data)
    assert args.optimize_mode == expected
    body = client.api_instance.patch_namespaced_custom_object.call_args.kwargs[
        "body"
    ]
    data["metadata"]["annotations"] = body["metadata"]["annotations"]
    restored, restored_client = initialize(data)
    assert restored.optimize_mode == expected
    restored_client.api_instance.patch_namespaced_custom_object.assert_not_called()
    assert data["spec"] == original_spec


@pytest.mark.parametrize("mode", [None, "single-job", "cluster"])
@pytest.mark.parametrize("requested", ["vertical", "horizontal"])
def test_explicit_dimension_preserves_optimizer_mode(mode, requested):
    data = job()
    if mode is None:
        data["spec"].pop("optimizeMode")
    else:
        data["spec"]["optimizeMode"] = mode
    args, client = initialize(data, requested=requested)
    assert args.optimize_mode == (mode or "single-job")
    client.api_instance.patch_namespaced_custom_object.assert_not_called()


@pytest.mark.parametrize("mutation", ["invalid_json", "uid", "version"])
def test_invalid_persisted_decision(mutation):
    data = job()
    saved = resolve_policy(data)
    if mutation == "uid":
        saved["job_uid"] = "old-job"
    elif mutation == "version":
        saved["schema_version"] = 2
    raw = "{" if mutation == "invalid_json" else json.dumps(saved)
    data["metadata"]["annotations"] = {POLICY_ANNOTATION: raw}
    with pytest.raises(ValueError, match="POLICY_RECOVERY_CONFLICT"):
        initialize(data)


def test_watcher_preserves_allocated_gpu_on_recovery():
    from kubernetes import client as k8s
    from dlrover.python.master.watcher.k8s_watcher import (
        _parse_container_resource,
    )

    container = k8s.V1Container(
        name="main",
        resources=k8s.V1ResourceRequirements(
            limits={
                "cpu": "4",
                "memory": "16Gi",
                "nvidia.com/gpu": "8",
            }
        ),
    )
    resource = _parse_container_resource(container)
    assert (resource.cpu, resource.memory) == (4, 16384)
    assert (resource.gpu_type, resource.gpu_num) == ("nvidia.com/gpu", 8)


def test_tuning_acknowledges_already_matched_gpu_count():
    from dlrover.python.common.constants import VerticalElasticPhaseType
    from dlrover.python.elastic_agent.torch.training import (
        _auto_param_tuning_by_vertical_elastic,
    )

    master = MagicMock()
    master.get_vertical_elastic_phase.return_value = SimpleNamespace(
        ready=True,
        phase=VerticalElasticPhaseType.TUNE_PARAM,
        adjust_gpus=8,
        plan_id="plan-1",
    )
    config = SimpleNamespace(nproc_per_node=8)
    with patch(
        "dlrover.python.elastic_agent.torch.training.MasterClient.singleton_instance",
        return_value=master,
    ):
        _auto_param_tuning_by_vertical_elastic(config, [])
    master.set_param_tunning_ready.assert_called_once()
    assert master.set_param_tunning_ready.call_args.kwargs["plan_id"] == "plan-1"


def test_invalid_plan_is_rejected_before_checkpoint_or_replacement():
    context = Context.singleton_instance()
    resource = JobResource()
    resource.node_group_resources[NodeType.WORKER] = NodeGroupResource(
        1, NodeResource(4, 16384, "nvidia.com/gpu", 4)
    )
    optimizer = MagicMock()
    plan = ResourcePlan()
    plan.node_group_resources[NodeType.WORKER] = NodeGroupResource(
        1, NodeResource(4, 16384, "nvidia.com/gpu", 9)
    )
    optimizer.get_job_resource_plan.return_value = plan
    scaler = AllreduceTrainingResourceScaler(
        resource, optimizer, MagicMock(), MagicMock(), MagicMock()
    )
    with patch.object(context, "auto_elasticity", True), patch.object(
        context, "enable_elastic_resource", True
    ), patch.object(scaler, "_wait_for_ckpt_ready") as checkpoint:
        with pytest.raises(ValueError, match="CAPACITY_EXCEEDED"):
            scaler._try_execute_optimization(1, 1, 1)
        checkpoint.assert_not_called()
        scaler._scaler.scale.assert_not_called()


def test_horizontal_plan_can_omit_unchanged_gpu_fields():
    current = NodeGroupResource(2, NodeResource(4, 16384, "nvidia.com/gpu", 4))
    plan = ResourcePlan()
    plan.node_group_resources[NodeType.WORKER] = NodeGroupResource(
        3, NodeResource(4, 16384)
    )
    validate_resource_plan(plan, current, "horizontal")


@pytest.mark.parametrize(
    "requested,replicas,dimension,brain_available",
    [
        (None, 1, "vertical", True),
        (None, 2, "horizontal", False),
        ("auto", 1, "vertical", True),
        ("auto", 1, "vertical", False),
        ("auto", 2, "horizontal", False),
        ("vertical", 1, "vertical", False),
        ("horizontal", 2, "horizontal", False),
    ],
)
def test_master_applies_policy_before_constructing_controllers(
    requested, replicas, dimension, brain_available
):
    from dlrover.python.master import main

    client = MagicMock()
    data = job(replicas)
    data["spec"].pop("optimizeMode")
    client.get_custom_resource.return_value = data
    client.client.list_namespaced_pod.return_value = SimpleNamespace(items=[])
    cli_args = ["--job_name", "test"]
    if requested is not None:
        cli_args.extend(["--elastic_dimension", requested])
    args = parse_master_args(cli_args)
    requested = requested or "auto"
    context = Context.singleton_instance()
    with patch.dict(context.__dict__, context.__dict__.copy()), patch.object(
        k8sClient, "singleton_instance", return_value=client
    ), patch.object(context, "config_master_port"), patch.object(
        context, "set_params_from_brain"
    ), patch(
        "dlrover.python.brain.client.GlobalBrainClient.BRAIN_CLIENT.available",
        return_value=brain_available,
    ) as available, patch(
        "dlrover.python.master.dist_master.DistributedJobMaster"
    ) as master:
        auto_vertical = requested == "auto" and dimension == "vertical"
        if auto_vertical and not brain_available:
            with pytest.raises(RuntimeError, match="available Brain service"):
                main.run(args)
            master.assert_not_called()
            return
        main.run(args)
        effective = master.call_args.args[1]
        assert effective.requested_elastic_dimension == requested
        assert effective.runtime_elastic_dimension == dimension
        assert effective.optimize_mode == ("cluster" if auto_vertical else "single-job")
        assert effective.resource_monitor_switch == ("on" if auto_vertical else "off")
        assert effective.configmap_manual_scale_switch == "off"
        assert context.runtime_elastic_dimension == dimension
        assert context.auto_elasticity == (requested == "auto")
        assert available.call_count == int(auto_vertical)
        if requested == "auto":
            client.api_instance.patch_namespaced_custom_object.assert_called_once()
        else:
            client.api_instance.patch_namespaced_custom_object.assert_not_called()


@pytest.mark.parametrize("legacy_annotation", [False, True])
def test_restore_ignores_diagnostic_resource_source_changes(legacy_annotation):
    data = job()
    del resources(data)["requests"]["nvidia.com/gpu"]
    original = resolve_policy(data)
    saved = original if legacy_annotation else policy_record(original)
    data["metadata"]["annotations"] = {POLICY_ANNOTATION: json.dumps(saved)}
    resources(data)["requests"]["nvidia.com/gpu"] = 4
    args, client = initialize(data)
    assert args.runtime_elastic_dimension == "vertical"
    assert args.resource_monitor_switch == "on"
    client.api_instance.patch_namespaced_custom_object.assert_not_called()


def test_annotation_contains_only_identity_and_effective_controls():
    args, client = initialize(job())
    body = client.api_instance.patch_namespaced_custom_object.call_args.kwargs["body"]
    assert json.loads(body["metadata"]["annotations"][POLICY_ANNOTATION]) == {
        "schema_version": 1,
        "job_uid": "job-uid",
        "elastic_dimension": "vertical",
        "resource_monitor_switch": "on",
        "configmap_manual_scale_switch": "off",
    }
    assert args.node_args[NodeType.WORKER].group_resource.node_resource.gpu_num == 4


@pytest.mark.parametrize("position", ["main", "sidecar", "init"])
def test_unrelated_extended_resources_are_preserved(position):
    data = job()
    pod = worker(data)["template"]["spec"]
    extra = {"example.com/other-device": 1, "rdma/hca_shared_devices_a": 1}
    if position == "main":
        resources(data)["requests"].update(extra)
        resources(data)["limits"].update(extra)
    else:
        container = {"name": "other", "resources": {"requests": extra}}
        if position == "sidecar":
            pod["containers"].append(container)
        else:
            pod["initContainers"] = [container]
    before = copy.deepcopy(data)
    assert resolve_policy(data)["initial_gpu_per_worker"] == 4
    assert data == before


@pytest.mark.parametrize("gpu_key", ["nvidia.com/gpu", "nvidia.com/mig-1g.5gb"])
def test_accelerators_in_sidecars_remain_unsupported(gpu_key):
    data = job()
    worker(data)["template"]["spec"]["containers"].append(
        {
            "name": "sidecar",
            "resources": {"requests": {gpu_key: 1}},
        }
    )
    with pytest.raises(ValueError, match="outside main"):
        resolve_policy(data)


@pytest.mark.parametrize("auto", [False, True])
def test_strict_horizontal_plan_shape_is_auto_only(auto):
    from dlrover.python.master.scaler.base_scaler import ScalePlan

    resource = JobResource()
    resource.node_group_resources[NodeType.WORKER] = NodeGroupResource(
        2, NodeResource(4, 16384, "nvidia.com/gpu", 4)
    )
    plan = ResourcePlan()
    plan.node_group_resources[NodeType.WORKER] = copy.deepcopy(
        resource.node_group_resources[NodeType.WORKER]
    )
    plan.node_group_resources[NodeType.WORKER].count = 3
    # Legacy AllReduce execution ignores individual node resources.
    plan.node_resources["worker-0"] = NodeResource(4, 16384)
    workers = MagicMock()
    workers.adjust_worker.return_value = ScalePlan()
    executor = MagicMock()
    scaler = AllreduceTrainingAutoScaler(
        resource, MagicMock(), MagicMock(), workers, executor
    )
    context = Context.singleton_instance()
    with patch.object(context, "runtime_elastic_dimension", "horizontal"), patch.object(
        context, "auto_elasticity", auto
    ):
        if auto:
            with pytest.raises(ValueError, match="worker group only"):
                scaler.execute_job_optimization_plan(plan)
            executor.scale.assert_not_called()
        else:
            scaler.execute_job_optimization_plan(plan)
            executor.scale.assert_called_once()
            current = resource.get_node_group_resource(NodeType.WORKER)
            assert current.count == 3
            assert (
                current.node_resource.gpu_type,
                current.node_resource.gpu_num,
            ) == ("nvidia.com/gpu", 4)


@pytest.mark.parametrize("auto", [False, True])
def test_legacy_vertical_optimizer_still_normalizes_replica_count(auto):
    from dlrover.python.master.resource.job import (
        AllreduceJobResourceOptimizer,
    )

    initial = NodeGroupResource(1, NodeResource(4, 16384, "nvidia.com/gpu", 4))
    plan = ResourcePlan()
    plan.node_group_resources[NodeType.WORKER] = NodeGroupResource(
        2, NodeResource(4, 16384, "nvidia.com/gpu", 8)
    )
    generator = MagicMock()
    generator.generate_resource_plan_with_optimizer.return_value = plan
    context = Context.singleton_instance()
    with patch.object(context, "runtime_elastic_dimension", "vertical"), patch.object(
        context, "auto_elasticity", auto
    ), patch(
        "dlrover.python.master.resource.job.new_allreduce_resource_optimizer",
        return_value=generator,
    ):
        optimizer = AllreduceJobResourceOptimizer(initial, "cluster")
        if auto:
            with pytest.raises(ValueError, match="changed replicas"):
                optimizer.get_job_resource_plan("add_gpus")
        else:
            result = optimizer.get_job_resource_plan("add_gpus")
            assert result.node_group_resources[NodeType.WORKER].count == 1
