"""Startup elasticity selection. This module has no cluster side effects."""

from decimal import Decimal, InvalidOperation

from dlrover.python.common.constants import (
    DistributionStrategy,
    ElasticDimension,
    NodeType,
    OptimizeMode,
    WorkerResourceScaleAction,
)
from dlrover.python.common.log import default_logger as logger

POLICY_ANNOTATION = "dlrover.io/startup-elastic-policy"
MAX_GPUS_PER_WORKER = 8
GPU_RESOURCE_TYPES = {
    "nvidia.com/gpu": "nvidia",
    "huawei.com/Ascend910B3": "ascend",
}
VERTICAL_ACTIONS = frozenset(
    (
        WorkerResourceScaleAction.ADD_GPUS,
        WorkerResourceScaleAction.REDUCE_GPUS,
        WorkerResourceScaleAction.MANUAL,
    )
)


def policy_record(policy):
    """Persist only identity and effective controls, not diagnostic inputs."""
    return {
        key: policy[key]
        for key in (
            "schema_version",
            "job_uid",
            "elastic_dimension",
            "resource_monitor_switch",
            "configmap_manual_scale_switch",
        )
    }


def is_accelerator_resource(key):
    """Recognize GPU/NPU resources without treating every device as a GPU."""
    return key in GPU_RESOURCE_TYPES or key.startswith(
        ("nvidia.com/mig-", "huawei.com/Ascend", "mthreads.com/gpu")
    )


def integer_quantity(value, field):
    """Reject fractional and boolean values instead of truncating them."""
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number != number.to_integral_value():
            raise ValueError()
        return int(number)
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError(f"INVALID_GPU_RESOURCE: {field} must be an integer")


def training_container(spec):
    containers = spec.get("template", {}).get("spec", {}).get("containers", [])
    candidates = (
        containers
        if len(containers) == 1
        else [c for c in containers if c.get("name") == "main"]
    )
    if len(candidates) != 1:
        raise ValueError(
            "UNSUPPORTED_AUTO_TOPOLOGY: require one main container"
        )
    main = candidates[0]
    # An accelerator sidecar would make the worker resource model incomplete.
    pod_spec = spec["template"]["spec"]
    others = [c for c in containers if c is not main]
    others += pod_spec.get("initContainers", [])
    for container in others:
        for resources in (container.get("resources") or {}).values():
            if any(is_accelerator_resource(key) for key in (resources or {})):
                raise ValueError(
                    "UNSUPPORTED_AUTO_TOPOLOGY: device resources outside main"
                )
    return main


def resolve_policy(
    job, resource_monitor=None, manual_scale=None, xpu="nvidia"
):
    """Resolve an admitted ElasticJob into a stable, JSON-serializable policy."""
    spec = job["spec"]
    if spec.get("distributionStrategy") != DistributionStrategy.ALLREDUCE:
        raise ValueError("UNSUPPORTED_AUTO_TOPOLOGY: auto requires AllReduce")
    if (
        spec.get(
            "runMode",
            (job.get("metadata", {}).get("annotations") or {}).get(
                "dlrover.io/run-mode", ""
            ),
        ).lower()
        == "workspace"
    ):
        raise ValueError(
            "UNSUPPORTED_AUTO_TOPOLOGY: workspace requires explicit mode"
        )
    replicas = spec.get("replicaSpecs", {})
    if set(replicas) - {NodeType.WORKER, NodeType.DLROVER_MASTER}:
        raise ValueError(
            "UNSUPPORTED_AUTO_TOPOLOGY: only worker training role supported"
        )
    worker = replicas.get(NodeType.WORKER)
    if not worker:
        raise ValueError("UNSUPPORTED_AUTO_TOPOLOGY: worker spec is required")
    count = integer_quantity(worker.get("replicas", 0), "worker.replicas")
    if count == 0 or spec.get("suspended", False):
        raise ValueError(
            "NOT_READY: resume the job with positive worker replicas"
        )
    if count < 0:
        raise ValueError(
            "INVALID_GPU_RESOURCE: worker.replicas must be positive"
        )
    if str(worker.get("autoScale", True)).lower() not in (
        "true",
        "yes",
        "t",
        "y",
    ):
        raise ValueError(
            "EXPLICIT_CONFIG_CONFLICT: auto requires worker.autoScale=true"
        )
    resources = training_container(worker).get("resources", {})
    requests = resources.get("requests", {}) or {}
    limits = resources.get("limits", {}) or {}
    device_keys = {
        key
        for key in set(requests) | set(limits)
        if is_accelerator_resource(key)
    }
    if len(device_keys) != 1 or not device_keys.issubset(GPU_RESOURCE_TYPES):
        raise ValueError(
            "INVALID_GPU_RESOURCE: require exactly one supported whole-device key: "
            + ", ".join(sorted(GPU_RESOURCE_TYPES))
        )
    key = device_keys.pop()
    if GPU_RESOURCE_TYPES[key] != xpu:
        raise ValueError(
            f"EXPLICIT_CONFIG_CONFLICT: --xpu_type does not match {key}"
        )
    gpu = integer_quantity(requests.get(key, limits.get(key)), key)
    if key in limits and integer_quantity(limits[key], key) != gpu:
        raise ValueError(
            "INVALID_GPU_RESOURCE: device requests and limits differ"
        )
    if not 1 <= gpu <= MAX_GPUS_PER_WORKER:
        raise ValueError(
            "CAPACITY_EXCEEDED: each worker must request 1 to 8 GPUs"
        )
    quota = spec.get("resourceLimits", {}).get("gpu")
    if (
        quota is not None
        and integer_quantity(quota, "resourceLimits.gpu") < count * gpu
    ):
        raise ValueError(
            "CAPACITY_EXCEEDED: total GPUs exceed resourceLimits.gpu"
        )
    vertical = count == 1
    monitor = "on" if vertical else "off"
    if resource_monitor not in (None, monitor) or manual_scale not in (
        None,
        "off",
    ):
        raise ValueError(
            f"EXPLICIT_CONFIG_CONFLICT: auto requires resource_monitor_switch={monitor}, "
            "configmap_manual_scale_switch=off"
        )
    uid = job.get("metadata", {}).get("uid")
    if not uid:
        raise ValueError("POLICY_RECOVERY_CONFLICT: ElasticJob UID is missing")
    return {
        "schema_version": 1,
        "job_uid": uid,
        "source": "auto",
        "initial_replicas": count,
        "initial_gpu_per_worker": gpu,
        "gpu_resource_key": key,
        "gpu_resource_source": "requests" if key in requests else "limits",
        "gpu_limit": (
            integer_quantity(quota, "resourceLimits.gpu")
            if quota is not None
            else None
        ),
        "optimize_mode": (
            OptimizeMode.CLUSTER
            if vertical
            else spec.get("optimizeMode", OptimizeMode.SINGLE_JOB)
        ),
        "elastic_dimension": (
            ElasticDimension.VERTICAL
            if vertical
            else ElasticDimension.HORIZONTAL
        ),
        "resource_monitor_switch": monitor,
        "configmap_manual_scale_switch": "off",
        "reason_code": (
            "SINGLE_WORKER_VERTICAL" if vertical else "MULTI_WORKER_HORIZONTAL"
        ),
    }


def validate_resource_plan(plan, current, dimension):
    """Reject cross-dimension plans before any worker or job state is changed."""
    if not plan or plan.empty():
        return
    if (
        set(plan.node_group_resources) - {NodeType.WORKER}
        or plan.node_resources
    ):
        raise ValueError(
            "DIMENSION_MISMATCH: AllReduce plan must target worker group only"
        )
    target = plan.node_group_resources.get(NodeType.WORKER)
    if target is None or current is None:
        raise ValueError("INVALID_GPU_RESOURCE: worker group is missing")
    before, after = current.node_resource, target.node_resource
    if dimension == ElasticDimension.VERTICAL:
        if target.count != current.count:
            raise ValueError(
                "DIMENSION_MISMATCH: vertical plan changes replicas"
            )
        gpu = integer_quantity(after.gpu_num, "target GPU count")
        if not 1 <= gpu <= MAX_GPUS_PER_WORKER:
            raise ValueError(
                "CAPACITY_EXCEEDED: target GPUs must be between 1 and 8"
            )
        if after.gpu_type != before.gpu_type:
            raise ValueError(
                "INVALID_GPU_RESOURCE: vertical plan changes device type"
            )
    elif after.gpu_num not in (0, before.gpu_num) or after.gpu_type not in (
        None,
        "",
        before.gpu_type,
    ):
        raise ValueError(
            "DIMENSION_MISMATCH: horizontal plan changes GPU specification"
        )


def validate_worker_launch(config, dimension, gpu_count, replicas):
    """Validate an auto-managed launch without mutation; return allocated GPUs."""
    gpu_count = integer_quantity(gpu_count, "allocated GPU count")
    replicas = integer_quantity(replicas, "worker replicas")
    if not 1 <= gpu_count <= MAX_GPUS_PER_WORKER or replicas < 1:
        raise ValueError("Invalid auto-managed worker allocation")
    if not 1 <= config.min_nodes <= replicas <= config.max_nodes:
        raise ValueError(
            "EXPLICIT_CONFIG_CONFLICT: nnodes must contain worker replicas"
        )
    if dimension == ElasticDimension.VERTICAL:
        if (config.min_nodes, config.max_nodes, replicas) != (1, 1, 1):
            raise ValueError(
                "EXPLICIT_CONFIG_CONFLICT: auto vertical requires nnodes=1"
            )
    elif config.nproc_per_node != gpu_count:
        raise ValueError(
            "EXPLICIT_CONFIG_CONFLICT: nproc_per_node must match GPUs per worker"
        )
    return gpu_count
