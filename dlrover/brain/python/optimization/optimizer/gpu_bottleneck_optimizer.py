import time

from dlrover.brain.python.common.job import (
    JobOptimizePlan,
    JobMeta,
    JobResource,
    NodeGroupResource,
    OptimizeConfig,
    NodeResource,
)
from dlrover.brain.python.common.constants import (
    Node,
)
from dlrover.brain.python.optimization.optimizer_utils import (
    safe_int,
    safe_parse_resource,
    fast_parse_memory,
)
from dlrover.python.common.constants import WorkerResourceScaleAction
from dlrover.brain.python.platform.k8s.configmap import ConfigMapReader
from dlrover.brain.python.platform.k8s.monitor import ResourceMonitor
from dlrover.python.common.log import default_logger as logger

WORKER_GPU_SCALE_ACTIONS = {
    WorkerResourceScaleAction.ADD_GPUS,
    WorkerResourceScaleAction.REDUCE_GPUS
}

JSON_KEY = "config.json"
DEFAULT_INCREMENT_FACTOR = 1
DEFAULT_REDUCTION_FACTOR = 1
MIN_GPU_LIMIT = 1  # 防止缩容到 0

class GpuBottleneckOptimizer:
    def __init__(self):
        pass

    @staticmethod
    def get_name() -> str:
        return "GpuBottleneckOptimizer"

    def optimize(self, job: JobMeta, conf: OptimizeConfig) -> JobOptimizePlan:
        logger.info(f"GPU bottleneck optimizer started for job {job.name}/{job.uuid}, config: {conf}")

        original_worker_replicas = safe_int(conf.customized_config.get("original_worker_replicas"), 1)
        current_node_resource = self.generate_node_resource(job, conf)
        return JobOptimizePlan(
            timestamp=int(time.time() * 1000),
            job_resource=JobResource(
                node_group_resources={
                    Node.NODE_TYPE_WORKER: NodeGroupResource(
                        count=original_worker_replicas,
                        resource=current_node_resource,
                    )
                },
            ),
            job_meta=job,
        )

    def generate_node_resource(self, job: JobMeta, conf: OptimizeConfig) -> NodeResource:
        
        # 1. Pre check original worker resource
        customized_config = conf.customized_config if conf.customized_config else {}
        original_worker_resource = safe_parse_resource(customized_config.get("original_worker_resource"))

        if not original_worker_resource or not isinstance(original_worker_resource, dict):
            logger.warning("Original worker resource is missing or invalid. Returning default empty resource.")
            return NodeResource()

        logger.info(f"original_worker_resource: {original_worker_resource}")

        # 2. Pre check Scale Action
        scale_action = customized_config.get("scale_action")
        if scale_action not in WORKER_GPU_SCALE_ACTIONS:
            logger.info(f"Scale action '{scale_action}' is not a GPU scaling action. Returning default empty resource.")
            return NodeResource()

        # 3. 解析基础资源
        try:
            ori_worker_cpu = safe_int(original_worker_resource.get("cpu"), 1)
            original_memory_mi = original_worker_resource.get("memory")
            ori_worker_memory_bytes = fast_parse_memory(f"{original_memory_mi}Mi", 1)
            
            # 提取 GPU 信息
            ori_worker_gpu_nums = safe_int(original_worker_resource.get("gpu_num"), 0)
            gpu_type = original_worker_resource.get("gpu_type")
            
            if not gpu_type:
                logger.error("GPU type missing. Aborting.")
                return NodeResource()
                
        except Exception as e:
            logger.exception(f"Resource parsing failed: {e}")
            return NodeResource()

        # 4. 仅在需要决策时获取空闲 GPU 数量
        # 如果只是减少 GPU 且当前已经是 1，或者动作无效，不需要查集群空闲资源
        needs_cluster_check = True
        
        if scale_action == WorkerResourceScaleAction.REDUCE_GPUS and ori_worker_gpu_nums <= MIN_GPU_LIMIT:
            needs_cluster_check = False # 无法再减，无需查集群
        
        cluster_idle_gpus = None
        if needs_cluster_check:
            try:
                cluster_idle_gpus = ResourceMonitor(job.namespace, gpu_type).get_cluster_idle_gpus()
            except Exception as e:
                logger.warning(f"Cluster idle gpus check failed: {e}. Fallback to current.")
                return NodeResource()

            if not cluster_idle_gpus and scale_action == WorkerResourceScaleAction.ADD_GPUS:
                logger.warning("No idle GPUs. Scale up aborted.")
                return NodeResource()

        # 5. 获取优化配置
        opt_config = ConfigMapReader(job.namespace, "brain-opt-config").get_opt_config()

        # 6. 计算逻辑
        opt_gpu_nums = ori_worker_gpu_nums

        if scale_action == WorkerResourceScaleAction.ADD_GPUS:
            increment_factor = safe_int(opt_config.get("incremental_gpus_factor"), DEFAULT_INCREMENT_FACTOR)
            opt_gpu_nums = ori_worker_gpu_nums + increment_factor
            logger.info(f"Scale UP: {ori_worker_gpu_nums} -> {opt_gpu_nums}")

        elif scale_action == WorkerResourceScaleAction.REDUCE_GPUS:
            if ori_worker_gpu_nums > MIN_GPU_LIMIT:
                reduction_factor = safe_int(opt_config.get("reduction_gpus_factor"), DEFAULT_REDUCTION_FACTOR)
                opt_gpu_nums = max(MIN_GPU_LIMIT, ori_worker_gpu_nums - reduction_factor)
                logger.info(f"Scale DOWN: {ori_worker_gpu_nums} -> {opt_gpu_nums}")
            else:
                logger.debug(f"Already at min GPU limit ({MIN_GPU_LIMIT}).")

        return NodeResource(
            cpu=ori_worker_cpu,
            memory=ori_worker_memory_bytes,
            gpu=opt_gpu_nums,
            gpu_type=gpu_type,
        )
