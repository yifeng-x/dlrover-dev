# Copyright 2023 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import threading
import time
import os
from abc import ABCMeta, abstractmethod
from typing import Dict

from dlrover.python.common.constants import (
    DistributionStrategy,
    ElasticDimension,
    NodeEventType,
    NodeStatus,
    NodeType,
    VerticalElasticPhaseType,
    WorkerResourceScaleAction,
)
from dlrover.python.common.global_context import Context
from dlrover.python.master.elasticity_policy import (
    VERTICAL_ACTIONS,
    validate_resource_plan,
)
from dlrover.python.common.log import default_logger as logger
from dlrover.python.common.node import NodeResource, NodeGroupResource
from dlrover.python.master.monitor.perf_monitor import PerfMonitor
from dlrover.python.master.node.job_context import get_job_context
from dlrover.python.master.node.ps import ParameterServerManager
from dlrover.python.master.node.worker import WorkerManager
from dlrover.python.master.resource.job import (
    JobResource,
    JobResourceOptimizer,
)
from dlrover.python.master.resource.optimizer import ResourcePlan
from dlrover.python.master.scaler.base_scaler import ScalePlan, Scaler
from dlrover.python.common.vertical_elastic_phase import VerticalElasticPhase, PhaseMetadata, PlanIdGenerator
from dlrover.python.master.elastic_training.ucp.task_manager import (
    UcpTaskManager,
    UcpTaskStatus,
)

_dlrover_context = Context.singleton_instance()


def new_job_resource_scaler(
    job_strategy,
    job_resource: JobResource,
    job_optimizer: JobResourceOptimizer,
    perf_monitor: PerfMonitor,
    worker_manager: WorkerManager,
    node_scaler: Scaler,
):
    if job_strategy == DistributionStrategy.ALLREDUCE:
        return AllreduceTrainingResourceScaler(
            job_resource,
            job_optimizer,
            perf_monitor,
            worker_manager,
            node_scaler,
        )
    else:
        raise ValueError("No job auto scaler for %s", job_strategy)


class JobResourceScaler(metaclass=ABCMeta):
    """JobResourceScaler scale up/down nodes resource of job."""

    def __init__(
        self,
        job_resource: JobResource,
        job_optimizer: JobResourceOptimizer,
        perf_monitor: PerfMonitor,
        node_scaler: Scaler,
        scale_interval: int,
    ):
        self._job_resource = job_resource
        self._job_optimizer = job_optimizer
        self._perf_monitor = perf_monitor
        self._scaler = node_scaler
        self._scale_interval = scale_interval
        self._job_context = get_job_context()

        self._suggested_stop = False
        self._autoscaling_started = False

    def suggested_stop(self):
        return self._suggested_stop

    @abstractmethod
    def start_resource_scaling(self, scale_action: str):
        """Start vertical elastic nodes of a job"""
        pass

    @abstractmethod
    def execute_job_optimization_plan(self, plan: ResourcePlan):
        """Scale nodes of a job by a ResourcePlan"""
        if plan and not plan.empty():
            logger.info(f"Execute job optimization plan: {plan.to_json()}.")

    @abstractmethod
    def get_phase_info(self):
        """Set the status of the job to start executing the optimization plan"""
        pass

    @abstractmethod
    def set_save_ckpt_status_by_vertical_elastic(
        self,
        save_ckpt_ready: bool,
        reason: str = "",
        plan_id: str = "",
        rank: int = -1,
        step: int = 0,
        checkpoint_path: str = "",
    ):
        """set save_ckpt_status by vertical elastic"""
        pass

    @abstractmethod
    def set_param_tunning_ready(
        self,
        param_tunning_ready: bool,
        node_id: int = -1,
        plan_id: str = "",
        rank: int = -1,
    ):
        """set param tunning ready by vertical elastic"""
        pass

class AllreduceTrainingResourceScaler(JobResourceScaler):
    """Scale a Job resource with Allreduce strategy"""

    def __init__(
        self,
        job_resource: JobResource,
        job_optimizer: JobResourceOptimizer,
        perf_monitor: PerfMonitor,
        worker_manager: WorkerManager,
        node_scaler: Scaler,
    ) -> None:
        super().__init__(
            job_resource,
            job_optimizer,
            perf_monitor,
            node_scaler,
            1800,
        )
        self._worker_manager = worker_manager
        self.scale_action: str = ""
        self.last_plan_time = 0
        self.valid_opt_num = 0
        # ucp task manager
        self._ucp_task_manager = UcpTaskManager.singleton_instance()
        # vertical elastic phase
        self.vertical_elastic_phase_manager = VerticalElasticPhase()
        self._plan_id_generator = PlanIdGenerator()
        
        # 判断是否已经保存了ucp ckpt
        self._save_ckpt_status = False
        self._save_ckpt_step = 0
        self._save_ckpt_path = ""
        self._checkpoint_task_id = ""
        self._old_worker_ids = None
        self._param_tunning_ready_nodes = set()
        self._target_tunning_nodes = set()
        self._adjust_gpu_nums = 0
        self._scaling_lock = threading.Lock()

    def get_job_nodes(self, node_type=""):
        if node_type == "":
            return self._job_context.job_nodes()
        return self._job_context.job_nodes_by_type(node_type)

    def start_resource_scaling(self, scale_action: str):
        """Accept one vertical request at a time without overwriting its action."""
        if (
            _dlrover_context.runtime_elastic_dimension != ElasticDimension.VERTICAL
            or scale_action not in VERTICAL_ACTIONS
        ):
            return False
        if not self._scaling_lock.acquire(blocking=False):
            logger.info("Vertical scaling is already in progress")
            return False
        previous_enabled = _dlrover_context.enable_elastic_resource
        try:
            self.scale_action = scale_action
            _dlrover_context.enable_elastic_resource = True
            threading.Thread(
                target=self._run_resource_scaling,
                name="allreduce-worker-resource-scaler",
                daemon=True,
            ).start()
        except Exception:
            _dlrover_context.enable_elastic_resource = previous_enabled
            self._scaling_lock.release()
            raise
        return True
        
    def _run_resource_scaling(self):
        try:
            self._adjust_running_worker_resource()
        finally:
            try:
                phase = self.get_phase_info()
                if (
                    phase
                    and phase.get("phase")
                    == VerticalElasticPhaseType.PREPARING
                ):
                    self.vertical_elastic_phase_manager.fail(
                        "No executable resource change"
                    )
            finally:
                _dlrover_context.enable_elastic_resource = False
                self._scaling_lock.release()

    def _adjust_running_worker_resource(self):
        """Adjust the resource of worker."""

        opt_interval = _dlrover_context.vertical_elastic_opt_interval
        max_opt_num = _dlrover_context.vertical_elastic_max_opt_num
        ckpt_wait_time = _dlrover_context.ckpt_save_status_check_max_wait_time
        # 检查点保存状态检查间隔
        ckpt_check_interval = _dlrover_context.ckpt_save_status_check_interval
        # 每5次检查记录一次日志
        log_step = _dlrover_context.log_interval_check_nums

        try:
            self._save_ckpt_status = False
            self._save_ckpt_step = 0
            self._save_ckpt_path = ""
            self._checkpoint_task_id = ""
            self._old_worker_ids = None
            self._param_tunning_ready_nodes = set()
            self._target_tunning_nodes = set()

            plan_id = self._plan_id_generator.generate_id()
            self.vertical_elastic_phase_manager.create(
                plan_id = plan_id,
            )
            logger.info(f"Create vertical elastic phase: {plan_id},"
             f"current phase is set to {VerticalElasticPhaseType.PREPARING},"
             f"expected next phase is set to {VerticalElasticPhaseType.SAVE_CHECKPOINT}")

            # Control the interval to query plans
            current_time = time.time()
            elapsed = current_time - self.last_plan_time
        
            logger.info(f"Elapsed: {elapsed:.1f}s,"
                f"current_time: {current_time}s,"
                f"last_plan_time: {self.last_plan_time}s,"
                f"opt_interval Config: {opt_interval}s,"
                f"Diff: {opt_interval - elapsed:.1f}s"
            )
            
            if self.valid_opt_num > max_opt_num:
                logger.warning(f"Reached max optimization attempts ({max_opt_num}). Skip scaling.")
                return

            manual_scale_switch = _dlrover_context.configmap_manual_scale_switch
            if elapsed <= opt_interval:
                logger.warning(f"The time elapsed since the last execution is too short; Manual scale switch is {manual_scale_switch}.")
                if manual_scale_switch == "on":
                    logger.warning(f"Continue manual scaling.")
                else:
                    logger.warning(f"Skip scaling.")
                    return
            
            logger.info(f"vertical elastic control params:\n"
                f"  vertical elastic opt_interval         is: {opt_interval}\n"
                f"  vertical elastic max_opt_num          is: {max_opt_num}\n"
                f"  ckpt_save_status_check_max_wait_time  is: {ckpt_wait_time}\n"
                f"  ckpt_save_status_check_interval       is: {ckpt_check_interval}\n"
                f"  log_interval_check_nums               is: {log_step}\n"
            )
            
            if not self._try_execute_optimization(
                ckpt_wait_time,
                ckpt_check_interval,
                log_step,
            ):
                return
            
            # 有效执行优化次数
            self.valid_opt_num += 1
            
            logger.info(f"Optimization round {self.valid_opt_num} completed successfully.")
        except Exception as e:
            logger.error(
                f"Failed to worker resource scale for AllReduce Training: {e}"
            )
            self.vertical_elastic_phase_manager.fail("Failed to worker resource scale for AllReduce Training.")
    
    def _try_execute_optimization(self, ckpt_wait_time, ckpt_check_sec, log_step):
        """具体的优化执行逻辑：获取计划 -> 等待 CKPT -> 执行计划。返回是否成功执行。"""
        # A. 获取计划
        alive_num = self._get_alive_worker_num()
        self._job_optimizer.set_alive_node_num(alive_num)
        self._sync_optimizer_current_worker_resource(
            self._get_current_worker_group_resource(alive_num)
        )
        
        plan = self._job_optimizer.get_job_resource_plan(self.scale_action)
        if not plan or plan.empty():
            logger.warning("No valid resource plan generated.")
            return False

        self.last_plan_time = time.time()

        if not getattr(_dlrover_context, 'enable_elastic_resource', False):
            return False
        
        if _dlrover_context.auto_elasticity:
            validate_resource_plan(
                plan,
                self._job_resource.get_node_group_resource(NodeType.WORKER),
                ElasticDimension.VERTICAL,
            )
        
        self._adjust_gpu_nums = plan.node_group_resources[NodeType.WORKER].node_resource.gpu_num
        if self._adjust_gpu_nums <= 0:
            self.vertical_elastic_phase_manager.fail("Invalid target gpu number.")
            return False
        
        current_gpu_nums = self._get_current_worker_gpu_num()
        if current_gpu_nums == self._adjust_gpu_nums:
            logger.warning(
                "Target gpu number %s is equal to current worker gpu number, "
                "skip vertical scaling.",
                self._adjust_gpu_nums,
            )
            return False

        self._old_worker_ids = {w.id for w in self._get_replaceable_workers()}
        if not self._old_worker_ids:
            raise RuntimeError("No workers available for vertical replacement")

        # B. 设置垂直弹性阶段标识
        self.update_vertical_elastic_phase(
            self._adjust_gpu_nums, 
            VerticalElasticPhaseType.SAVE_CHECKPOINT, 
            VerticalElasticPhaseType.REPLACING_WORKERS
        )

        # B. 等待 Checkpoint 完成
        if not self._wait_for_ckpt_ready(ckpt_wait_time, ckpt_check_sec, log_step):
            self.vertical_elastic_phase_manager.fail("Save checkpoint not ready or timeout.")
            return False

        # C. 执行计划
        try:
            self.update_vertical_elastic_phase(
                self._adjust_gpu_nums, 
                VerticalElasticPhaseType.REPLACING_WORKERS, 
                VerticalElasticPhaseType.TUNE_PARAM
            )

            self._wait_for_replacement_exits()
            self.execute_job_optimization_plan(plan)

            if not self._wait_for_param_tunning_ready(
                ckpt_wait_time, ckpt_check_sec, log_step
            ):
                self.vertical_elastic_phase_manager.fail("Param tunning not ready or timeout.")
                return False
            
            self.update_vertical_elastic_phase(
                self._adjust_gpu_nums,
                VerticalElasticPhaseType.RENDEZVOUS_READY,
                VerticalElasticPhaseType.COMPLETED,
            )
            self.vertical_elastic_phase_manager.complete(success=True)
            return True
        except Exception as e:
            logger.error(f"Failed to execute plan: {e}", exc_info=True)
            self.vertical_elastic_phase_manager.fail("Execute plan failed.")
            return False

    def _wait_for_replacement_exits(self):
        """Let old agents finish before Pod deletion; checkpoint is already safe."""
        timeout = int(os.getenv("DLROVER_VERTICAL_DRAIN_TIMEOUT", "60"))
        deadline = time.monotonic() + timeout
        while True:
            workers = self.get_job_nodes(NodeType.WORKER)
            pending = [
                node_id for node_id in self._old_worker_ids
                if node_id in workers
                and workers[node_id].status not in (
                    NodeStatus.FAILED, NodeStatus.SUCCEEDED,
                    NodeStatus.FINISHED, NodeStatus.DELETED,
                )
                and workers[node_id].get_reported_status()
                != NodeEventType.VERTICAL_ELASTIC_EXITED
            ]
            if not pending:
                return
            if time.monotonic() >= deadline:
                logger.warning(
                    "Old workers %s did not exit in %ss; delete their Pods "
                    "using the committed recovery checkpoint.", pending, timeout,
                )
                return
            time.sleep(1)

    def _wait_for_ckpt_ready(self, max_wait, check_interval, log_step):
        """带超时和停止标志检查的轮询。"""
        start_time = time.time()
        count = 0
        
        while not self._save_ckpt_status:
            if not _dlrover_context.enable_elastic_resource:
                logger.warning(f"Enable elastic resource is {_dlrover_context.enable_elastic_resource}, Resource Scaling stopped while waiting for CKPT.")
                return False
                
            if (time.time() - start_time) > max_wait:
                logger.warning("Waiting for CKPT save timed out.")
                return False

            if (
                self.get_phase_info().get("phase")
                == VerticalElasticPhaseType.FAILED
            ):
                return False
            task = self._ucp_task_manager.get_task_by_id(
                self._checkpoint_task_id
            )
            if task and task.status in (
                UcpTaskStatus.FAILED, UcpTaskStatus.CANCELED
            ):
                logger.error(
                    "Vertical UCP task %s failed: %s",
                    task.task_id, task.error_message,
                )
                return False
            if task and task.status == UcpTaskStatus.SUCCEEDED:
                logger.info("UCP CKPT task %s succeeded.", task.task_id)
                self._save_ckpt_status = self._ucp_task_manager.select_resume_checkpoint(
                    task.task_id
                )
            
            count += 1
            if count % log_step == 0:
                logger.info(f"Waiting UCP task exec succeeded for CKPT... ({time.time() - start_time:.1f}s)")
            
            time.sleep(check_interval)
        
        if (
            self.get_phase_info().get("phase")
            == VerticalElasticPhaseType.FAILED
        ):
            return False
        logger.info("CKPT save succeeded. Proceeding with optimization.")
        return True

    def _wait_for_param_tunning_ready(self, max_wait, check_interval, log_step):
        if not self._target_tunning_nodes:
            logger.warning("No target workers for param tunning.")
            return False
        start_time = time.time()
        count = 0
        while not self._target_tunning_nodes.issubset(
            self._param_tunning_ready_nodes
        ):
            if not _dlrover_context.enable_elastic_resource:
                logger.warning(
                    "Resource scaling stopped while waiting for param tunning."
                )
                return False
            if (time.time() - start_time) > max_wait:
                logger.warning(
                    "Waiting for param tunning timed out, ready=%s, target=%s.",
                    sorted(self._param_tunning_ready_nodes),
                    sorted(self._target_tunning_nodes),
                )
                return False
            count += 1
            if count % log_step == 0:
                logger.info(
                    f"Waiting for param tunning... ({time.time() - start_time:.1f}s)," 
                    f"ready={sorted(self._param_tunning_ready_nodes)} "
                    f"target={sorted(self._target_tunning_nodes)}."
                )
            time.sleep(check_interval)
        logger.info("Param tunning completed on all replacement workers.")
        return True

    def _get_alive_worker_num(self):
        worker_num = 0
        workers = self.get_job_nodes(NodeType.WORKER)
        for _, worker in workers.items():
            if worker.status in [
                NodeStatus.RUNNING,
                NodeStatus.PENDING,
                NodeStatus.INITIAL,
                NodeStatus.SUCCEEDED,
            ]:
                worker_num += 1
        return worker_num

    def _get_current_worker_gpu_num(self):
        workers = self.get_job_nodes(NodeType.WORKER)
        for _, worker in workers.items():
            if worker.status in [
                NodeStatus.RUNNING,
                NodeStatus.PENDING,
                NodeStatus.INITIAL,
            ]:
                return worker.config_resource.gpu_num
        group = self._job_resource.get_node_group_resource(NodeType.WORKER)
        if group:
            return group.node_resource.gpu_num
        return 0

    def execute_job_optimization_plan(self, plan: ResourcePlan):
        """Execute the optimization plan of the training job.
        The plan may adjust the number of PS and workers or
        adjust the cpu and memory of nodes.
        """
        if _dlrover_context.runtime_elastic_dimension != ElasticDimension.VERTICAL:
            raise ValueError(
                "DIMENSION_MISMATCH: vertical controller is disabled"
            )
        if _dlrover_context.auto_elasticity:
            validate_resource_plan(
                plan,
                self._job_resource.get_node_group_resource(NodeType.WORKER),
                ElasticDimension.VERTICAL,
            )
        
        super().execute_job_optimization_plan(plan)

        scale_plan = ScalePlan()
        if not plan or plan.empty():
            return scale_plan
        for node_type, group in plan.node_group_resources.items():
            if node_type != NodeType.WORKER:
                continue
            if group.count > 0:
                self._job_resource.update_node_group_resource(
                    node_type,
                    group.count,
                    group.node_resource.cpu,
                    group.node_resource.memory,
                    group.node_resource.gpu_type,
                    group.node_resource.gpu_num,
                )
                group = self._job_resource.get_node_group_resource(node_type)
                self._perf_monitor.set_target_worker_num(group.count)
                if _dlrover_context.enable_elastic_resource:
                    worker_plan = self.get_worker_plan_with_vertical_elastic(group)
                else:
                    worker_plan = self.get_worker_plan_with_horizontal_elastic(group)
                scale_plan.merge(worker_plan)

        self._scaler.scale(scale_plan)
        worker_group = self._job_resource.get_node_group_resource(NodeType.WORKER)
        self._sync_optimizer_current_worker_resource(worker_group)
        return scale_plan
    
    def get_phase_info(self):
        return self.vertical_elastic_phase_manager.get()
    
    def set_save_ckpt_status_by_vertical_elastic(
        self,
        save_ckpt_ready: bool,
        reason: str = "",
        plan_id: str = "",
        rank: int = -1,
        step: int = 0,
        checkpoint_path: str = "",
    ):
        phase_info = self.vertical_elastic_phase_manager.get()
        if not plan_id or plan_id != phase_info.get("plan_id"):
            logger.warning(
                "Ignore checkpoint status from stale plan %s, current plan %s.",
                plan_id,
                phase_info.get("plan_id"),
            )
            return
        if phase_info.get("phase") != VerticalElasticPhaseType.SAVE_CHECKPOINT:
            logger.warning(
                "Ignore checkpoint status in phase %s.",
                phase_info.get("phase"),
            )
            return
        if not save_ckpt_ready:
            logger.warning(reason)
            self.vertical_elastic_phase_manager.fail(
                reason or "Save checkpoint failed."
            )
            return
            
        if checkpoint_path:
            task = self._ucp_task_manager.get_checkpoint_task(
                step, checkpoint_path
            )
            if not task:
                self.vertical_elastic_phase_manager.fail(
                    "Checkpoint has no UCP task"
                )
                return
            if (
                self._checkpoint_task_id
                and self._checkpoint_task_id != task.task_id
            ):
                self.vertical_elastic_phase_manager.fail(
                    "Conflicting checkpoint for plan"
                )
                return
            self._save_ckpt_step = step
            self._save_ckpt_path = checkpoint_path
            self._checkpoint_task_id = task.task_id
        else:
            self._save_ckpt_status = True

    def set_param_tunning_ready(
        self,
        param_tunning_ready: bool,
        node_id: int = -1,
        plan_id: str = "",
        rank: int = -1,
    ):
        phase_info = self.vertical_elastic_phase_manager.get()
        if not plan_id or plan_id != phase_info.get("plan_id"):
            logger.warning(
                "Ignore param tunning status from stale plan %s, current %s.",
                plan_id,
                phase_info.get("plan_id"),
            )
            return
        if phase_info.get("phase") not in [
            VerticalElasticPhaseType.REPLACING_WORKERS,
            VerticalElasticPhaseType.TUNE_PARAM,
            VerticalElasticPhaseType.RENDEZVOUS_READY,
        ]:
            logger.warning(
                "Ignore param tunning status in phase %s.",
                phase_info.get("phase"),
            )
            return
        if param_tunning_ready:
            self._param_tunning_ready_nodes.add(node_id)
        else:
            self._param_tunning_ready_nodes.discard(node_id)
        logger.info(
            "Param tunning ready nodes: %s/%s, rank=%s.",
            sorted(self._param_tunning_ready_nodes),
            sorted(self._target_tunning_nodes),
            rank,
        )

    def get_worker_plan_with_vertical_elastic(self, worker_resource: NodeGroupResource):
        logger.info("[AllReduce] start to adjust worker resource by vertical elastic")

        worker_plan = ScalePlan()
        self.update_vertical_elastic_phase(
            self._adjust_gpu_nums,
            VerticalElasticPhaseType.TUNE_PARAM,
            VerticalElasticPhaseType.RENDEZVOUS_READY,
        )
        if self._old_worker_ids is None:
            self._old_worker_ids = {w.id for w in self._get_replaceable_workers()}
        target_resource = copy.deepcopy(worker_resource)
        target_resource.count = len(self._old_worker_ids)
        if not target_resource.count:
            raise RuntimeError("No workers selected for vertical replacement")
        worker_plan = self._worker_manager.replace_workers(
            self._old_worker_ids, target_resource
        )
        self._target_tunning_nodes = {
            node.id for node in worker_plan.launch_nodes
        }

        logger.info("[AllReduce] after adjust worker, worker plan is: %s", worker_plan.to_json())
        return worker_plan

    def _sync_optimizer_current_worker_resource(
        self, worker_resource: NodeGroupResource
    ):
        sync_func = getattr(
            self._job_optimizer, "update_current_worker_resource", None
        )
        if worker_resource and callable(sync_func):
            sync_func(worker_resource)
    
    def _get_current_worker_group_resource(self, alive_num=None):
        configured_group = self._job_resource.get_node_group_resource(
            NodeType.WORKER
        )
        group_count = configured_group.count if configured_group else 0
        for worker in self._get_replaceable_workers():
            if worker.config_resource:
                return NodeGroupResource(
                    group_count or alive_num or 0,
                    copy.deepcopy(worker.config_resource),
                )
        return copy.deepcopy(configured_group) if configured_group else None

    def _get_replaceable_workers(self):
        workers = []
        for worker in self.get_job_nodes(NodeType.WORKER).values():
            if not worker.is_released and worker.status in [
                NodeStatus.RUNNING,
                NodeStatus.PENDING,
                NodeStatus.INITIAL,
            ]:
                workers.append(worker)
        return sorted(workers, key=lambda worker: worker.rank_index)

    def get_worker_plan_with_horizontal_elastic(self, worker_resource: NodeGroupResource):
        return self._worker_manager.adjust_worker(worker_resource)

    def update_vertical_elastic_phase(self, adjust_gpus: int, current_phase: str, expected_next_phase: str):
        update_data = self.vertical_elastic_phase_manager.build_phase_update(
            adjust_gpus=adjust_gpus,
            phase=current_phase,
            current_state=current_phase,
            expected_next_phase=expected_next_phase,
        )
        self.vertical_elastic_phase_manager.update(update_data)
        logger.info(f"Update vertical elastic phase,"
             f"current phase is set to {current_phase},"
             f"expected next phase is set to {expected_next_phase}")