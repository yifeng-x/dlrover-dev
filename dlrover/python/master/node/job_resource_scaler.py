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
from dlrover.python.master.monitor.vertical_metrics import VerticalExecutionMetrics
from dlrover.python.master.monitor.vertical_events import VerticalElasticEvents, resource_snapshot
import uuid
from dlrover.python.master.resource.benefit_log import BenefitLogger
from dlrover.python.master.resource.benefit import BenefitGuard
from dlrover.python.master.resource.exploration import Exploration, unpack
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
    vertical_metrics=None,
    benefit_job=None,
    benefit_generation_provider=None,
    benefit_context_provider=None,
    benefit_task_id=None,
):
    if job_strategy == DistributionStrategy.ALLREDUCE:
        return AllreduceTrainingResourceScaler(
            job_resource,
            job_optimizer,
            perf_monitor,
            worker_manager,
            node_scaler,
            vertical_metrics=vertical_metrics,
            benefit_guard=BenefitGuard(
                job=benefit_job, generation_provider=benefit_generation_provider,
                platform_task_id=benefit_task_id,
                training_context_provider=benefit_context_provider,
            ),
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
        vertical_metrics=None,
        benefit_guard=None,
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
        self.vertical_metrics = vertical_metrics or VerticalExecutionMetrics()
        self._benefit_log = BenefitLogger("execution")
        self.benefit_guard = benefit_guard or BenefitGuard()
        self.benefit_guard.recovery_provider = self.vertical_metrics.training_observation_status
        self.exploration = Exploration(self.benefit_guard)
        self._benefit_stop = threading.Event()
        self.events = VerticalElasticEvents(
            job=self.benefit_guard.job,
            task_id=getattr(self.benefit_guard.training_metrics, "task_id", None),
            observation_provider=self._event_resource_observation,
        )
        self.vertical_metrics.events = self.events
        self._event_request_id = None
        self._event_trigger_source = "unknown"

    def _event_generation(self):
        try:
            provider = self.benefit_guard.generation_provider
            return provider() if provider else None
        except Exception:
            return None

    def _event_resource_observation(self, target_ids):
        nodes = self.get_job_nodes(NodeType.WORKER)
        # No inferred resource allocation from the target plan.
        selected = [nodes.get(node_id) for node_id in target_ids]
        if not selected or any(node is None or node.is_released or
                               node.status != NodeStatus.RUNNING for node in selected):
            return {"status": "unknown_or_partial", "resources": None,
                    "generation": self._event_generation()}
        snapshots = [resource_snapshot(NodeGroupResource(1, node.config_resource))
                     for node in selected]
        if any(snapshot != snapshots[0] for snapshot in snapshots):
            return {"status": "mixed", "resources": None,
                    "generation": self._event_generation()}
        resource = snapshots[0]
        resource.update(workers=len(selected), total_gpus=len(selected) * resource["gpus_per_worker"])
        return {"status": "observed", "resources": resource,
                "generation": self._event_generation(), "basis": "dlrover_worker_state"}


    def get_job_nodes(self, node_type=""):
        if node_type == "":
            return self._job_context.job_nodes()
        return self._job_context.job_nodes_by_type(node_type)

    def start_resource_scaling(self, scale_action: str, trigger_source="unknown"):
        """Accept one vertical request at a time without overwriting its action."""
        request_id = uuid.uuid4().hex
        def report(result, reason="none"):
            self.vertical_metrics.request(scale_action, result, reason,
                                          event_request_id=request_id,
                                          trigger_source=trigger_source)
        if (
            _dlrover_context.runtime_elastic_dimension != ElasticDimension.VERTICAL
            or scale_action not in VERTICAL_ACTIONS
        ):
            reason = (
                "invalid_action" if scale_action not in VERTICAL_ACTIONS
                else "dimension_mismatch"
            )
            report("rejected", reason)
            return False
        
        if not self._scaling_lock.acquire(blocking=False):
            report("rejected", "busy")
            logger.info("Vertical scaling is already in progress")
            return False

        if not self.exploration.enter():
            self.exploration.leave()
            self._scaling_lock.release()
            report("rejected", "exploration_fenced")
            return False

        self.vertical_metrics.inflight.set(1)
        self._event_request_id = request_id
        self._event_trigger_source = trigger_source
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
            self.vertical_metrics.inflight.set(0)
            report("error", "internal_error")
            self.exploration.leave()
            self._scaling_lock.release()
            raise

        report("accepted")
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
                self.vertical_metrics.finish("failed", "internal_error")
                self.vertical_metrics.inflight.set(0)
                _dlrover_context.enable_elastic_resource = False
                try:
                    if self.exploration.active and not self._benefit_stop.is_set():
                        self.exploration.finish("FAILED", "interrupted_execution")
                    state = self.exploration.state
                    if state and state["status"] in ("FAILED", "INCONCLUSIVE"):
                        self.events.exploration_decided(state, state["status"])
                finally:
                    self.exploration.leave()
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
            plan_id += "-" + uuid.uuid4().hex

            self.vertical_elastic_phase_manager.create(
                plan_id = plan_id,
            )
            self.vertical_metrics.start(self.scale_action)
            self.events.start(
                plan_id, 
                self.scale_action, 
                self._event_request_id,
                self._event_trigger_source
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
                self.vertical_metrics.finish("skipped", "max_attempts")
                logger.warning(f"Reached max optimization attempts ({max_opt_num}). Skip scaling.")
                return

            manual_scale_switch = _dlrover_context.configmap_manual_scale_switch
            if elapsed <= opt_interval:
                logger.warning(f"The time elapsed since the last execution is too short; Manual scale switch is {manual_scale_switch}.")
                if manual_scale_switch == "on":
                    logger.warning(f"Continue manual scaling.")
                else:
                    self.vertical_metrics.finish("skipped", "cooldown")
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
            self._fail_vertical_plan(
                "internal_error",
                "Failed to worker resource scale for AllReduce Training.",
            )
    
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
            self.vertical_metrics.finish("skipped", "empty_plan")
            logger.warning("No valid resource plan generated.")
            return False

        self.last_plan_time = time.time()

        if not getattr(_dlrover_context, 'enable_elastic_resource', False):
            self.vertical_metrics.finish("cancelled", "disabled")
            return False
        
        if _dlrover_context.auto_elasticity:
            try:
                validate_resource_plan(
                    plan,
                    self._job_resource.get_node_group_resource(NodeType.WORKER),
                    ElasticDimension.VERTICAL,
                )
            except ValueError as error:
                self._fail_vertical_plan("invalid_plan", str(error))
                raise
        
        self._adjust_gpu_nums = plan.node_group_resources[NodeType.WORKER].node_resource.gpu_num
        if self._adjust_gpu_nums <= 0:
            self._fail_vertical_plan("invalid_plan", "Invalid target gpu number.")
            return False
        
        current_gpu_nums = self._get_current_worker_gpu_num()
        self.vertical_metrics.gpus.labels("before").set(current_gpu_nums)
        self.vertical_metrics.gpus.labels("target").set(self._adjust_gpu_nums)

        self.events.resources(
            self._get_current_worker_group_resource(alive_num),
            plan.node_group_resources[NodeType.WORKER], 
            self._event_generation(),
        )
        
        if current_gpu_nums == self._adjust_gpu_nums:
            self.vertical_metrics.finish("skipped", "same_resource")
            logger.warning(
                "Target gpu number %s is equal to current worker gpu number, "
                "skip vertical scaling.",
                self._adjust_gpu_nums,
            )
            return False

        self._old_worker_ids = {w.id for w in self._get_replaceable_workers()}
        if not self._old_worker_ids:
            raise RuntimeError("No workers available for vertical replacement")

        if not self._check_resource_benefit(plan, alive_num):
            return False

        if not self._execute_resource_plan(plan, ckpt_wait_time, ckpt_check_sec, log_step):
            if self.exploration.active:
                self.exploration.finish("FAILED", "switch_failed")
            return False
        
        if self.exploration.active:
            return self._observe_exploration()
        return True

    def _execute_resource_plan(self, plan, ckpt_wait_time, ckpt_check_sec, log_step,
                               rollback=False):
        self._benefit_log.emit(
            "execution_started",
            plan_id=self.vertical_elastic_phase_manager.get_plan_id(),
            rollback=rollback,
            target_gpus=plan.node_group_resources[NodeType.WORKER].node_resource.gpu_num
        )
        # Reuse the existing checkpoint/replacement protocol in both directions.
        self._adjust_gpu_nums = plan.node_group_resources[NodeType.WORKER].node_resource.gpu_num
        self._save_ckpt_status = False
        self._save_ckpt_step = 0
        self._save_ckpt_path = ""
        self._param_tunning_ready_nodes = set()
        self._target_tunning_nodes = set()
        if self.exploration.active:
            ckpt_wait_time = min(ckpt_wait_time, max(0, self.exploration.state["expires_at"] - self.exploration.clock()))

        # B. 设置垂直弹性阶段标识
        self.update_vertical_elastic_phase(
            self._adjust_gpu_nums, 
            VerticalElasticPhaseType.SAVE_CHECKPOINT, 
            VerticalElasticPhaseType.REPLACING_WORKERS
        )

        # B. 等待 Checkpoint 完成
        if not self._wait_for_ckpt_ready(ckpt_wait_time, ckpt_check_sec, log_step):
            self._fail_vertical_plan(
                "checkpoint_timeout", "Save checkpoint not ready or timeout."
            )
            return False

        self._benefit_log.emit(
            "checkpoint_ready",
            plan_id=self.vertical_elastic_phase_manager.get_plan_id(),
            checkpoint_step=self._save_ckpt_step
        )
        # C. 执行计划
        try:
            if self.exploration.active:
                if self._benefit_stop.is_set() or self.exploration.clock() >= self.exploration.state["expires_at"]:
                    return False
                self.exploration.switching(self._save_ckpt_step, self._save_ckpt_path, rollback)
                if (not self.exploration.authorize_mutation(rollback)
                        or self._benefit_stop.is_set()
                        or self.exploration.clock() >= self.exploration.state["expires_at"]):
                    self._fail_vertical_plan("reservation_lost", "Exploration reservation not confirmed")
                    return False
            
            self.update_vertical_elastic_phase(
                self._adjust_gpu_nums, 
                VerticalElasticPhaseType.REPLACING_WORKERS, 
                VerticalElasticPhaseType.TUNE_PARAM
            )

            self._wait_for_replacement_exits()
            self.execute_job_optimization_plan(plan)
            self._benefit_log.emit(
                "replacement_submitted",
                plan_id=self.vertical_elastic_phase_manager.get_plan_id(),
                target_workers=sorted(self._target_tunning_nodes)
            )

            if self.exploration.active:
                ckpt_wait_time = min(ckpt_wait_time, max(0, self.exploration.state["expires_at"] - self.exploration.clock()))

            if not self._wait_for_param_tunning_ready(
                ckpt_wait_time, ckpt_check_sec, log_step
            ):
                self._fail_vertical_plan(
                    "param_timeout", "Param tunning not ready or timeout."
                )
                return False
            
            self.update_vertical_elastic_phase(
                self._adjust_gpu_nums,
                VerticalElasticPhaseType.RENDEZVOUS_READY,
                VerticalElasticPhaseType.COMPLETED,
            )
            self.vertical_elastic_phase_manager.complete(success=True)
            
            self.events.checkpoint(
                self._save_ckpt_step, 
                self._save_ckpt_path,
                self._target_tunning_nodes
            )
            
            self.vertical_metrics.finish("completed")
            self.vertical_metrics.await_recovery(
                self._target_tunning_nodes, self._save_ckpt_step, ckpt_wait_time
            )
            if self.exploration.active:
                self.exploration.observing(rollback)
            return True
        except Exception as e:
            logger.error(f"Failed to execute plan: {e}", exc_info=True)
            self._fail_vertical_plan("scale_failed", "Execute plan failed.")
            return False

    def _observe_exploration(self):
        trial = self.exploration
        while not self._benefit_stop.is_set():
            action = trial.inspect()
            if self._benefit_stop.is_set():
                return False
            if action == "rollback":
                self.events.exploration_decided(trial.state, "ROLLBACK_REQUESTED")
                self.vertical_metrics.stop_recovery()
                self.vertical_elastic_phase_manager.create(trial.state["rollback_plan_id"])
                self.vertical_metrics.start(
                    "reduce_gpus" if trial.state["original"]["gpu_num"] < trial.state["target"]["gpu_num"] else "add_gpus"
                )
                
                self.events.start(
                    trial.state["rollback_plan_id"], 
                    self.vertical_metrics._action,
                    trigger_source="exploration_rollback", 
                    root_plan_id=trial.state["id"],
                    parent_plan_id=trial.state["id"], 
                    exploring=True,
                )
                self.events.resources(
                    unpack(trial.state["target"]),
                    unpack(trial.state["original"]), 
                    self._event_generation()
                )
                
                plan = ResourcePlan()
                plan.node_group_resources[NodeType.WORKER] = unpack(trial.state["original"])
                # Fresh checkpoint of the CURRENT training state, never the old trial checkpoint.
                if not self._execute_resource_plan(
                    plan, trial.state["rollback_seconds"],
                    getattr(_dlrover_context, "ckpt_save_status_check_interval", 1),
                    getattr(_dlrover_context, "log_interval_check_nums", 5), rollback=True,
                ):
                    trial.finish("FAILED", "rollback_failed")
                    return False
            elif action != "wait":
                self.events.exploration_decided(trial.state, action)
                self.vertical_metrics.explorations.labels(action.lower()).inc()
                if action in ("ACCEPTED", "RETAINED", "ROLLED_BACK"):
                    current = unpack(trial.state["original"] if action == "ROLLED_BACK" else trial.state["target"])
                    self._job_resource.node_group_resources[NodeType.WORKER] = current
                    self._sync_optimizer_current_worker_resource(current)
                    trial.release_reservation()
                    return True
                return False
            remaining = max(0, trial.state["expires_at"] - trial.clock())
            self._benefit_stop.wait(min(trial.poll, remaining))
        # Keep durable observation state for another Master; no further mutation.
        return False

    def resume_exploration(self):
        """Resume observation only. Interrupted Kubernetes mutations stay fenced."""
        if self.exploration.store is None or not self._scaling_lock.acquire(False):
            return
        if not self.exploration.enter(recovering=True):
            self.exploration.leave()
            self._scaling_lock.release()
            return
        self.vertical_metrics.inflight.set(1)
        _dlrover_context.enable_elastic_resource = True

        state = self.exploration.state
        rolling_back = state["status"] == "VERIFYING_ROLLBACK"
        self.events.start(
            state.get("rollback_plan_id", 
            state["id"]), 
            "unknown",
            trigger_source="master_recovery", 
            root_plan_id=state["id"],
            parent_plan_id=state["id"] if rolling_back else None, 
            resumed=True, 
            exploring=True,
        )
        self.events.resources(
            unpack(state["target"] if rolling_back else state["original"]),
            unpack(state["original"] if rolling_back else state["target"])
        )
        self.events.checkpoint(
            state.get("checkpoint_step"), 
            state.get("checkpoint_path"), 
            []
        )
        
        def observe():
            try:
                self._observe_exploration()
            except Exception:
                logger.exception("Exploration recovery failed; journal remains fenced")
                self.events.phase("observation_interrupted")
            finally:
                state = self.exploration.state
                if state and state["status"] in ("FAILED", "INCONCLUSIVE"):
                    self.events.exploration_decided(state, state["status"])
                     
                self.vertical_metrics.inflight.set(0)
                _dlrover_context.enable_elastic_resource = False
                self.exploration.leave()
                self._scaling_lock.release()
        try:
            threading.Thread(target=observe, name="vertical-exploration-recovery", daemon=True).start()
        except Exception:
            self.vertical_metrics.inflight.set(0)
            _dlrover_context.enable_elastic_resource = False
            self.exploration.leave()
            self._scaling_lock.release()
            raise

    def stop(self):
        self._benefit_stop.set()
        self.events.phase("suspended" if self.exploration.active else "stopping")

    def _check_resource_benefit(self, plan, alive_num):
        guard = self.benefit_guard
        if guard.mode == "off":
            self._benefit_log.emit(
                "gate_decision",
                plan_id=self.vertical_elastic_phase_manager.get_plan_id(),
                mode="off", 
                decision="allow", 
                reason="disabled"
            )
            self.events.evaluated(guard, True, False)
            return True
        allowed = guard.check(
            self._get_current_worker_group_resource(alive_num),
            plan.node_group_resources[NodeType.WORKER],
            self.vertical_elastic_phase_manager.get_plan_id(),
        )
        current = self._get_current_worker_group_resource(alive_num)
        workers = self._get_replaceable_workers() if self.exploration.enabled else []
        healthy = current is not None and len(workers) == current.count and all(
            worker.status == NodeStatus.RUNNING and not worker.is_released
            and worker.config_resource.gpu_num == current.node_resource.gpu_num
            for worker in workers
        )
        if not allowed and healthy:
            allowed = self.exploration.prepare(
                current,
                plan.node_group_resources[NodeType.WORKER],
                self.vertical_elastic_phase_manager.get_plan_id(),
            )
        self.events.evaluated(
            guard, 
            allowed, 
            self.exploration.active
        )
        result = guard.last_result
        self._benefit_log.emit(
            "gate_decision", 
            plan_id=self.vertical_elastic_phase_manager.get_plan_id(),
            request_id=(guard.last_request or {}).get("request_id"), 
            mode=guard.mode,
            status=result.status, 
            reason=result.reason,
            decision="explore" if self.exploration.active else "allow" if allowed else "reject",
            exploration_enabled=self.exploration.enabled, 
            workers_healthy=healthy,
        )
        self.vertical_metrics.benefit_evaluations.labels(
            guard.mode if guard.valid else "invalid", result.status, result.reason
        ).inc()
        logger.info("Vertical benefit plan=%s mode=%s result=%s",
                    self.vertical_elastic_phase_manager.get_plan_id(),
                    guard.mode, result.to_dict())
        if not allowed:
            self.vertical_metrics.finish("skipped", "benefit_" + result.status)
            self.vertical_elastic_phase_manager.fail(
                "Benefit gate: " + result.reason
            )
        return allowed

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
            if self._benefit_stop.is_set():
                return False
            
            if (
                self.get_phase_info().get("phase")
                == VerticalElasticPhaseType.FAILED
            ):
                self.vertical_metrics.finish("skip check ckpt ready", "Vertical elastic phase failed.")
                return False
            
            if not _dlrover_context.enable_elastic_resource:
                self.vertical_metrics.finish("skip check ckpt ready", "enable_elastic_resource switch disabled")
                logger.warning(f"Enable elastic resource is {_dlrover_context.enable_elastic_resource}, Resource Scaling stopped while waiting for CKPT.")
                return False
                
            if (time.time() - start_time) > max_wait:
                reason = f"Waiting for ckpt save timed out, max wait is {max_wait}s."
                self.vertical_metrics.finish("skip check ckpt ready", reason)
                logger.warning(reason)
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
                self.vertical_metrics.finish("skip check ckpt ready", f"UCP task {task.task_id} failed: {task.error_message}")
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
        
        logger.info("CKPT save succeeded. Proceeding with optimization.")
        return True

    def _wait_for_param_tunning_ready(self, max_wait, check_interval, log_step):
        if not self._target_tunning_nodes:
            self.vertical_metrics.finish("skip param tunning", "no target workers for param tunning.")
            logger.warning("No target workers for param tunning.")
            return False
        start_time = time.time()
        count = 0
        while not self._target_tunning_nodes.issubset(
            self._param_tunning_ready_nodes
        ):
            if self._benefit_stop.is_set():
                return False
            
            if not _dlrover_context.enable_elastic_resource:
                self.vertical_metrics.finish("skip param tunning", "enable_elastic_resource switch disabled")
                logger.warning(
                    "Resource scaling stopped while waiting for param tunning."
                )
                return False
            
            if (time.time() - start_time) > max_wait:
                self.vertical_metrics.finish("skip param tunning", f"Waiting for param tunning timed out, max wait is {max_wait}s.")
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

    def _fail_vertical_plan(self, reason, message):
        self.vertical_metrics.finish("failed", reason)
        if (
            self.vertical_elastic_phase_manager.get_phase()
            != VerticalElasticPhaseType.FAILED
        ):
            self.vertical_elastic_phase_manager.fail(message)
    
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
        if ((self.exploration.active and plan_id != phase_info.get("plan_id"))
                or (plan_id and plan_id != phase_info.get("plan_id"))):
            self.vertical_metrics.reports.labels("checkpoint", "stale_plan").inc()
            logger.warning(
                "Ignore checkpoint status from stale plan %s, current plan %s.",
                plan_id,
                phase_info.get("plan_id"),
            )
            return
        
        if phase_info.get("phase") != VerticalElasticPhaseType.SAVE_CHECKPOINT:
            self.vertical_metrics.reports.labels(
                "checkpoint", "phase_mismatch"
            ).inc()
            logger.warning(
                "Ignore checkpoint status in phase %s.",
                phase_info.get("phase"),
            )
            return
        
        if not save_ckpt_ready:
            logger.warning(reason)
            self.vertical_metrics.reports.labels("checkpoint", "failed").inc()
            self._fail_vertical_plan(
                "checkpoint_failed", reason or "Save checkpoint failed."
            )
            return
            
        if checkpoint_path:
            task = self._ucp_task_manager.get_checkpoint_task(
                step, checkpoint_path
            )
            if not task:
                self.vertical_metrics.reports.labels("checkpoint", "no_ucp_task").inc()
                self._fail_vertical_plan(
                    "checkpoint_failed", "Checkpoint has no UCP task"
                )
                return
            
            if (
                self._checkpoint_task_id
                and self._checkpoint_task_id != task.task_id
            ):
                self.vertical_metrics.reports.labels("checkpoint", "conflicting").inc()
                self._fail_vertical_plan(
                    "checkpoint_failed", "Conflicting checkpoint for plan"
                )
                return
            
            self._save_ckpt_step = step
            self._save_ckpt_path = checkpoint_path
            self._checkpoint_task_id = task.task_id
        else:
            result = "duplicate" if self._save_ckpt_status else "accepted"
            self.vertical_metrics.reports.labels("checkpoint", result).inc()
            self._save_ckpt_status = True

    def set_param_tunning_ready(
        self,
        param_tunning_ready: bool,
        node_id: int = -1,
        plan_id: str = "",
        rank: int = -1,
    ):
        phase_info = self.vertical_elastic_phase_manager.get()
        if ((self.exploration.active and plan_id != phase_info.get("plan_id"))
                or (plan_id and plan_id != phase_info.get("plan_id"))):
            self.vertical_metrics.reports.labels("param_tunning", "stale_plan").inc()
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
            self.vertical_metrics.reports.labels("param_tunning", "phase_mismatch").inc()
            logger.warning(
                "Ignore param tunning status in phase %s.",
                phase_info.get("phase"),
            )
            return

        if node_id not in self._target_tunning_nodes:
            self.vertical_metrics.reports.labels("param", "non_target").inc()
        else:
            duplicate = param_tunning_ready == (
                node_id in self._param_tunning_ready_nodes
            )
            self.vertical_metrics.reports.labels(
                "param", "duplicate" if duplicate else "accepted"
            ).inc()
        
        if param_tunning_ready:
            self._param_tunning_ready_nodes.add(node_id)
        else:
            self._param_tunning_ready_nodes.discard(node_id)

        self.vertical_metrics.set_workers(
            self._target_tunning_nodes, self._param_tunning_ready_nodes
        )
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

        self.vertical_metrics.track_replacement_workers(self._target_tunning_nodes)
        self.events.checkpoint(
            self._save_ckpt_step, 
            self._save_ckpt_path,
            self._target_tunning_nodes
        )
        self.vertical_metrics.set_workers(
            self._target_tunning_nodes, 
            self._param_tunning_ready_nodes
        )

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
        self.vertical_metrics.transition(current_phase)
        logger.info(f"Update vertical elastic phase,"
             f"current phase is set to {current_phase},"
             f"expected next phase is set to {expected_next_phase}")