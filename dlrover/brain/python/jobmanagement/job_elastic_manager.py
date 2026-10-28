# Copyright 2026 The DLRover Authors. All rights reserved.
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
import queue
import threading
import json
import time
import os
import requests

from typing import Optional
from collections import Counter
from dataclasses import dataclass

from dlrover.brain.python.common.job import JobMeta
from dlrover.brain.python.common.job_metric import (
    JobMetrics,
    PodResource,
    RuntimeInfo,
    GPUStats,
)
from dlrover.brain.python.jobmanagement.elastic.persist_manager import (
    MysqlMetricsPersister,
)
from dlrover.python.common.log import default_logger as logger
from dlrover.brain.python.jobmanagement.elastic.vertical_elastic import VerticalElasticService
from dlrover.brain.python.common.constants import WorkerResourceScaleAction
from dlrover.brain.python.jobmanagement.master_client_pool import MasterClientPool
from dlrover.brain.python.jobmanagement.dispatch_result import DispatchResult
from concurrent.futures import ThreadPoolExecutor, Future
from dlrover.brain.python.common.distributed_lock import (
    RedisDistributedLock,
    _redis_client,
)
from dlrover.brain.python.jobmanagement.job_owner import (
    JobOwner,
    JobOwnerManager,
    JobOwnerStoreError,
)

_MAX_RETRY_ATTEMPTS = 3
_RETRY_DELAY = 0.1 # seconds, 100 ms


@dataclass(frozen=True)
class _OwnedJobMetrics:
    metrics: JobMetrics
    owner_token: str = ""

class JobElasticManager:
    """
    Manages job runtime info.
    """
    def __init__(
        self,
        master_client_pool: Optional[MasterClientPool] = None,
        job_owner_manager: Optional[JobOwnerManager] = None,
        vertical_elastic_service: Optional[VerticalElasticService] = None,
    ):
        self.report_queue = queue.Queue(maxsize = 1000)
    
        self.master_client_pool = master_client_pool or MasterClientPool()
        self._vertical_elastic_service = (
            vertical_elastic_service or VerticalElasticService()
        )
        self._mysql_persister = MysqlMetricsPersister()
        # 是否由JobElasticManager负责该对象的start()和stop()
        self._managers_job_owner_lifecycle = job_owner_manager is None
        # 实际执行owner管理的对象，用于查询owner、校验token、续租、去重等
        self._job_owner_manager = job_owner_manager or JobOwnerManager()
        self._forward_session = requests.Session()
        try:
            self._forward_timeout_seconds = float(
                os.getenv("DLROVER_BRAIN_FORWARD_TIMEOUT_SECONDS", "3")
            )
        except ValueError:
            self._forward_timeout_seconds = 3.0

        self._persist_executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="persist_thread")
        self._forward_timeout_seconds = max(0.1, self._forward_timeout_seconds)
        self._stop_event =  threading.Event()
        self._metrics_lock = threading.Lock()
        self._dispatch_locks = [threading.Lock() for _ in range(64)]

        # metrics
        self._reports_total = 0 # total number of reports received(接收的运行时指标上报数量,包括成功和失败的上报)
        self._reports_rejected_total = 0 # total number of reports rejected(接收的运行时指标失败的上报数量,包括队列满和异常的上报)
        self._reports_processed_total = 0 # total number of reports processed（已从运行时指标队列取出并开始处理的数据条数）
        self._processing_errors_total = 0 # total number of processing errors（处理运行时指标时发生的异常次数）

        self._redis_available = 0 # 1 if redis is available, 0 if not (redis是否可用的指标，1表示可用，0表示不可用)
        self._redis_last_check_timestamp = 0.0 # last time redis was checked(上一次检查redis的时间戳)
        self._redis_checks = Counter() # total number of redis checks(检查redis的次数)
        self._distributed_locks = Counter() # total number of distributed lock attempts(分布式锁的尝试次数)

        self._elastic_decisions = Counter() # total number of elastic decisions made(弹性决策的次数)
        self._elastic_triggers = Counter() # total number of elastic triggers(弹性触发次数)
        self._job_dispatches = Counter()
        self._process_thread = None

        # redis probe thread
        self._redis_probe_thread = None
        try:
            redis_probe_interval = int(
                os.getenv("DLROVER_BRAIN_REDIS_PROBE_INTERVAL_SECONDS", "15")
            )
        except ValueError:
            redis_probe_interval = 15
            logger.warning(
                "Invalid DLROVER_BRAIN_REDIS_PROBE_INTERVAL_SECONDS; use 15."
            )
        self._redis_probe_interval = max(5, redis_probe_interval)

    def receive_job_runtime_info(self, job_metrics: JobMetrics):
        """
        Receive job runtime info from job workers.
        """
        return self.dispatch_job_runtime_info(job_metrics).success
        

    def dispatch_job_runtime_info(
        self,
        job_metrics: JobMetrics,
        forwarded: bool = False,
        owner_token: str = "",
    ) -> DispatchResult:
        """
        Route a report to its sticky owner and enqueue it idempotently.
        """
        validation_error = self._validate_job_metrics(job_metrics)
        if validation_error:
            logger.warning("Reject invalid job runtime info: %s", validation_error)
            with self._metrics_lock:
                self._reports_rejected_total += 1
            return DispatchResult(False, validation_error)
            
        job_name = job_metrics.job_meta.name
        job_id = job_metrics.job_meta.uuid

        if self._job_owner_manager.enabled:
            try:
                if forwarded:
                    if not self._job_owner_manager.accept_forwarded(
                        job_id, owner_token
                    ):
                        self._increment_metric(
                            self._job_dispatches, "owner_mismatch"
                        )
                        return DispatchResult(
                            False, "job owner changed", retryable=True
                        )
                    return self._enqueue_owned_report(
                        job_metrics, owner_token, deduplicate=True
                    )

                owner = self._job_owner_manager.get_owner(job_id)
                if owner is None:
                    self._increment_metric(
                        self._job_dispatches, "owner_not_registered"
                    )
                    with self._metrics_lock:
                        self._reports_rejected_total += 1
                    return DispatchResult(
                        False,
                        "job owner is not registered",
                        retryable=True,
                    )
                
                if owner.instance_id == self._job_owner_manager.instance_id:
                    # Refresh and fence an existing local claim before enqueue.
                    if not self._job_owner_manager.accept_forwarded(
                        job_id, owner.token
                    ):
                        self._increment_metric(
                            self._job_dispatches, "owner_mismatch"
                        )
                        return DispatchResult(
                            False, "job owner changed", retryable=True
                        )
                    self._increment_metric(self._job_dispatches, "local")
                    return self._enqueue_owned_report(
                        job_metrics, owner.token, deduplicate=True
                    )

                return self._forward_to_owner(job_metrics, owner)
            except JobOwnerStoreError as e:
                self._increment_metric(self._job_dispatches, "redis_error")
                with self._metrics_lock:
                    self._reports_rejected_total += 1
                logger.error(
                    "Route runtime report for %s/%s failed: %s",
                    job_name,
                    job_id,
                    e,
                )
                return DispatchResult(False, str(e), retryable=True)

        return self._enqueue_owned_report(job_metrics, "", deduplicate=False)
    
    @staticmethod
    def _validate_job_metrics(job_metrics: JobMetrics) -> str:
        """Return a reason when a runtime report cannot be processed."""
        if job_metrics is None:
            return "runtime info is empty"
        job_meta = getattr(job_metrics, "job_meta", None)
        if job_meta is None:
            return "job meta is empty"
        if not getattr(job_meta, "uuid", ""):
            return "job uuid is empty"
        if getattr(job_metrics, "runtime_info", None) is None:
            return "runtime info payload is empty"
        return ""

    def _enqueue_owned_report(
        self,
        job_metrics: JobMetrics,
        owner_token: str,
        deduplicate: bool,
    ) -> DispatchResult:
        # Serialize dedup reservation and queue insertion. Reports for a job
        # are already sticky to this process, so this also prevents two HTTP
        # forwarding threads from racing on the same report id.
        dispatch_lock = self._dispatch_locks[
            hash(job_metrics.job_meta.uuid) % len(self._dispatch_locks)
        ]

        with dispatch_lock:
            return self._enqueue_owned_report_locked(
                job_metrics, owner_token, deduplicate
            )

    def _enqueue_owned_report_locked(
        self,
        job_metrics: JobMetrics,
        owner_token: str,
        deduplicate: bool,
    ) -> DispatchResult:
        job_name = job_metrics.job_meta.name
        job_id = job_metrics.job_meta.uuid
        logger.info(f"Enqueue runtime report for {job_name}/{job_id}, owner_token: {owner_token}")

        report_id = ""
        if deduplicate:
            report_id = self._report_id(job_metrics)
            if report_id:
                try:
                    if not self._job_owner_manager.mark_report_once(
                        job_id, report_id
                    ):
                        self._increment_metric(
                            self._job_dispatches, "duplicate"
                        )
                        return DispatchResult(True, "duplicate report ignored")
                except JobOwnerStoreError as e:
                    self._increment_metric(self._job_dispatches, "redis_error")
                    return DispatchResult(False, str(e), retryable=True)

        max_retries = _MAX_RETRY_ATTEMPTS
        retry_delay = _RETRY_DELAY
        for i in range(max_retries):
            try:
                queued_report = (
                    _OwnedJobMetrics(job_metrics, owner_token)
                    if owner_token
                    else job_metrics
                )
                self.report_queue.put_nowait(queued_report)
                with self._metrics_lock:
                    self._reports_total += 1
                return DispatchResult(True, "success")
            except queue.Full:
                # queue full, wait -> retry -> drop data
                if i < max_retries - 1:
                    logger.warning(f"Queue full, retrying in {retry_delay}s... (Attempt {i+1}/{max_retries})")
                    time.sleep(retry_delay)
                else:
                    # retry failed, drop data
                    logger.error(
                        f"Queue full after retries," 
                        f"dropping job_metrics for {job_name}"
                    )
                    with self._metrics_lock:
                        self._reports_rejected_total += 1
                    self._rollback_report_dedup(job_id, report_id)
                    return DispatchResult(
                        False, "runtime report queue is full", retryable=True
                    )
            except Exception as e:
                logger.error(f"Unexpected error putting metrics to queue: {e}")
                with self._metrics_lock:
                    self._reports_rejected_total += 1
                self._rollback_report_dedup(job_id, report_id)
                return DispatchResult(False, str(e), retryable=True)

    def _rollback_report_dedup(self, job_id: str, report_id: str):
        if not report_id:
            return
        try:
            self._job_owner_manager.forget_report(job_id, report_id)
        except JobOwnerStoreError as e:
            logger.warning(
                "Rollback report dedup for %s/%s failed: %s",
                job_id,
                report_id,
                e,
            )

    def _forward_to_owner(
        self, job_metrics: JobMetrics, owner: JobOwner
    ) -> DispatchResult:
        url = f"{owner.endpoint}/internal/report_runtime_node_stats"
        try:
            response = self._forward_session.post(
                url,
                json={"job_metrics": job_metrics.model_dump()},
                headers={"X-DLRover-Owner-Token": owner.token},
                timeout=self._forward_timeout_seconds,
            )
        except requests.RequestException as e:
            self._record_forward_failure(owner)
            return self._forward_failure_result(job_metrics, owner, str(e))
        except Exception as e:
            # A local serialization/programming failure is not evidence that
            # the remote Brain should be removed from allocator candidates.
            return self._forward_failure_result(job_metrics, owner, str(e))

        if response.status_code >= 500:
            self._record_forward_failure(owner)
            reason = (
                f"owner returned HTTP {response.status_code}: "
                f"{response.text[:256]}"
            )
            return self._forward_failure_result(job_metrics, owner, reason)

        if response.status_code != 200:
            self._record_forward_success(owner)
            reason = (
                f"owner returned HTTP {response.status_code}: "
                f"{response.text[:256]}"
            )
            return self._forward_failure_result(job_metrics, owner, reason)

        try:
            body = response.json()
            result = body.get("response", {})
        except (TypeError, ValueError, AttributeError) as e:
            self._record_forward_failure(owner)
            return self._forward_failure_result(job_metrics, owner, str(e))

        self._record_forward_success(owner)
        if not bool(result.get("success")):
            reason = result.get("reason", "owner rejected runtime report")
            return self._forward_failure_result(job_metrics, owner, reason)
        self._increment_metric(self._job_dispatches, "forwarded")
        logger.info("Forward runtime report for {job_metrics.job_meta.name} to {owner.instance_id} success")
        return DispatchResult(True, "forwarded to job owner")

    def _forward_failure_result(
        self, job_metrics: JobMetrics, owner: JobOwner, reason: str
    ) -> DispatchResult:
        self._increment_metric(self._job_dispatches, "forward_failed")
        with self._metrics_lock:
            self._reports_rejected_total += 1
        logger.warning(
            "Forward runtime report for %s/%s to %s failed: %s",
            job_metrics.job_meta.name,
            job_metrics.job_meta.uuid,
            owner.instance_id,
            reason,
        )
        return DispatchResult(False, reason, retryable=True)

    def _record_forward_failure(self, owner: JobOwner) -> None:
        try:
            self._job_owner_manager.record_forward_failure(owner)
        except JobOwnerStoreError as e:
            logger.warning(
                "Record forwarding failure for Brain %s failed: %s",
                owner.instance_id,
                e,
            )

    def _record_forward_success(self, owner: JobOwner) -> None:
        try:
            self._job_owner_manager.record_forward_success(owner)
        except JobOwnerStoreError as e:
            logger.warning(
                "Clear forwarding failure for Brain %s failed: %s",
                owner.instance_id,
                e,
            )

    @staticmethod
    def _report_id(job_metrics: JobMetrics) -> str:
        report_ts = int(getattr(job_metrics, "time_stamp", 0) or 0)
        if report_ts:
            return str(report_ts)
        runtime = getattr(job_metrics, "runtime_info", None)
        runtime_ts = int(getattr(runtime, "time_stamp", 0) or 0)
        global_step = int(getattr(runtime, "global_step", 0) or 0)
        if runtime_ts or global_step:
            return f"{runtime_ts}:{global_step}"
        return ""    

    def start_process_loop(self):
        logger.info(f"JobElasticManager process loop start...")
        while not self._stop_event.is_set():
            job_metrics = None
            queue_item_received = False
            try:
                # Get and init job meta/runtime_info
                try:
                    queued_report = self.report_queue.get(timeout=1.0)
                except queue.Empty:
                    logger.debug(f"JobElasticManager process loop: queue is empty, skip this round.")
                    continue

                queue_item_received = True
                if isinstance(queued_report, _OwnedJobMetrics):
                    job_metrics = queued_report.metrics
                    owner_token = queued_report.owner_token
                    logger.debug(f"Start process loop for job, owner_token: {owner_token}")
                else:
                    # Compatibility for existing in-process callers/tests.
                    job_metrics = queued_report
                    owner_token = ""
                logger.info(f"Current process job runtime info: {job_metrics}")

                with self._metrics_lock:
                    self._reports_processed_total += 1

                validation_error = self._validate_job_metrics(job_metrics)
                if validation_error:
                    logger.warning(
                        "Discard invalid queued runtime report (%s): %s",
                        type(queued_report).__name__,
                        validation_error,
                    )
                    with self._metrics_lock:
                        self._reports_rejected_total += 1
                        self._processing_errors_total += 1
                    continue

                job_meta = self._init_job_meta(job_metrics = job_metrics)
                runtime_node_stats = self._init_job_runtime_info(job_metrics = job_metrics)
                logger.info(f"Start process job-[{job_meta.name}/{job_meta.uuid}] runtime info.")

                if not self._pre_check(job_meta, runtime_node_stats):
                    logger.warning(f"Report success, but Pre check job_metrics failed, skip elastic.")
                    continue

                if owner_token and not self._is_current_owner(
                    job_meta.uuid, owner_token
                ):
                    self._increment_metric(
                        self._job_dispatches, "owner_lost"
                    )
                    logger.warning(
                        "Skip stale queued report for %s/%s after ownership loss.",
                        job_meta.name,
                        job_meta.uuid,
                    )
                    continue

                # async persist job metrics
                logger.info(f"  step 1: async persist job metrics to DB...")
                self._async_save_to_db(job_metrics)

                if not self._redis_available_now():
                    self._increment_metric(self._elastic_decisions, "redis_unavailable")
                    logger.warning(
                        f"Redis is unavailable, skip elastic decision for "
                        f"{job_meta.name}/{job_meta.uuid}"
                    )
                    continue

                # elastic decision
                logger.info(f"  step 2: make elastic decision...")
                elastic_result, reason = self._make_elastic_decision(
                    runtime_node_stats, 
                    job_meta,
                    owner_token
                )
                logger.info(f"Elastic decision result: {elastic_result}, reason: {reason}")    
            except Exception as e:
                logger.error(f"JobElasticManager process loop error: {e}")
                with self._metrics_lock:
                    self._processing_errors_total += 1
            finally:
                if queue_item_received:
                    self.report_queue.task_done()

    def stop(self):
        self._stop_event.set()
        for thread in (self._process_thread, self._redis_probe_thread):
            if thread and thread.is_alive():
                thread.join(timeout=2)
        if self._managers_job_owner_lifecycle:
            self._job_owner_manager.stop()
        self._forward_session.close()
        self._persist_executor.shutdown(wait=False)

    def _is_current_owner(self, job_id: str, owner_token: str) -> bool:
        try:
            return self._job_owner_manager.is_current_owner(
                job_id, owner_token
            )
        except JobOwnerStoreError as e:
            logger.error("Validate job owner %s failed: %s", job_id, e)
            return False      

    def _pre_check(self, job_meta: JobMeta, runtime_node_stats: RuntimeInfo):
        if not job_meta or not runtime_node_stats:
            return False
        
        # define required fields
        required_fields = [
            (job_meta, 'name'),
            (job_meta, 'uuid'),
            (job_meta, 'namespace'),
            (runtime_node_stats, 'running_pods'),
        ]

        for obj, field in required_fields:
            if not getattr(obj, field, None):
                return False
        
        return True

    def _async_save_to_db(self, job_metrics: JobMetrics):
        future: Future = self._persist_executor.submit(self.async_persist_metrics, job_metrics)
        future.add_done_callback(lambda f: self._persist_metrics_callback(f, job_metrics.job_meta.name))          

    def _persist_metrics_callback(self, future: Future, job_name: str):
        """
        持久化完成后的回调处理函数。
        从 Future 中获取结果并记录日志。
        """
        try:
            # 获取异步执行的结果
            success, reason = future.result()
            if success:
                logger.info(f"Persisted metrics for {job_name} successfully.")
            else:
                logger.warning(f"Failed to persist metrics for {job_name}: {reason}")
        except Exception as e:
            logger.error(f"Error retrieving persist metrics for {job_name}: {e}")
    
    def _init_job_meta(self, job_metrics: JobMetrics) -> JobMeta:
       return JobMeta(
            uuid=job_metrics.job_meta.uuid,
            cluster=job_metrics.job_meta.cluster,
            namespace=job_metrics.job_meta.namespace,
            user=job_metrics.job_meta.user,
            app=job_metrics.job_meta.app,
            name=job_metrics.job_meta.name,
        )

    def _init_job_runtime_info(self, job_metrics: JobMetrics) -> RuntimeInfo:
        return RuntimeInfo(
            global_step=job_metrics.runtime_info.global_step,
            running_pods=job_metrics.runtime_info.running_pods,
            time_stamp=job_metrics.runtime_info.time_stamp,
            speed=job_metrics.runtime_info.speed,
        )

    def trigger_elastic(self, elastic_dimension: str, scale_action: str, job_meta: JobMeta) -> bool:
        logger.info(f"  step 3: trigger elastic.")
        job_name = job_meta.name
        job_id = job_meta.uuid
        
        if not self._redis_available_now():
            self._increment_metric(self._elastic_triggers, "redis_unavailable")
            logger.warning(
                f"Redis is unavailable, skip trigger elastic for {job_name}/{job_id}"
            )
            return False

        try:
            master_client = self.master_client_pool.get_master_client(
                job_name, job_id, refresh_from_shared_store=True
            )
            if master_client is None:
                self._increment_metric(self._elastic_triggers, "master_unavailable")
                logger.warning(f"{job_name} master service is not found, skip elastic.")
                return False
            
            if not self._redis_available_now():
                self._increment_metric(self._elastic_triggers, "redis_unavailable")
                logger.warning(f"Redis is unavailable, skip trigger elastic for {job_name}/{job_id}")
                return False

            master_client.trigger_elastic(elastic_dimension, scale_action, job_meta)
            self._increment_metric(self._elastic_triggers, "success")
            logger.info(f"{job_name} trigger elastic success.")
            return True
        except Exception as e:
            logger.warning(
                f"{job_name} trigger elastic error: {e}, retry with refreshed master client."
            )
            self.master_client_pool.invalidate_local_client(job_id)
            try:
                master_client = self.master_client_pool.get_master_client(
                    job_name, job_id, refresh_from_shared_store=True
                )
                if master_client is None:
                    self._increment_metric(self._elastic_triggers, "master_unavailable")
                    logger.warning(
                        f"{job_name} retry trigger elastic failed, master service is not found, skip elastic."
                    )
                    return False
                
                if not self._redis_available_now():
                    self._increment_metric(self._elastic_triggers, "redis_unavailable")
                    logger.warning(
                        f"Redis is unavailable, skip trigger elastic for {job_name}/{job_id}"
                    )
                    return False

                master_client.trigger_elastic(elastic_dimension, scale_action, job_meta)
                self._increment_metric(self._elastic_triggers, "retry_success")
                logger.info(f"{job_name} retry trigger elastic success.")
                return True
            except Exception as retry_e:
                self._increment_metric(self._elastic_triggers, "failed")
                logger.warning(f"{job_name} trigger elastic error: {retry_e}, skip elastic.")
                return False
    
    def async_persist_metrics(self, job_metrics: JobMetrics) -> tuple[bool, str]:
        """
        TODO: persist job metrics
        """
        
        job_name = job_metrics.job_meta.name
        job_id = job_metrics.job_meta.uuid
        namespace = job_metrics.job_meta.namespace if job_metrics.job_meta.namespace else ""
        cluster = job_metrics.job_meta.cluster if job_metrics.job_meta.cluster else ""
        job_feature = job_metrics.job_feature if job_metrics.job_feature is not None else {}
        training_set_feature = job_metrics.training_set_feature if job_metrics.training_set_feature is not None else {}
        model_feature = job_metrics.model_feature if job_metrics.model_feature is not None else {}
        runtime_info = job_metrics.runtime_info if job_metrics.runtime_info is not None else {}
        job_exit_reason = job_metrics.job_exit_reason if job_metrics.job_exit_reason else ""
        extra_info = job_metrics.extra_info if job_metrics.extra_info else ""
        type = job_metrics.type if job_metrics.type else ""
        resource = job_metrics.resource if job_metrics.resource else ""
        customized_data = job_metrics.customized_data if job_metrics.customized_data else ""

        if not self._mysql_persister:
            return False, "Persister is not initialized, skip persist."
        
        try:
            self._mysql_persister.save_job_metrics(
                job_name=job_name, 
                job_uuid=job_id,
                cluster=cluster,
                namespace=namespace,
                job_feature=job_feature,
                training_set_feature=training_set_feature,
                model_feature=model_feature,
                runtime_info=runtime_info,
                job_exit_reason=job_exit_reason,
                resource=resource,
                customized_data=customized_data,
                type=type,
                extra_info=extra_info
            )
            return True, "Persist job metrics to DB success."
        except Exception as e:
            logger.warning(f"Persist job metrics to DB error: {e}")
            return False, "Persist job metrics to DB error."


    def _make_elastic_decision(
        self, 
        runtime_node_stats: RuntimeInfo, 
        job_meta: JobMeta,
        owner_token: str = "",
    ) -> tuple[bool, str]:
        """
        calculate elastic decision
        """
        job_name = job_meta.name
        job_id = job_meta.uuid
        namespace = job_meta.namespace if job_meta.namespace else ""
        cluster = job_meta.cluster if job_meta.cluster else "default-cluster"

        if not self._redis_available_now():
            self._increment_metric(self._elastic_decisions, "redis_unavailable")
            logger.warning(
                f"Redis is unavailable, skip elastic decision for {job_name}/{job_id}."
            )
            return False, "Redis is unavailable, skip elastic decision."
            
        lcok_key = f"dlrover:brain:vertical_elastic:{job_id}"
        lock = RedisDistributedLock(lcok_key, ttl_seconds=30)
        if not lock.available:
            self._increment_metric(self._distributed_locks, "unavailable")
            self._increment_metric(self._elastic_decisions, "lock_unavailable")
            logger.warning(
                f"Redis distributed lock is unavailable."
                f"Skip elastic decision for {job_name}/{job_id}.")
            return False, "Redis distributed lock unavailable."
        
        if not lock.acquire():
            if lock.last_error:
                self._increment_metric(self._distributed_locks, "error")
                self._increment_metric(self._elastic_decisions, "lock_error")
                logger.error(
                    f"Redis distributed lock error for {job_meta.name}/{job_meta.uuid}; "
                    "skip elastic decision."
                )
                return False, "redis distributed lock unavailable."

            self._increment_metric(self._distributed_locks, "contended")
            self._increment_metric(self._elastic_decisions, "lock_contended")
            logger.info(
                f"Skip elastic decision for {job_meta.name}/{job_meta.uuid}, "
                "another brain instance is processing."
            )
            return False, "distributed lock not acquired."

        self._increment_metric(self._distributed_locks, "acquired")

        if owner_token and not self._is_current_owner(
            job_meta.uuid, owner_token
        ):
            self._increment_metric(self._job_dispatches, "owner_lost")
            lock.release()
            return False, "job ownership lost."

        try:
            # 1. init vertical elastic opt config
            self._vertical_elastic_service.set_opt_config(job_meta.namespace)

            active_pod_name = ""
            valid_scale_action = None
            for pod in runtime_node_stats.running_pods:
                pod_name = pod.pod_name
                used_resource = getattr(pod, "used_resource", None)
                if used_resource is None:
                    logger.warning(f"{pod_name} used_resource is None, skip.")
                    continue

                gpu_stats = used_resource.gpu_stats

                if gpu_stats is None:
                    logger.warning(f"{job_name}/{pod_name} gpu_stats is None, skip.")
                    continue
                
                # 2. calculate elastic decision
                action = self._vertical_elastic_service.verify_trigger_gpu_resource_elastic(
                    job_name = job_name, 
                    job_id = job_id, 
                    namespace = namespace, 
                    cluster = cluster, 
                    gpu_stats = gpu_stats, 
                    pod_name = pod_name
                )
                
                if action is not None and action != WorkerResourceScaleAction.NOT_ADJUST:
                    active_pod_name = pod_name
                    valid_scale_action = action
                    break  # 如果有一个pod触发了弹性扩缩，则立即返回
                
                # 如果当前 pod 不需要调整，记录警告，继续检查下一个 pod
                logger.warning(f"{pod_name} is not adjust, skip elastic.")
                    

            if active_pod_name is None or valid_scale_action is None:
                self._increment_metric(self._elastic_decisions, "no_action")
                return False, f"No valid scale action, skip elastic."

            if not lock.is_acquired:
                self._increment_metric(self._distributed_locks, "lost")
                self._increment_metric(self._elastic_decisions, "lock_lost")
                logger.error(
                    f"Redis distributed lock ownership was lost for "
                    f"{job_meta.name}/{job_meta.uuid}; skip elastic trigger."
                )
                return False, "redis distributed lock ownership lost."

            if owner_token and not self._is_current_owner(
                job_meta.uuid, owner_token
            ):
                self._increment_metric(self._job_dispatches, "owner_lost")
                logger.error(
                    "Job ownership was lost for %s/%s; skip elastic trigger.",
                    job_meta.name,
                    job_meta.uuid,
                )
                return False, "job ownership lost."
            
            # 3. trigger elastic
            result = self.trigger_elastic(
                elastic_dimension="vertical", 
                scale_action=valid_scale_action, 
                job_meta=job_meta
            )

            # 4. cleanup redis time-window state
            self._vertical_elastic_service.cleanup_redis_state(job_id, active_pod_name)
            if not result:
                self._increment_metric(self._elastic_decisions, "trigger_failed")
                return False, "trigger elastic failed."
            
            self._increment_metric(self._elastic_decisions, "triggered")
            return True, f"trigger elastic success, action is {valid_scale_action}."
        except Exception as e:
            logger.error(f"{job_name} make elastic decision error: {e}")
            return False, f"make elastic decision error: {e}"
        finally:
            lock.release()

    def _redis_available_now(self) -> bool:
        client = _redis_client()
        if client is None:
            with self._metrics_lock:
                self._redis_available = 0
                self._redis_last_check_timestamp = time.time()
                self._redis_checks["failed"] += 1
            return False
        try:
            client.ping()
            with self._metrics_lock:
                self._redis_available = 1
                self._redis_last_check_timestamp = time.time()
                self._redis_checks["success"] += 1
            return True
        except Exception as e:
            logger.warning(f"Redis health check failed: {e}")
            with self._metrics_lock:
                self._redis_available = 0
                self._redis_last_check_timestamp = time.time()
                self._redis_checks["failed"] += 1
            return False

    def _redis_probe_loop(self):
        while not self._stop_event.is_set():
            self._redis_available_now()
            self._stop_event.wait(self._redis_probe_interval)

    def _increment_metric(self, counter, result):
        with self._metrics_lock:
            counter[result] += 1

    def metrics_snapshot(self):
        """Return a bounded snapshot for the Brain Prometheus endpoint."""
        owned_jobs = self._job_owner_manager.local_owner_count()
        instance_registered = int(
            self._job_owner_manager.is_instance_registered()
        )
        active_instances = self._job_owner_manager.active_instance_count()
        quarantined_instances = (
            self._job_owner_manager.quarantined_instance_count()
        )
        with self._metrics_lock:
            return {
                "reports_total": self._reports_total,
                "reports_rejected_total": self._reports_rejected_total,
                "reports_processed_total": self._reports_processed_total,
                "processing_errors_total": self._processing_errors_total,
                "redis_available": self._redis_available,
                "redis_last_check_timestamp": self._redis_last_check_timestamp,
                "redis_checks": dict(self._redis_checks),
                "distributed_locks": dict(self._distributed_locks),
                "elastic_decisions": dict(self._elastic_decisions),
                "elastic_triggers": dict(self._elastic_triggers),
                "job_dispatches": dict(self._job_dispatches),
                "owned_jobs": owned_jobs,
                "owner_allocator_instance_registered": instance_registered,
                "owner_allocator_active_instances": active_instances,
                "owner_allocator_quarantined_instances": (
                    quarantined_instances
                ),
                "processor_ready": int(
                    self._process_thread is not None
                    and self._process_thread.is_alive()
                ),
                "queue_size": self.report_queue.qsize(),
                "queue_capacity": self.report_queue.maxsize,
            }

    def is_ready(self) -> bool:
        processor_ready = bool(
            self._process_thread is not None
            and self._process_thread.is_alive()
        )
        if not processor_ready:
            return False
        if self._job_owner_manager.enabled:
            return (
                self._job_owner_manager.is_instance_registered()
                and self._redis_available_now()
            )
        return True

    
    def start(self):
        logger.info("Job Runtime info process initializing...")
        
        self._stop_event.clear()

        if self._managers_job_owner_lifecycle:
            self._job_owner_manager.start()

        self._process_thread = threading.Thread(
            target=self.start_process_loop,
            name="process_runtime_info",
            daemon=True,
        )
        self._redis_probe_thread = threading.Thread(
            target=self._redis_probe_loop,
            name="brain_redis_probe",
            daemon=True,
        )

        self._process_thread.start()
        self._redis_probe_thread.start()
        if self._process_thread.is_alive():
            logger.info("Job Runtime info process initialized successfully")