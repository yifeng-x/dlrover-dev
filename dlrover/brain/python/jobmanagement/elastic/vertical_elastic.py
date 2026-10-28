from datetime import datetime
import os
import time
import json
from typing import Dict, List, Optional, Any

from concurrent.futures import ThreadPoolExecutor

from dlrover.python.common.log import default_logger as logger
from dlrover.python.common.comm import GPUStats
from dlrover.python.common.constants import (
    WorkerResourceScaleAction,
)
from dlrover.brain.python.common.constants import (
    OptConfigKeys,
    DefaultOptValues,
)
from dlrover.brain.python.platform.k8s.configmap import ConfigMapReader
from dlrover.brain.python.jobmanagement.elastic.persist_manager import (
    MysqlMetricsPersister, 
    IVerticalElasticMetricsPersister,
)
from dlrover.brain.python.common.distributed_lock import (
    _redis_client,
    redis_fail_closed_for_brain,
)

DEFAULT_OPT_CONFIGMAP_NAME = "brain-opt-config"
REDIS_GPU_TIME_WINDOW_KEY_PREFIX = "dlrover:brain:vertical_elastic:gpu_tw"

PROMETHEUS_SERVER_DEFAULT_PORT = 9091

class VerticalElasticService(object):
    def __init__(
        self,
        metrics_persister: Optional[IVerticalElasticMetricsPersister] = None,
    ):
        self.gpu_stats = []
        self._redis = _redis_client()
        self._mysql_persister = MysqlMetricsPersister()
        self._prometheus_persister = metrics_persister

        self.async_mysql_persist_thread = ThreadPoolExecutor(
            max_workers=4, 
            thread_name_prefix="async_mysql_persist_thread"
        )

    def set_opt_config(self, namespace):
        opt_config = ConfigMapReader(namespace, DEFAULT_OPT_CONFIGMAP_NAME).get_opt_config()

        # gpu 核心利用率阈值
        self.optimize_add_gpus_utilization_threshold = opt_config.get(
            OptConfigKeys.OPTIMIZE_ADD_GPUS_UTILIZATION_THRESHOLD,
            DefaultOptValues.OPTIMIZE_ADD_GPUS_UTILIZATION_THRESHOLD,
        )
        self.optimize_reduce_gpus_utilization_threshold = opt_config.get(
            OptConfigKeys.OPTIMIZE_REDUCE_GPUS_UTILIZATION_THRESHOLD,
            DefaultOptValues.OPTIMIZE_REDUCE_GPUS_UTILIZATION_THRESHOLD,
        )
        # 较高gpu 核心利用率阈值
        self.optimize_gpu_emergency_high_utilization_threshold = opt_config.get(
            OptConfigKeys.OPTIMIZE_GPU_EMERGENCY_HIGH_UTILIZATION_THRESHOLD,
            DefaultOptValues.OPTIMIZE_GPU_EMERGENCY_HIGH_UTILIZATION_THRESHOLD,
        )

        # gpu 显存使用率阈值
        self.optimize_add_gpus_memory_utilization_threshold = opt_config.get(
            OptConfigKeys.OPTIMIZE_ADD_GPUS_MEMORY_UTILIZATION_THRESHOLD,
            DefaultOptValues.OPTIMIZE_ADD_GPUS_MEMORY_UTILIZATION_THRESHOLD,
        )
        self.optimize_reduce_gpus_memory_utilization_threshold = opt_config.get(
            OptConfigKeys.OPTIMIZE_REDUCE_GPUS_MEMORY_UTILIZATION_THRESHOLD,
            DefaultOptValues.OPTIMIZE_REDUCE_GPUS_MEMORY_UTILIZATION_THRESHOLD,
        )
        self.optimize_gpu_emergency_high_memory_utilization_threshold = opt_config.get(
            OptConfigKeys.OPTIMIZE_GPU_EMERGENCY_HIGH_MEMORY_UTILIZATION_THRESHOLD,
            DefaultOptValues.OPTIMIZE_GPU_EMERGENCY_HIGH_MEMORY_UTILIZATION_THRESHOLD,
        )

        # 【关键配置】触发资源弹性时间窗口长度（秒）
        # 表示我们只关心过去 xx 秒内的数据平均值
        self.trigger_resource_elastic_time_window = opt_config.get(
            OptConfigKeys.TRIGGER_RESOURCE_ELASTIC_TIME_WINDOW,
            DefaultOptValues.TRIGGER_RESOURCE_ELASTIC_TIME_WINDOW,
        )
        # 【辅助配置】最小采样点数
        # 即使在时间窗口内，也至少需要采集xxx个点才计算平均值，防止因数据丢失导致只有1个点就触发
        self.min_samples_in_window = opt_config.get(
            OptConfigKeys.MIN_SAMPLES_IN_WINDOW,
            DefaultOptValues.MIN_SAMPLES_IN_WINDOW,
        )
        # 【安全阀】最大缓冲点数 (防止内存溢出)
        # 即使时间窗口很长，内存中也最多只保留这么多条记录
        self.max_buffer_size = opt_config.get(
            OptConfigKeys.MAX_BUFFER_SIZE,
            DefaultOptValues.MAX_BUFFER_SIZE,
        )

    def verify_trigger_gpu_resource_elastic(
        self,
        job_name: str,
        job_id: str,
        namespace: str,
        cluster: str,
        gpu_stats: Optional[GPUStats] = None, 
        pod_name: str = "default_pod"
    ) -> WorkerResourceScaleAction:
        """
        Check whether the resource elastic trigger is ready based on average GPU utilization over a time window.
        
        基于【真实时间窗口】和【木桶效应】的 GPU 弹性触发主入口。
            逻辑流程：
            1. 从Redis 加载历史数据（带时间戳）。
            2. 将当前数据（附加当前时间戳）加入列表。
            3. 【核心】过滤数据：只保留 [当前时间 - 时间窗口] 范围内的数据。
            4. 检查保留下来的数据点数是否满足最小采样数。
            5. 计算指标并决策。
            6. 持久化状态 (关键：保留窗口数据以供下次使用)。
        
            参数:
                cpu: CPU 使用量
                memory: 内存使用量
                gpu_stats: List[GPUStats], 过去一段时间内的 GPU 监控快照列表
        """
        logger.info(f"[{pod_name}] Start GPU elastic check...")

        if self._fail_closed_on_redis_unavailable_now():
            logger.warning(
                f"[{pod_name}] Redis is required for vertical elastic state, "
                "skip resource elastic decision."
            )
            return WorkerResourceScaleAction.NOT_ADJUST

        # 0.准备上下文
        logger.info(f"  step 0: preparing context and advanced param...")
        current_time = int(time.time() * 1000) # 当前时间戳, ms
        
        # 1. 加载历史数据 (带容错)
        logger.info(f"  step 1: Loading history data...")
        history_data = self._load_history_data(job_id, pod_name)
        
        # 2. 处理当前快照 (木桶效应聚合)
        logger.info(f"  step 2: Preparing new data...")
        new_record = self._aggregate_current_gpu_stats(gpu_stats, current_time, pod_name)

        # 合并数据
        if new_record:
            history_data.append(new_record)
            # TODO 同步暴露到prometheus/异步持久化历史数据至DB
            self._save_gpu_stats_record_for_prometheum(
                job_name = job_name,
                job_uuid = job_id,
                cluster = cluster,
                namespace = namespace,
                pod_name = pod_name,
                record = new_record
            )
            self._async_save_metrics_to_db(
                job_name=job_name, 
                job_uuid=job_id, 
                cluster=cluster, 
                namespace=namespace, 
                pod_name=pod_name, 
                record=new_record
            )

        if not history_data:
            logger.warning("No history data available.")
            return WorkerResourceScaleAction.NOT_ADJUST
        logger.info(f"History data size: {len(history_data)}")

        # 3. 【核心优化】内存保护 & 时间窗口过滤
        logger.info(f"  step 3: Filtering data by time window...")
        valid_data = self._filter_time_window(history_data, current_time)
        
        # 4. 检查最小采样数
        logger.info(f"  step 4: Check min samples in window...")
        min_samples = int(self.min_samples_in_window)
        current_count = len(valid_data)

        # 检查是否满足最小采样数
        if current_count < min_samples:
            logger.warning(
                f"Insufficient samples: {current_count}/{min_samples},"
                f"check window: {self.trigger_resource_elastic_time_window}"
            )
            
            # 尝试紧急降级决策
            logger.info(f"Trying emergency decision when samples not enough...")
            is_emergency, action = self._emergency_decision_logic(valid_data, pod_name)
            
            if is_emergency:
                logger.info(f"Emergency Trigger: {action} despite insufficient samples.")

                # 即使触发了紧急动作，也要保存数据，以便后续统计
                if not self._persist_state(job_id, pod_name, valid_data):
                    logger.warning(f"[{pod_name}] emergency trigger, Redis unavailable, persist state failed, skip elastic.")
                    return WorkerResourceScaleAction.NOT_ADJUST

                self._save_elastic_decision_for_prometheum(
                    job_name = job_name,
                    job_uuid = job_id,
                    cluster = cluster,
                    namespace = namespace,
                    pod_name = pod_name,
                    action = action,
                    avg_utilization = 0.0,
                    avg_mem_utilization = 0.0,
                    sample_count = current_count,
                    emergency_trigger = is_emergency,
                    emergency_trigger_time = datetime.now(),
                )
                
                # TODO 异步持久化有效数据
                self._async_save_elastic_decision_to_db(
                    job_name=job_name, 
                    job_uuid=job_id,
                    cluster=cluster,
                    namespace=namespace, 
                    pod_name=pod_name, 
                    action=action, 
                    avg_utilization=0,
                    avg_mem_utilization=0,
                    sample_count=current_count, 
                    emergency_trigger=is_emergency, 
                    emergency_trigger_time=datetime.now(),
                )
                return action
                
            # 如果紧急逻辑也没触发，则正常保存并退出
            if not self._persist_state(job_id, pod_name, valid_data):
                logger.warning(f"[{pod_name}] Redis unavailable, persist state failed, skip elastic.")
                return WorkerResourceScaleAction.NOT_ADJUST

            self._save_elastic_decision_for_prometheum(
                job_name = job_name,
                job_uuid = job_id,
                cluster = cluster,
                namespace = namespace,
                pod_name = pod_name,
                action = WorkerResourceScaleAction.NOT_ADJUST,
                avg_utilization = 0.0,
                avg_mem_utilization = 0.0,
                sample_count = current_count,
                emergency_trigger = False,
                emergency_trigger_time = None,
            )

            # 异步持久化有效数据
            self._async_save_elastic_decision_to_db(
                job_name = job_name, 
                job_uuid = job_id,
                cluster = cluster,
                namespace = namespace, 
                pod_name = pod_name, 
                action = WorkerResourceScaleAction.NOT_ADJUST, 
                avg_utilization = 0,
                avg_mem_utilization = 0,
                sample_count = current_count, 
                emergency_trigger = is_emergency, 
                emergency_trigger_time = None,
            )

            return WorkerResourceScaleAction.NOT_ADJUST

        # 5. 数据充足，计算指标与平滑决策 (基于窗口内的最大值序列)
        logger.info(f"  step 5: Calculating average...")
        avg_util, avg_mem_util, action = self._make_decision(valid_data, pod_name)

        # 6. 持久化状态 (无论动作如何，必须保留窗口数据以维持滑动窗口逻辑)
        logger.info(f"  step 6: persist state...")
        if not self._persist_state(job_id, pod_name, valid_data):
            logger.warning(f"[{pod_name}] Redis unavailable, persist state failed, skip elastic.")
            return WorkerResourceScaleAction.NOT_ADJUST

        self._save_elastic_decision_for_prometheum(
            job_name = job_name,
            job_uuid = job_id,
            cluster = cluster,
            namespace = namespace,
            pod_name = pod_name,
            action = action,
            avg_utilization = avg_util,
            avg_mem_utilization = avg_mem_util,
            sample_count = current_count,
            emergency_trigger = False,
            emergency_trigger_time = None,
        )
        
        # 异步持久化有效数据
        self._async_save_elastic_decision_to_db(
            job_name=job_name, 
            job_uuid=job_id,
            cluster=cluster,
            namespace=namespace, 
            pod_name=pod_name, 
            action=action, 
            avg_utilization=avg_util, 
            avg_mem_utilization=avg_mem_util,
            sample_count=current_count, 
            emergency_trigger=False, 
            emergency_trigger_time=None,
        )

        logger.info(f"[{pod_name}] Check completed, action is: {action}.")
        return action
    
    def _load_history_data(self, job_id: str, pod_name: str) -> List[Dict]:
        """从 Redis 加载历史 GPU时间窗口 数据，带容错处理。"""
        if self._redis:
            data = self._load_history_data_from_shared_store(job_id, pod_name)
            if data is not None:
                return data if isinstance(data, list) else []
            return []
        return []

    def _shared_store_key(self, job_id: str, pod_name: str) -> str:
        return f"{REDIS_GPU_TIME_WINDOW_KEY_PREFIX}:{job_id}:{pod_name}"

    def _get_shared_store_ttl_seconds(self) -> int:
        ttl_from_env = int(os.getenv("DLROVER_BRAIN_GPU_TW_TTL_SECONDS", "0"))
        if ttl_from_env > 0:
            return ttl_from_env
        return max(600, int(self.trigger_resource_elastic_time_window) * 4)

    def _load_history_data_from_shared_store(self, job_id: str, pod_name: str) -> Optional[List[Dict]]:
        try:
            key = self._shared_store_key(job_id, pod_name)
            data = self._redis.get(key)
            if not data:
                return []
            parsed = json.loads(data)
            if not isinstance(parsed, list):
                logger.warning(f"Unexpected shared-store history data type for {key}.")
                return []
            return parsed
        except Exception as e:
            logger.warning(f"Load gpu time-window data from redis failed: {e}")
            return None

    def _save_history_data_to_shared_store(self, job_id: str, pod_name: str, data: List[Dict]) -> bool:
        try:
            key = self._shared_store_key(job_id, pod_name)
            if not data:
                self._redis.delete(key)
                return True
            self._redis.set(
                key,
                json.dumps(data),
                ex=self._get_shared_store_ttl_seconds(),
            )
            return True
        except Exception as e:
            logger.warning(f"Save gpu time-window data to redis failed: {e}")
            return False

    def _aggregate_current_gpu_stats(
        self, 
        gpu_stats: Optional[List[Any]], 
        current_time: float, 
        pod_name: str
    ) -> Optional[Dict]:
        """
        聚合当前 GPU 状态。
        策略：木桶效应 (取所有卡利用率的最大值)。
        """
        if not gpu_stats or not isinstance(gpu_stats, list) or len(gpu_stats) == 0:
            if gpu_stats:
                logger.warning(f"[{pod_name}] gpu_stats is not a valid non-empty list.")
            return None

        try:
            utilizations = []
            gpu_used_memory_mbs = []
            gpu_memory_utilizations = []
            for gpu in gpu_stats:
                util = getattr(gpu, 'gpu_utilization', 0.0)
                used_memory_mb = getattr(gpu, 'used_memory_mb', 0)
                total_memory_mb = getattr(gpu, 'total_memory_mb', 0)
                if total_memory_mb > 0:
                    gpu_memory_util = round((used_memory_mb / total_memory_mb) * 100.0, 2)
                
                if isinstance(util, (int, float)):
                    utilizations.append(float(util))
                if isinstance(used_memory_mb, (int, float)):
                    gpu_used_memory_mbs.append(used_memory_mb)
                if isinstance(gpu_memory_util, (int, float)):
                    gpu_memory_utilizations.append(gpu_memory_util)

            if not utilizations:
                logger.warning(f"[{pod_name}] No valid utilization values found.")
                return None
            if not gpu_used_memory_mbs:
                logger.warning(f"[{pod_name}] No valid used_memory_mb values found.")
                return None
            if not gpu_memory_utilizations:
                logger.warning(f"[{pod_name}] No valid gpu_memory_utilizations values found.")
                return None

            # 【核心策略】木桶效应：取最大值
            max_util = max(utilizations)
            max_gpu_used_memory_mb = max(gpu_used_memory_mbs)
            max_gpu_memory_util = max(gpu_memory_utilizations)
            
            record = {
                'timestamp': current_time,
                'max_gpu_utilization': max_util,
                'max_gpu_used_memory_mb': max_gpu_used_memory_mb,
                'max_gpu_memory_utilization': max_gpu_memory_util,
                'gpu_count': len(gpu_stats),
                'raw_utils': utilizations, # 保留原始数据,调试时可取消注释
                'raw_gpu_used_memory_mbs': gpu_used_memory_mbs,
                'raw_gpu_memory_utils': gpu_memory_utilizations 
            }
            # 调试完修改为debug
            logger.info(
                f"[{pod_name}] Aggregated GPU util/Used Memory/Used Memory Util: Max_util={max_util:.2f}%, "
                f"Max_used_memory={max_gpu_used_memory_mb}MB, Max_memory_util={max_gpu_memory_util:.2f}% (Count={len(utilizations)})"
            )
            
            return record

        except Exception as e:
            logger.error(f"[{pod_name}] Error aggregating GPU stats: {e}")
            return None

    def _filter_time_window(self, history_data: List[Dict], current_time: float) -> List[Dict]:
        """
        过滤出时间窗口内的有效数据，并应用内存保护限制。
        返回按时间顺序排列 (旧 -> 新) 的列表。
        """
        window_seconds = self.trigger_resource_elastic_time_window
        max_buffer = self.max_buffer_size
        cutoff_time = int(current_time - window_seconds * 1000) # 时间窗口的截止时间, ms

        valid_data = []
        
        # 从新到旧遍历，便于快速截断过期数据和限制长度 new -> old
        for item in reversed(history_data):
            ts = int(item.get('timestamp', 0))
            
            if ts < cutoff_time:
                # 由于是倒序遍历，遇到第一个过期的，后面的肯定都过期（假设时间戳有序）
                # 但为了稳健性，这里继续循环或 break 均可，这里选择 continue 以防时间戳乱序
                continue
            
            valid_data.insert(0, item) # 恢复正序 old -> new
            
            # 内存硬限制
            if len(valid_data) >= max_buffer:
                logger.warning(f"Hit max buffer size ({max_buffer}), truncating old history.")
                break
        
        return valid_data
    
    def _make_decision(self, valid_data: List[Dict], pod_name: str) -> tuple[float, float, WorkerResourceScaleAction]:
        """
        基于窗口内数据计算平均值并做出扩容/缩容决策。
        防止因为某一秒的瞬时毛刺（Spikes）导致频繁扩缩容震荡。只有当“最忙的那张卡”在“一段时间内”持续高负载时，才触发扩容。
        """
        try:
            count_util = 0
            sum_util = 0
            count_mem_util = 0
            sum_mem_util = 0
            for item in valid_data:
                if 'max_gpu_utilization' in item:
                    sum_util += item['max_gpu_utilization']
                    count_util += 1

                if 'max_gpu_memory_utilization' in item:
                    sum_mem_util += item['max_gpu_memory_utilization']
                    count_mem_util += 1
            
            # 使用窗口期内的平均值来平滑瞬时毛刺
            avg_util = (sum_util / count_util) if count_util > 0 else 0.0
            avg_mem_util = (sum_mem_util / count_mem_util) if count_mem_util > 0 else 0.0
            
            logger.info(f"[{pod_name}] Window Stats: Samples={count_util}, AvgUtil={avg_util:.2f}%, AvgMemUtil={avg_mem_util:.2f}%")

            action = WorkerResourceScaleAction.NOT_ADJUST

            util_threshold_add = self.optimize_add_gpus_utilization_threshold
            util_threshold_reduce = self.optimize_reduce_gpus_utilization_threshold

            mem_util_threshold_add = self.optimize_add_gpus_memory_utilization_threshold
            mem_util_threshold_reduce = self.optimize_reduce_gpus_memory_utilization_threshold

            # 扩容：计算利用率高 OR 显存利用率高
            if avg_util > util_threshold_add or avg_mem_util > mem_util_threshold_add:
                action = WorkerResourceScaleAction.ADD_GPUS
                reason_parts = []
                if avg_util > util_threshold_add:
                    reason_parts.append(f"AvgUtil={avg_util:.2f}% > {util_threshold_add}%")
                if avg_mem_util > mem_util_threshold_add:
                    reason_parts.append(f"AvgMemUtil={avg_mem_util:.2f}% > {mem_util_threshold_add}%")
                logger.info(f"[{pod_name}] Trigger ADD_GPUS: {' AND '.join(reason_parts)}")

            # 缩容：计算利用率低 AND 显存利用率低, 避免因为显存占用高但计算空闲时错误缩容，或者反之
            elif avg_util < util_threshold_reduce and avg_mem_util < mem_util_threshold_reduce:
                action = WorkerResourceScaleAction.REDUCE_GPUS
                logger.info(
                    f"[{pod_name}] Trigger REDUCE_GPUS: AvgUtil={avg_util:.2f}% < {util_threshold_reduce}%" 
                    f"AND AvgMemUtil={avg_mem_util:.2f}% < {mem_util_threshold_reduce}%"
                )
            
            return avg_util, avg_mem_util, action

        except Exception as e:
            logger.error(f"[{pod_name}] Decision calculation error: {e}")
            return 0.0, 0.0, WorkerResourceScaleAction.NOT_ADJUST

    def _emergency_decision_logic(self, valid_data: List[Dict], pod_name: str) -> tuple[bool, WorkerResourceScaleAction]:
        """
        当采样数据不足时，执行的降级决策逻辑。
            策略：
            1. 如果完全没有数据 -> 不操作。
            2. 如果有少量数据，但最新数据利用率极高 (木桶效应) -> 立即扩容 (防止卡死)。
        """
        if not valid_data:
            return False, WorkerResourceScaleAction.NOT_ADJUST

        # 获取最新的一条数据（通常是当前时刻的）
        latest_record = valid_data[-1]
        current_util = latest_record.get('gpu_utilization', 0.0)
        
        # 策略 A: 极端高负载立即触发 (例如 > 95%)
        # 无论采样够不够，只要单点极高，大概率是瓶颈，先扩容再说
        EMERGENCY_HIGH_THRESHOLD = self.optimize_gpu_emergency_high_utilization_threshold 
        if current_util >= EMERGENCY_HIGH_THRESHOLD:
            logger.warning(
                f"[{pod_name}] EMERGENCY: Utilization {current_util:.2f}% >= {EMERGENCY_HIGH_THRESHOLD}%. "
                f"Triggering ADD_GPUS despite insufficient samples ({len(valid_data)})."
            )
            return True, WorkerResourceScaleAction.ADD_GPUS

        # 策略 B: 数据太少且负载不高 -> 维持现状，等待更多数据
        return False, WorkerResourceScaleAction.NOT_ADJUST


    def _persist_state(self, job_id: str, pod_name: str, data: List[Dict]) -> bool:
        """
        将过滤后的 valid_data（即窗口内的有效数据）持久化到redis。
        """
        if self._redis:
            saved = self._save_history_data_to_shared_store(job_id, pod_name, data)
            if saved:
                return True
        elif self._fail_closed_on_redis_unavailable_now():
            return False

    def _fail_closed_on_redis_unavailable_now(self, stage: str = "elastic decision"):
        return redis_fail_closed_for_brain(stage=stage)

    def _save_gpu_stats_record_for_prometheum(
        self,
        job_name: str,
        job_uuid: str,
        cluster: str,
        namespace: str,
        pod_name: str,
        record: Dict
    ):
        if self._prometheus_persister is None:
            return
        try:
            self._prometheus_persister.save_metrics(
                job_name,
                job_uuid,
                cluster,
                namespace,
                pod_name,
                record,
            )
        except Exception:
            logger.exception("Failed to update vertical elastic metrics")

    def _save_elastic_decision_for_prometheum(
        self,
        job_name: str,
        job_uuid: str,
        cluster: str,
        namespace: str,
        pod_name: str,
        action: str,
        avg_utilization: float,
        avg_mem_utilization: float,
        sample_count: int,
        emergency_trigger: int,
        emergency_trigger_time: datetime,
    ):
        if self._prometheus_persister is None:
            return
        try:
            self._prometheus_persister.save_decision(
                job_name=job_name,
                job_uuid=job_uuid,
                cluster=cluster,
                namespace=namespace,
                pod_name=pod_name,
                action=action,
                avg_utilization=avg_utilization,
                avg_mem_utilization=avg_mem_utilization,
                sample_count=sample_count,
                emergency_trigger=emergency_trigger,
                emergency_trigger_time=emergency_trigger_time,
            )
        except Exception:
            logger.exception("Failed to update vertical elastic decision metrics")

    def _async_save_metrics_to_db(
        self, 
        job_name: str, 
        job_uuid: str, 
        cluster: str, 
        namespace: str, 
        pod_name: str, 
        record: Dict
    ):
        if self._mysql_persister is None:
            logger.warning("Mysql persister is not set, skip saving metrics")
            return
        self.async_mysql_persist_thread.submit(lambda: self._mysql_persister.save_metrics(
                job_name, 
                job_uuid, 
                cluster, 
                namespace, 
                pod_name, 
                record
            )
        )
       
    def _async_save_elastic_decision_to_db(
        self,
        job_name: str, 
        job_uuid: str, 
        cluster: str,
        namespace: str, 
        pod_name: str, 
        action: str, 
        avg_utilization: float,
        avg_mem_utilization: float,
        sample_count: int,
        emergency_trigger: int,
        emergency_trigger_time: datetime,
    ):
        if self._mysql_persister is None:
            logger.warning("Mysql persister is not set, skip saving elastic decision")
            return
        self.async_mysql_persist_thread.submit(lambda: self._mysql_persister.save_decision(
                job_name = job_name, 
                job_uuid = job_uuid,
                cluster = cluster,
                namespace = namespace, 
                pod_name = pod_name, 
                action = action, 
                avg_utilization = avg_utilization, 
                avg_mem_utilization = avg_mem_utilization,
                sample_count = sample_count, 
                emergency_trigger = emergency_trigger, 
                emergency_trigger_time = emergency_trigger_time,
            )
        )

    def cleanup_redis_state(self, job_id: str = "", pod_name: str = ""):
        if not self._redis or self._fail_closed_on_redis_unavailable_now():
            logger.warning(f"Redis is required for vertical elastic state, skip cleanup.")
            return

        if not job_id and not pod_name:
            logger.warning(f"Both job_id and pod_name are empty, skip cleanup.")
            return

        try:
            self._redis.delete(self._shared_store_key(job_id, pod_name))
        except Exception as e:
            logger.warning(f"Failed to cleanup redis shared store for {job_id} {pod_name}: {e}")

    def cleanup_job_state(self, job_id: str) -> bool:
        """Idempotently remove every GPU time-window key for a job."""
        if not self._redis:
            self._redis = _redis_client()
        if not self._redis:
            return False
        
        escaped_job_id = self._glob_escape(job_id)
        pattern = f"{REDIS_GPU_TIME_WINDOW_KEY_PREFIX}:{escaped_job_id}:*"
        try:
            # Delete one key at a time to avoid Redis Cluster CROSSSLOT.
            for key in self._redis.scan_iter(match=pattern, count=100):
                self._redis.delete(key)
            return True
        except Exception as e:
            logger.warning(
                f"Cleanup all GPU time-window data for {job_id} failed: {e}"
            )
            return False

    @staticmethod
    def _glob_escape(value: str) -> str:
        escaped = value.replace("\\", "\\\\")
        for char in ("*", "?", "["):
            escaped = escaped.replace(char, f"\\{char}")
        return escaped
        
        
    

    
