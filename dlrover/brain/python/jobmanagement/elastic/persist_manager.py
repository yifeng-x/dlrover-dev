# Copyright 2026 The DLRover Authors. All rights reserved.
import uuid
import json
import time
import threading
from datetime import datetime
from typing import Dict, Protocol, Set, Tuple, runtime_checkable
from dlrover.python.common.log import default_logger as logger
from prometheus_client import Gauge, Counter, start_http_server, CollectorRegistry

# DB ORM
from dlrover.brain.python.common.db import Base, get_db_session
from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, Text, BIGINT
from dlrover.python.common.constants import (
    WorkerResourceScaleAction,
)

elastic_action_map = {
    WorkerResourceScaleAction.ADD_GPUS: 1,
    WorkerResourceScaleAction.REDUCE_GPUS: 2,
}

METRIC_LABELS = (
    "job_name",
    "job_uuid",
    "cluster",
    "namespace",
    "pod_name",
)

@runtime_checkable
class IVerticalElasticMetricsPersister(Protocol):
    def save_metrics(
        self, 
        job_name: str, 
        job_uuid: str, 
        cluster: str, 
        namespace: str, 
        pod_name: str, 
        record: Dict
    ): ...

    def save_decision(
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
        emergency_trigger: bool,
        emergency_trigger_time: int,
    ): ...

    def cleanup_job(self, job_uuid: str): ...

@runtime_checkable
class IJobMetricsPersister(Protocol):
    def save_job_metrics(
        self, 
        job_name: str, 
        job_uuid: str, 
        namespace: str,
        cluster: str,
        job_feature: str,
        training_set_feature: str,
        model_feature: str,
        runtime_info: str,
        job_exit_reason: str,
        resource: str,
        customized_data: str,
        type: str,
        extra_info: str, 
    ): ...

class PrometheusMetricsPersister():
    """
    Update metrics in a caller-owned registry without opening a port.
    """

    def __init__(self, registry: CollectorRegistry):
        self._series_lock = threading.Lock()
        self._gpu_series: Dict[str, Set[Tuple[str, ...]]] = {}
        self._decision_series: Dict[str, Set[Tuple[str, ...]]] = {}
        self._decision_counter_series: Dict[
            str, Set[Tuple[str, ...]]
        ] = {}
        metric_labels = METRIC_LABELS + ("gpu_count",)

        self.max_gpu_utilization = Gauge(
            "dlrover_brain_vertical_max_gpu_utilization_percent",
            "Maximum GPU utilization in the worker pod.",
            metric_labels,
            registry=registry,
        )
        self.max_gpu_used_memory = Gauge(
            "dlrover_brain_vertical_max_gpu_used_memory_mb",
            "Maximum used GPU memory in the worker pod.",
            metric_labels,
            registry=registry,
        )
        self.max_gpu_memory_utilization = Gauge(
            "dlrover_brain_vertical_max_gpu_memory_utilization_percent",
            "Maximum GPU memory utilization in the worker pod.",
            metric_labels,
            registry=registry,
        )
        self.last_metric_update_time = Gauge(
            "dlrover_brain_vertical_metric_update_timestamp_seconds",
            "Unix timestamp of the latest worker GPU metric update.",
            metric_labels,
            registry=registry,
        )
        self.avg_gpu_utilization = Gauge(
            "dlrover_brain_vertical_decision_avg_gpu_utilization_percent",
            "Average GPU utilization used by the latest decision.",
            METRIC_LABELS,
            registry=registry,
        )
        self.avg_gpu_mem_utilization = Gauge(
            "dlrover_brain_vertical_decision_avg_gpu_memory_utilization_percent",
            "Average GPU memory utilization used by the latest decision.",
            METRIC_LABELS,
            registry=registry,
        )
        self.sample_count = Gauge(
            "dlrover_brain_vertical_decision_sample_count",
            "Number of samples used by the latest decision.",
            METRIC_LABELS,
            registry=registry,
        )
        self.emergency_trigger = Gauge(
            "dlrover_brain_vertical_decision_emergency",
            "Whether the latest decision used the emergency path.",
            METRIC_LABELS,
            registry=registry,
        )
        self.last_decision_update_time = Gauge(
            "dlrover_brain_vertical_decision_timestamp_seconds",
            "Unix timestamp of the latest vertical elastic decision.",
            METRIC_LABELS,
            registry=registry,
        )
        self.elastic_decisions = Counter(
            "dlrover_brain_vertical_decisions_total",
            "Number of vertical elastic decisions.",
            METRIC_LABELS + ("action", "emergency"),
            registry=registry,
        )

    def save_metrics(
        self,
        job_name: str,
        job_uuid: str,
        cluster: str,
        namespace: str,
        pod_name: str,
        record: Dict,
    ) -> None:
        base_labels = tuple(
            str(value)
            for value in (job_name, job_uuid, cluster, namespace, pod_name)
        )
        labels = base_labels + (str(record.get("gpu_count", 0)),)
        with self._series_lock:
            self._gpu_series.setdefault(job_uuid, set()).add(labels)
            self.max_gpu_utilization.labels(*labels).set(
                float(record.get("max_gpu_utilization", 0.0))
            )
            self.max_gpu_used_memory.labels(*labels).set(
                float(record.get("max_gpu_used_memory_mb", 0.0))
            )
            self.max_gpu_memory_utilization.labels(*labels).set(
                float(record.get("max_gpu_memory_utilization", 0.0))
            )
            self.last_metric_update_time.labels(*labels).set(time.time())

    def save_decision(
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
        emergency_trigger: bool,
        emergency_trigger_time=None,
    ) -> None:
        labels = tuple(
            str(value)
            for value in (job_name, job_uuid, cluster, namespace, pod_name)
        )
        emergency = "true" if emergency_trigger else "false"
        counter_labels = labels + (str(action), emergency)
        with self._series_lock:
            self._decision_series.setdefault(job_uuid, set()).add(labels)
            self._decision_counter_series.setdefault(job_uuid, set()).add(
                counter_labels
            )
            self.avg_gpu_utilization.labels(*labels).set(avg_utilization)
            self.avg_gpu_mem_utilization.labels(*labels).set(
                avg_mem_utilization
            )
            self.sample_count.labels(*labels).set(sample_count)
            self.emergency_trigger.labels(*labels).set(int(emergency_trigger))
            self.last_decision_update_time.labels(*labels).set(time.time())
            self.elastic_decisions.labels(*counter_labels).inc()

    def cleanup_job(self, job_uuid: str) -> None:
        """Remove all per-job series owned by this Brain instance."""
        with self._series_lock:
            gpu_series = self._gpu_series.pop(job_uuid, set())
            decision_series = self._decision_series.pop(job_uuid, set())
            counter_series = self._decision_counter_series.pop(
                job_uuid, set()
            )
            for labels in gpu_series:
                for metric in (
                    self.max_gpu_utilization,
                    self.max_gpu_used_memory,
                    self.max_gpu_memory_utilization,
                    self.last_metric_update_time,
                ):
                    metric.remove(*labels)
            for labels in decision_series:
                for metric in (
                    self.avg_gpu_utilization,
                    self.avg_gpu_mem_utilization,
                    self.sample_count,
                    self.emergency_trigger,
                    self.last_decision_update_time,
                ):
                    metric.remove(*labels)
            for labels in counter_series:
                self.elastic_decisions.remove(*labels)


class GPUMetricsRecord(Base):
    __tablename__ = "gpu_tw_metric_records"
    uid = Column(String(128), primary_key=True)
    job_name = Column(String(100))
    job_id = Column(String(128))
    cluster = Column(String(128))
    namespace = Column(String(100))
    pod_name = Column(String(100))
    record_timestamp = Column(BIGINT)
    max_gpu_utilization = Column(Float)
    max_gpu_used_memory_mb = Column(BIGINT)
    max_gpu_memory_utilization = Column(Float)
    gpu_count = Column(Integer)
    raw_utils = Column(Text)
    raw_gpu_used_memory = Column(Text)
    raw_gpu_memory_utils = Column(Text)
    created_at = Column(DateTime)
    

class ElasticDecisionRecord(Base):
    __tablename__ = "gpu_tw_metric_decision_records"
    uid = Column(String(128), primary_key=True)
    job_name = Column(String(100))
    job_id = Column(String(64))
    cluster = Column(String(128))
    namespace = Column(String(100))
    pod_name = Column(String(100))
    avg_gpu_utilization = Column(Float)
    avg_gpu_memory_utilization = Column(Float)
    sample_count = Column(Integer)
    elastic_action = Column(String(100))
    emergency_trigger = Column(Boolean)
    emergency_trigger_time = Column(BIGINT)
    created_at = Column(DateTime)

class JobMetricRecord(Base):
    __tablename__ = "job_metrics"
    uid = Column(String(128), primary_key=True)
    job_name = Column(String(100))
    job_id = Column(String(64))
    namespace = Column(String(32))
    cluster = Column(String(32))
    job_feature = Column(Text)
    training_set_feature = Column(Text)
    model_feature = Column(Text)
    job_runtime = Column(Text)
    exit_reason = Column(Text)
    optimization = Column(Text)
    resource = Column(Text)
    customized_data = Column(Text)
    type = Column(String(32))
    extra_info = Column(Text)
    created_at = Column(DateTime)

class MysqlMetricsPersister():
    def __init__(self):
        pass

    def save_metrics(
        self, 
        job_name: str, 
        job_uuid: str,
        cluster: str,
        namespace: str, 
        pod_name: str, 
        record: Dict
    ):
        # create a new session for each save_metrics call, auto managered lifetime
        with get_db_session() as session:
            try:
                with session.begin(): # auto deal commit/rollback state
                    session.add(
                        GPUMetricsRecord(
                            uid=uuid.uuid4().hex,
                            job_name=job_name,
                            job_id=job_uuid,
                            cluster=cluster,
                            namespace=namespace,
                            pod_name=pod_name,
                            record_timestamp=record["timestamp"],
                            max_gpu_utilization=record["max_gpu_utilization"],
                            max_gpu_used_memory_mb=record["max_gpu_used_memory_mb"],
                            max_gpu_memory_utilization=record["max_gpu_memory_utilization"],
                            gpu_count=record["gpu_count"],
                            raw_utils=json.dumps(record["raw_utils"]),
                            raw_gpu_used_memory=json.dumps(record["raw_gpu_used_memory_mbs"]),
                            raw_gpu_memory_utils=json.dumps(record["raw_gpu_memory_utils"]),
                            created_at=datetime.now(),
                        )
                    )
                return True # commit success
            except Exception as e:
                logger.warning(f"Mysql save metrics failed: {e}")
                return False # commit failed, auto rollback
    
    def save_decision(
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
        emergency_trigger: bool,
        emergency_trigger_time: datetime,
    ):
        with get_db_session() as session:
            try:
                with session.begin():
                    session.add(
                        ElasticDecisionRecord(
                            uid=uuid.uuid4().hex,
                            job_name=job_name,
                            job_id=job_uuid,
                            cluster=cluster,
                            namespace=namespace,
                            pod_name=pod_name,
                            avg_gpu_utilization=avg_utilization,
                            avg_gpu_memory_utilization=avg_mem_utilization,
                            sample_count=sample_count,
                            elastic_action=action,
                            emergency_trigger=emergency_trigger,
                            emergency_trigger_time=emergency_trigger_time,
                            created_at=datetime.now(),
                        )
                    )
                return True # commit success
            except Exception as e:
                logger.warning(f"Mysql save decision failed: {e}")
                return False # commit failed, auto rollback

    def save_job_metrics(
        self, 
        job_name: str, 
        job_uuid: str,
        cluster: str,
        namespace: str,
        job_feature: str,
        training_set_feature: str,
        model_feature: str,
        runtime_info: str,
        job_exit_reason: str,
        resource: str,
        customized_data: str,
        type: str,
        extra_info: str, 
    ):
        with get_db_session() as session:
            try:
                with session.begin():
                    session.add(
                        JobMetricRecord(
                            uid=uuid.uuid4().hex,
                            job_name=job_name,
                            job_id=job_uuid,
                            cluster=cluster,
                            namespace=namespace,
                            job_feature=job_feature,
                            training_set_feature=training_set_feature,
                            model_feature=model_feature,
                            job_runtime=runtime_info,
                            exit_reason=job_exit_reason,
                            resource=resource,
                            customized_data=customized_data,
                            type=type,
                            extra_info=extra_info,
                            created_at=datetime.now(),
                        )
                    )
                return True # commit success
            except Exception as e:
                logger.warning(f"Mysql save decision failed: {e}")
                return False # commit failed, auto rollback



