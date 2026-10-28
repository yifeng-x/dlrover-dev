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

from pydantic import (
    Field,
    BaseModel,
)
from typing import Dict,List
from dlrover.brain.python.common.job import JobMeta
from dlrover.brain.python.common.constants import MetricsType

class TrainingSetFeature(BaseModel):
    dataset_size: int = 0
    dataset_name: str = ""
    sparse_item_count: int = 0
    sparse_features: str = ""
    sparse_feature_groups: str = ""
    sparse_feature_shapes: str = ""
    dense_features: str = ""
    dense_feature_shapes: str = ""
    storage_size: int = 0

class ModelFeature(BaseModel):
    model_name: str = ""
    variable_count: int = 0
    op_count: int = 0
    embedding_dimension: int = 0
    total_variable_size: int = 0
    max_variable_size: int = 0
    update_op_count: int = 0
    read_op_count: int = 0
    input_fetch_dur: int = 0
    flops: int = 0
    recv_op_count: int = 0

class JobFeature(BaseModel):
    job_name: str = ""
    job_id: str = ""
    user_id: str = ""
    job_type: str = ""

class GPUStats(BaseModel):
    index: int = 0
    total_memory_mb: int = 0
    used_memory_mb: int = 0
    gpu_utilization: float = 0.0
    
class PodResource(BaseModel):
    memory: int = 0
    cpu: float = 0.0
    gpu: float = 0.0
    gpu_type: str = ""
    priority: str = ""
    gpu_stats: List[GPUStats] = []

class PodMeta(BaseModel):
    pod_name: str = ""
    pod_ip: str = ""
    node_ip: str = ""
    host_name: str = ""
    namespace: str = ""
    is_mixed: bool = False
    used_resource: PodResource = PodResource()

class RuntimeInfo(BaseModel):
    global_step: int = 0
    running_pods: List[PodMeta] = []
    time_stamp: int = 0
    speed: float = 0.0

class JobMetrics(BaseModel):
    time_stamp: int = 0 # ms
    data_store: str = ""
    job_meta: JobMeta = JobMeta()
    metrics_type: str = MetricsType.Runtime_Info
    job_feature: JobFeature = JobFeature()
    training_set_feature: TrainingSetFeature = TrainingSetFeature()
    model_feature: ModelFeature = ModelFeature()
    runtime_info: RuntimeInfo = RuntimeInfo()
    job_exit_reason: str = ""
    extra_info: str = ""
    type: str = ""
    resource: str = ""
    customized_data: str = ""