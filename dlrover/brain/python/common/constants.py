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


class Node(object):
    NODE_TYPE_WORKER = "worker"


class DefaultResource(object):
    WORKER_CPU = 4
    WORKER_MEM = 8 * 1024 * 1024

class UnitConvertor(object):
    GIB_TO_BYTES = 1024 ** 3

class MetricsType(object):
    Training_Hyper_Params = "Training_Hyper_Params"
    job_Feature = "job_Feature"
    Training_Set_Feature = "Training_Set_Feature"
    Model_Feature = "Model_Feature"
    Runtime_Info = "Runtime_Info"
    Job_Exit_Reason = "Job_Exit_Reason"

class WorkerResourceScaleAction(object):
    REDUCE_GPUS = "reduce_gpus"
    ADD_GPUS = "add_gpus"
    MANUAL = "manual"
    NOT_ADJUST = "not_adjust"

class OptConfigKeys(object):
    OPTIMIZE_ADD_GPUS_UTILIZATION_THRESHOLD = "optimize_add_gpus_utilization_threshold"
    OPTIMIZE_REDUCE_GPUS_UTILIZATION_THRESHOLD = "optimize_reduce_gpus_utilization_threshold"
    OPTIMIZE_GPU_EMERGENCY_HIGH_UTILIZATION_THRESHOLD = "optimize_gpu_emergency_high_utilization_threshold"

    OPTIMIZE_ADD_GPUS_MEMORY_UTILIZATION_THRESHOLD = "optimize_add_gpus_memory_utilization_threshold"
    OPTIMIZE_REDUCE_GPUS_MEMORY_UTILIZATION_THRESHOLD = "optimize_reduce_gpus_memory_utilization_threshold"
    OPTIMIZE_GPU_EMERGENCY_HIGH_MEMORY_UTILIZATION_THRESHOLD = "optimize_gpu_emergency_high_memory_utilization_threshold"
    
    TRIGGER_RESOURCE_ELASTIC_TIME_WINDOW = "trigger_resource_elastic_time_window"
    MIN_SAMPLES_IN_WINDOW = "min_samples_in_window"
    MAX_BUFFER_SIZE = "max_buffer_size"

class DefaultOptValues(object):
    """GPU/NPU elastic config"""
    OPTIMIZE_ADD_GPUS_UTILIZATION_THRESHOLD = 10
    OPTIMIZE_REDUCE_GPUS_UTILIZATION_THRESHOLD = 5
    OPTIMIZE_GPU_EMERGENCY_HIGH_UTILIZATION_THRESHOLD = 95

    OPTIMIZE_ADD_GPUS_MEMORY_UTILIZATION_THRESHOLD = 20
    OPTIMIZE_REDUCE_GPUS_MEMORY_UTILIZATION_THRESHOLD = 5
    OPTIMIZE_GPU_EMERGENCY_HIGH_MEMORY_UTILIZATION_THRESHOLD = 95

    TRIGGER_RESOURCE_ELASTIC_TIME_WINDOW = 300  # seconds
    MIN_SAMPLES_IN_WINDOW = 25
    MAX_BUFFER_SIZE = 200

class PersistBackendType(object):
    """
    Persist backend type
    """
    MYSQL = "mysql"
    PROMETHEUS = "prometheus"
    PERSIST_BACKEND_TYPE_NONE = "none"
