# Copyright 2020 The DLRover Authors. All rights reserved.
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
import time
import os
from dlrover.brain.python.common.job import JobMeta
import dlrover.brain.python.common.job_metric as brain_job_metric

from dlrover.proto import brain_pb2, brain_pb2_grpc
from dlrover.python.common.comm import build_grpc_channel, grpc_server_ready
from dlrover.python.common.log import default_logger as logger
from dlrover.brain.python.client.client import BrainClient as HttpBrainClient
import dlrover.brain.python.common.http_schemas as http_schemas
from google.protobuf import json_format

from dlrover.python.common.singleton import Singleton

DATA_STORE = "base_datastore"
OPTIMIZE_PROCESSOR = "running_training_job_optimize_request_processor"
BASE_OPTIMIZE_PROCESSOR = "base_optimize_processor"

_ENV_BRAIN_ADDR_KEY = "DLROVER_BRAIN_SERVICE_ADDR"
_ENV_BRAIN_PROTOCOL_KEY = "DLROVER_BRAIN_PROTOCOL"
_DEFAULT_BRAIN_ADDR = "dlrover-brain.dlrover.svc.cluster.local:50001"

_PROTOCOL_HTTP = "http"
_PROTOCOL_GRPC = "grpc"


def catch_exception(func):
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            logger.warning(
                "Fail to %s.%s by %s",
                self.__class__.__name__,
                func.__name__,
                e,
            )

    return wrapper


def init_job_metrics_message(job_meta):
    job_metrics = brain_pb2.JobMetrics()
    job_metrics.data_store = DATA_STORE
    job_metrics.job_meta.uuid = job_meta.uuid
    job_metrics.job_meta.name = job_meta.name
    job_metrics.job_meta.user = job_meta.user
    job_metrics.job_meta.cluster = job_meta.cluster
    job_metrics.job_meta.namespace = job_meta.namespace
    return job_metrics

class BrainClient(object):
    """
    BrainClient provides APIs to access Brain service.
    """

    def __init__(self, brain_addr, protocol="grpc"):
        """Initialize a Brain client.
        Args:
            brain_addr: grpc.Channel (gRPC) or URL string (HTTP)
                For gRPC, the gRPC channel object connects to master gRPC server.
                For HTTP, the Brain URL string

            protocol: string
                http or grpc
        """
        self._protocol = protocol
        self._brain_addr = brain_addr
        self._brain_stub = None
        if protocol == _PROTOCOL_GRPC:
            if brain_addr:
                self._brain_stub = brain_pb2_grpc.BrainStub(brain_addr)
        elif protocol == _PROTOCOL_HTTP:
            self._brain_stub = HttpBrainClient(brain_addr)

    def available(self):
        return self._brain_stub is not None

    def _set_proto_used_resource(self, pod_meta, resource):
        if resource is None or not hasattr(pod_meta, "used_resource"):
            return
        used_resource = pod_meta.used_resource
        used_resource.memory = int(getattr(resource, "memory", 0))
        used_resource.cpu = float(getattr(resource, "cpu", 0.0))
        if hasattr(used_resource, "gpu"):
            used_resource.gpu = float(getattr(resource, "gpu_num", 0.0))
        if hasattr(used_resource, "gpu_type"):
            used_resource.gpu_type = str(getattr(resource, "gpu_type", ""))
        if hasattr(used_resource, "priority"):
            used_resource.priority = str(getattr(resource, "priority", ""))

        gpu_stats = getattr(resource, "gpu_stats", []) or []
        if not gpu_stats or not hasattr(used_resource, "gpu_stats"):
            return
        proto_gpu_stats = used_resource.gpu_stats
        if hasattr(proto_gpu_stats, "add"):
            for stat in gpu_stats:
                self._set_proto_gpu_stat(proto_gpu_stats.add(), stat)

    def _set_proto_gpu_stat(self, proto_gpu_stat, gpu_stat):
        proto_gpu_stat.index = int(getattr(gpu_stat, "index", 0))
        proto_gpu_stat.total_memory_mb = int(
            getattr(gpu_stat, "total_memory_mb", 0)
        )
        proto_gpu_stat.used_memory_mb = int(
            getattr(gpu_stat, "used_memory_mb", 0)
        )
        proto_gpu_stat.gpu_utilization = float(
            getattr(gpu_stat, "gpu_utilization", 0.0)
        )

    def report_metrics(self, job_metrics):
        """Report job metrics to administer service"""
        if self._protocol == _PROTOCOL_HTTP:
            try:
                # 1. 将 protobuf 对象 序列化为 字典
                metrics_dict = json_format.MessageToDict(
                    job_metrics,
                    preserving_proto_field_name=True,  # 保持原始字段名
                )
                job_name=metrics_dict["job_meta"]['name']
                logger.info(f"Start report job-[{job_name}] metrics to brain.")
                logger.debug(f"metrics_json: {metrics_dict}")

                runtime_info = metrics_dict.get('runtime_info', {})
                if runtime_info is None:
                    logger.warning("No runtime_info in job metrics, skip report.")
                    return

                runing_pods = runtime_info.get("running_pods", [])
                if len(runing_pods) == 0:
                    logger.warning("No running_pods in job metrics, skip report.")
                    return

                http_job_metric_request = http_schemas.JobMetricsRequest()
                http_job_metric_request.job_metrics = brain_job_metric.JobMetrics(
                    time_stamp=int(time.time() * 1000),
                    data_store=metrics_dict.get('data_store', ""),
                    job_meta=brain_job_metric.JobMeta(
                        uuid=metrics_dict['job_meta']['uuid'],
                        name=job_name,
                        namespace=metrics_dict["job_meta"]['namespace'],
                        cluster=metrics_dict["job_meta"].get('cluster', ""),
                        user=metrics_dict["job_meta"].get('user', "")
                    ),
                    metrics_type=metrics_dict.get('metrics_type', ""),
                    runtime_info=brain_job_metric.RuntimeInfo(
                        global_step=metrics_dict['runtime_info'].get('global_step', 0),
                        time_stamp=metrics_dict['runtime_info'].get('time_stamp', 0),
                        speed=metrics_dict["runtime_info"].get('speed', 0.0),
                        running_pods=metrics_dict["runtime_info"]['running_pods']
                    ),
                    job_exit_reason=metrics_dict.get('job_exit_reason', "")
                )

                http_resp = self._brain_stub.report_runtime_node_stats(
                    http_job_metric_request
                )
                if http_resp is None:
                    logger.error("Job node runtime metric report failed.")
                    return
                if not http_resp.response.success:
                    logger.error(
                        "Job node runtime metric rejected by brain: %s",
                        http_resp.response.reason,
                    )
                    return
                logger.info(f"Job node runtime metric reported success.")
                return
            except Exception as e:
                logger.error(f"Job node runtime metric reported error: {str(e)}", exc_info=True)
                return
            
        return self._brain_stub.persist_metrics(job_metrics)

    def get_job_metrics(self, job_uuid):
        """Get the job metrics by the job uuid.
        Examples:
            ```
            import json

            client = build_brain_client()
            metrics_res = client.get_job_metrics("xxxx")
            metrics = json.loads(metrics_res.job_metrics)
            ```
        """
        if self._protocol == _PROTOCOL_HTTP:
            return brain_pb2.JobMetrics()
        request = brain_pb2.JobMetricsRequest()
        request.job_uuid = job_uuid
        return self._brain_stub.get_job_metrics(request)
            

    def request_optimization(self, opt_request):
        """Get the optimization plan from the processor service"""
        logger.debug("Optimization request is %s", opt_request)
        if self._protocol == "grpc":
            return self._brain_stub.optimize(opt_request)
        elif self._protocol == "http":
            http_opt_request = http_schemas.OptimizeRequest()
            http_opt_request.type = opt_request.type

            http_opt_request.config = http_schemas.OptimizeConfig(
                optimizer=opt_request.config.brain_processor,
            )
            if len(opt_request.jobs) > 0:
                http_opt_request.job = http_schemas.JobMeta(
                    uuid=opt_request.jobs[0].uid,
                    cluster=opt_request.jobs[0].cluster,
                    namespace=opt_request.jobs[0].namespace,
                )
            http_resp = self._brain_stub.optimize(http_opt_request)

            resp = brain_pb2.OptimizeResponse()

            resp.response = brain_pb2.Response()
            resp.response.success = http_resp.response.success
            resp.response.reason = http_resp.response.reason

            opt_plan = brain_pb2.JobOptimizePlan()
            job_resource = brain_pb2.JobResource()
            task_group_resources = {}
            for (
                name,
                res,
            ) in http_resp.job_opt_plan.job_resource.node_group_resources.items():
                tg_res = brain_pb2.TaskGroupResource()
                tg_res.count = res.count
                tg_res.resource.cpu = res.resource.cpu
                tg_res.resource.memory = res.resource.memory
                tg_res.resource.gpu = res.resource.gpu
                task_group_resources[name] = tg_res
            job_resource.task_group_resources = task_group_resources
            opt_plan.resource = job_resource
            resp.job_optimize_plans[0] = opt_plan

            return resp

    def report_training_hyper_params(self, job_meta, hyper_params):
        job_metrics = init_job_metrics_message(job_meta)
        job_metrics.metrics_type = brain_pb2.MetricsType.Training_Hyper_Params
        metrics = job_metrics.training_hyper_params
        metrics.batch_size = hyper_params.batch_size
        metrics.epoch = hyper_params.epoch
        metrics.max_steps = hyper_params.max_steps
        return self.report_metrics(job_metrics)

    def report_workflow_feature(self, job_meta, workflow_feature):
        job_metrics = init_job_metrics_message(job_meta)
        job_metrics.job_meta.name = workflow_feature.job_name
        job_metrics.job_meta.user = workflow_feature.user_id
        job_metrics.metrics_type = brain_pb2.MetricsType.Workflow_Feature

        metrics = job_metrics.workflow_feature
        metrics.job_name = workflow_feature.job_name
        metrics.user_id = workflow_feature.user_id
        metrics.code_address = workflow_feature.code_address
        metrics.workflow_id = workflow_feature.workflow_id
        metrics.node_id = workflow_feature.node_id
        metrics.odps_project = workflow_feature.odps_project
        metrics.is_prod = workflow_feature.is_prod
        return self.report_metrics(job_metrics)

    def report_training_set_metric(self, job_meta, dataset_metric):
        job_metrics = init_job_metrics_message(job_meta)
        job_metrics.metrics_type = brain_pb2.MetricsType.Training_Set_Feature
        metrics = job_metrics.training_set_feature
        metrics.dataset_size = dataset_metric.dataset_size
        metrics.dataset_name = dataset_metric.dataset_name
        sparse_features = dataset_metric.sparse_features
        metrics.sparse_item_count = sparse_features.item_count
        metrics.sparse_features = ",".join(sparse_features.feature_names)
        metrics.sparse_feature_groups = ",".join(
            [str(i) for i in sparse_features.feature_groups]
        )
        metrics.sparse_feature_shapes = ",".join(
            [str(i) for i in sparse_features.feature_shapes]
        )
        metrics.dense_features = ",".join(
            dataset_metric.dense_features.feature_names
        )
        metrics.dense_feature_shapes = ",".join(
            [str(i) for i in dataset_metric.dense_features.feature_shapes]
        )
        metrics.storage_size = dataset_metric.storage_size
        return self.report_metrics(job_metrics)

    def report_model_feature(self, job_meta, tensor_stats, op_stats):
        job_metrics = init_job_metrics_message(job_meta)
        job_metrics.metrics_type = brain_pb2.MetricsType.Model_Feature
        metrics = job_metrics.model_feature
        metrics.variable_count = tensor_stats.variable_count
        metrics.total_variable_size = tensor_stats.total_variable_size
        metrics.max_variable_size = tensor_stats.max_variable_size
        metrics.kv_embedding_dims.extend(tensor_stats.kv_embedding_dims)
        metrics.tensor_alloc_bytes.update(tensor_stats.tensor_alloc_bytes)
        metrics.op_count = op_stats.op_count
        metrics.update_op_count = op_stats.update_op_count
        metrics.read_op_count = op_stats.read_op_count
        metrics.input_fetch_dur = op_stats.input_fetch_dur
        metrics.flops = op_stats.flops
        metrics.recv_op_count = op_stats.recv_op_count
        return self.report_metrics(job_metrics)

    def report_node_runtime_stats(self, job_meta, namespace, runtime_metric):
        job_metrics = init_job_metrics_message(job_meta)
        job_metrics.metrics_type = brain_pb2.MetricsType.Runtime_Info
        metrics = job_metrics.runtime_info
        metrics.global_step = runtime_metric.global_step
        metrics.time_stamp = runtime_metric.timestamp
        metrics.speed = runtime_metric.speed
        for pod in runtime_metric.running_nodes:
            pod_meta = brain_pb2.PodMeta()
            pod_meta.pod_name = pod.name
            pod_meta.pod_id = pod.id
            pod_meta.host_ip = pod.host_ip
            pod_meta.host_name = pod.host_name
            pod_meta.namespace = namespace
            pod_meta.is_mixed = pod.qos == "SigmaBestEffort"
            # pod_meta.used_resource = pod.used_resource
            metrics.running_pods.append(pod_meta)
        return self.report_metrics(job_metrics)
    
    def report_node_runtime_resource_usage(self, job_meta, namespace, runtime_metric):
        logger.debug("Start report job node resource usage.")
        logger.debug(
            f"Report job node resource usage:\n"
            f"  job_meta: job_uuid: {job_meta.uuid}, job_name: {job_meta.name}, cluster: {job_meta.cluster}, namespace: {job_meta.namespace}\n"
            f"  namespace: {namespace}\n"
            f"  runtime_metric: {runtime_metric}"
        )
        if job_meta is None or namespace is None or runtime_metric is None:
            raise ValueError("job_meta, namespace, and runtime_metric must not be None")

        try:
            job_uuid = job_meta.uuid
            job_name = job_meta.name
            job_cluster = job_meta.cluster
            job_ns = job_meta.namespace
        except AttributeError as e:
            raise ValueError(f"job_meta missing required attributes: {e}")

        # 3. 快速值有效性检查 (空字符串检查)
        if not job_uuid or not job_name:
            raise ValueError("job_meta uuid, name, and cluster cannot be empty")
        
        if not isinstance(namespace, str) or not namespace:
            raise ValueError("namespace must be a non-empty string")

        # 4. 检查 running_nodes
        if not hasattr(runtime_metric, 'running_nodes'):
            raise ValueError("runtime_metric missing 'running_nodes' attribute")

        job_metrics = init_job_metrics_message(job_meta)
        job_metrics.metrics_type = brain_pb2.MetricsType.Runtime_Info
        metrics = job_metrics.runtime_info
        for pod in runtime_metric.running_nodes:
            pod_meta = brain_pb2.PodMeta()
            pod_meta.pod_name = pod.name
            pod_meta.host_name = pod.host_name
            pod_meta.node_ip = pod.host_ip
            pod_meta.namespace = namespace
            # pod_meta.pod_ip = pod.used_resource.to_json()
            self._set_proto_used_resource(
                pod_meta, getattr(pod, "used_resource", None)
            )
            metrics.running_pods.append(pod_meta)
        return self.report_metrics(job_metrics)

    def get_optimization_plan(self, job_uuid, stage, opt_retriever, config={}):
        request = brain_pb2.OptimizeRequest()
        request.type = stage
        request.config.optimizer_config_retriever = opt_retriever
        request.config.data_store = DATA_STORE
        request.config.brain_processor = OPTIMIZE_PROCESSOR
        for key, value in config.items():
            request.config.customized_config[key] = value
        request.jobs.add()
        request.jobs[0].uid = job_uuid
        return self.request_optimization(request)

    def get_optimizer_resource_plan(self, job_uuid, opt_retriever, config={}):
        request = brain_pb2.OptimizeRequest()
        request.config.optimizer_config_retriever = opt_retriever
        request.config.data_store = DATA_STORE
        request.config.brain_processor = BASE_OPTIMIZE_PROCESSOR
        for key, value in config.items():
            request.config.customized_config[key] = value
        request.jobs.add()
        request.jobs[0].uid = job_uuid
        return self.request_optimization(request)

    def get_oom_resource_plan(
        self, nodes, job_uuid, stage, opt_retriever, config={}
    ):
        request = brain_pb2.OptimizeRequest()
        request.type = stage
        request.config.optimizer_config_retriever = opt_retriever
        request.config.data_store = DATA_STORE
        request.config.brain_processor = OPTIMIZE_PROCESSOR
        for key, value in config.items():
            request.config.customized_config[key] = value
        request.jobs.add()
        job = request.jobs[0]
        job.uid = job_uuid
        for node in nodes:
            job.state.pods[node.name].is_oom = True
            job.state.pods[node.name].name = node.name
        return self.request_optimization(request)

    def report_job_exit_reason(self, job_meta, reason):
        job_metrics = init_job_metrics_message(job_meta)
        job_metrics.metrics_type = brain_pb2.MetricsType.Job_Exit_Reason
        job_metrics.job_exit_reason = reason
        return self.report_metrics(job_metrics)

    @catch_exception
    def get_config(self, key):
        request = brain_pb2.ConfigRequest()
        request.config_key = key
        response = self._brain_stub.get_config(request)
        if response.response.success:
            return response.config_value
        return None

    def supports_master_heartbeat(self):
        return self._protocol == _PROTOCOL_HTTP and self.available()

    def _master_control_request(
        self, request_type, method, master_addr, job_name, job_uuid,
        namespace, cluster,
    ):
        if not self.supports_master_heartbeat():
            return False
        request = request_type(
            job=JobMeta(
                uuid=job_uuid, name=job_name, namespace=namespace,
                cluster=cluster,
            ),
            master_addr=master_addr,
        )
        response = getattr(self._brain_stub, method)(request)
        return response is not None and response.response.success

    def register_to_brain(self, mast_addrs, job_name, job_uuid, namespace, cluster):
        return self._master_control_request(
            http_schemas.RegisterMasterClientRequest, "register_master_client",
            mast_addrs, job_name, job_uuid, namespace, cluster,
        )

    def report_master_heartbeat(
        self, mast_addrs, job_name, job_uuid, namespace, cluster
    ):
        return self._master_control_request(
            http_schemas.MasterHeartBeatRequest, "report_heart_beat",
            mast_addrs, job_name, job_uuid, namespace, cluster,
        )

    def get_optimization_plan_from_brain(self, job_name, job_uuid, namespace, cluster, config={}):
        if self._protocol == _PROTOCOL_HTTP:
            http_optimize_request = http_schemas.OptimizeRequest()
            http_optimize_request.job = JobMeta(
                uuid=job_uuid,
                name=job_name,
                namespace=namespace,
                cluster=cluster,
            )
            http_optimize_request.config = config

            return self._brain_stub.optimize(http_optimize_request)

def build_brain_client():
    """Build a client of the EasyDL server.

    Example:
        ```
        import os
        os.environ["DLROVER_BRAIN_SERVICE_ADDR"] = "xxx"
        os.environ["PROTOCOL"] = "grpc" or "http"
        client = build_brain_client()
        ```
    """
    brain_addr = os.getenv(_ENV_BRAIN_ADDR_KEY, _DEFAULT_BRAIN_ADDR)
    # protocol = os.getenv(_ENV_BRAIN_PROTOCOL_KEY, "grpc")
    protocol = os.getenv(_ENV_BRAIN_PROTOCOL_KEY, "http")

    if protocol == "http":
        return BrainClient(brain_addr, protocol)
    else:
        channel = build_grpc_channel(brain_addr)
        if channel and grpc_server_ready(channel):
            return BrainClient(channel, protocol)
        else:
            logger.info("The GRPC service of brain is not available.")
            return BrainClient(None)


class GlobalBrainClient(object):
    BRAIN_CLIENT = build_brain_client()
