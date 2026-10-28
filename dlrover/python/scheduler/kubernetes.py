# Copyright 2022 The DLRover Authors. All rights reserved.
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

import json
import os
import time
from typing import Dict, Optional

from kubernetes import client, config
from kubernetes.utils.quantity import parse_quantity

from dlrover.python.common.constants import (
    DefaultResourceLimits,
    ElasticJobLabel,
    NodeType,
    OptimizeMode,
    k8sAPIExceptionReason,
)
from dlrover.python.common.global_context import Context
from dlrover.python.common.log import default_logger as logger
from dlrover.python.common.node import NodeGroupResource, NodeResource
from dlrover.python.common.singleton import Singleton
from dlrover.python.scheduler.job import ElasticJob, JobArgs, NodeArgs
from dlrover.python.master.elasticity_policy import (
    POLICY_ANNOTATION,
    policy_record,
    resolve_policy,
    training_container,
)

NODE_SERVICE_PORTS = {
    NodeType.WORKER: 3333,
    NodeType.EVALUATOR: 3333,
    NodeType.CHIEF: 3333,
    NodeType.PS: 2222,
    NodeType.MASTER: 3333,
    NodeType.DLROVER_MASTER: 50001,
}

UCP_SERVICE_ANNOTATION = "dlrover/ucp-service"
LEGACY_UCP_SERVICE_ANNOTATION = "ucpService"


def _get_ucp_service_config(job):
    spec_config = job.get("spec", {}).get("ucpService", {})
    annotations = job.get("metadata", {}).get("annotations", {}) or {}
    annotation_config = (
        annotations.get(UCP_SERVICE_ANNOTATION)
        or annotations.get(LEGACY_UCP_SERVICE_ANNOTATION)
        or ""
    )
    if not annotation_config:
        return spec_config
    try:
        config = json.loads(annotation_config)
        if isinstance(config, dict):
            return config
        logger.warning(
            "Ignore UCP service annotation because it is not a JSON object."
        )
    except Exception:
        logger.warning(
            "Ignore invalid UCP service annotation: %s", annotation_config
        )
    return spec_config

JOB_SUFFIX = "-edljob-"
USER_AGENT = "dlrover/29.0.0/python"

_dlrover_context = Context.singleton_instance()


def convert_memory_to_mb(memory: str):
    return int(parse_quantity(memory) / 1024 / 1024)


def convert_memory_to_byte(memory: str):
    return parse_quantity(memory)


def convert_cpu_to_decimal(cpu: str):
    return round(float(parse_quantity(cpu)), 1)


def parse_bool(s: str):
    return s.lower() in ["true", "yes", "t", "y"]


def get_pod_name(job_name, pod_type, node_id):
    return "%s-%s" % (job_name + JOB_SUFFIX + pod_type, str(node_id))


def get_main_container(pod: client.V1Pod):
    if len(pod.spec.containers) == 1:
        return pod.spec.containers[0]
    else:
        for container in pod.spec.containers:
            if container.name == "main":
                return container


def set_container_resource(
    container: client.V1Container,
    res_requests: NodeResource,
    res_limits: NodeResource,
):
    def _update_resource_dict(resource_dict, node_resource):
        resource_dict.pop(None, None)
        resource_dict["cpu"] = node_resource.cpu
        resource_dict["memory"] = f"{node_resource.memory}Mi"
        if node_resource.gpu_type and node_resource.gpu_num > 0:
            resource_dict[node_resource.gpu_type] = node_resource.gpu_num
        return resource_dict

    if container.resources is None:
        container.resources = client.V1ResourceRequirements(
            requests=_update_resource_dict({}, res_requests),
            limits=_update_resource_dict({}, res_limits),
        )
        logger.info(f"[Set container resource] container resource is none, will use new resource.\n"
            f"  container limits: {container.resources.limits}\n"
            f"  container requests: {container.resources.requests}\n"
        )
    else:
        res = container.resources
        if res.requests:
            res.requests = _update_resource_dict(res.requests, res_requests)
        else:
            res.requests = _update_resource_dict({}, res_requests)

        if res.limits:
            res.limits = _update_resource_dict(res.limits, res_limits)
        else:
            res.limits = _update_resource_dict({}, res_limits)
        logger.info(f"[Set container resource] container resource is not none, will adjust limits and requests resource.\n"
            f"container limits resource:\n"
            f"  limits cpu: {res_limits.cpu}\n"
            f"  limits memory: {res_limits.memory}Mi\n"
            f"  limits gpu: {res_limits.gpu_type or 'none'}={res_limits.gpu_num}\n"
            f"container requests resource:\n"
            f"  requests cpu: {res_requests.cpu}\n"
            f"  requests memory: {res_requests.memory}Mi\n"
            f"  requests gpu: {res_requests.gpu_type or 'none'}={res_requests.gpu_num}\n"
        )

def retry_k8s_request(func):
    def wrapper(self, *args, **kwargs):
        retry = kwargs.get("retry", 5)
        exception = None
        for _ in range(retry):
            try:
                return func(self, *args, **kwargs)
            except client.rest.ApiException as e:
                if e.reason == k8sAPIExceptionReason.NOT_FOUND:
                    return None
                exception = e
                time.sleep(3)
            except Exception as e:
                exception = e
                time.sleep(3)
        if exception:
            logger.error("Fail to execute %s: %s", func.__name__, exception)
            return None

    return wrapper


class k8sClient(Singleton):
    def __init__(self, namespace):
        """
        DLRover k8s client.

        Args:
            image_name: Docker image path for DLRover pod.
            namespace: The name of the Kubernetes namespace where DLRover
                pods will be created.
            event_callback: If not None, an event watcher will be created and
                events passed to the callback.
            periodic_call_func: If not None, call this method periodically.
        """
        try:
            if os.getenv("KUBERNETES_SERVICE_HOST"):
                # We are running inside a k8s cluster
                config.load_incluster_config()
                logger.info("Load the incluster config.")
            else:
                # Use user's kube config
                config.load_kube_config()
                logger.info("Load the kube config file.")
        except Exception as ex:
            logger.error(
                "Failed to load configuration for Kubernetes:\n%s", ex
            )

        self.client = client.CoreV1Api()
        self.apps_client = client.AppsV1Api()
        self.setup_user_agent()
        self.api_instance = client.CustomObjectsApi()
        self._namespace = namespace

    def setup_user_agent(self):
        self.client.api_client.user_agent = USER_AGENT

    @property
    def api_client(self):
        return self.client.api_client

    @retry_k8s_request
    def list_namespaced_pod(self, label_selector):
        """List the pods in the namespace with the label selector.

        Args:
            label_selector: str like "label0=value0,lable1=value1"
        """
        pod_list = self.client.list_namespaced_pod(
            self._namespace,
            label_selector=label_selector,
            timeout_seconds=60,
        )
        return pod_list

    @retry_k8s_request
    def create_custom_resource(self, group, version, plural, body):
        self.api_instance.create_namespaced_custom_object(
            group=group,
            version=version,
            namespace=self._namespace,
            plural=plural,
            body=body,
        )

    @retry_k8s_request
    def patch_custom_resource(self, group, version, plural, name, body):
        self.api_instance.patch_namespaced_custom_object(
            group=group,
            version=version,
            namespace=self._namespace,
            plural=plural,
            name=name,
            body=body,
        )

    def delete_custom_resource(self, group, version, plural, name):
        try:
            self.api_instance.delete_namespaced_custom_object(
                group=group,
                version=version,
                namespace=self._namespace,
                plural=plural,
                name=name,
            )
        except client.rest.ApiException as e:
            if e.reason != k8sAPIExceptionReason.NOT_FOUND:
                logger.error("Fail to delete %s", name)

    @retry_k8s_request
    def get_custom_resource(self, name, group, version, plural):
        crd_object = self.api_instance.get_namespaced_custom_object(
            namespace=self._namespace,
            name=name,
            group=group,
            version=version,
            plural=plural,
        )
        return crd_object

    @retry_k8s_request
    def get_configmap(self, name):
        configmap = self.client.read_namespaced_config_map(
            namespace=self._namespace, name=name
        )
        return configmap

    def create_pod(self, pod):
        try:
            self.client.create_namespaced_pod(self._namespace, pod)
            return True
        except client.rest.ApiException as e:
            logger.warning(
                "Failed to create %s pod: %s\n", pod.metadata.name, e
            )
            return False

    @retry_k8s_request
    def get_pod(self, name):
        """Get the pod with the pod name:

        Args:
            name: str, the pod name.
        """
        return self.client.read_namespaced_pod(
            namespace=self._namespace, name=name
        )

    def delete_pod(self, name):
        """Delete the pod with the pod name:

        Args:
            name: str, the pod name.
        """
        try:
            self.client.delete_namespaced_pod(
                name,
                self._namespace,
                body=client.V1DeleteOptions(),
            )
            return True
        except client.ApiException as e:
            if e.reason == k8sAPIExceptionReason.NOT_FOUND:
                return True
            logger.warning("Exception when removing pod %s: %s\n" % (name, e))
            return False

    @retry_k8s_request
    def patch_labels_to_pod(self, name, labels: Dict[str, str]):
        """Patch labels to a Pod.

        Args:
            name: str, the pod name.
            labels: dict, the key and value are str.
        """
        body = {"metadata": {"labels": labels}}
        return self.client.patch_namespaced_pod(
            name=name, namespace=self._namespace, body=body
        )

    @retry_k8s_request
    def patch_annotations_to_pod(self, name, annotations: Dict[str, str]):
        """Patch annotations to a Pod.

        Args:
            name: str, the pod name.
            annotations: dict, the key and value are str.
        """
        body = {"metadata": {"annotations": annotations}}
        return self.client.patch_namespaced_pod(
            name=name, namespace=self._namespace, body=body
        )

    @retry_k8s_request
    def get_pod_annotation(self, name, key) -> str:
        """Get a specific annotation value of a Pod.

        Args:
            name: str, the pod name.
            key: str, the annotation key.

        Returns:
            str, the annotation value. Empty string if the key is not found.
        """
        pod = self.client.read_namespaced_pod(
            namespace=self._namespace, name=name
        )
        annotations = pod.metadata.annotations or {}
        return annotations.get(key, "")

    def cordon_node(self, node_name):
        try:
            body = {"spec": {"unschedulable": True}}
            self.client.patch_node(node_name, body)
            return True
        except Exception as e:
            logger.error(f"Failed to patch node {e}")
            return False

    def create_service(self, service: client.V1Service):
        """Create a service

        Args:
            service: client.V1Service.
        """
        try:
            self.client.create_namespaced_service(self._namespace, service)
            return True
        except client.rest.ApiException as e:
            logger.warning(
                "Failed to create %s service: %s\n"
                % (service.metadata.name, e)
            )
            return False

    def create_deployment(self, deployment: client.V1Deployment):
        name = (
            deployment.get("metadata", {}).get("name")
            if isinstance(deployment, dict)
            else deployment.metadata.name
        )
        try:
            self.apps_client.create_namespaced_deployment(
                self._namespace, deployment
            )
            return True
        except client.rest.ApiException as e:
            if e.reason == k8sAPIExceptionReason.ALREADY_EXISTS:
                logger.info("Deployment %s already exists.", name)
                return True
            logger.warning(
                "Failed to create %s deployment: %s\n",
                name,
                e,
            )
            return False

    def patch_service(self, name, service: client.V1Service):
        """Patch a service

        Args:
            name: str, the service name.
            service: client.V1Service.
        """
        try:
            self.client.patch_namespaced_service(
                name, self._namespace, service
            )
            return True
        except client.rest.ApiException as e:
            logger.warning("Failed to patch %s service: %s\n" % (name, e))
            return False

    @retry_k8s_request
    def get_service(self, name):
        """Get a k8s service object.

        Args:
            name: str, the service name.
        """
        return self.client.read_namespaced_service(
            name=name,
            namespace=self._namespace,
        )

    def create_pvc(self, pvc):
        try:
            self.client.create_namespaced_persistent_volume_claim(
                self._namespace, pvc
            )
            return True
        except client.rest.ApiException as e:
            logger.warning(
                "Failed to create %s persistent volume claim: %s\n"
                % (pvc.metadata.name, e)
            )
            return False

    @classmethod
    def create_owner_reference(cls, api_version, kind, name, uid):
        owner_ref = client.V1OwnerReference(
            api_version=api_version,
            block_owner_deletion=True,
            kind=kind,
            name=name,
            uid=uid,
        )
        return owner_ref


class K8sElasticJob(ElasticJob):
    def __init__(self, job_name, namespace):
        """
        ElasticJob manages Pods by K8s Python APIs. The example of an elastic
        job is in dlrover/go/elasticjob_operator/config/samples/
        elastic_v1alpha1_elasticjob.yaml
        Args:
            image_name: Docker image path for DLRover pod.
            namespace: The name of the Kubernetes namespace where DLRover
                pods will be created.
            job_name: DLRover job name, should be unique in the namespace.
                Used as pod name prefix and value for "elastic" label.
        """
        self._k8s_client = k8sClient.singleton_instance(namespace)
        self._namespace = namespace
        self._job_name = job_name

    def get_node_name(self, type, id):
        return get_pod_name(self._job_name, type, id)

    def get_node_service_addr(self, type, id):
        service_name = get_pod_name(self._job_name, type, id)
        return "%s.%s.svc:%d" % (
            service_name,
            self._namespace,
            NODE_SERVICE_PORTS[type],
        )


class K8sJobArgs(JobArgs):
    def __init__(self, platform, namespace, job_name):
        super(K8sJobArgs, self).__init__(platform, namespace, job_name)

    def initilize(self):
        self.user = os.getenv("USER", "")
        k8s_client = k8sClient.singleton_instance(self.namespace)
        job = self._retry_to_get_job(k8s_client)
        self.raw_job = job
        self.ucp_service = _get_ucp_service_config(job)
        self.job_uuid = self._get_job_uuid(job)

        policy = None
        if self.requested_elastic_dimension == "auto":
            policy = resolve_policy(
                job,
                self.elastic_explicit_switches.get("resource_monitor_switch"),
                self.elastic_explicit_switches.get(
                    "configmap_manual_scale_switch"
                ),
                self.xpu_type_name,
            )
        
        if "distributionStrategy" in job["spec"]:
            self.distribution_strategy = job["spec"]["distributionStrategy"]
        limit_config = job["spec"].get("resourceLimits", {})
        self.resource_limits.cpu = convert_cpu_to_decimal(
            limit_config.get("cpu", DefaultResourceLimits.CPU_LIMIT)
        )
        self.resource_limits.memory = convert_memory_to_byte(
            limit_config.get("memory", DefaultResourceLimits.MEMORY_LIMIT)
        )
        self.resource_limits.gpu_num = int(
            limit_config.get("gpu", DefaultResourceLimits.GPU_LIMIT)
        )
        self.optimize_mode = job["spec"].get(
            "optimizeMode", OptimizeMode.SINGLE_JOB
        )
        self.enable_suspended = job["spec"].get("suspended", False)
        annotations = job.get("metadata", {}).get("annotations", {}) or {}
        self.run_mode = str(
            job["spec"].get(
                "runMode", annotations.get("dlrover.io/run-mode", "")
            )
        ).lower()
        self.workspace_exit_policy = str(
            annotations.get("dlrover.io/workspace-exit-policy", "retain")
        ).lower()

        for replica, spec in job["spec"]["replicaSpecs"].items():
            if replica == NodeType.WORKER:
                restart_policy = spec["template"]["spec"].get(
                    "restartPolicy", ""
                )
                self.relaunch_always = restart_policy == "Always"

            priority = spec.get("priority", "")
            num = int(spec.get("replicas", 0))
            container = (
                training_container(spec)
                if policy and replica == NodeType.WORKER
                else spec["template"]["spec"]["containers"][0]
            )
            resources = container.get("resources", {})
            requests = resources.get("requests", {}) or {}
            if policy and replica == NodeType.WORKER:
                requests = {**(resources.get("limits", {}) or {}), **requests}
            cpu = convert_cpu_to_decimal(requests.get("cpu", 0))
            if "memory" in requests:
                memory = convert_memory_to_mb(requests["memory"])
            else:
                memory = 0
            gpu_type = None
            gpu_num = 0
            if policy and replica == NodeType.WORKER:
                gpu_type = policy["gpu_resource_key"]
                gpu_num = policy["initial_gpu_per_worker"]
            else:
                for k, v in requests.items():
                    if "nvidia.com" in k or "huawei.com" in k or "metax-tech.com" in k or "kunlunxin.com" in k:
                        gpu_type = k
                        gpu_num = int(v)
            
            group_resource = NodeGroupResource(
                num,
                NodeResource(
                    cpu=cpu,
                    memory=memory,
                    gpu_type=gpu_type,
                    gpu_num=gpu_num,
                    priority=priority,
                ),
            )
            restart_count = int(spec.get("restartCount", 3))
            auto_scale = parse_bool(str(spec.get("autoScale", "true")))
            restart_timeout = int(spec.get("restartTimeout", 0))
            critical_nodes = spec.get("criticalNodes", "")
            self.node_args[replica] = NodeArgs(
                group_resource,
                auto_scale,
                restart_count,
                restart_timeout,
                critical_nodes,
                _dlrover_context.seconds_to_timeout_task_process,
            )
        if policy:
            self._save_startup_elastic_policy(k8s_client, job, policy)
            self.runtime_elastic_dimension = policy["elastic_dimension"]
            for key in (
                "elastic_dimension",
                "resource_monitor_switch",
                "configmap_manual_scale_switch",
            ):
                setattr(self, key, policy[key])
            logger.info(
                "Startup elasticity policy: %s",
                json.dumps(policy, sort_keys=True),
            )
        logger.info("Job args = %s", self.__dict__)

    def _save_startup_elastic_policy(self, k8s_client, job, policy):
        # Use the API directly: the generic retry helper swallows failures.
        api = k8s_client.api_instance
        original_spec = job["spec"]
        record = policy_record(policy)
        kwargs = dict(
            group="elastic.iml.github.io",
            version="v1alpha1",
            namespace=self.namespace,
            plural="elasticjobs",
            name=self.job_name,
        )
        for attempt in range(3):
            metadata = job["metadata"]
            raw = (metadata.get("annotations") or {}).get(POLICY_ANNOTATION)
            if raw is not None:
                try:
                    saved = policy_record(json.loads(raw))
                except (KeyError, TypeError, ValueError) as exc:
                    raise ValueError(
                        "POLICY_RECOVERY_CONFLICT: invalid policy annotation"
                    ) from exc
                if saved != record:
                    raise ValueError(
                        "POLICY_RECOVERY_CONFLICT: effective elasticity controls changed; "
                        "resubmit the job instead of switching a running job"
                    )
                logger.info(
                    "Restore startup elasticity policy for %s", self.job_uuid
                )
                return
            # Do not reclassify an already-running job after losing its policy.
            selector = (
                f"{ElasticJobLabel.JOB_KEY}={self.job_name},"
                f"{ElasticJobLabel.REPLICA_TYPE_KEY}={NodeType.WORKER}"
            )
            pods = k8s_client.client.list_namespaced_pod(
                self.namespace, label_selector=selector, _request_timeout=30
            )
            if pods.items:
                raise ValueError(
                    "POLICY_RECOVERY_CONFLICT: workers exist without startup policy"
                )
            if not metadata.get("resourceVersion"):
                raise ValueError(
                    "POLICY_RECOVERY_CONFLICT: resourceVersion is missing"
                )
            body = {
                "metadata": {
                    "resourceVersion": metadata["resourceVersion"],
                    "uid": self.job_uuid,
                    "annotations": {
                        POLICY_ANNOTATION: json.dumps(record, sort_keys=True)
                    },
                }
            }
            try:
                api.patch_namespaced_custom_object(
                    **kwargs, body=body, _request_timeout=30
                )
                return
            except client.rest.ApiException as exc:
                if exc.status != 409 or attempt == 2:
                    raise
                job = api.get_namespaced_custom_object(
                    **kwargs, _request_timeout=30
                )
                candidate = resolve_policy(
                    job,
                    self.elastic_explicit_switches.get(
                        "resource_monitor_switch"
                    ),
                    self.elastic_explicit_switches.get(
                        "configmap_manual_scale_switch"
                    ),
                    self.xpu_type_name,
                )
                if candidate != policy or job["spec"] != original_spec:
                    raise ValueError(
                        "POLICY_RECOVERY_CONFLICT: job changed during startup"
                    )

    def _retry_to_get_job(self, k8s_client: k8sClient):
        for _ in range(3):
            job = k8s_client.get_custom_resource(
                name=self.job_name,
                group="elastic.iml.github.io",
                version="v1alpha1",
                plural="elasticjobs",
            )
            if job:
                return job
            else:
                time.sleep(5)
        raise ValueError("Cannot get the training job %s" % self.job_name)

    def _get_job_uuid(self, job):
        if job and "uid" in job["metadata"]:
            return job["metadata"]["uid"]
        return ""


class k8sServiceFactory(object):
    """
    The factory creates the k8s service for Pods.

    Arguments:
        namespace (str): the namespace on a k8s cluster.
        job_name (str): the name of an ElasticJob.
    """

    def __init__(self, namespace: str, job_name: str):
        self._namespace = namespace
        self._job_name = job_name
        self._k8s_client = k8sClient.singleton_instance(self._namespace)

    def create_service(
        self,
        name: str,
        port: int,
        target_port: int,
        selector: Dict[str, str],
        owner_ref: client.V1OwnerReference,
        retry_num=5,
        patch_if_exists: bool = True,
        port_name="grpc",
        service_type=None,
        node_port=None,
        labels: Optional[Dict[str, str]] = None,
    ):
        """
        Create a new service if the service dose not exist, otherwise
        the method patch the service with modifications.

        Args:
            labels: extra labels merged into the Service metadata (both
                labels and annotations). Keys here override the default
                labels. When None, only the default labels are applied.
        """
        service = self._create_service_obj(
            name=name,
            port=port,
            target_port=target_port,
            selector=selector,
            owner_ref=owner_ref,
            port_name=port_name,
            service_type=service_type,
            node_port=node_port,
            new_labels=labels,
        )

        svc = self.get_service(name)
        if not svc:
            return self._create_new_service(service, retry_num)
        else:
            if patch_if_exists:
                return self._patch_service(name, service, retry_num)
            return True

    def get_service(self, name):
        return self._k8s_client.get_service(name)

    def _create_new_service(self, service: client.V1Service, retry_num: int):
        for _ in range(retry_num):
            succeed = self._k8s_client.create_service(service)
            if succeed:
                return succeed
            time.sleep(5)
        return False

    def _patch_service(
        self, name: str, service: client.V1Service, retry_num: int
    ):
        for _ in range(retry_num):
            succeed = self._k8s_client.patch_service(name, service)
            if succeed:
                return succeed
            time.sleep(5)
        return False

    def _create_service_obj(
        self,
        name: str,
        port: int,
        target_port: int,
        selector: Dict[str, str],
        owner_ref: client.V1OwnerReference,
        port_name="grpc",
        service_type=None,
        node_port=None,
        new_labels: Optional[Dict[str, str]] = None,
    ):
        labels = {
            "app": ElasticJobLabel.APP_NAME,
            ElasticJobLabel.JOB_KEY: self._job_name,
        }
        # `new_labels` are merged on top of the defaults so callers (e.g. a
        # vendor-specific ServiceFactory) can attach or override labels; keys
        # in `new_labels` win over the defaults.
        if new_labels:
            labels = {**labels, **new_labels}

        metadata = client.V1ObjectMeta(
            name=name,
            labels=labels,
            # Note: We have to add at least one annotation here.
            # Otherwise annotation is `None` and cannot be modified
            # using `with_service()` for cluster specific information.
            annotations=labels,
            owner_references=[owner_ref],
            namespace=self._namespace,
        )
        spec = client.V1ServiceSpec(
            ports=[
                client.V1ServicePort(
                    name=port_name,
                    port=port,
                    target_port=target_port,
                    node_port=node_port,
                )
            ],
            selector=selector,
            type=service_type,
        )
        service = client.V1Service(
            api_version="v1", kind="Service", metadata=metadata, spec=spec
        )
        return service
