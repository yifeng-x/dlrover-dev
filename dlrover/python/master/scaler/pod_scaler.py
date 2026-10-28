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

import copy
import json
import os
import shlex
import socket
import telnetlib
import threading
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Deque, Dict, List, Optional

from kubernetes import client
from kubernetes.client import V1EnvVar, V1EnvVarSource, V1ObjectFieldSelector

from dlrover.python.common.constants import (
    DistributionStrategy,
    ElasticJobLabel,
    SchedulingLabel,
    EventReportConstants,
    NodeEnv,
    NodeStatus,
    NodeType,
)
from dlrover.python.common.event.reporter import get_event_reporter
from dlrover.python.common.global_context import Context
from dlrover.python.common.log import default_logger as logger
from dlrover.python.common.node import Node, NodeResource
from dlrover.python.master.node.job_context import get_job_context
from dlrover.python.master.resource.job import resolve_group_id
from dlrover.python.master.node.workspace_state import (
    is_releasing_workspace_node,
)
from dlrover.python.master.scaler.base_scaler import ScalePlan, Scaler
from dlrover.python.scheduler.kubernetes import (
    NODE_SERVICE_PORTS,
    convert_cpu_to_decimal,
    convert_memory_to_mb,
    get_main_container,
    get_pod_name,
    k8sClient,
    k8sServiceFactory,
    set_container_resource,
)

_dlrover_context = Context.singleton_instance()
_job_context = get_job_context()


class WorkspaceMode:
    ANNO_RUN_MODE = "dlrover.io/run-mode"
    ANNO_EXIT_POLICY = "dlrover.io/workspace-exit-policy"
    ANNO_DEBUG_MODE = "dlrover.io/workspace-debug-mode"
    ANNO_JUPYTER_ENABLED = "dlrover.io/jupyter-enabled"
    ANNO_JUPYTER_PORT = "dlrover.io/jupyter-port"
    ANNO_JUPYTER_SERVICE_TYPE = "dlrover.io/jupyter-service-type"
    ANNO_JUPYTER_NODE_PORT = "dlrover.io/jupyter-node-port"
    ANNO_JUPYTER_TOKEN = "dlrover.io/jupyter-token"
    ANNO_JUPYTER_ARGS = "dlrover.io/jupyter-args"
    ANNO_IDLE_COMMAND = "dlrover.io/idle-command"
    ANNO_INIT_COMMAND = "dlrover.io/init-command"
    ANNO_RUNTIME_MONITOR_ENABLED = "dlrover.io/runtime-monitor-enabled"
    LABEL_RUN_MODE = "dlrover.io/run-mode"
    LABEL_EXIT_POLICY = "dlrover.io/workspace-exit-policy"

    WORKSPACE_RUN_MODE = "workspace"
    RUN_MODE = WORKSPACE_RUN_MODE
    EXIT_POLICY_RETAIN = "retain"
    EXIT_POLICY_RELEASE = "release"
    JUPYTER_PORT = 8888
    JUPYTER_TOKEN = "dlrover"
    IDLE_COMMAND = "sleep infinity"
    SERVICE_SUFFIX = "jupyter"
    SERVICE_TYPE_CLUSTER_IP = "ClusterIP"
    SERVICE_TYPE_NODE_PORT = "NodePort"
    SERVICE_TYPE_LOAD_BALANCER = "LoadBalancer"
    SUPPORTED_SERVICE_TYPES = {
        SERVICE_TYPE_CLUSTER_IP,
        SERVICE_TYPE_NODE_PORT,
        SERVICE_TYPE_LOAD_BALANCER,
    }
    ENV_MODE = "DLROVER_WORKSPACE_MODE"
    ENV_RUN_MODE = "DLROVER_RUN_MODE"
    ENV_EXIT_POLICY = "DLROVER_WORKSPACE_EXIT_POLICY"
    ENV_DEBUG_MODE = "DLROVER_WORKSPACE_DEBUG_MODE"
    ENV_DEBUG_READY_FILE = "DLROVER_WORKSPACE_DEBUG_READY_FILE"
    DEBUG_READY_FILE = "/tmp/dlrover-workspace-debug-ready"
    RELEASE_SUPERVISOR_COMMAND = (
        "python_bin=${DLROVER_WORKSPACE_AGENTCTL_PYTHON:-python}; "
        '"$python_bin" -m dlrover.python.workspace.release_supervisor wait'
    )
    ENV_ROLE = "DLROVER_WORKSPACE_ROLE"
    ENV_JUPYTER_SERVICE = "DLROVER_JUPYTER_SERVICE"
    ENV_NAMESPACE = "NAMESPACE"
    ENV_RUNTIME_MONITOR = "DLROVER_RUNTIME_MONITOR"
    ROLE_JUPYTER = "jupyter"
    ROLE_WORKER = "worker"


class FakeKubeResponse:
    def __init__(self, obj):
        self.data = json.dumps(obj)


def append_pod_ip_to_env(env):
    pod_ip_var = V1EnvVar(
        name="POD_IP",
        value_from=V1EnvVarSource(
            field_ref=V1ObjectFieldSelector(field_path="status.podIP")
        ),
    )
    node_ip_var = V1EnvVar(
        name="NODE_IP",
        value_from=V1EnvVarSource(
            field_ref=V1ObjectFieldSelector(field_path="status.hostIP")
        ),
    )
    if env:
        env.append(pod_ip_var)
        env.append(node_ip_var)
    else:
        env = [pod_ip_var, node_ip_var]
    return env


class PodScaler(Scaler):
    """PodScaler launches or removes Pods using Kubernetes Python APIs
    by a ScalePlan. After PodScaler receives a ScalePlan, it will
    list all alive Pods of the job and push new `Node`s to a queue.
    The thread of creating Pods will create Pods if there is `Node`
    in a queue.
    """

    def __init__(self, job_name, namespace):
        super(PodScaler, self).__init__(job_name)
        self._k8s_client = k8sClient.singleton_instance(namespace)
        self._svc_factory = k8sServiceFactory(namespace, job_name)
        self._namespace = namespace
        self._replica_template: Dict[str, client.V1Pod] = {}
        self._create_node_queue: Deque[Node] = deque()
        self._create_node_futures: List[Future] = []
        self._scaling_lock = threading.Lock()
        self._plan = ScalePlan()
        self._ps_addrs: List[str] = []
        self._pod_stats: Dict[str, int] = {}
        self._alive_pod_stats: Dict[str, int] = {}
        self._var_lock = threading.Lock()
        self._job_uid = ""
        self.api_client = client.ApiClient()
        self._master_addr = ""
        self._master_service_type = _dlrover_context.master_service_type
        self._event_reporter = get_event_reporter()
        self._started = False
        self._job_context = get_job_context()

        # for concurrency scaling
        self._pending_plan: Optional[ScalePlan] = None
        self._plan_merge_lock = threading.Lock()
        self._current_scaling: Optional[Future] = None
        self._scaling_executor = ThreadPoolExecutor(max_workers=1)
        # Node group affinity injected by DistributedJobManager; when set,
        # newly created worker pods are labeled with their group so the
        # scheduler can place them by affinity.
        self._group_affinity: Optional[Dict[int, int]] = None
        
        # dlrover workspace mode
        self._workspace_mode = False
        self._workspace_jupyter_enabled = True
        self._workspace_jupyter_port = WorkspaceMode.JUPYTER_PORT
        self._workspace_jupyter_service_type = (
            WorkspaceMode.SERVICE_TYPE_NODE_PORT
        )
        self._workspace_jupyter_node_port = None
        self._workspace_jupyter_token = WorkspaceMode.JUPYTER_TOKEN
        self._workspace_jupyter_args = ""
        self._workspace_idle_command = WorkspaceMode.IDLE_COMMAND
        self._workspace_init_command = ""
        self._workspace_debug_mode = False
        self._workspace_debug_setup_command = ""
        self._workspace_debug_launch_command = ""
        self._workspace_run_mode = ""
        self._workspace_exit_policy = WorkspaceMode.EXIT_POLICY_RETAIN
        self._runtime_monitor_enabled = False

    def set_group_affinity(self, group_affinity):
        """Store the group affinity mapping for labeling created pods."""
        self._group_affinity = group_affinity

    def start(self):
        self._job = self._retry_to_get_job()
        if not self._job:
            raise ValueError(f"Cannot get the training job {self._job_name}.")
        self._init_pod_config_by_job()
        self._master_addr = self._get_master_addr()
        self._started = True
        threading.Thread(
            target=self._periodic_create_pod, name="pod-creater", daemon=True
        ).start()

    def stop(self):
        self._started = False

    def _safe_get_pod_status(self, key, default):
        with self._var_lock:
            if key is None:
                # return whole dict
                return self._pod_stats
            # return specified value
            return self._pod_stats.get(key, default)

    def _safe_set_pod_status(self, key, value):
        with self._var_lock:
            logger.info(f"Set pod number {value} for {key}.")
            self._pod_stats[key] = value

    def _safe_get_alive_pod_status(self, key, default):
        with self._var_lock:
            if key is None:
                # return whole dict
                return self._alive_pod_stats
            # return specified value
            return self._alive_pod_stats.get(key, default)

    def _safe_set_alive_pod_status(self, key, value):
        with self._var_lock:
            logger.info(f"Set alive pod number {value} for {key}.")
            self._alive_pod_stats[key] = value

    def _get_master_addr(self):
        svc_name = f"elasticjob-{self._job_name}-dlrover-master"
        port = NODE_SERVICE_PORTS[NodeType.DLROVER_MASTER]
        master_addr = f"{svc_name}:{port}"
        target_port = _dlrover_context.master_port
        if not self._check_master_service_avaliable(svc_name, target_port):
            # On some clusters, the k8s service may not be available because of
            # incorrect DNS configurations. In such cases, it is necessary to
            # revert to use the Master's IP address for worker to connect with
            # the master. Note, that failover of master is not supported if
            # the service is not available.
            logger.info(
                f"The service {master_addr} is not available and "
                f"use the IP of master Pod."
            )
            master_ip = os.getenv("POD_IP", "")
            if not master_ip:
                raise ValueError("The master Pod must have the POD_IP env.")
            master_addr = f"{master_ip}:{_dlrover_context.master_port}"
        return master_addr

    def _init_pod_config_by_job(self):
        self._distribution_strategy = self._job["spec"].get(
            "distributionStrategy", None
        )
        self._job_uid = self._job["metadata"]["uid"]
        self._init_workspace_config()
        worker_spec = self._job["spec"]["replicaSpecs"][NodeType.WORKER]
        self._config_worker_num = worker_spec.get("replicas", 0)
        if "replicaSpecs" in self._job["spec"]:
            for replica, spec in self._job["spec"]["replicaSpecs"].items():
                if replica == NodeType.DLROVER_MASTER:
                    continue
                pod = spec["template"]
                pod["apiVesion"] = "v1"
                pod["kind"] = "Pod"
                res = FakeKubeResponse(pod)
                v1pod = self._k8s_client.api_client.deserialize(res, "V1Pod")
                self._replica_template[replica] = v1pod

    def _init_workspace_config(self):
        annotations = self._job.get("metadata", {}).get("annotations", {}) or {}
        spec = self._job.get("spec", {}) or {}
        run_mode = str(
            spec.get("runMode", annotations.get(WorkspaceMode.ANNO_RUN_MODE, ""))
        ).strip().lower()
        if run_mode == "runtime":
            raise ValueError(
                "run mode 'runtime' is unsupported; use run-mode=workspace, "
                "jupyter-enabled=false and workspace-exit-policy=retain"
            )
        self._workspace_mode = run_mode == WorkspaceMode.WORKSPACE_RUN_MODE
        if not self._workspace_mode:
            if WorkspaceMode.ANNO_EXIT_POLICY in annotations:
                logger.warning(
                    "Ignore %s because run-mode is not workspace.",
                    WorkspaceMode.ANNO_EXIT_POLICY,
                )
            logger.warning(
                f"Workspace mode is disabled for job {self._job_name} because "
                f"the run mode is {run_mode}."
            )
            return
        
        self._workspace_jupyter_enabled = str(
            annotations.get(WorkspaceMode.ANNO_JUPYTER_ENABLED, "true")
        ).lower() in ["true", "1", "yes", "y"]
        exit_policy = str(
            annotations.get(
                WorkspaceMode.ANNO_EXIT_POLICY,
                WorkspaceMode.EXIT_POLICY_RETAIN,
            )
        ).strip().lower()
        if exit_policy not in (
            WorkspaceMode.EXIT_POLICY_RETAIN,
            WorkspaceMode.EXIT_POLICY_RELEASE,
        ):
            raise ValueError("unsupported workspace exit policy: %s" % exit_policy)
        if (
            exit_policy == WorkspaceMode.EXIT_POLICY_RELEASE
            and self._workspace_jupyter_enabled
        ):
            raise ValueError(
                "workspace exit policy 'release' requires "
                "jupyter-enabled=false"
            )
        self._workspace_exit_policy = exit_policy
        self._workspace_debug_mode = str(
            annotations.get(WorkspaceMode.ANNO_DEBUG_MODE, "false")
        ).strip().lower() in ["true", "1", "yes", "y"]
        if (
            self._workspace_debug_mode
            and exit_policy != WorkspaceMode.EXIT_POLICY_RELEASE
        ):
            raise ValueError(
                "workspace-debug-mode is only supported with "
                "workspace-exit-policy=release"
            )
        self._workspace_run_mode = run_mode
        self._runtime_monitor_enabled = str(
            annotations.get(
                WorkspaceMode.ANNO_RUNTIME_MONITOR_ENABLED, "false")
        ).strip().lower() in ["true", "1", "yes", "y"]
        self._workspace_jupyter_port = self._parse_int_annotation(
            annotations,
            WorkspaceMode.ANNO_JUPYTER_PORT,
            WorkspaceMode.JUPYTER_PORT,
        )
        self._workspace_jupyter_service_type = (
            self._parse_jupyter_service_type(annotations)
        )
        self._workspace_jupyter_node_port = (
            self._parse_jupyter_node_port(annotations)
        )
        self._workspace_jupyter_token = annotations.get(
            WorkspaceMode.ANNO_JUPYTER_TOKEN,
            WorkspaceMode.JUPYTER_TOKEN,
        )
        self._workspace_jupyter_args = annotations.get(
            WorkspaceMode.ANNO_JUPYTER_ARGS, ""
        )
        self._workspace_idle_command = annotations.get(
            WorkspaceMode.ANNO_IDLE_COMMAND,
            WorkspaceMode.IDLE_COMMAND,
        )
        self._workspace_init_command = annotations.get(
            WorkspaceMode.ANNO_INIT_COMMAND, ""
        ).strip()
        if (
            self._workspace_exit_policy == WorkspaceMode.EXIT_POLICY_RELEASE
            and self._workspace_init_command
        ):
            valid_init_command = self._is_release_launch_command(
                self._workspace_init_command
            )
            if not valid_init_command and self._workspace_debug_mode:
                debug_commands = self._split_debug_init_command(
                    self._workspace_init_command
                )
                if debug_commands:
                    (
                        self._workspace_debug_setup_command,
                        self._workspace_debug_launch_command,
                    ) = debug_commands
                    logger.warning(
                        "Workspace debug mode is enabled; run setup command "
                        "on every worker before the rank-0 workspace launch."
                    )
                    valid_init_command = True
            if not valid_init_command:
                raise ValueError(
                    "release init-command must be a full workspace launch "
                    "with --network-mode all and --framework-cmd; enable "
                    "workspace-debug-mode only for '<setup> && "
                    "dlrover-workspace launch ...'"
                )

    def _split_debug_init_command(self, command):
        marker = "dlrover-workspace launch"
        offset = 0
        while True:
            index = command.find(marker, offset)
            if index < 0:
                return None
            launch_command = command[index:].strip()
            if self._is_release_launch_command(launch_command):
                prefix = command[:index].rstrip()
                for connector in ("&&", ";"):
                    if prefix.endswith(connector):
                        setup_command = prefix[: -len(connector)].strip()
                        if setup_command:
                            return setup_command, launch_command
            offset = index + len(marker)

    def _is_release_launch_command(self, command):
        try:
            lexer = shlex.shlex(
                command,
                posix=True,
                punctuation_chars=True,
            )
            lexer.whitespace_split = True
            tokens = list(lexer)
        except ValueError:
            return False
        if tokens[:2] != ["dlrover-workspace", "launch"]:
            return False
        if any(token in {";", "&&", "||", "|", "&"} for token in tokens):
            return False
        if any(
            token in {"--cmd", "--foreground", "--ranks"}
            or token.startswith(("--cmd=", "--ranks="))
            for token in tokens[2:]
        ):
            return False

        network_mode = None
        has_framework_cmd = False
        for index, token in enumerate(tokens[2:], start=2):
            if token == "--network-mode" and index + 1 < len(tokens):
                network_mode = tokens[index + 1]
            elif token.startswith("--network-mode="):
                network_mode = token.split("=", 1)[1]
            elif token == "--framework-cmd" and index + 1 < len(tokens):
                has_framework_cmd = bool(tokens[index + 1].strip())
            elif token.startswith("--framework-cmd="):
                has_framework_cmd = bool(token.split("=", 1)[1].strip())
        return network_mode == "all" and has_framework_cmd

    def _parse_int_annotation(self, annotations, key, default):
        try:
            return int(annotations.get(key, default))
        except (TypeError, ValueError):
            logger.warning(
                "Invalid annotation %s=%s, use default %s.",
                key,
                annotations.get(key),
                default,
            )
            return default

    def _parse_jupyter_service_type(self, annotations):
        service_type = annotations.get(
            WorkspaceMode.ANNO_JUPYTER_SERVICE_TYPE,
            WorkspaceMode.SERVICE_TYPE_NODE_PORT,
        )
        normalized_types = {
            service_type.lower(): service_type
            for service_type in WorkspaceMode.SUPPORTED_SERVICE_TYPES
        }
        normalized_service_type = str(service_type).strip().lower()
        if normalized_service_type in normalized_types:
            return normalized_types[normalized_service_type]

        logger.warning(
            "Invalid annotation %s=%s, use default %s. Supported values are %s.",
            WorkspaceMode.ANNO_JUPYTER_SERVICE_TYPE,
            service_type,
            WorkspaceMode.SERVICE_TYPE_NODE_PORT,
            sorted(WorkspaceMode.SUPPORTED_SERVICE_TYPES),
        )
        return WorkspaceMode.SERVICE_TYPE_NODE_PORT

    def _parse_jupyter_node_port(self, annotations):
        raw_node_port = annotations.get(WorkspaceMode.ANNO_JUPYTER_NODE_PORT)
        if raw_node_port is None or str(raw_node_port).strip() == "":
            return None

        if (
            self._workspace_jupyter_service_type
            != WorkspaceMode.SERVICE_TYPE_NODE_PORT
        ):
            logger.warning(
                "Ignore annotation %s=%s because %s is %s.",
                WorkspaceMode.ANNO_JUPYTER_NODE_PORT,
                raw_node_port,
                WorkspaceMode.ANNO_JUPYTER_SERVICE_TYPE,
                self._workspace_jupyter_service_type,
            )
            return None

        try:
            node_port = int(raw_node_port)
        except (TypeError, ValueError):
            logger.warning(
                "Invalid annotation %s=%s, let Kubernetes allocate nodePort.",
                WorkspaceMode.ANNO_JUPYTER_NODE_PORT,
                raw_node_port,
            )
            return None

        if node_port <= 0 or node_port > 65535:
            logger.warning(
                "Invalid annotation %s=%s, let Kubernetes allocate nodePort.",
                WorkspaceMode.ANNO_JUPYTER_NODE_PORT,
                raw_node_port,
            )
            return None

        return node_port

    def _retry_to_get_job(self):
        for _ in range(3):
            job = self._k8s_client.get_custom_resource(
                name=self._job_name,
                group="elastic.iml.github.io",
                version="v1alpha1",
                plural="elasticjobs",
            )
            if job:
                return job
            else:
                time.sleep(5)
        return None

    def scale(self, plan: ScalePlan, **kwargs):
        """
        Scale with automatic plan merging and execution(set with_merge=True).
        If a scaling task is running, merge the new plan into pending plan.
        When current task finishes, automatically execute the merged pending plan.

        Args:
            plan (ScalePlan): The scaling plan to execute
            with_merge (bool, optional): If true, enable automatic plan merging.
                Defaults to False.
        """

        with_merge = kwargs.pop("with_merge", False)

        if with_merge:
            with self._plan_merge_lock:
                if (
                    self._current_scaling is None
                    or self._current_scaling.done()
                ):
                    # no scaling running, execute immediately
                    self._current_scaling = self._scaling_executor.submit(
                        self._execute_scale_with_pending, plan
                    )
                else:
                    # scaling is running, merge into pending plan
                    if self._pending_plan is None:
                        self._pending_plan = copy.deepcopy(plan)
                    else:
                        self._pending_plan.merge(plan)
                    logger.info(
                        "Merged plan into pending, "
                        f"current launch nodes: {len(self._pending_plan.launch_nodes)}, "
                        f"remove nodes: {len(self._pending_plan.remove_nodes)}"
                        f"full plan: {self._pending_plan.to_json()}"
                    )
        else:
            self._scale(plan)

    def _execute_scale_with_pending(self, plan: ScalePlan):
        """
        Execute scale plan and handle pending plans after completion.
        This method ensures pending plans are executed in sequence.
        """

        try:
            # execute the current plan
            self._scale(plan)
        except Exception:
            logger.exception(f"Failed to execute scale plan: {plan.to_json()}")
        finally:
            # after completion, check and execute any pending plans
            with self._plan_merge_lock:
                if self._pending_plan is None or self._pending_plan.empty():
                    # no more pending plans, mark as completed
                    self._current_scaling = None
                    return

                # get the pending plan and clear it
                next_plan = copy.deepcopy(self._pending_plan)
                self._pending_plan = None

            logger.info(f"Executing pending plan: {next_plan.to_json()}")

            # execute the pending plan
            self._current_scaling = self._scaling_executor.submit(
                self._execute_scale_with_pending, next_plan
            )

    def _scale(self, plan: ScalePlan):
        if not self._elasticjob_exists():
            plan_json = plan.to_json()
            logger.info(
                f"Skip the scale plan {plan_json} because the job does not exist."
            )
            return

        while True:
            if not self._job_context.is_suspended():
                break
            logger.info("Waiting for elasticJob which is suspended")
            time.sleep(5)

        self._remove_nodes(plan)
        while self._started:
            if (
                len(self._create_node_queue) > 0
                and not _job_context.is_stopped()
            ):
                logger.info(
                    f"Wait nodes {self._create_node_queue} to completed."
                )
                time.sleep(5)
            else:
                if all(future.done() for future in self._create_node_futures):
                    # wait async pod creation completed
                    logger.debug("Async pod creation finished.")
                    time.sleep(1)
                    break
                else:
                    logger.debug("Waiting for async pod creation...")
                    time.sleep(1)
                    continue

        with self._scaling_lock:
            if plan.empty():
                return
            self._plan = plan
            job_pods = self._list_job_pods()
            logger.info("Scale the job by plan %s", plan.to_json())
            if plan.ps_addrs:
                self._ps_addrs = plan.ps_addrs
            for type, group_resource in plan.node_group_resources.items():
                type_pods = job_pods.get(type, [])
                max_pod_id = self._get_max_pod_id(type_pods)
                normal_pods = []
                for node in type_pods:
                    if node.status in [
                        NodeStatus.PENDING,
                        NodeStatus.RUNNING,
                        NodeStatus.SUCCEEDED,
                    ]:
                        normal_pods.append(node)
                cur_pods = normal_pods + self._get_type_pod_in_queue(type)
                if group_resource.count > len(cur_pods):
                    self._scale_up_pods(type, plan, cur_pods, max_pod_id)
                elif group_resource.count < len(cur_pods):
                    self._scale_down_pods(type, plan, cur_pods)
            for node in plan.launch_nodes:
                self._create_node_queue.append(node)
            self._update_job_pods(job_pods)

    def _remove_nodes(self, plan: ScalePlan):
        for node in plan.remove_nodes:
            removed = self._remove_not_create_pod(node.name)
            if not removed and node.name:
                if self._is_failed_release_workspace_pod(node):
                    logger.info(
                        "Retain or defer deletion of release workspace pod %s.",
                        node.name,
                    )
                    continue
                self._k8s_client.delete_pod(node.name)

    def _is_failed_release_workspace_pod(self, node):
        if not is_releasing_workspace_node(node):
            return False
        try:
            pod = self._k8s_client.get_pod(node.name)
        except Exception:
            logger.warning(
                "Cannot verify release Pod %s; defer deletion.", node.name
            )
            return True
        # get_pod returns None both on 404 and on exhausted API retries.
        # Neither case authorizes deleting an unverified release Pod.
        return pod is None or pod.status.phase == NodeStatus.FAILED

    def reconcile_workspace_release(self, nodes, force=False):
        """One bounded cleanup pass; return observed phases and completed IDs.

        Failed Pods are retained. A delete response is not completion: the
        next successful list must confirm absence before the Master exits.
        """
        if not nodes:
            return {}, set()
        deadline = time.monotonic() + 5
        # Check creation before listing so an in-flight create cannot be
        # mistaken for a Pod that has already been deleted.
        with self._scaling_lock:
            for node in nodes:
                self._remove_not_create_pod(node.name)
            creating = any(
                not future.done() for future in self._create_node_futures
            ) or bool(self._create_node_queue)
        try:
            # Use the raw, bounded API: the convenience wrapper conflates an
            # unavailable API with an absent Pod after its retries.
            pods = self._k8s_client.client.list_namespaced_pod(
                namespace=self._namespace,
                label_selector=f"{ElasticJobLabel.JOB_KEY}={self._job_name}",
                _request_timeout=(2, 3),
            )
        except Exception as error:
            logger.warning("Workspace cleanup cannot list Pods: %s", error)
            return {}, set()
        by_name = {pod.metadata.name: pod for pod in pods.items}
        phases, completed = {}, set()
        for node in nodes:
            pod = by_name.get(node.name)
            if pod is None:
                if not creating:
                    phases[node.id] = NodeStatus.DELETED
                    completed.add(node.id)
                continue
            phase = pod.status.phase
            phases[node.id] = phase
            if phase == NodeStatus.FAILED:
                logger.info("Retain failed workspace Pod %s.", node.name)
                completed.add(node.id)
                continue
            if not force and phase != NodeStatus.SUCCEEDED:
                continue
            if pod.metadata.deletion_timestamp or time.monotonic() >= deadline:
                continue
            try:
                # If phase/identity changed since LIST (e.g. Running ->
                # Failed), the API rejects this delete; recheck next pass.
                self._k8s_client.client.delete_namespaced_pod(
                    name=node.name,
                    namespace=self._namespace,
                    body=client.V1DeleteOptions(
                        preconditions=client.V1Preconditions(
                            uid=pod.metadata.uid,
                            resource_version=pod.metadata.resource_version,
                        )
                    ),
                    _request_timeout=(2, 3),
                )
            except Exception as error:
                logger.warning(
                    "Workspace cleanup delete %s: %s", node.name, error
                )
        return phases, completed

    def _update_job_pods(self, job_pods: Dict[str, List[Node]]):
        for node_type in [
            NodeType.CHIEF,
            NodeType.MASTER,
            NodeType.PS,
            NodeType.WORKER,
            NodeType.EVALUATOR,
        ]:
            cur_pods = job_pods.get(
                node_type, []
            ) + self._get_type_pod_in_queue(node_type)
            self._safe_set_pod_status(node_type, len(cur_pods))
            self._safe_set_alive_pod_status(
                node_type,
                len(
                    [
                        cur_pod
                        for cur_pod in cur_pods
                        if cur_pod.status != NodeStatus.FAILED
                        and cur_pod.status != NodeStatus.DELETED
                    ]
                ),
            )

    def _get_type_pod_in_queue(self, node_type):
        pods = []
        for pod in self._create_node_queue:
            if pod.type == node_type:
                pods.append(pod)
        return pods

    def _remove_not_create_pod(self, pod_name):
        not_created_pod = None
        for pod in self._create_node_queue:
            if pod_name == get_pod_name(self._job_name, pod.type, pod.id):
                not_created_pod = pod
                break
        if not_created_pod:
            self._create_node_queue.remove(not_created_pod)
            return True
        return False

    def _list_job_pods(self):
        pod_list = self._wait_list_pods()
        job_pods: Dict[str, List[Node]] = {}
        if not pod_list:
            return job_pods
        for pod in pod_list.items:
            pod_type = pod.metadata.labels[ElasticJobLabel.REPLICA_TYPE_KEY]
            if pod_type == NodeType.DLROVER_MASTER:
                continue
            job_pods.setdefault(pod_type, [])
            pod_id = int(
                pod.metadata.labels[ElasticJobLabel.REPLICA_INDEX_KEY]
            )
            task_id = int(pod.metadata.labels[ElasticJobLabel.RANK_INDEX_KEY])
            pod_resource = self._get_pod_resource(pod)
            node = Node(
                node_type=pod_type,
                node_id=pod_id,
                name=pod.metadata.name,
                rank_index=task_id,
                status=pod.status.phase,
                config_resource=pod_resource,
                max_relaunch_count=_dlrover_context.max_relaunch_count,
            )
            if node.type != NodeType.WORKER and node.status not in [
                NodeStatus.PENDING,
                NodeStatus.RUNNING,
            ]:
                continue
            job_pods[pod_type].append(node)
        return job_pods

    def _wait_list_pods(self, timeout=1800):
        job_selector = ElasticJobLabel.JOB_KEY + "=" + self._job_name
        start = time.time()
        while True:
            pod_list = self._k8s_client.list_namespaced_pod(job_selector)
            if pod_list:
                return pod_list
            if time.time() - start < timeout:
                time.sleep(60)
            else:
                raise TimeoutError(f"Timeout {timeout} to list Pods.")

    def _get_pod_resource(self, pod):
        main = get_main_container(pod) or pod.spec.containers[0]
        resources = main.resources
        cpu = convert_cpu_to_decimal(resources.requests.get("cpu", "0"))
        if "memory" in resources.requests:
            memory = convert_memory_to_mb(resources.requests["memory"])
        else:
            memory = 0
        return NodeResource(cpu, memory)

    def _scale_up_pods(
        self,
        type,
        plan: ScalePlan,
        cur_pods: List[Node],
        max_pod_id,
    ):
        """The method will create a Node instances and push it into a queue.
        The thread to create Pods will periodicall create Pods by
        the Node instance in the queue."""
        cur_num = len(cur_pods)
        group_resource = plan.node_group_resources[type]
        up_num = group_resource.count - cur_num
        for i in range(up_num):
            node_id = max_pod_id + 1 + i
            task_id = cur_num + i
            group_id = resolve_group_id(self._group_affinity, type, task_id)
            group_size = (
                len(self._group_affinity)
                if group_id is not None and self._group_affinity
                else None
            )
            node = Node(
                type,
                node_id,
                copy.deepcopy(group_resource.node_resource),
                rank_index=task_id,
                name=get_pod_name(self._job_name, type, node_id),
                service_addr=self.get_node_service_addr(type, task_id),
                node_group=group_id,
                node_group_size=group_size,
                node_group_id=group_id,
            )
            self._create_node_queue.append(node)

    def _get_max_pod_id(self, pods: List[Node]):
        max_id = -1
        for pod in pods:
            max_id = max(pod.id, max_id)
        return max_id

    def _scale_down_pods(
        self,
        type,
        plan: ScalePlan,
        cur_pods: List[Node],
    ):
        """Delete Pods to scale down Pods."""
        group_resource = plan.node_group_resources[type]
        down_num = len(cur_pods) - group_resource.count

        not_created_pods = []
        for pending_pod in self._create_node_queue:
            if pending_pod.type == type:
                not_created_pods.append(pending_pod)
        while down_num > 0 and not_created_pods:
            pod = not_created_pods.pop()
            self._create_node_queue.remove(pod)
            down_num -= 1
        cur_pods.sort(key=lambda x: x.id, reverse=True)
        for pod in cur_pods:
            if down_num <= 0:
                break
            self._k8s_client.delete_pod(pod.name)
            down_num -= 1

    def get_node_service_addr(self, type, id):
        service_name = get_pod_name(self._job_name, type, id)
        return "%s.%s.svc:%d" % (
            service_name,
            self._namespace,
            NODE_SERVICE_PORTS[type],
        )

    def get_typed_pod(self, pod_type, id):
        pod_name = get_pod_name(self._job_name, pod_type, id)
        return self._k8s_client.get_pod(pod_name)

    def _periodic_create_pod(self):
        logger.info("Start the thread to create Pod.")
        with ThreadPoolExecutor(max_workers=4) as executor:
            while self._started:
                # Register every submitted create before cleanup can inspect
                # the queue/futures and declare an absent Pod released.
                with self._scaling_lock:
                    while self._create_node_queue:
                        self._create_node_futures.append(
                            executor.submit(
                                self._create_pod_from_queue,
                                self._create_node_queue.popleft(),
                            )
                        )
                time.sleep(3)

    def _create_pod_from_queue(self, node_from_queue=None):
        """
        Notice: we must ensure the sync operation of getting node happens
        before the async execution, so we set 'node_from_queue' in the params
        instead of pop the element in the current function to avoid invalid
        async execution calls.

        Args:
            node_from_queue (Node): List of Node instances.
        """

        if node_from_queue is None:
            return True

        succeed = False

        try:
            if self._check_cluster_ready_for_pod(node_from_queue):
                pod = self._create_pod(node_from_queue)
                succeed = self._k8s_client.create_pod(pod)
            if not succeed:
                self._create_node_queue.appendleft(node_from_queue)
            else:
                # create svs for succeed pod
                if not self._create_service_for_pod(node_from_queue):
                    self._create_node_queue.appendleft(node_from_queue)
                elif not self._create_workspace_service_if_needed(
                    node_from_queue
                ):
                    self._create_node_queue.appendleft(node_from_queue)
        except Exception as e:
            logger.error(
                f"Failed to create pod by unexpected error: {e}", exc_info=True
            )
            succeed = False

        return succeed

    def _check_cluster_ready_for_pod(self, node: Node):
        """Check whether the resource of a cluster is enough to
        create a node"""
        return True

    def _create_pod(self, node: Node):
        # Find that master pod that will be used as the owner reference
        # for the ps or worker pod.
        node.update_priority(self._safe_get_pod_status(node.type, 0))
        pod_name = get_pod_name(self._job_name, node.type, node.id)
        logger.info(
            "Create Pod %s with resource %s",
            pod_name,
            node.config_resource.to_resource_dict(),
        )
        env: List[V1EnvVar] = []
        env = append_pod_ip_to_env(env)

        env.append(V1EnvVar(name=NodeEnv.JOB_NAME, value=self._job_name))
        env.append(V1EnvVar(name=NodeEnv.JOB_UID, value=self._job_uid))

        # History background1: https://chromium.googlesource.com/external/
        # github.com/grpc/grpc/+/refs/tags/v1.19.0-pre1/doc/fork_support.md
        #
        # History background2: https://github.com/grpc/grpc/issues/18075
        # resolved by: https://github.com/grpc/grpc/pull/32935
        env.append(V1EnvVar(name=NodeEnv.GRPC_ENABLE_FORK, value="false"))

        worker_num = self._config_worker_num
        if worker_num == 0:
            worker_num = self._safe_get_pod_status(node.type, 0)

        env.append(V1EnvVar(name=NodeEnv.NODE_TYPE, value=node.type))
        env.append(V1EnvVar(name=NodeEnv.NODE_ID, value=str(node.id)))
        env.append(V1EnvVar(name=NodeEnv.NODE_NUM, value=str(worker_num)))
        env.append(
            V1EnvVar(name=NodeEnv.NODE_RANK, value=str(node.rank_index))
        )

        # The two env vars is compatible with kubeflow/PytorchJob because
        # users may use the scripts of kubeflow/PytorchJob in the ElasticJob.
        if self._distribution_strategy == DistributionStrategy.ALLREDUCE:
            env.append(
                V1EnvVar(name=NodeEnv.WORLD_SIZE, value=str(worker_num))
            )
            env.append(V1EnvVar(name=NodeEnv.RANK, value=str(node.rank_index)))

        env.append(
            V1EnvVar(name=NodeEnv.DLROVER_MASTER_ADDR, value=self._master_addr)
        )
        env.append(
            V1EnvVar(
                name=NodeEnv.DLROVER_MASTER_SERVICE_TYPE,
                value=self._master_service_type,
            )
        )
        env.append(
            V1EnvVar(
                name=NodeEnv.DLROVER_TRAINING_ELASTIC_MODE,
                value=_dlrover_context.training_elastic_mode,
            )
        )
        # runtime elastic dimension
        env.append(
            V1EnvVar(
                name=NodeEnv.DLROVER_ELASTIC_DIMENSION,
                value=_dlrover_context.runtime_elastic_dimension,
            )
        )
        if _dlrover_context.auto_elasticity:
            env.extend(
                [
                    V1EnvVar(name="DLROVER_ELASTIC_POLICY", value="auto"),
                    V1EnvVar(
                        name="DLROVER_WORKER_GPU_NUM",
                        value=str(node.config_resource.gpu_num),
                    ),
                ]
            )
        # resource report switch
        env.append(
            V1EnvVar(
                name=NodeEnv.DLROVER_RESOURCE_MONITOR_SWITCH,
                value=_dlrover_context.resource_monitor_switch,
            )
        )

        env.append(
            V1EnvVar(
                name=NodeEnv.POD_NAME,
                value_from=V1EnvVarSource(
                    field_ref=V1ObjectFieldSelector(field_path="metadata.name")
                ),
            )
        )
        env.append(
            V1EnvVar(
                name=NodeEnv.DLROVER_EXTENSION_DYNAMIC_FAILOVER,
                value=_dlrover_context.dynamic_failover_extension,
            )
        )

        # Deprecated env vars
        env.append(V1EnvVar(name=NodeEnv.WORKER_TYPE, value=node.type))
        env.append(V1EnvVar(name=NodeEnv.WORKER_ID, value=str(node.id)))
        env.append(V1EnvVar(name=NodeEnv.WORKER_NUM, value=str(worker_num)))
        env.append(
            V1EnvVar(name=NodeEnv.WORKER_RANK, value=str(node.rank_index))
        )
        if self._workspace_mode:
            env.extend(self._new_workspace_envs(node))

        node_type = node.type
        if node.type not in self._replica_template:
            if node.type in [NodeType.CHIEF, NodeType.EVALUATOR]:
                node_type = NodeType.WORKER
        if node_type not in self._replica_template:
            raise ValueError(
                "No replica %s specification in job %s",
                node.type,
                self._job_name,
            )
        pod_template = self._replica_template[node_type]
        labels = {
            "app": ElasticJobLabel.APP_NAME,
            ElasticJobLabel.JOB_KEY: self._job_name,
        }
        if self._workspace_mode:
            labels[WorkspaceMode.LABEL_RUN_MODE] = self._workspace_run_mode
            labels[WorkspaceMode.LABEL_EXIT_POLICY] = (
                self._workspace_exit_policy
            )
        pod = self._create_pod_obj(
            name=pod_name,
            pod_template=pod_template,
            resource_requests=node.config_resource,
            resource_limits=node.config_resource,
            priority=node.config_resource.priority,
            env=env,
            lifecycle=None,
            labels=labels,
        )
        pod_meta: client.V1ObjectMeta = pod.metadata

        logger.debug(
            f"Add pod {pod_name} info into meta: {node.type} "
            f"{node.id} {node.rank_index} {node.relaunch_count} "
            f"{node.group} {node.group_size} {node.group_id}"
        )
        # Add replica type and index
        pod_meta.labels[ElasticJobLabel.REPLICA_TYPE_KEY] = node.type
        pod_meta.labels[ElasticJobLabel.REPLICA_INDEX_KEY] = str(node.id)
        pod_meta.labels[ElasticJobLabel.RANK_INDEX_KEY] = str(node.rank_index)
        pod_meta.labels[ElasticJobLabel.RELAUNCH_COUNT] = str(
            node.relaunch_count
        )
        if node.group is not None:
            pod_meta.labels[SchedulingLabel.NODE_GROUP] = str(node.group)
        if node.group_size is not None:
            pod_meta.labels[SchedulingLabel.NODE_GROUP_SIZE] = str(
                node.group_size
            )
        if node.group_id is not None:
            pod_meta.labels[SchedulingLabel.NODE_GROUP_ID] = str(node.group_id)
        pod.spec.containers[0].env.append(
            V1EnvVar(name=NodeEnv.MONITOR_ENABLED, value="true")
        )
        self._set_workspace_container_command(pod.spec.containers[0], pod_meta)
        self._patch_tf_config_into_env(pod, node)
        if self._event_reporter:
            self._event_reporter.report(
                EventReportConstants.TYPE_INFO,
                pod_name,
                EventReportConstants.ACTION_WORKER_CREATE,
                "",
                {},
            )

        return pod

    def _check_master_service_avaliable(self, host, port, timeout=15):
        """Verify that the master grpc servicer is available."""
        for i in range(timeout):
            try:
                telnetlib.Telnet(host=host, port=port, timeout=3)
                logger.info(f"Master service check pass with {host}:{port}")
                return True
            except socket.gaierror:
                logger.info(
                    f"Attempt {i}: Encountered gaierror while "
                    "performing master service check. "
                    "Service may still be unavailable."
                )
                time.sleep(1)
            except Exception as e:
                logger.info(
                    f"Attempt {i}: Encountered {str(e)} while "
                    "performing master service check. "
                    "Service may still be unavailable."
                )
                time.sleep(1)

        logger.warning(
            f"Master service check {host}:{port} failed after {timeout} retries."
        )
        return False

    def _patch_tf_config_into_env(self, pod, node: Node):
        if (
            self._distribution_strategy == DistributionStrategy.PS
            and self._ps_addrs
        ):
            tf_config = new_tf_config(
                self._safe_get_pod_status(None, None),
                self.get_node_service_addr,
                node.type,
                node.rank_index,
                self._ps_addrs,
            )
            if tf_config:
                pod.spec.containers[0].env.append(
                    V1EnvVar(name="TF_CONFIG", value=json.dumps(tf_config))
                )

    def _delete_typed_pod(self, pod_type, id):
        pod_name = get_pod_name(self._job_name, pod_type, id)
        self._k8s_client.delete_pod(pod_name)

    def _create_service_for_pod(self, node: Node):
        # create or patch worker service
        logger.info(f"create service for node {node}")
        service_ready = True
        if node.service_addr:
            service_name = node.service_addr.split(".")[0]
        else:
            service_name = get_pod_name(
                self._job_name, node.type, node.rank_index
            )
        selector = {
            ElasticJobLabel.JOB_KEY: self._job_name,
            ElasticJobLabel.REPLICA_TYPE_KEY: node.type,
            ElasticJobLabel.RANK_INDEX_KEY: str(node.rank_index),
        }
        succeed = self._svc_factory.create_service(
            service_name,
            port=NODE_SERVICE_PORTS[node.type],
            target_port=NODE_SERVICE_PORTS[node.type],
            selector=selector,
            owner_ref=self._create_job_owner_reference(),
        )
        service_ready = service_ready and succeed
        if not service_ready:
            logger.error(
                "Fail to create service %s for the %s pod %s",
                service_name,
                node.type,
                node.id,
            )
            self._delete_typed_pod(node.type, node.id)
            service_ready = False
        return service_ready

    def _create_pod_obj(
        self,
        name,
        pod_template: client.V1Pod,
        resource_requests: NodeResource,
        resource_limits: NodeResource,
        lifecycle,
        env,
        priority,
        labels,
        termination_period=None,
    ):
        pod = copy.deepcopy(pod_template)
        main_container: Optional[client.V1Container] = get_main_container(pod)

        if main_container is None:
            raise ValueError("The Pod config must have a main container.")
        set_container_resource(
            main_container, resource_requests, resource_limits
        )

        # Master-owned settings must have exactly one effective value.
        injected_names = {item.name for item in env}
        main_container.env = [
            item
            for item in (main_container.env or [])
            if item.name not in injected_names
        ] + env

        # if main_container.env is None:
        #     main_container.env = env
        # else:
        #     main_container.env.extend(env)

        main_container.lifecycle = lifecycle
        pod.spec.priority_class_name = priority
        pod.spec.restart_policy = "Never"
        pod.spec.termination_grace_period_seconds = termination_period

        if not pod.metadata:
            pod.metadata = client.V1ObjectMeta(
                name=name,
                namespace=self._namespace,
                labels=labels,
            )
        pod.metadata.name = name
        pod.metadata.namespace = self._namespace
        if pod.metadata.labels:
            pod.metadata.labels.update(labels)
        else:
            pod.metadata.labels = labels
        pod.metadata.owner_references = [self._create_job_owner_reference()]
        return pod

    def _new_workspace_envs(self, node: Node):
        role = WorkspaceMode.ROLE_WORKER
        if (
            self._workspace_jupyter_enabled
            and node.type == NodeType.WORKER
            and node.rank_index == 0
        ):
            role = WorkspaceMode.ROLE_JUPYTER
        return [
            V1EnvVar(name=WorkspaceMode.ENV_MODE, value="true"),
            V1EnvVar(
                name=WorkspaceMode.ENV_RUN_MODE,
                value=self._workspace_run_mode,
            ),
            V1EnvVar(
                name=WorkspaceMode.ENV_EXIT_POLICY,
                value=self._workspace_exit_policy,
            ),
            V1EnvVar(
                name=WorkspaceMode.ENV_DEBUG_MODE,
                value="true" if self._workspace_debug_mode else "false",
            ),
            V1EnvVar(
                name=WorkspaceMode.ENV_DEBUG_READY_FILE,
                value=WorkspaceMode.DEBUG_READY_FILE,
            ),
            V1EnvVar(name=WorkspaceMode.ENV_ROLE, value=role),
            V1EnvVar(
                name=WorkspaceMode.ENV_JUPYTER_SERVICE,
                value=self._get_workspace_service_name(),
            ),
            V1EnvVar(
                name=WorkspaceMode.ENV_NAMESPACE,
                value_from=V1EnvVarSource(
                    field_ref=V1ObjectFieldSelector(
                        field_path="metadata.namespace"
                    )
                ),
            ),
            V1EnvVar(
                name=WorkspaceMode.ENV_RUNTIME_MONITOR,
                value="true" if self._runtime_monitor_enabled else "false",
            ),
        ]

    def _set_workspace_container_command(
        self,
        container: client.V1Container,
        metadata: Optional[client.V1ObjectMeta],
    ):
        if not self._workspace_mode:
            return

        rank_index = ""
        if metadata and metadata.labels:
            rank_index = metadata.labels.get(ElasticJobLabel.RANK_INDEX_KEY, "")

        if self._workspace_jupyter_enabled and rank_index == "0":
            container.command = ["/bin/bash", "-lc"]
            container.args = [
                self._build_workspace_command(
                    self._build_jupyter_command(), rank_index
                )
            ]
        else:
            command = self._workspace_idle_command
            if (
                self._workspace_exit_policy
                == WorkspaceMode.EXIT_POLICY_RELEASE
            ):
                command = WorkspaceMode.RELEASE_SUPERVISOR_COMMAND
            container.command = ["/bin/bash", "-lc"]
            container.args = [
                self._build_workspace_command(
                    command, rank_index
                )
            ]

    def _build_jupyter_command(self):
        token = self._workspace_jupyter_token.replace('"', '\\"')
        args = self._workspace_jupyter_args
        return (
            "jupyter lab "
            "--ip=0.0.0.0 "
            f"--port={self._workspace_jupyter_port} "
            "--no-browser "
            "--allow-root "
            f'--ServerApp.token="${{JUPYTER_TOKEN:-{token}}}" '
            f"{args}"
        ).strip()

    def _with_runtime_monitor(self, command):
        if not self._runtime_monitor_enabled:
            return command
        return (
            "python -m dlrover.python.workspace.runtime_monitor & "
            "monitor_pid=$!; "
            "trap 'kill ${monitor_pid} >/dev/null 2>&1 || true' TERM INT EXIT; "
            f"{command}"
        )

    def _with_workspace_init(self, command, rank_index=""):
        if not self._workspace_init_command:
            return command
        if (
            self._workspace_exit_policy == WorkspaceMode.EXIT_POLICY_RELEASE
            and self._workspace_debug_mode
            and self._workspace_debug_setup_command
        ):
            ready_file = shlex.quote(WorkspaceMode.DEBUG_READY_FILE)
            setup = self._workspace_debug_setup_command
            prefix = f"set -e; {setup}; touch {ready_file}; "
            if rank_index == "0":
                return (
                    f"{prefix}{self._workspace_debug_launch_command}; "
                    f"{command}"
                )
            return f"{prefix}{command}"
        if (
            self._workspace_exit_policy == WorkspaceMode.EXIT_POLICY_RELEASE
            and rank_index != "0"
        ):
            if (
                self._workspace_debug_mode
                and self._workspace_debug_setup_command
            ):
                return (
                    f"set -e; {self._workspace_debug_setup_command}; "
                    f"{command}"
                )
            return command
        return f"set -e; {self._workspace_init_command}; {command}"

    def _build_workspace_command(self, command, rank_index=""):
        return self._with_runtime_monitor(
            self._with_workspace_init(command, rank_index)
        )

    def _create_workspace_service_if_needed(self, node: Node):
        if (
            not self._workspace_mode
            or not self._workspace_jupyter_enabled
            or node.type != NodeType.WORKER
            or node.rank_index != 0
        ):
            return True

        service_name = self._get_workspace_service_name()
        selector = {
            ElasticJobLabel.JOB_KEY: self._job_name,
            ElasticJobLabel.REPLICA_TYPE_KEY: NodeType.WORKER,
            ElasticJobLabel.RANK_INDEX_KEY: "0",
        }
        succeed = self._svc_factory.create_service(
            service_name,
            port=self._workspace_jupyter_port,
            target_port=self._workspace_jupyter_port,
            selector=selector,
            owner_ref=self._create_job_owner_reference(),
            port_name=WorkspaceMode.SERVICE_SUFFIX,
            service_type=self._workspace_jupyter_service_type,
            node_port=self._workspace_jupyter_node_port,
        )
        if not succeed:
            logger.error("Fail to create workspace service %s", service_name)
            self._delete_typed_pod(node.type, node.id)
        return succeed

    def _get_workspace_service_name(self):
        return f"{self._job_name}-{WorkspaceMode.SERVICE_SUFFIX}"

    def _create_job_owner_reference(self):
        owner_ref = k8sClient.create_owner_reference(
            api_version="elastic.iml.github.io/v1alpha1",
            kind="ElasticJob",
            name=self._job["metadata"]["name"],
            uid=self._job["metadata"]["uid"],
        )
        return owner_ref

    def _elasticjob_exists(self):
        job = self._retry_to_get_job()
        return job is not None


def new_tf_config(
    pod_stats: Dict[str, int],
    new_service_fn,
    type_key,
    index_key,
    ps_addrs,
):
    """Get tf.estimator config spec data. The detail is in
    https://www.tensorflow.org/api_docs/python/tf/estimator/RunConfig
    """
    cluster_dict = {}
    cluster_dict[NodeType.PS] = ps_addrs
    if NodeType.WORKER in pod_stats:
        worker_num = pod_stats[NodeType.WORKER]
        if type_key == NodeType.WORKER and index_key >= worker_num:
            worker_num = index_key + 1
        workers = []
        for worker_id in range(worker_num):
            workers.append(new_service_fn(NodeType.WORKER, worker_id))
        if len(workers) > 0:
            cluster_dict[NodeType.WORKER] = workers
    if NodeType.EVALUATOR in pod_stats:
        evaluator_num = pod_stats[NodeType.EVALUATOR]
        evaluators = []
        for worker_id in range(evaluator_num):
            evaluators.append(new_service_fn(NodeType.EVALUATOR, worker_id))
        if len(evaluators) > 0:
            cluster_dict[NodeType.EVALUATOR] = evaluators
    if NodeType.CHIEF in pod_stats:
        chief_num = pod_stats[NodeType.CHIEF]
        chiefs = []
        for worker_id in range(chief_num):
            chiefs.append(new_service_fn(NodeType.CHIEF, worker_id))
        if len(chiefs) > 0:
            cluster_dict[NodeType.CHIEF] = chiefs

    task_dict = {}
    task_dict["type"] = type_key
    task_dict["index"] = index_key
    return {"cluster": cluster_dict, "task": task_dict}
