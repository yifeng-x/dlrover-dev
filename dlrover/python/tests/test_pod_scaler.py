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
import os
import time
import unittest
from collections import deque
from threading import Thread
from types import SimpleNamespace
from unittest import mock
from unittest.mock import MagicMock

from dlrover.python.common.constants import (
    DistributionStrategy,
    ElasticJobLabel,
    NodeEventType,
    NodeStatus,
    NodeType,
    SchedulingLabel,
)
from dlrover.python.common.global_context import Context
from dlrover.python.common.node import Node, NodeGroupResource, NodeResource
from dlrover.python.master.scaler.base_scaler import ScalePlan
from dlrover.python.master.scaler.pod_scaler import (
    PodScaler,
    WorkspaceMode,
    new_tf_config,
)
from dlrover.python.tests.test_utils import mock_k8s_client

_dlrover_ctx = Context.singleton_instance()


def new_service_fn(node_type, node_id):
    return str(node_type) + "_" + str(node_id)


class PodScalerTest(unittest.TestCase):
    def setUp(self) -> None:
        os.environ["POD_IP"] = "127.0.0.1"
        mock_k8s_client()

    def tearDown(self) -> None:
        os.environ.clear()

    def test_init_pod_template(self):
        scaler = PodScaler("elasticjob-sample", "default")
        scaler._check_master_service_avaliable = mock.MagicMock(
            return_value=True
        )
        scaler.start()
        self.assertEqual(
            scaler._distribution_strategy,
            DistributionStrategy.PS,
        )
        worker_pod = scaler._replica_template[NodeType.WORKER]
        main_container = worker_pod.spec.containers[0]
        self.assertEqual(
            main_container.image, "dlrover/elasticjob:iris_estimator"
        )
        self.assertEqual(worker_pod.spec.restart_policy, "Always")
        self.assertListEqual(
            main_container.command,
            [
                "python",
                "-m",
                "model_zoo.iris.dnn_estimator",
                "--batch_size=32",
                "--training_steps=1000",
            ],
        )

    def test_check_master_service_avaliable(self):
        scaler = PodScaler("elasticjob-sample", "default")
        _dlrover_ctx.config_master_port()
        port = _dlrover_ctx.master_port
        if 22222 == port:
            wrong_port = 11111
        else:
            wrong_port = 22222
        passed = scaler._check_master_service_avaliable(
            "elasticjob-test-master", wrong_port, 1
        )
        self.assertFalse(passed)

        passed = scaler._check_master_service_avaliable(
            "localhost", wrong_port, 1
        )
        self.assertFalse(passed)

        passed = scaler._check_master_service_avaliable("localhost", port, 1)
        self.assertFalse(passed)

    def test_periodic_create_pod(self):
        scaler = PodScaler("elasticjob-sample", "default")
        scaler._check_master_service_avaliable = unittest.mock.MagicMock(
            return_value=True
        )
        scaler._create_pod = unittest.mock.MagicMock(return_value=True)
        scaler._create_service_for_pod = unittest.mock.MagicMock(
            return_value=True
        )

        scaler._create_node_queue = deque()
        test_num = 10
        for i in range(test_num):
            node = Node(
                NodeType.WORKER, i, NodeResource(4, 8192), rank_index=i
            )
            scaler._create_node_queue.append(node)

        scaler.start()
        time.sleep(3)
        scaler.stop()
        self.assertEqual(scaler._create_pod.call_count, test_num)
        self.assertEqual(scaler._create_service_for_pod.call_count, test_num)

    def test_remove_nodes_retains_failed_release_workspace_pod(self):
        scaler = PodScaler("elasticjob-sample", "default")
        scaler._k8s_client = MagicMock()
        scaler._k8s_client.get_pod.side_effect = lambda name: SimpleNamespace(
            status=SimpleNamespace(
                phase=("Failed" if name.endswith("-0") else "Running")
            )
        )
        resource = NodeResource(
            1,
            1024,
            pod_labels={
                "dlrover.io/run-mode": "workspace",
                "dlrover.io/workspace-exit-policy": "release",
            },
        )
        failed_node = Node(
            NodeType.WORKER,
            0,
            copy.deepcopy(resource),
            name="workspace-worker-0",
        )
        running_node = Node(
            NodeType.WORKER,
            1,
            copy.deepcopy(resource),
            name="workspace-worker-1",
        )
        plan = ScalePlan()
        plan.remove_nodes = [failed_node, running_node]

        scaler._remove_nodes(plan)

        scaler._k8s_client.delete_pod.assert_called_once_with(
            "workspace-worker-1"
        )

    def test_remove_nodes_waits_for_failed_training_pod_phase(self):
        scaler = PodScaler("elasticjob-sample", "default")
        scaler._k8s_client = MagicMock()
        scaler._k8s_client.client.list_namespaced_pod.side_effect = [
            SimpleNamespace(items=[SimpleNamespace(
                metadata=SimpleNamespace(name="workspace-worker-0"),
                status=SimpleNamespace(phase=phase),
            )])
            for phase in ("Running", "Failed")
        ]
        node = Node(
            NodeType.WORKER,
            0,
            NodeResource(
                1,
                1024,
                pod_labels={
                    "dlrover.io/run-mode": "workspace",
                    "dlrover.io/workspace-exit-policy": "release",
                },
            ),
            name="workspace-worker-0",
        )
        node.update_reported_status(NodeEventType.FAILED_EXITED)
        self.assertEqual(
            scaler.reconcile_workspace_release([node]),
            ({0: NodeStatus.RUNNING}, set()),
        )
        self.assertEqual(
            scaler.reconcile_workspace_release([node]),
            ({0: NodeStatus.FAILED}, {0}),
        )
        scaler._k8s_client.client.delete_namespaced_pod.assert_not_called()

    def test_remove_nodes_deletes_failed_reporter_on_phase_timeout(self):
        scaler = PodScaler("elasticjob-sample", "default")
        scaler._k8s_client = MagicMock()
        scaler._k8s_client.client.list_namespaced_pod.return_value = SimpleNamespace(
            items=[SimpleNamespace(
                metadata=SimpleNamespace(
                    name="workspace-worker-0", uid="pod-uid",
                    resource_version="42", deletion_timestamp=None,
                ),
                status=SimpleNamespace(phase="Running"),
            )]
        )
        node = Node(
            NodeType.WORKER,
            0,
            NodeResource(
                1,
                1024,
                pod_labels={
                    "dlrover.io/run-mode": "workspace",
                    "dlrover.io/workspace-exit-policy": "release",
                },
            ),
            name="workspace-worker-0",
        )
        node.update_reported_status(NodeEventType.FAILED_EXITED)
        self.assertEqual(
            scaler.reconcile_workspace_release([node], force=True),
            ({0: NodeStatus.RUNNING}, set()),
        )
        delete = scaler._k8s_client.client.delete_namespaced_pod
        delete.assert_called_once()
        self.assertEqual(delete.call_args.kwargs["body"].preconditions.uid, "pod-uid")
        self.assertEqual(
            delete.call_args.kwargs["body"].preconditions.resource_version, "42"
        )
        scaler._k8s_client.client.list_namespaced_pod.return_value.items = []
        self.assertEqual(
            scaler.reconcile_workspace_release([node], force=True),
            ({0: NodeStatus.DELETED}, {0}),
        )

    def test_create_pod(self):
        scaler = PodScaler("elasticjob-sample", "default")
        scaler._check_master_service_avaliable = mock.MagicMock(
            return_value=True
        )
        _dlrover_ctx.config_master_port()

        scaler.start()
        scaler._init_pod_config_by_job()
        scaler._distribution_strategy = DistributionStrategy.PS
        resource = NodeResource(4, 8192)
        node = Node(NodeType.WORKER, 0, resource, rank_index=0)
        node.group = 1024
        node.group_size = 1
        node.group_id = "rack-0"

        # mock field
        scaler._pod_stats = {
            NodeType.WORKER: 3,
            NodeType.CHIEF: 1,
            NodeType.PS: 2,
        }
        scaler._ps_addrs = [
            "elasticjob-sample-edljob-ps-0",
            "elasticjob-sample-edljob-ps-1",
        ]
        scaler._config_worker_num = 2
        pod = scaler._create_pod(node)
        self.assertEqual(
            pod.metadata.name, "elasticjob-sample-edljob-worker-0"
        )
        self.assertEqual(
            pod.metadata.labels[SchedulingLabel.NODE_GROUP], str(node.group)
        )

        main_container = pod.spec.containers[0]
        self.assertEqual(main_container.resources.limits["cpu"], 4)
        self.assertEqual(main_container.resources.limits["memory"], "8192Mi")
        self.assertEqual(main_container.env[-1].name, "TF_CONFIG")
        self.assertTrue(
            """{"type": "worker", "index": 0}"""
            in main_container.env[-1].value
        )
        env_worker_num = 0
        env_job_name = ""
        env_job_uid = ""
        host_ports = ""
        for env in main_container.env:
            if env.name == "WORKER_NUM":
                env_worker_num = int(env.value)
            elif env.name == "ELASTIC_JOB_NAME":
                env_job_name = env.value
            elif env.name == "JOB_UID":
                env_job_uid = env.value
            elif env.name == "HOST_PORTS":
                host_ports = env.value

        self.assertEqual(env_worker_num, 2)
        self.assertEqual(env_job_name, "elasticjob-sample")
        self.assertEqual(env_job_uid, "111-222")
        self.assertEqual(host_ports, "1,2,3,4,5")

        node = Node(NodeType.CHIEF, 0, resource, rank_index=0)
        pod = scaler._create_pod(node)
        main_container = pod.spec.containers[0]
        self.assertTrue(
            """{"type": "chief", "index": 0}""" in main_container.env[-1].value
        )
        node = Node(NodeType.PS, 0, resource, rank_index=0)
        pod = scaler._create_pod(node)
        main_container = pod.spec.containers[0]
        self.assertTrue(
            """{"type": "ps", "index": 0}""" in main_container.env[-1].value
        )

        node = Node(NodeType.WORKER, 0, resource, rank_index=0)
        pod = scaler._create_pod(node)
        main_container = pod.spec.containers[0]
        self.assertEqual(len(pod.spec.volumes), 1)
        self.assertEqual(pod.spec.volumes[0].name, "pvc-nas")
        self.assertEqual(len(main_container.volume_mounts), 1)

        scaler._distribution_strategy = DistributionStrategy.ALLREDUCE
        node = Node(NodeType.WORKER, 0, resource, rank_index=0)
        scaler._ps_addrs = []
        pod = scaler._create_pod(node)
        main_container = pod.spec.containers[0]
        world_size = -1
        rank = -1
        for env in main_container.env:
            if env.name == "WORLD_SIZE":
                world_size = int(env.value)
            elif env.name == "RANK":
                rank = int(env.value)
        self.assertEqual(world_size, 2)
        self.assertEqual(rank, 0)

    def test_create_workspace_pod(self):
        scaler = PodScaler("elasticjob-sample", "default")
        scaler._check_master_service_avaliable = mock.MagicMock(
            return_value=True
        )
        _dlrover_ctx.config_master_port()

        job = scaler._retry_to_get_job()
        job.setdefault("metadata", {}).setdefault("annotations", {})
        job["metadata"]["annotations"].update(
            {
                WorkspaceMode.ANNO_RUN_MODE: WorkspaceMode.RUN_MODE,
                WorkspaceMode.ANNO_JUPYTER_PORT: "18888",
                WorkspaceMode.ANNO_JUPYTER_SERVICE_TYPE: (
                    WorkspaceMode.SERVICE_TYPE_CLUSTER_IP
                ),
                WorkspaceMode.ANNO_JUPYTER_NODE_PORT: "30088",
                WorkspaceMode.ANNO_JUPYTER_TOKEN: "test-token",
                WorkspaceMode.ANNO_IDLE_COMMAND: "tail -f /dev/null",
                WorkspaceMode.ANNO_INIT_COMMAND: "echo init-workspace",
                WorkspaceMode.ANNO_RUNTIME_MONITOR_ENABLED: "false",
            }
        )
        scaler._job = job
        scaler._init_pod_config_by_job()
        scaler._master_addr = scaler._get_master_addr()

        resource = NodeResource(4, 8192)
        worker0 = Node(NodeType.WORKER, 0, resource, rank_index=0)
        pod = scaler._create_pod(worker0)
        main_container = pod.spec.containers[0]
        self.assertEqual(main_container.command, ["/bin/bash", "-lc"])
        self.assertIn("jupyter lab", main_container.args[0])
        self.assertIn("echo init-workspace", main_container.args[0])
        self.assertLess(
            main_container.args[0].index("echo init-workspace"),
            main_container.args[0].index("jupyter lab"),
        )
        self.assertIn("--port=18888", main_container.args[0])
        self.assertIn(
            '--ServerApp.token="${JUPYTER_TOKEN:-test-token}"',
            main_container.args[0],
        )
        envs = {env.name: env.value for env in main_container.env}
        self.assertEqual(envs[WorkspaceMode.ENV_MODE], "true")
        self.assertEqual(
            envs[WorkspaceMode.ENV_ROLE], WorkspaceMode.ROLE_JUPYTER
        )
        self.assertEqual(
            envs[WorkspaceMode.ENV_JUPYTER_SERVICE],
            "elasticjob-sample-jupyter",
        )
        self.assertEqual(
            pod.metadata.labels[WorkspaceMode.LABEL_RUN_MODE],
            WorkspaceMode.RUN_MODE,
        )

        worker1 = Node(NodeType.WORKER, 1, resource, rank_index=1)
        pod = scaler._create_pod(worker1)
        main_container = pod.spec.containers[0]
        self.assertEqual(main_container.command, ["/bin/bash", "-lc"])
        self.assertIn("echo init-workspace", main_container.args[0])
        self.assertLess(
            main_container.args[0].index("echo init-workspace"),
            main_container.args[0].index("tail -f /dev/null"),
        )
        envs = {env.name: env.value for env in main_container.env}
        self.assertEqual(envs[WorkspaceMode.ENV_ROLE], WorkspaceMode.ROLE_WORKER)

    def test_reject_legacy_runtime_mode(self):
        scaler = PodScaler("elasticjob-sample", "default")

        job = scaler._retry_to_get_job()
        job.setdefault("metadata", {}).setdefault("annotations", {})
        job["metadata"]["annotations"].update(
            {
                WorkspaceMode.ANNO_RUN_MODE: "runtime",
            }
        )
        scaler._job = job
        with self.assertRaisesRegex(ValueError, "unsupported"):
            scaler._init_pod_config_by_job()

    def test_release_workspace_rejects_jupyter(self):
        scaler = PodScaler("elasticjob-sample", "default")
        job = scaler._retry_to_get_job()
        annotations = job.setdefault("metadata", {}).setdefault(
            "annotations", {}
        )
        annotations.update(
            {
                WorkspaceMode.ANNO_RUN_MODE: WorkspaceMode.RUN_MODE,
                WorkspaceMode.ANNO_JUPYTER_ENABLED: "true",
                WorkspaceMode.ANNO_EXIT_POLICY: "release",
            }
        )
        scaler._job = job

        with self.assertRaisesRegex(ValueError, "jupyter-enabled=false"):
            scaler._init_pod_config_by_job()

    def test_release_workspace_runs_init_only_on_rank_zero(self):
        scaler = PodScaler("elasticjob-sample", "default")
        scaler._check_master_service_avaliable = mock.MagicMock(
            return_value=True
        )
        job = scaler._retry_to_get_job()
        annotations = job.setdefault("metadata", {}).setdefault(
            "annotations", {}
        )
        annotations.update(
            {
                WorkspaceMode.ANNO_RUN_MODE: WorkspaceMode.RUN_MODE,
                WorkspaceMode.ANNO_JUPYTER_ENABLED: "false",
                WorkspaceMode.ANNO_EXIT_POLICY: "release",
                WorkspaceMode.ANNO_INIT_COMMAND: (
                    "dlrover-workspace launch --network-mode all "
                    "--framework-cmd 'train.py'"
                ),
                WorkspaceMode.ANNO_RUNTIME_MONITOR_ENABLED: "false",
            }
        )
        scaler._job = job
        scaler._init_pod_config_by_job()
        scaler._master_addr = scaler._get_master_addr()
        resource = NodeResource(4, 8192)

        pod0 = scaler._create_pod(
            Node(NodeType.WORKER, 0, resource, rank_index=0)
        )
        pod1 = scaler._create_pod(
            Node(NodeType.WORKER, 1, resource, rank_index=1)
        )
        self.assertIn("dlrover-workspace launch", pod0.spec.containers[0].args[0])
        self.assertNotIn("dlrover-workspace launch", pod1.spec.containers[0].args[0])
        self.assertIn(
            "dlrover.python.workspace.release_supervisor wait",
            pod0.spec.containers[0].args[0],
        )
        self.assertIn(
            "dlrover.python.workspace.release_supervisor wait",
            pod1.spec.containers[0].args[0],
        )
        self.assertNotIn("sleep infinity", pod0.spec.containers[0].args[0])
        self.assertNotIn("sleep infinity", pod1.spec.containers[0].args[0])
        envs = {env.name: env.value for env in pod0.spec.containers[0].env}
        self.assertEqual(envs[WorkspaceMode.ENV_EXIT_POLICY], "release")
        self.assertEqual(envs[WorkspaceMode.ENV_DEBUG_MODE], "false")
        self.assertEqual(
            pod0.metadata.labels[WorkspaceMode.LABEL_EXIT_POLICY], "release"
        )

    def test_release_workspace_rejects_init_shell_chain(self):
        scaler = PodScaler("elasticjob-sample", "default")
        job = scaler._retry_to_get_job()
        annotations = job.setdefault("metadata", {}).setdefault(
            "annotations", {}
        )
        annotations.update(
            {
                WorkspaceMode.ANNO_RUN_MODE: WorkspaceMode.RUN_MODE,
                WorkspaceMode.ANNO_JUPYTER_ENABLED: "false",
                WorkspaceMode.ANNO_EXIT_POLICY: "release",
                WorkspaceMode.ANNO_INIT_COMMAND: (
                    "echo prepare && dlrover-workspace launch "
                    "--network-mode all --framework-cmd 'train.py'"
                ),
            }
        )
        scaler._job = job

        with self.assertRaisesRegex(ValueError, "full workspace launch"):
            scaler._init_pod_config_by_job()

    def test_release_debug_mode_runs_setup_on_every_worker(self):
        scaler = PodScaler("elasticjob-sample", "default")
        scaler._check_master_service_avaliable = mock.MagicMock(
            return_value=True
        )
        job = scaler._retry_to_get_job()
        annotations = job.setdefault("metadata", {}).setdefault(
            "annotations", {}
        )
        annotations.update(
            {
                WorkspaceMode.ANNO_RUN_MODE: WorkspaceMode.RUN_MODE,
                WorkspaceMode.ANNO_JUPYTER_ENABLED: "false",
                WorkspaceMode.ANNO_EXIT_POLICY: "release",
                WorkspaceMode.ANNO_DEBUG_MODE: "true",
                WorkspaceMode.ANNO_INIT_COMMAND: (
                    "pip install jupyter && "
                    "dlrover-workspace launch --network-mode all "
                    "--framework-cmd 'train.py'"
                ),
                WorkspaceMode.ANNO_RUNTIME_MONITOR_ENABLED: "false",
            }
        )
        scaler._job = job
        scaler._init_pod_config_by_job()
        scaler._master_addr = scaler._get_master_addr()
        resource = NodeResource(4, 8192)

        pod0 = scaler._create_pod(
            Node(NodeType.WORKER, 0, resource, rank_index=0)
        )
        pod1 = scaler._create_pod(
            Node(NodeType.WORKER, 1, resource, rank_index=1)
        )
        rank0_command = pod0.spec.containers[0].args[0]
        rank1_command = pod1.spec.containers[0].args[0]
        self.assertIn("pip install jupyter", rank0_command)
        self.assertIn("touch /tmp/dlrover-workspace-debug-ready", rank0_command)
        self.assertIn("dlrover-workspace launch", rank0_command)
        self.assertIn("pip install jupyter", rank1_command)
        self.assertIn("touch /tmp/dlrover-workspace-debug-ready", rank1_command)
        self.assertNotIn("dlrover-workspace launch", rank1_command)
        envs = {env.name: env.value for env in pod1.spec.containers[0].env}
        self.assertEqual(envs[WorkspaceMode.ENV_DEBUG_MODE], "true")
        self.assertEqual(
            envs[WorkspaceMode.ENV_DEBUG_READY_FILE],
            WorkspaceMode.DEBUG_READY_FILE,
        )

    def test_debug_mode_requires_release_policy(self):
        scaler = PodScaler("elasticjob-sample", "default")
        job = scaler._retry_to_get_job()
        annotations = job.setdefault("metadata", {}).setdefault(
            "annotations", {}
        )
        annotations.update(
            {
                WorkspaceMode.ANNO_RUN_MODE: WorkspaceMode.RUN_MODE,
                WorkspaceMode.ANNO_DEBUG_MODE: "true",
            }
        )
        scaler._job = job

        with self.assertRaisesRegex(ValueError, "only supported"):
            scaler._init_pod_config_by_job()

    def test_create_workspace_service(self):
        scaler = PodScaler("elasticjob-sample", "default")
        scaler._check_master_service_avaliable = mock.MagicMock(
            return_value=True
        )
        job = scaler._retry_to_get_job()
        job.setdefault("metadata", {}).setdefault("annotations", {})
        job["metadata"]["annotations"].update(
            {WorkspaceMode.ANNO_RUN_MODE: WorkspaceMode.RUN_MODE}
        )
        scaler._job = job
        scaler._init_pod_config_by_job()
        scaler._svc_factory.create_service = mock.MagicMock(return_value=True)

        node = Node(NodeType.WORKER, 0, NodeResource(4, 8192), rank_index=0)
        self.assertTrue(scaler._create_workspace_service_if_needed(node))
        scaler._svc_factory.create_service.assert_called_once()
        _, kwargs = scaler._svc_factory.create_service.call_args
        self.assertEqual(kwargs["port"], WorkspaceMode.JUPYTER_PORT)
        self.assertEqual(kwargs["port_name"], WorkspaceMode.SERVICE_SUFFIX)
        self.assertEqual(
            kwargs["service_type"], WorkspaceMode.SERVICE_TYPE_NODE_PORT
        )
        self.assertIsNone(kwargs["node_port"])
        self.assertEqual(
            kwargs["selector"][ElasticJobLabel.RANK_INDEX_KEY], "0"
        )

        job["metadata"]["annotations"].update(
            {
                WorkspaceMode.ANNO_JUPYTER_NODE_PORT: "30088",
                WorkspaceMode.ANNO_JUPYTER_SERVICE_TYPE: (
                    WorkspaceMode.SERVICE_TYPE_LOAD_BALANCER
                )
            }
        )
        scaler._init_pod_config_by_job()
        scaler._svc_factory.create_service.reset_mock()
        node = Node(NodeType.WORKER, 0, NodeResource(4, 8192), rank_index=0)
        self.assertTrue(scaler._create_workspace_service_if_needed(node))
        _, kwargs = scaler._svc_factory.create_service.call_args
        self.assertEqual(
            kwargs["service_type"], WorkspaceMode.SERVICE_TYPE_LOAD_BALANCER
        )
        self.assertIsNone(kwargs["node_port"])

        job["metadata"]["annotations"].update(
            {
                WorkspaceMode.ANNO_JUPYTER_SERVICE_TYPE: (
                    WorkspaceMode.SERVICE_TYPE_NODE_PORT
                ),
                WorkspaceMode.ANNO_JUPYTER_NODE_PORT: "30088",
            }
        )
        scaler._init_pod_config_by_job()
        scaler._svc_factory.create_service.reset_mock()
        node = Node(NodeType.WORKER, 0, NodeResource(4, 8192), rank_index=0)
        self.assertTrue(scaler._create_workspace_service_if_needed(node))
        _, kwargs = scaler._svc_factory.create_service.call_args
        self.assertEqual(
            kwargs["service_type"], WorkspaceMode.SERVICE_TYPE_NODE_PORT
        )
        self.assertEqual(kwargs["node_port"], 30088)

        job["metadata"]["annotations"].update(
            {WorkspaceMode.ANNO_JUPYTER_NODE_PORT: "invalid"}
        )
        scaler._init_pod_config_by_job()
        scaler._svc_factory.create_service.reset_mock()
        node = Node(NodeType.WORKER, 0, NodeResource(4, 8192), rank_index=0)
        self.assertTrue(scaler._create_workspace_service_if_needed(node))
        _, kwargs = scaler._svc_factory.create_service.call_args
        self.assertEqual(
            kwargs["service_type"], WorkspaceMode.SERVICE_TYPE_NODE_PORT
        )
        self.assertIsNone(kwargs["node_port"])

        job["metadata"]["annotations"].update(
            {WorkspaceMode.ANNO_JUPYTER_SERVICE_TYPE: "invalid"}
        )
        scaler._init_pod_config_by_job()
        scaler._svc_factory.create_service.reset_mock()
        node = Node(NodeType.WORKER, 0, NodeResource(4, 8192), rank_index=0)
        self.assertTrue(scaler._create_workspace_service_if_needed(node))
        _, kwargs = scaler._svc_factory.create_service.call_args
        self.assertEqual(
            kwargs["service_type"], WorkspaceMode.SERVICE_TYPE_NODE_PORT
        )

        scaler._svc_factory.create_service.reset_mock()
        node = Node(NodeType.WORKER, 1, NodeResource(4, 8192), rank_index=1)
        self.assertTrue(scaler._create_workspace_service_if_needed(node))
        scaler._svc_factory.create_service.assert_not_called()

    def test_create_pod_from_queue(self):
        scaler = PodScaler("elasticjob-sample", "default")
        worker0 = Node(
            NodeType.WORKER,
            0,
            NodeResource(1, 1024, gpu_num=1),
            name="test-worker-0",
        )
        scaler._pod_stats = {}
        scaler._pod_stats[NodeType.WORKER] = 3
        scaler._create_node_queue.append(worker0)
        scaler._create_service_for_pod = mock.MagicMock(  # type: ignore
            return_value=True
        )
        scaler._create_pod_from_queue(scaler._create_node_queue.popleft())
        self.assertEqual(len(scaler._create_node_queue), 0)

        scaler._create_service_for_pod = mock.MagicMock(  # type: ignore
            return_value=False
        )
        worker1 = Node(
            NodeType.WORKER,
            1,
            NodeResource(1, 1024),
            name="test-worker-0",
        )
        scaler._create_node_queue.extend([worker0, worker1])

        scaler._create_pod = mock.MagicMock(side_effect=Exception())
        scaler._create_pod_from_queue(scaler._create_node_queue.popleft())

    def test_scale(self):
        scaler = PodScaler("elasticjob-sample", "default")
        scaler._started = True
        scaler._distribution_strategy = DistributionStrategy.PS
        resource = NodeResource(4, 8192)

        scaler._elasticjob_exists = mock.MagicMock(return_value=True)
        scaler.scale(ScalePlan())

        scale_plan = ScalePlan()
        self.assertTrue(scale_plan.empty())
        scale_plan.node_group_resources = {
            NodeType.WORKER: NodeGroupResource(5, resource),
            NodeType.CHIEF: NodeGroupResource(1, resource),
            NodeType.PS: NodeGroupResource(2, resource),
        }
        scale_plan.ps_addrs = ["ps-0:22222"]
        scaler.scale(scale_plan)
        self.assertEqual(len(scaler._create_node_queue), 3)
        self.assertListEqual(scaler._ps_addrs, scale_plan.ps_addrs)

        worker_ids = []
        chief_ids = []
        for node in scaler._create_node_queue:
            if node.type == NodeType.WORKER:
                worker_ids.append(node.id)
            elif node.type == NodeType.CHIEF:
                chief_ids.append(node.id)
        self.assertListEqual(chief_ids, [0])
        self.assertListEqual(worker_ids, [3, 4])
        scaler._create_node_queue.clear()

        scale_plan.node_group_resources = {
            NodeType.WORKER: NodeGroupResource(3, resource),
            NodeType.CHIEF: NodeGroupResource(1, resource),
            NodeType.PS: NodeGroupResource(2, resource),
        }
        scale_plan.launch_nodes.append(
            Node(NodeType.WORKER, 1, NodeResource(0, 0))
        )
        scale_plan.remove_nodes.append(
            Node(NodeType.WORKER, 3, NodeResource(0, 0))
        )
        plan_json = scale_plan.to_json()
        self.assertTrue("paral_config" not in plan_json)
        scaler.scale(scale_plan)
        self.assertFalse(scale_plan.empty())
        self.assertEqual(len(scaler._create_node_queue), 2)

        # test wait async execution
        scaler._create_node_queue.clear()
        mock_future = MagicMock()
        mock_future.done.return_value = False
        scaler._create_node_futures.append(mock_future)

        thread = Thread(target=scaler.scale, args=(scale_plan,))
        thread.start()
        thread.join(timeout=1)
        self.assertTrue(thread.is_alive())
        scaler._started = False

    def test_scale_thread(self):
        scaler = PodScaler("elasticjob-sample", "default")
        scaler._check_master_service_avaliable = mock.MagicMock(
            return_value=True
        )
        scaler.start()
        scaler._distribution_strategy = DistributionStrategy.PS
        resource = NodeResource(4, 8192)
        scale_plan = ScalePlan()
        scale_plan.launch_nodes.append(Node(NodeType.WORKER, 1, resource))
        scale_plan.ps_addrs = ["ps-0:22222"]
        scaler.scale(scale_plan)
        self.assertEqual(len(scaler._create_node_queue), 1)
        scaler._create_node_queue.clear()
        scale_plan = ScalePlan()
        scale_plan.launch_nodes.append(Node(NodeType.WORKER, 2, resource))
        scale_plan.ps_addrs = ["ps-0:22222"]
        scaler.scale(scale_plan)
        self.assertEqual(len(scaler._create_node_queue), 1)
        scaler._create_node_queue.clear()

    def test_new_tf_config(self):
        pod_stats = {NodeType.WORKER: 1}

        tf_config = new_tf_config(
            pod_stats, new_service_fn, NodeType.WORKER, 0, []
        )
        self.assertDictEqual(
            tf_config,
            {
                "cluster": {"ps": [], "worker": ["worker_0"]},
                "task": {"type": "worker", "index": 0},
            },
        )

    def test_scale_up_pods(self):
        scaler = PodScaler("elasticjob-sample", "default")
        scaler._distribution_strategy = DistributionStrategy.PS
        resource = NodeResource(4, 8192)
        scale_plan = ScalePlan()
        self.assertTrue(scale_plan.empty())
        scale_plan.node_group_resources = {
            NodeType.WORKER: NodeGroupResource(5, resource),
            NodeType.CHIEF: NodeGroupResource(1, resource),
            NodeType.PS: NodeGroupResource(2, resource),
        }
        cur_nodes = [Node(NodeType.WORKER, 1, rank_index=0)]
        scaler._scale_up_pods(NodeType.WORKER, scale_plan, cur_nodes, 1)
        self.assertEqual(len(scaler._create_node_queue), 4)
        self.assertEqual(
            scaler._create_node_queue[0].service_addr,
            "elasticjob-sample-edljob-worker-1.default.svc:3333",
        )
        # Without group affinity, created nodes carry no group info.
        for node in scaler._create_node_queue:
            self.assertIsNone(node.group)
            self.assertIsNone(node.group_size)
            self.assertIsNone(node.group_id)

    def test_scale_up_pods_with_group_affinity(self):
        scaler = PodScaler("elasticjob-sample", "default")
        scaler._distribution_strategy = DistributionStrategy.PS
        scaler.set_group_affinity({0: 1, 1: 1})
        resource = NodeResource(4, 8192)
        scale_plan = ScalePlan()
        scale_plan.node_group_resources = {
            NodeType.WORKER: NodeGroupResource(2, resource),
        }
        scaler._scale_up_pods(NodeType.WORKER, scale_plan, [], 0)
        self.assertEqual(len(scaler._create_node_queue), 2)
        rank0 = scaler._create_node_queue[0]
        rank1 = scaler._create_node_queue[1]
        self.assertEqual(rank0.rank_index, 0)
        self.assertEqual(rank0.group, 0)
        self.assertEqual(rank0.group_size, 2)
        self.assertEqual(rank0.group_id, 0)
        self.assertEqual(rank1.rank_index, 1)
        self.assertEqual(rank1.group, 1)
        self.assertEqual(rank1.group_size, 2)
        self.assertEqual(rank1.group_id, 1)

    def test_update_job_pods(self):
        scaler = PodScaler("elasticjob-sample", "default")

        test_nodes = [
            Node(NodeType.WORKER, 0, rank_index=0, status=NodeStatus.RUNNING),
            Node(NodeType.WORKER, 1, rank_index=1, status=NodeStatus.RUNNING),
            Node(NodeType.WORKER, 2, rank_index=2, status=NodeStatus.FAILED),
            Node(NodeType.WORKER, 3, rank_index=3, status=NodeStatus.RUNNING),
        ]
        job_pods = {NodeType.WORKER: test_nodes}
        scaler._update_job_pods(job_pods)
        self.assertEqual(scaler._safe_get_pod_status(NodeType.WORKER, 0), 4)
        self.assertEqual(
            scaler._safe_get_alive_pod_status(NodeType.WORKER, 0), 3
        )

    def test_multi_scales_with_merge(self):
        scaler = PodScaler("elasticjob-sample", "default")
        scaler._check_master_service_avaliable = mock.MagicMock(
            return_value=True
        )
        scaler._elasticjob_exists = mock.MagicMock(return_value=True)
        scaler._job_context.is_suspended = mock.MagicMock(return_value=False)
        scaler._remove_nodes = mock.MagicMock()
        scaler.start()

        resource = NodeResource(4, 8192)

        first_last_time = 1
        second_last_time = 1
        third_last_time = 1
        try:
            scale_plan1 = ScalePlan()
            scale_plan1.launch_nodes.append(
                Node(NodeType.WORKER, 0, resource, relaunch_count=1)
            )

            start_time = time.time()
            scaler.scale(scale_plan1, with_merge=True)
            first_last_time = time.time() - start_time
            self.assertFalse(scaler._pending_plan)

            scale_plan2 = ScalePlan()
            scale_plan2.launch_nodes.append(
                Node(NodeType.WORKER, 1, resource, relaunch_count=1)
            )

            start_time = time.time()
            scaler.scale(scale_plan2, with_merge=True)
            second_last_time = time.time() - start_time
            self.assertTrue(scaler._pending_plan)
            self.assertTrue(len(scaler._pending_plan.launch_nodes), 1)

            scale_plan3 = ScalePlan()
            scale_plan3.launch_nodes.append(
                Node(NodeType.WORKER, 2, resource, relaunch_count=1)
            )

            start_time = time.time()
            scaler.scale(scale_plan3, with_merge=True)
            third_last_time = time.time() - start_time
            self.assertTrue(scaler._pending_plan)
            self.assertTrue(len(scaler._pending_plan.launch_nodes), 2)
        finally:
            self.assertTrue(first_last_time < 1)
            self.assertTrue(second_last_time < 1)
            self.assertTrue(third_last_time < 1)
            scaler.stop()

    def test_execute_scale_with_pending(self):
        scaler = PodScaler("elasticjob-sample", "default")
        scaler._check_master_service_avaliable = mock.MagicMock(
            return_value=True
        )
        scaler._elasticjob_exists = mock.MagicMock(return_value=True)
        scaler._job_context.is_suspended = mock.MagicMock(return_value=False)
        scaler._remove_nodes = mock.MagicMock()
        scaler._scale = mock.MagicMock()
        scaler.start()

        try:
            pending_plan = ScalePlan()
            pending_plan.launch_nodes.append(
                Node(NodeType.WORKER, 1, NodeResource(1, 1024))
            )
            scaler._pending_plan = pending_plan
            scaler._execute_scale_with_pending(ScalePlan())

            self.assertIsNone(scaler._pending_plan)
            self.assertEqual(scaler._scale.call_count, 2)  # initial + pending
        finally:
            scaler.stop()

    def test_scale_waiting_for_node_creation_queue(self):
        scaler = PodScaler("test", "default")
        scaler._remove_nodes = mock.MagicMock()
        scaler._elasticjob_exists = mock.MagicMock(return_value=True)
        scaler._job_context.is_suspended = mock.MagicMock(return_value=False)
        scaler._job_context.is_stopped = mock.MagicMock(return_value=False)
        scaler._list_job_pods = mock.MagicMock(return_value=[])

        # Add nodes to queue
        test_nodes = [
            Node(NodeType.WORKER, 1, NodeResource(1, 1024)),
            Node(NodeType.WORKER, 2, NodeResource(1, 1024)),
        ]
        for node in test_nodes:
            scaler._create_node_queue.append(node)

        # Track sleep calls to verify behavior
        sleep_calls = []

        def mock_sleep(seconds):
            sleep_calls.append(seconds)
            # After 2 sleep calls (10 seconds), clear queue to exit
            if len(sleep_calls) >= 2:
                scaler._create_node_queue.clear()

        # Mock sleep and run scale
        with mock.patch("time.sleep", side_effect=mock_sleep):
            scaler._started = True

            # Create a simple plan
            plan = ScalePlan()
            plan.launch_nodes = test_nodes

            # Run scale (this should hit the waiting loop)
            import threading

            scale_thread = threading.Thread(target=scaler._scale, args=(plan,))
            scale_thread.daemon = True
            scale_thread.start()

            # Wait briefly for execution
            scale_thread.join(timeout=0.5)

            # Stop the scaler
            scaler._started = False

        # Verify 5-second sleep was called for queue waiting
        sleep_5_calls = [s for s in sleep_calls if s == 5]
        self.assertGreater(
            len(sleep_5_calls), 0, "Expected 5-second sleep calls"
        )
