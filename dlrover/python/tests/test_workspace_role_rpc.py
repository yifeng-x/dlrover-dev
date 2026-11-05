# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest
from types import SimpleNamespace
from unittest import mock

from dlrover.python.common import comm
from dlrover.python.elastic_agent.master_client import (
    GrpcMasterClient,
    HttpMasterClient,
)
from dlrover.python.master.node.workspace_task import WorkspaceTaskCoordinator


class TaskTransportTests(unittest.TestCase):
    def test_grpc_is_single_attempt_and_bounded(self):
        instance = object.__new__(GrpcMasterClient)
        instance._channel = None
        instance._node_id, instance._node_type = 0, "worker"
        instance._stub = mock.Mock()
        instance._gen_request_meta = mock.Mock(return_value=())
        instance._stub.get.return_value = SimpleNamespace(
            data=comm.WorkspaceTaskResponse({"state": "RUNNING"}).serialize()
        )
        self.assertEqual(
            instance.workspace_task("status", "run", "epoch")["state"],
            "RUNNING",
        )
        self.assertEqual(instance._stub.get.call_args.kwargs["timeout"], 5)
        instance._stub.get.side_effect = RuntimeError("disconnected")
        instance._stub.reset_mock()
        with self.assertRaises(RuntimeError):
            instance.workspace_task("heartbeat", "run", "epoch")
        self.assertEqual(instance._stub.get.call_count, 1)

    def test_http_has_explicit_transport_timeout(self):
        instance = object.__new__(HttpMasterClient)
        instance._node_id, instance._node_type = 0, "worker"
        instance._master_addr = "master:1234"
        instance._get_headers = mock.Mock(return_value={})
        with mock.patch(
            "dlrover.python.elastic_agent.master_client.requests.post"
        ) as post:
            post.side_effect = RuntimeError("timeout")
            with self.assertRaises(RuntimeError):
                instance.workspace_task("status", "run", "epoch")
        self.assertEqual(post.call_count, 1)
        self.assertEqual(post.call_args.kwargs["timeout"], 5)

    def test_serialization_roundtrip(self):
        original = comm.WorkspaceTaskRequest(
            "heartbeat", "run", "epoch", {"rank": 3, "cleaned": True}
        )
        self.assertEqual(
            comm.deserialize_message(original.serialize()), original
        )

    def test_old_event_does_not_change_new_run(self):
        coordinator = WorkspaceTaskCoordinator()
        member = {
            "rank": 0,
            "uid": "u",
            "ip": "10.0.0.1",
            "launch_id": "l",
            "roles": ["trainer"],
        }
        coordinator.exchange(
            "register", "new", "", {"members": [member], "ready_timeout": 600}
        )
        result = coordinator.exchange(
            "stop", "old", coordinator.epoch, {"outcome": "FAILED"}
        )
        self.assertEqual(result["state"], "FENCED")
        self.assertEqual(coordinator.task["state"], "PREPARING")
        coordinator.observe_pod(0, "u", "10.0.0.2", True)
        self.assertEqual(coordinator.task["state"], "STOPPING")


class TaskServicerTests(unittest.TestCase):
    def setUp(self):
        from dlrover.python.common.node import Node, NodeResource
        from dlrover.python.master.servicer import GrpcMasterServicer

        self.node = Node(
            "worker", 0, status="Running", config_resource=NodeResource(1, 100)
        )
        self.node.config_resource.kwargs.update(
            pod_labels={"dlrover.io/run-mode": "workspace"},
            workspace_pod_uid="uid",
            workspace_pod_ip="10.0.0.1",
        )
        self.server = object.__new__(GrpcMasterServicer)
        self.server._workspace_tasks = WorkspaceTaskCoordinator()
        self.server._job_manager = SimpleNamespace(
            _job_args=SimpleNamespace(runtime_elastic_dimension="horizontal"),
            _job_autoscaler=SimpleNamespace(_autoscaling_started=False),
        )
        self.context = mock.patch(
            "dlrover.python.master.servicer._job_ctx"
        ).start()
        self.addCleanup(mock.patch.stopall)
        self.context.job_node.return_value = self.node
        self.context.job_nodes_by_type.return_value = {0: self.node}
        self.member = {
            "rank": 0,
            "uid": "uid",
            "ip": "10.0.0.1",
            "launch_id": "launch",
            "roles": ["trainer"],
        }

    def get(self, operation, run="run", epoch="", **payload):
        message = comm.WorkspaceTaskRequest(operation, run, epoch, payload)
        request = SimpleNamespace(
            node_type="worker", node_id=0, data=message.serialize()
        )
        context = mock.Mock()
        context.invocation_metadata.return_value = []
        return comm.deserialize_message(
            self.server.get(request, context).data
        ).payload

    def test_register_and_heartbeat_validate_snapshot(self):
        response = self.get(
            "register", members=[self.member], ready_timeout=600
        )
        self.assertNotIn("error", response)
        response = self.get(
            "heartbeat", epoch=response["epoch"], **self.member
        )
        self.assertEqual(response["state"], "SERVICES_STARTING")
        self.node.config_resource.kwargs["workspace_pod_ip"] = "10.0.0.2"
        response = self.get(
            "heartbeat", epoch=response["epoch"], **self.member
        )
        self.assertEqual(response["state"], "STOPPING")

    def test_non_workspace_rejected_without_creating_task(self):
        self.node.config_resource.kwargs["pod_labels"] = {}
        response = self.get(
            "register", members=[self.member], ready_timeout=600
        )
        self.assertIn("error", response)
        self.assertIsNone(self.server._workspace_tasks.task)

    def test_stale_heartbeat_cannot_fail_current_task(self):
        response = self.get(
            "register", members=[self.member], ready_timeout=600
        )
        self.node.config_resource.kwargs["workspace_pod_ip"] = "old-ip"
        response = self.get(
            "heartbeat", run="old", epoch=response["epoch"], **self.member
        )
        self.assertEqual(response["state"], "FENCED")
        self.assertEqual(
            self.server._workspace_tasks.task["state"], "PREPARING"
        )


if __name__ == "__main__":
    unittest.main()
