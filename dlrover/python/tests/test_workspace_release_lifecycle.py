# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import copy
import os
import threading
import unittest
from collections import deque
from types import SimpleNamespace
from unittest import mock

from dlrover.python.common.constants import (
    NodeEventType, NodeExitReason, NodeStatus, NodeType,
)
from dlrover.python.common.node import Node, NodeEvent, NodeResource
from dlrover.python.common.log import default_logger as logger
from dlrover.python.elastic_agent.master_client import GrpcMasterClient
from dlrover.python.master.dist_master import DistributedJobMaster
from dlrover.python.master.monitor.perf_monitor import PerfMonitor
from dlrover.python.master.node.dist_job_manager import create_job_manager
from dlrover.python.master.node.job_context import get_job_context
from dlrover.python.master.scaler.pod_scaler import PodScaler
from dlrover.python.tests.test_utils import (
    MockK8sAllreduceJobArgs,
    mock_k8s_client,
)


class ReleaseTerminalReportTest(unittest.TestCase):
    def setUp(self):
        self.env = mock.patch.dict(os.environ, {
            "DLROVER_WORKSPACE_MODE": "true",
            "DLROVER_WORKSPACE_EXIT_POLICY": "release",
        })
        self.env.start()
        self.addCleanup(self.env.stop)
        with mock.patch.object(GrpcMasterClient, "_open_grpc_channel"), mock.patch(
            "dlrover.python.elastic_agent.master_client.find_free_port",
            return_value=12345,
        ):
            self.client = GrpcMasterClient("master:50001", 0, "worker")
        self.client._channel = None
        self.client._stub = mock.MagicMock()

    def test_report_failure_does_not_replace_training_exception(self):
        self.client._stub.report.side_effect = RuntimeError("master unavailable")
        original = ValueError("model path does not exist")
        with mock.patch("dlrover.python.elastic_agent.master_client.time.sleep") as sleep:
            try:
                try:
                    raise original
                finally:
                    self.client.report_failed_exited()
            except ValueError as error:
                self.assertIs(error, original)
        self.assertEqual(self.client._stub.report.call_count, 3)
        self.assertEqual(sleep.call_args_list, [mock.call(0.5)] * 2)
        for call in self.client._stub.report.call_args_list:
            self.assertEqual(call.kwargs["timeout"], 2)

    def test_lost_ack_then_success(self):
        self.client._stub.report.side_effect = [
            RuntimeError("ACK lost"), SimpleNamespace(success=True),
        ]
        with mock.patch("dlrover.python.elastic_agent.master_client.time.sleep"):
            self.assertTrue(self.client.report_succeeded_exited().success)
        self.assertEqual(self.client._stub.report.call_count, 2)

    def test_retain_uses_existing_report_path(self):
        with mock.patch.dict(os.environ, {"DLROVER_WORKSPACE_EXIT_POLICY": "retain"}):
            with mock.patch.object(self.client, "_report") as report:
                self.client.report_failed_exited()
        report.assert_called_once()
        self.client._stub.report.assert_not_called()


class ReleaseLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.k8s = mock.patch(
            "dlrover.python.scheduler.kubernetes.k8sClient.singleton_instance",
            return_value=mock_k8s_client(),
        )
        self.k8s.start()
        self.addCleanup(self.k8s.stop)
        self.context = get_job_context()
        self.context.clear_job_nodes()
        args = MockK8sAllreduceJobArgs()
        args.initilize(worker_count=2)
        args.run_mode = "workspace"
        args.workspace_exit_policy = "release"
        self.manager = create_job_manager(args, PerfMonitor())
        self.manager._init_nodes()
        self.manager._scaler = mock.MagicMock()
        self.manager._scaler.reconcile_workspace_release.return_value = (
            {0: NodeStatus.RUNNING, 1: NodeStatus.RUNNING}, set()
        )
        for node_id in (0, 1):
            node = self.context.job_node(NodeType.WORKER, node_id)
            node.status = NodeStatus.RUNNING

    def report(self, node_id, event=NodeEventType.FAILED_EXITED):
        self.manager.process_reported_node_event(NodeEvent(
            event, self.context.job_node(NodeType.WORKER, node_id)
        ))

    def test_rpc_returns_without_cleanup_or_false_pod_exit(self):
        self.report(0)
        self.report(0)
        self.manager._scaler.scale.assert_not_called()
        self.manager._scaler.reconcile_workspace_release.assert_not_called()
        self.assertFalse(self.manager.all_workers_exited())
        node = self.context.job_node(NodeType.WORKER, 0)
        self.assertEqual(node.status, NodeStatus.RUNNING)
        self.assertFalse(node.is_released)

    def test_shared_deadline_is_not_reset_by_duplicate_report(self):
        self.report(0)
        with mock.patch("time.monotonic", return_value=100):
            self.assertIsNone(self.manager.reconcile_workspace_release())
        self.report(0)
        with mock.patch("time.monotonic", return_value=161):
            self.assertIsNone(self.manager.reconcile_workspace_release())
        self.assertEqual(self.manager._workspace_release_started_at, 100)
        self.assertTrue(
            self.manager._scaler.reconcile_workspace_release.call_args.kwargs["force"]
        )

    def test_api_outage_never_claims_cleanup_complete(self):
        self.report(0)
        self.manager._scaler.reconcile_workspace_release.return_value = ({}, set())
        with mock.patch("time.monotonic", return_value=100):
            self.assertIsNone(self.manager.reconcile_workspace_release())
        with mock.patch("time.monotonic", return_value=221):
            with self.assertLogs(logger, level="ERROR") as logs:
                self.assertFalse(self.manager.reconcile_workspace_release())
        self.assertIn("resources may remain", " ".join(logs.output))
        self.assertFalse(self.context.job_node(NodeType.WORKER, 0).is_released)

    def test_success_without_rpc_is_recovered_from_pod_events(self):
        for node_id in (0, 1):
            event_node = copy.deepcopy(self.context.job_node(NodeType.WORKER, node_id))
            event_node.status = NodeStatus.SUCCEEDED
            self.manager._process_event(NodeEvent(NodeEventType.MODIFIED, event_node))
        self.manager._scaler.reconcile_workspace_release.side_effect = [
            ({0: NodeStatus.SUCCEEDED, 1: NodeStatus.SUCCEEDED}, set()),
            ({0: NodeStatus.DELETED, 1: NodeStatus.DELETED}, {0, 1}),
        ]
        self.assertIsNone(self.manager.reconcile_workspace_release())
        self.assertTrue(self.manager.reconcile_workspace_release())

    def test_concurrent_failure_report_is_not_overwritten_by_list(self):
        self.report(0, NodeEventType.SUCCEEDED_EXITED)
        self.report(1, NodeEventType.SUCCEEDED_EXITED)

        def observe(*args, **kwargs):
            node = copy.deepcopy(self.context.job_node(NodeType.WORKER, 1))
            node.status = NodeStatus.FAILED
            self.manager._process_event(NodeEvent(NodeEventType.MODIFIED, node))
            return {0: NodeStatus.SUCCEEDED, 1: NodeStatus.RUNNING}, set()

        self.manager._scaler.reconcile_workspace_release.side_effect = observe
        self.assertIsNone(self.manager.reconcile_workspace_release())
        self.assertTrue(self.manager._workspace_release_failed)
        self.assertEqual(
            self.context.job_node(NodeType.WORKER, 1).status, NodeStatus.FAILED
        )

    def test_report_during_pod_query_is_preserved(self):
        self.report(0)

        def observe(*args, **kwargs):
            self.report(1, NodeEventType.SUCCEEDED_EXITED)
            return {0: NodeStatus.RUNNING, 1: NodeStatus.RUNNING}, set()

        self.manager._scaler.reconcile_workspace_release.side_effect = observe
        self.assertIsNone(self.manager.reconcile_workspace_release())
        self.assertTrue(
            self.context.job_node(NodeType.WORKER, 1).is_succeeded_and_exited()
        )

    def test_heartbeat_failure_does_not_claim_pod_exit(self):
        node = copy.deepcopy(self.context.job_node(NodeType.WORKER, 0))
        node.exit_reason = NodeExitReason.NO_HEARTBEAT
        node.status = NodeStatus.FAILED
        self.manager._process_event(NodeEvent(NodeEventType.DELETED, node))
        self.assertIsNone(self.manager.reconcile_workspace_release())
        current = self.context.job_node(NodeType.WORKER, 0)
        self.assertEqual(current.status, NodeStatus.RUNNING)
        self.assertFalse(current.is_released)

    def test_master_waits_for_cleanup_even_if_logical_nodes_exited(self):
        master = DistributedJobMaster.__new__(DistributedJobMaster)
        master._workspace_release = True
        master._exit_reason = ""
        master._job_ctx = mock.MagicMock()
        master._job_ctx.is_request_stopped.return_value = False
        master.job_manager = self.manager
        master.job_manager.should_early_stop = mock.Mock(return_value=(False, "", ""))
        master.job_manager.reconcile_workspace_release = mock.Mock(
            side_effect=[None, None, False]
        )
        master.job_manager.all_workers_exited = mock.Mock(return_value=True)
        master.job_manager.stop = mock.Mock()
        master.diagnosis_manager = mock.MagicMock()
        master._event_reporter = mock.MagicMock()
        master._job_evt = mock.MagicMock()
        master._job_args = mock.MagicMock()
        master.stop = mock.Mock()
        with mock.patch("dlrover.python.master.dist_master.time.sleep") as sleep:
            self.assertEqual(master.run(), 1)
        self.assertEqual(sleep.call_count, 2)
        master.job_manager.all_workers_exited.assert_not_called()
        master.stop.assert_called_once()

    def test_master_waits_for_inflight_rpc_on_shutdown(self):
        master = DistributedJobMaster.__new__(DistributedJobMaster)
        master._workspace_release = True
        master._exit_code = 1
        master._exit_reason = "TrainingFailed"
        master.stop_report_heartbeat = mock.Mock()
        master.job_metric_collector = None
        master._metrics_server = mock.Mock()
        master._master_server = mock.Mock()
        master.stop()
        master._master_server.stop.assert_called_once_with(grace=5)
        master._master_server.stop.return_value.wait.assert_called_once_with(
            timeout=5
        )


class ReleasePodReconcileTest(unittest.TestCase):
    def setUp(self):
        self.scaler = PodScaler.__new__(PodScaler)
        self.scaler._namespace = "default"
        self.scaler._job_name = "test"
        self.scaler._k8s_client = mock.MagicMock()
        self.scaler._create_node_futures = []
        self.scaler._create_node_queue = deque()
        self.scaler._scaling_lock = threading.Lock()
        self.scaler._remove_not_create_pod = mock.Mock()
        self.node = Node("worker", 0, NodeResource(1, 1024), name="test-worker-0")

    def pod(self, phase):
        return SimpleNamespace(
            metadata=SimpleNamespace(
                name=self.node.name, uid="uid", resource_version="12",
                deletion_timestamp=None,
            ),
            status=SimpleNamespace(phase=phase),
        )

    def test_unknown_api_state_does_not_delete(self):
        api = self.scaler._k8s_client.client
        api.list_namespaced_pod.side_effect = RuntimeError("API unavailable")
        self.assertEqual(
            self.scaler.reconcile_workspace_release([self.node], force=True),
            ({}, set()),
        )
        api.delete_namespaced_pod.assert_not_called()

    def test_phase_changed_before_delete_is_retained_next_pass(self):
        api = self.scaler._k8s_client.client
        api.list_namespaced_pod.side_effect = [
            SimpleNamespace(items=[self.pod("Running")]),
            SimpleNamespace(items=[self.pod("Failed")]),
        ]
        api.delete_namespaced_pod.side_effect = RuntimeError("409 resource version changed")
        self.assertEqual(
            self.scaler.reconcile_workspace_release([self.node], force=True)[1], set()
        )
        self.assertEqual(
            self.scaler.reconcile_workspace_release([self.node], force=True)[1], {0}
        )
        api.delete_namespaced_pod.assert_called_once()
        precondition = api.delete_namespaced_pod.call_args.kwargs["body"].preconditions
        self.assertEqual(precondition.resource_version, "12")

    def test_inflight_creation_is_not_mistaken_for_completed_deletion(self):
        self.scaler._create_node_futures = [mock.Mock()]
        self.scaler._create_node_futures[0].done.return_value = False
        self.scaler._k8s_client.client.list_namespaced_pod.return_value.items = []
        self.assertEqual(
            self.scaler.reconcile_workspace_release([self.node], force=True),
            ({}, set()),
        )


if __name__ == "__main__":
    unittest.main()
