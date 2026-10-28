"""Regression tests for planned exits and checkpoint-bound replacement."""

import os
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from dlrover.python.common import comm
from dlrover.python.common.constants import (
    ElasticDimension,
    NodeEventType,
    NodeStatus,
    NodeType,
    VerticalElasticPhaseType as Phase,
)
from dlrover.python.common.node import Node, NodeGroupResource, NodeResource
from dlrover.python.master.elastic_training.ucp.task_manager import (
    UcpTaskManager,
    UcpTaskStatus,
)
from dlrover.python.master.node.job_context import get_job_context
from dlrover.python.master.node.job_resource_scaler import (
    AllreduceTrainingResourceScaler,
    _dlrover_context,
)
from dlrover.python.master.node.worker import WorkerManager
from dlrover.python.master.resource.job import JobResource


class ReplacementTest(unittest.TestCase):
    def setUp(self):
        self.context = get_job_context()
        self.context.clear_job_nodes()
        self.addCleanup(self.context.clear_job_nodes)
        resource = JobResource()
        self.target = NodeGroupResource(2, NodeResource(4, 8192, "gpu", 2))
        resource.node_group_resources[NodeType.WORKER] = self.target
        manager = WorkerManager(resource, 3, lambda t, r: f"worker-{r}:3333",
                                lambda t, i: f"worker-{i}")
        self.scaler = AllreduceTrainingResourceScaler(
            resource, MagicMock(), MagicMock(), manager, MagicMock()
        )
        self.tasks = UcpTaskManager()
        self.scaler._ucp_task_manager = self.tasks
        self.scaler.vertical_elastic_phase_manager.create("plan-1")
        self.scaler.update_vertical_elastic_phase(2, Phase.SAVE_CHECKPOINT,
                                                  Phase.REPLACING_WORKERS)

    def task(self, step):
        return self.tasks.report_checkpoint_ready(comm.ReportCheckpointReady(
            namespace="ns", job_id="job", step=step, checkpoint_dir="/checkpoint",
            input_dir=f"/checkpoint/{step}", output_dir=f"/checkpoint/{step}/ucp",
        ))

    def test_replaces_terminal_and_critical_workers_only_from_snapshot(self):
        for i, status in enumerate((NodeStatus.FAILED, NodeStatus.SUCCEEDED,
                                    NodeStatus.RUNNING)):
            node = Node(NodeType.WORKER, i, self.target.node_resource,
                        name=f"worker-{i}", status=status, rank_index=i)
            node.critical = True
            self.context.update_job_node(node)
        self.scaler._old_worker_ids = {0, 1}
        plan = self.scaler.get_worker_plan_with_vertical_elastic(self.target)
        self.assertEqual([n.id for n in plan.remove_nodes], [0, 1])
        self.assertEqual([n.id for n in plan.launch_nodes], [3, 4])
        self.assertEqual([n.rank_index for n in plan.launch_nodes], [0, 1])
        self.assertTrue(all(n.is_released and not n.relaunchable for n in plan.remove_nodes))
        self.assertFalse(self.context.job_node(NodeType.WORKER, 2).is_released)

    def test_master_waits_for_exit_ack_then_bounds_unresponsive_workers(self):
        node = Node(NodeType.WORKER, 0, status=NodeStatus.RUNNING)
        self.context.update_job_node(node)
        self.scaler._old_worker_ids = {0}
        def report_exit(_):
            node.update_reported_status(NodeEventType.VERTICAL_ELASTIC_EXITED)
            self.context.update_job_node(node)
        with patch("dlrover.python.master.node.job_resource_scaler.time.sleep",
                   side_effect=report_exit) as sleep:
            self.scaler._wait_for_replacement_exits()
        sleep.assert_called_once_with(1)
        self.context.update_job_node(Node(NodeType.WORKER, 0, status=NodeStatus.RUNNING))
        with patch.dict(os.environ, {"DLROVER_VERTICAL_DRAIN_TIMEOUT": "0"}), patch(
            "dlrover.python.master.node.job_resource_scaler.time.sleep"
        ) as sleep:
            self.scaler._wait_for_replacement_exits()
        sleep.assert_not_called()

    def test_historical_ucp_success_cannot_approve_new_plan(self):
        old = self.task(10)
        old.status = UcpTaskStatus.SUCCEEDED
        with patch.object(_dlrover_context, "enable_elastic_resource", True), patch(
            "dlrover.python.master.node.job_resource_scaler.time.sleep"
        ), patch("dlrover.python.master.node.job_resource_scaler.time.time",
                 side_effect=[0, 0, 2, 2]):
            self.assertFalse(self.scaler._wait_for_ckpt_ready(1, 0, 100))
        current = self.task(11)
        self.scaler.set_save_ckpt_status_by_vertical_elastic(
            True, plan_id="plan-1", rank=0, step=11, checkpoint_path="/checkpoint"
        )
        self.assertEqual(self.scaler._checkpoint_task_id, current.task_id)
        current.status = UcpTaskStatus.SUCCEEDED
        with patch.object(_dlrover_context, "enable_elastic_resource", True), patch(
            "dlrover.python.master.node.job_resource_scaler.time.sleep"
        ):
            self.assertTrue(self.scaler._wait_for_ckpt_ready(1, 0, 100))
        self.assertEqual(self.tasks.get_resume_checkpoint("ns", "job"), "/checkpoint/11")

    def test_stale_checkpoint_ack_ignored_and_failed_task_stops_wait(self):
        task = self.task(12)
        self.scaler.set_save_ckpt_status_by_vertical_elastic(
            True, plan_id="old-plan", step=12, checkpoint_path="/checkpoint"
        )
        self.assertEqual(self.scaler._checkpoint_task_id, "")
        self.scaler.set_save_ckpt_status_by_vertical_elastic(
            True, plan_id="plan-1", step=12, checkpoint_path="/checkpoint"
        )
        task.status = UcpTaskStatus.FAILED
        with patch.object(_dlrover_context, "enable_elastic_resource", True):
            self.assertFalse(self.scaler._wait_for_ckpt_ready(1, 0, 100))


class AgentExitTest(unittest.TestCase):
    def setUp(self):
        from dlrover.python.elastic_agent.torch.training import ElasticTrainingAgent
        self.agent = object.__new__(ElasticTrainingAgent)
        self.agent._worker_group = SimpleNamespace(
            workers=[SimpleNamespace(global_rank=4), SimpleNamespace(global_rank=5)],
            group_rank=0,
            spec=MagicMock(),
        )
        self.agent._client = MagicMock()
        self.agent._pcontext = MagicMock()
        self.agent._vertical_elastic_exit_reported = False
        self.agent._graceful_exit_workers = MagicMock()
        self.agent._save_ckpt_to_storage = MagicMock()
        self.agent._stop_workers = MagicMock()
        self.agent.set_save_ckpt_status = MagicMock()
        self.agent._can_restore_vertical_elastic_from_ckpt = MagicMock(return_value=True)
        self.agent.ucp = MagicMock(return_value=SimpleNamespace(
            task_id="task-1", step=12, checkpoint_dir="/checkpoint"
        ))
        self.save = SimpleNamespace(plan_id="plan-1", phase=Phase.SAVE_CHECKPOINT)
        self.replace = SimpleNamespace(plan_id="plan-1", phase=Phase.TUNE_PARAM)

    def test_planned_exit_passes_real_torch_run_event_recording(self):
        from torch.distributed.elastic.agent.server.api import SimpleElasticAgent, WorkerState
        self.agent._invoke_run = lambda role: self.agent._vertical_exit_result()
        self.agent._record_metrics = MagicMock()
        self.agent._construct_event = MagicMock()
        self.agent._shutdown = MagicMock()
        self.agent._shutdown_timeout = 0
        with patch("torch.distributed.elastic.agent.server.api.record"):
            result = SimpleElasticAgent.run(self.agent)
        self.assertEqual(result.state, WorkerState.SUCCEEDED)
        self.assertEqual(result.return_values, {4: None, 5: None})
        self.assertEqual(self.agent._construct_event.call_count, 2)

    def test_planned_terminal_states_bypass_training_failure_and_success(self):
        from torch.distributed.elastic.agent.server.api import RunResult, WorkerState
        self.agent._config = SimpleNamespace(numa_affinity=False)
        self.agent.sync_training_ports = MagicMock()
        self.agent._inject_ucp_resume_checkpoint = MagicMock()
        self.agent._initialize_workers = MagicMock()
        self.agent._check_and_process_diagnosis_action = MagicMock()
        self.agent._remaining_failovers = 3
        self.agent._vertical_elastic_exit_reported = True
        self.agent._worker_group.spec.monitor_interval = 0
        self.agent._worker_group.spec.role = "worker"
        for state in (WorkerState.FAILED, WorkerState.UNHEALTHY, WorkerState.SUCCEEDED):
            with self.subTest(state=state), patch(
                "dlrover.python.elastic_agent.torch.training.AsyncCheckpointSaver.start_async_saving_ckpt"
            ):
                self.agent._worker_group.state = WorkerState.HEALTHY
                self.agent._monitor_workers = MagicMock(return_value=RunResult(state=state))
                result = self.agent._invoke_run()
            self.assertEqual(result.return_values, {4: None, 5: None})
            self.assertEqual(result.state, WorkerState.SUCCEEDED)
        self.agent._client.report_succeeded_exited.assert_not_called()
        self.agent._client.report_failed_exited.assert_not_called()

    def test_natural_exit_waits_for_conversion_without_force_stop(self):
        self.agent._client.get_vertical_elastic_phase.side_effect = [
            self.save, self.save, self.replace
        ]
        self.agent._pcontext.wait.return_value = SimpleNamespace()
        with patch.dict(os.environ, {"DLROVER_TRAINING_ELASTIC_MODE": "ucp"}), patch(
            "dlrover.python.elastic_agent.torch.training.time.sleep"
        ):
            self.agent._save_ckpt_by_vertical_elastic()
        self.agent._stop_workers.assert_not_called()
        self.agent.set_save_ckpt_status.assert_called_once_with(
            True, plan_id="plan-1", rank=0, step=12, checkpoint_path="/checkpoint"
        )
        self.agent._client.report_node_event.assert_called_once_with(
            NodeEventType.VERTICAL_ELASTIC_EXITED
        )

    def test_force_stop_only_after_conversion_and_grace_timeout(self):
        self.agent._client.get_vertical_elastic_phase.side_effect = [self.save, self.replace]
        self.agent._pcontext.wait.return_value = None
        with patch.dict(os.environ, {"DLROVER_VERTICAL_EXIT_GRACE_SECONDS": "0"}):
            self.agent._save_ckpt_by_vertical_elastic()
        self.agent._stop_workers.assert_called_once_with(self.agent._worker_group)
        self.assertTrue(self.agent._vertical_elastic_exit_reported)

    def test_missing_checkpoint_and_failed_conversion_are_not_success(self):
        self.agent._client.get_vertical_elastic_phase.return_value = self.save
        self.agent.ucp.return_value = None
        with self.assertRaisesRegex(RuntimeError, "No checkpoint"):
            self.agent._save_ckpt_by_vertical_elastic()
        self.agent._client.report_node_event.assert_not_called()
        self.assertFalse(self.agent._vertical_elastic_exit_reported)
        self.agent.ucp.return_value = SimpleNamespace(
            task_id="task-1", step=12, checkpoint_dir="/checkpoint"
        )
        self.agent._client.get_vertical_elastic_phase.side_effect = [
            self.save, SimpleNamespace(plan_id="plan-1", phase=Phase.FAILED)
        ]
        with self.assertRaisesRegex(RuntimeError, "conversion failed"):
            self.agent._save_ckpt_by_vertical_elastic()
        self.agent._stop_workers.assert_not_called()

    def test_exit_wait_is_bounded(self):
        self.agent._client.get_vertical_elastic_phase.return_value = self.save
        with patch.dict(os.environ, {"DLROVER_VERTICAL_EXIT_TIMEOUT": "0"}):
            with self.assertRaises(TimeoutError):
                self.agent._save_ckpt_by_vertical_elastic()
        self.agent._client.report_node_event.assert_not_called()

    def test_already_correct_process_count_reports_ready(self):
        from dlrover.python.elastic_agent.torch.training import _auto_param_tuning_by_vertical_elastic
        phase = SimpleNamespace(ready=True, phase=Phase.TUNE_PARAM,
                                adjust_gpus=2, plan_id="plan-1")
        self.agent._client.get_vertical_elastic_phase.return_value = phase
        with patch("dlrover.python.elastic_agent.torch.training.MasterClient.singleton_instance",
                   return_value=self.agent._client):
            _auto_param_tuning_by_vertical_elastic(SimpleNamespace(nproc_per_node=2), [])
        self.agent._client.set_param_tunning_ready.assert_called_once()


class MasterExitTest(unittest.TestCase):
    def setUp(self):
        from dlrover.python.master.node.dist_job_manager import DistributedJobManager
        self.manager = object.__new__(DistributedJobManager)
        self.manager._job_args = SimpleNamespace(runtime_elastic_dimension=ElasticDimension.VERTICAL)
        self.manager._job_resource_scaler = SimpleNamespace(_old_worker_ids={0})
        self.old = Node(NodeType.WORKER, 0, name="old", status=NodeStatus.FAILED)
        self.new = Node(NodeType.WORKER, 1, name="new", status=NodeStatus.FAILED)
        self.manager._enable_relaunch_node = True
        self.old.critical = True

    def test_planned_node_is_not_relaunched_before_agent_exit_report(self):
        self.assertFalse(self.manager._should_relaunch(self.old, MagicMock()))
        self.assertFalse(self.old.relaunchable)
        self.assertFalse(self.manager.is_vertical_replacement_node(self.new))

    def test_planned_terminal_events_do_not_finish_job_or_record_failure(self):
        from dlrover.python.master.node.event_callback import AllReduceNodeHandlingCallback
        callback = object.__new__(AllReduceNodeHandlingCallback)
        callback._master = MagicMock()
        callback._master.job_manager = self.manager
        callback._perf_monitor = MagicMock()
        callback._job_context = MagicMock()
        callback._remove_node_from_rdzv = MagicMock()
        for method in (callback.on_node_succeeded, callback.on_node_failed,
                       callback.on_node_deleted):
            method(self.old, MagicMock())
        callback._master.request_stop.assert_not_called()
        callback._job_context.report_failed_node.assert_not_called()
        self.assertEqual(callback._remove_node_from_rdzv.call_count, 3)

    def test_failed_replacement_stops_job_instead_of_silent_cold_restart(self):
        self.manager._job_resource_scaler.get_phase_info = lambda: {"phase": Phase.FAILED}
        should_stop, _, _ = self.manager.should_early_stop()
        self.assertTrue(should_stop)

    def test_successful_ucp_does_not_override_explicit_rendezvous_block(self):
        from dlrover.python.master.elastic_training.rdzv_manager import UcpRdzvManager
        rdzv = UcpRdzvManager()
        rdzv._ucp_task_manager = MagicMock()
        rdzv._ucp_task_manager.latest_task.return_value = SimpleNamespace(
            status=UcpTaskStatus.SUCCEEDED
        )
        rdzv.set_rdzv_blocked(True, "waiting for replacement")
        self.assertTrue(rdzv._pre_rdzv_check_hook()[0])
