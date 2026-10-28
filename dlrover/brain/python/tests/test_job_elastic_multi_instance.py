import unittest
from unittest.mock import MagicMock, patch
import os
import queue

import requests

from dlrover.brain.python.common.job import JobMeta
from dlrover.brain.python.common.job_metric import RuntimeInfo
from dlrover.brain.python.jobmanagement.job_elastic_manager import (
    DispatchResult,
    JobElasticManager,
)
from dlrover.brain.python.jobmanagement.job_owner import JobOwner
from dlrover.brain.python.jobmanagement.master_client_pool import MasterClientPool


class TestMasterClientPoolSharedStore(unittest.TestCase):
    def setUp(self):
        MasterClientPool._instance = None
        self.pool = MasterClientPool()

    @patch("dlrover.brain.python.jobmanagement.master_client_pool.build_master_client")
    def test_get_master_client_from_redis(self, mock_builder):
        redis_client = MagicMock()
        redis_client.hget.return_value = "master-svc:50001"
        self.pool._redis = redis_client
        client = MagicMock()
        client._master_addr = "master-svc:50001"
        mock_builder.return_value = client

        result = self.pool.get_master_client("job-a", "job-id-1")
        second_result = self.pool.get_master_client("job-a", "job-id-1")

        self.assertIs(result, client)
        self.assertIs(second_result, client)
        self.assertEqual(redis_client.hget.call_count, 2)
        mock_builder.assert_called_once()

    def test_register_writes_only_to_redis(self):
        redis_client = MagicMock()
        self.pool._redis = redis_client

        success = self.pool.register(
            "job-a", "job-id-1", "master-svc:50001"
        )

        self.assertTrue(success)
        mapping = redis_client.hset.call_args.kwargs["mapping"]
        self.assertEqual(mapping["master_addr"], "master-svc:50001")
        self.assertFalse(hasattr(self.pool, "master_addrs"))
        self.assertFalse(hasattr(self.pool, "last_heartbeat_time"))
        self.assertFalse(hasattr(self.pool, "job_mapping"))

    def test_register_fails_without_redis_even_in_single_instance(self):
        with patch.dict(os.environ, {"DLROVER_BRAIN_MULTI_INSTANCE": "true"}):
            MasterClientPool._instance = None
            pool = MasterClientPool()
            pool._redis = None

            success = pool.register("job-a", "job-id-1", "master-svc:50001")

            self.assertFalse(success)
            self.assertEqual(pool.master_clients, {})

    @patch(
        "dlrover.brain.python.jobmanagement.master_client_pool._redis_client"
    )
    def test_register_reconnects_after_redis_recovers(self, mock_redis_client):
        redis_client = MagicMock()
        mock_redis_client.return_value = redis_client
        self.pool._redis = None

        success = self.pool.register(
            "job-a", "job-id-1", "master-svc:50001"
        )

        self.assertTrue(success)
        self.assertIs(self.pool._redis, redis_client)
        redis_client.hset.assert_called_once()

    def test_register_fail_closed_when_shared_store_write_fails(self):
        redis_client = MagicMock()
        redis_client.hset.side_effect = RuntimeError("redis unavailable")
        self.pool._redis = redis_client

        success = self.pool.register("job-a", "job-id-1", "master-svc:50001")

        self.assertFalse(success)
        self.assertEqual(self.pool.master_clients, {})

    def test_heartbeat_fail_closed_when_shared_store_write_fails(self):
        cached_client = MagicMock()
        cached_client._master_addr = "master-svc:50001"
        self.pool.master_clients["job-id-1"] = cached_client
        redis_client = MagicMock()
        redis_client.hset.side_effect = RuntimeError("redis unavailable")
        self.pool._redis = redis_client

        success = self.pool.heartbeat(
            "job-a", "job-id-1", "master-svc:50002"
        )

        self.assertFalse(success)
        self.assertIs(self.pool.master_clients["job-id-1"], cached_client)
        cached_client._close_grpc_channel.assert_not_called()

    @patch("dlrover.brain.python.jobmanagement.master_client_pool.build_master_client")
    def test_get_master_client_fail_closed_when_shared_store_read_fails(self, mock_builder):
        cached_client = MagicMock()
        cached_client._master_addr = "master-svc:50001"
        self.pool.master_clients["job-id-1"] = cached_client
        redis_client = MagicMock()
        redis_client.hget.side_effect = RuntimeError("redis unavailable")
        self.pool._redis = redis_client

        client = self.pool.get_master_client(
            "job-a", "job-id-1", refresh_from_shared_store=True
        )

        self.assertIsNone(client)
        mock_builder.assert_not_called()

    def test_clean_timeout_jobs_from_shared_store(self):
        redis_client = MagicMock()
        redis_client.eval.return_value = 1
        redis_client.zrangebyscore.return_value = ["job-id-1"]
        redis_client.hgetall.return_value = {
            "job_name": "job-a",
            "job_id": "job-id-1",
            "master_addr": "master-svc:50001",
            "heartbeat_ts": "0",
        }
        self.pool._redis = redis_client
        cached_client = MagicMock()
        self.pool.master_clients["job-id-1"] = cached_client

        expired = self.pool.clean_timeout_jobs(timeout=1)

        self.assertEqual(expired, ["job-a/job-id-1"])
        self.assertNotIn("job-id-1", self.pool.master_clients)
        cached_client._close_grpc_channel.assert_called_once()

    def test_cleanup_does_not_delete_reactivated_master(self):
        redis_client = MagicMock()
        redis_client.eval.return_value = 0
        self.pool._redis = redis_client
        cached_client = MagicMock()
        self.pool.master_clients["job-id-1"] = cached_client

        cleaned = self.pool.cleanup_job_state(
            "job-id-1", "100", expire_before=200
        )

        self.assertFalse(cleaned)
        self.assertIs(self.pool.master_clients["job-id-1"], cached_client)
        cached_client._close_grpc_channel.assert_not_called()

    def test_cleanup_cycle_reconciles_other_instance_connection_cache(self):
        redis_client = MagicMock()
        redis_client.zrangebyscore.return_value = []
        redis_client.hget.return_value = None
        self.pool._redis = redis_client
        cached_client = MagicMock()
        cached_client._master_addr = "master-svc:50001"
        self.pool.master_clients["job-id-1"] = cached_client

        self.pool.clean_timeout_jobs(timeout=60)

        self.assertNotIn("job-id-1", self.pool.master_clients)
        cached_client._close_grpc_channel.assert_called_once()

    def test_heartbeat_updates_master_addr_into_shared_store(self):
        redis_client = MagicMock()
        self.pool._redis = redis_client

        self.pool.heartbeat("job-a", "job-id-1", "master-svc:50003")

        self.assertTrue(redis_client.hset.called)
        mapping = redis_client.hset.call_args.kwargs["mapping"]
        self.assertEqual(mapping["master_addr"], "master-svc:50003")

    def test_heartbeat_without_address_does_not_use_local_state(self):
        redis_client = MagicMock()
        self.pool._redis = redis_client

        self.assertTrue(self.pool.heartbeat("job-a", "job-id-1"))

        mapping = redis_client.hset.call_args.kwargs["mapping"]
        self.assertNotIn("master_addr", mapping)

    def test_stop_cleanup_thread_and_close_cached_connections(self):
        redis_client = MagicMock()
        redis_client.zrangebyscore.return_value = []
        self.pool._redis = redis_client
        cached_client = MagicMock()
        self.pool.master_clients["job-id-1"] = cached_client

        self.pool.start_auto_clean_thread(timeout=60)
        self.pool.stop()

        self.assertFalse(self.pool._cleanup_thread.is_alive())
        self.assertEqual(self.pool.master_clients, {})
        cached_client._close_grpc_channel.assert_called_once()

    @patch("dlrover.brain.python.jobmanagement.master_client_pool.build_master_client")
    def test_refresh_master_client_when_shared_addr_changes(self, mock_builder):
        old_client = MagicMock()
        old_client._master_addr = "master-svc:50001"
        new_client = MagicMock()
        new_client._master_addr = "master-svc:50002"
        mock_builder.side_effect = [old_client, new_client]

        redis_client = MagicMock()
        redis_client.hget.side_effect = ["master-svc:50001", "master-svc:50002"]
        self.pool._redis = redis_client

        self.pool.get_master_client("job-a", "job-id-1", refresh_from_shared_store=True)
        self.pool.get_master_client("job-a", "job-id-1", refresh_from_shared_store=True)

        self.assertEqual(mock_builder.call_count, 2)
        old_client._close_grpc_channel.assert_called_once()


class TestJobElasticManagerDistributedLock(unittest.TestCase):
    def test_injected_dependencies_are_shared_without_control_plane_methods(self):
        pool = MagicMock()
        owner_manager = MagicMock()
        vertical_service = MagicMock()

        manager = JobElasticManager(
            master_client_pool=pool,
            job_owner_manager=owner_manager,
            vertical_elastic_service=vertical_service,
        )

        self.assertIs(manager.master_client_pool, pool)
        self.assertIs(manager._job_owner_manager, owner_manager)
        self.assertIs(manager._vertical_elastic_service, vertical_service)
        self.assertFalse(manager._manages_job_owner_lifecycle)
        self.assertFalse(hasattr(manager, "register_master"))
        self.assertFalse(hasattr(manager, "heartbeat_master"))
        self.assertFalse(hasattr(manager, "cleanup_job"))

    def test_remote_owner_is_forwarded_without_local_enqueue(self):
        with patch.dict(os.environ, {"DLROVER_BRAIN_MULTI_INSTANCE": "true"}):
            manager = JobElasticManager()
        manager._job_owner_manager = MagicMock(
            enabled=True, instance_id="brain-0"
        )
        remote_owner = JobOwner(
            "brain-1", "http://10.0.0.11:50002", "token-1"
        )
        manager._job_owner_manager.get_owner.return_value = remote_owner
        manager._forward_to_owner = MagicMock(
            return_value=DispatchResult(True, "forwarded")
        )
        metrics = MagicMock()
        metrics.job_meta.name = "job-a"
        metrics.job_meta.uuid = "job-id-1"

        result = manager.dispatch_job_runtime_info(metrics)

        self.assertTrue(result.success)
        manager._forward_to_owner.assert_called_once_with(metrics, remote_owner)
        self.assertTrue(manager.report_queue.empty())

    def test_runtime_forward_connection_failure_updates_circuit(self):
        owner_manager = MagicMock(enabled=True, instance_id="brain-0")
        manager = JobElasticManager(job_owner_manager=owner_manager)
        remote_owner = JobOwner(
            "brain-1", "http://10.0.0.11:50002", "token-1"
        )
        manager._forward_session.post = MagicMock(
            side_effect=requests.ConnectionError("connection refused")
        )
        metrics = MagicMock()
        metrics.job_meta.name = "job-a"
        metrics.job_meta.uuid = "job-id-1"

        result = manager._forward_to_owner(metrics, remote_owner)

        self.assertFalse(result.success)
        owner_manager.record_forward_failure.assert_called_once_with(
            remote_owner
        )
        owner_manager.record_forward_success.assert_not_called()

    def test_runtime_forward_business_rejection_clears_circuit(self):
        owner_manager = MagicMock(enabled=True, instance_id="brain-0")
        manager = JobElasticManager(job_owner_manager=owner_manager)
        remote_owner = JobOwner(
            "brain-1", "http://10.0.0.11:50002", "token-1"
        )
        response = MagicMock(status_code=200)
        response.json.return_value = {
            "response": {"success": False, "reason": "owner changed"}
        }
        manager._forward_session.post = MagicMock(return_value=response)
        metrics = MagicMock()
        metrics.job_meta.name = "job-a"
        metrics.job_meta.uuid = "job-id-1"

        result = manager._forward_to_owner(metrics, remote_owner)

        self.assertFalse(result.success)
        owner_manager.record_forward_success.assert_called_once_with(
            remote_owner
        )
        owner_manager.record_forward_failure.assert_not_called()

    def test_local_owner_enqueues_with_fencing_token(self):
        with patch.dict(os.environ, {"DLROVER_BRAIN_MULTI_INSTANCE": "true"}):
            manager = JobElasticManager()
        manager._job_owner_manager = MagicMock(
            enabled=True, instance_id="brain-0"
        )
        local_owner = JobOwner(
            "brain-0", "http://10.0.0.10:50002", "token-0"
        )
        manager._job_owner_manager.get_owner.return_value = local_owner
        manager._job_owner_manager.accept_forwarded.return_value = True
        manager._job_owner_manager.mark_report_once.return_value = True
        metrics = MagicMock()
        metrics.time_stamp = 1000
        metrics.job_meta.name = "job-a"
        metrics.job_meta.uuid = "job-id-1"

        result = manager.dispatch_job_runtime_info(metrics)
        queued = manager.report_queue.get_nowait()

        self.assertTrue(result.success)
        self.assertIs(queued.metrics, metrics)
        self.assertEqual(queued.owner_token, "token-0")

    def test_runtime_report_does_not_claim_missing_owner(self):
        manager = JobElasticManager()
        manager._job_owner_manager = MagicMock(
            enabled=True, instance_id="brain-0"
        )
        manager._job_owner_manager.get_owner.return_value = None
        metrics = MagicMock()
        metrics.job_meta.name = "job-a"
        metrics.job_meta.uuid = "job-id-1"

        result = manager.dispatch_job_runtime_info(metrics)

        self.assertFalse(result.success)
        self.assertTrue(result.retryable)
        self.assertEqual(result.reason, "job owner is not registered")
        manager._job_owner_manager.claim_for_registration.assert_not_called()
        self.assertTrue(manager.report_queue.empty())

    @patch(
        "dlrover.brain.python.jobmanagement.job_elastic_manager.time.sleep"
    )
    def test_queue_rejection_rolls_back_report_dedup(self, _mock_sleep):
        with patch.dict(os.environ, {"DLROVER_BRAIN_MULTI_INSTANCE": "true"}):
            manager = JobElasticManager()
        manager.report_queue = queue.Queue(maxsize=1)
        manager.report_queue.put_nowait(object())
        manager._job_owner_manager = MagicMock(
            enabled=True, instance_id="brain-0"
        )
        local_owner = JobOwner(
            "brain-0", "http://10.0.0.10:50002", "token-0"
        )
        manager._job_owner_manager.get_owner.return_value = local_owner
        manager._job_owner_manager.accept_forwarded.return_value = True
        manager._job_owner_manager.mark_report_once.return_value = True
        metrics = MagicMock()
        metrics.time_stamp = 1000
        metrics.job_meta.name = "job-a"
        metrics.job_meta.uuid = "job-id-1"

        result = manager.dispatch_job_runtime_info(metrics)

        self.assertFalse(result.success)
        self.assertTrue(result.retryable)
        manager._job_owner_manager.forget_report.assert_called_once_with(
            "job-id-1", "1000"
        )

    @patch("dlrover.brain.python.jobmanagement.job_elastic_manager.RedisDistributedLock")
    def test_fail_closed_when_required_lock_unavailable(self, mock_lock_cls):
        with patch.dict(os.environ, {"DLROVER_BRAIN_MULTI_INSTANCE": "true"}):
            manager = JobElasticManager()
        manager._redis_available_now = MagicMock(return_value=True)
        lock = MagicMock()
        lock.available = False
        mock_lock_cls.return_value = lock

        job_meta = JobMeta(
            uuid="job-id-1",
            name="job-a",
            namespace="ns",
            cluster="c1",
        )
        runtime = RuntimeInfo(running_pods=[])
        success, reason = manager._make_elastic_decision(runtime, job_meta)

        self.assertFalse(success)
        self.assertIn("redis distributed lock unavailable", reason)

    @patch("dlrover.brain.python.jobmanagement.job_elastic_manager.RedisDistributedLock")
    def test_skip_when_lock_not_acquired(self, mock_lock_cls):
        manager = JobElasticManager()
        manager._redis_available_now = MagicMock(return_value=True)
        lock = MagicMock()
        lock.available = True
        lock.acquire.return_value = False
        lock.last_error = None
        mock_lock_cls.return_value = lock

        job_meta = JobMeta(
            uuid="job-id-1",
            name="job-a",
            namespace="ns",
            cluster="c1",
        )
        runtime = RuntimeInfo(running_pods=[])
        success, reason = manager._make_elastic_decision(runtime, job_meta)

        self.assertFalse(success)
        self.assertIn("distributed lock", reason)

    @patch("dlrover.brain.python.jobmanagement.job_elastic_manager.RedisDistributedLock")
    def test_fail_closed_when_lock_acquire_has_redis_error(self, mock_lock_cls):
        with patch.dict(os.environ, {"DLROVER_BRAIN_MULTI_INSTANCE": "true"}):
            manager = JobElasticManager()
        manager._redis_available_now = MagicMock(return_value=True)
        lock = MagicMock()
        lock.available = True
        lock.acquire.return_value = False
        lock.last_error = RuntimeError("redis unavailable")
        mock_lock_cls.return_value = lock

        job_meta = JobMeta(
            uuid="job-id-1",
            name="job-a",
            namespace="ns",
            cluster="c1",
        )
        runtime = RuntimeInfo(running_pods=[])
        success, reason = manager._make_elastic_decision(runtime, job_meta)

        self.assertFalse(success)
        self.assertIn("redis distributed lock unavailable", reason)

    @patch("dlrover.brain.python.jobmanagement.job_elastic_manager.RedisDistributedLock")
    def test_skip_decision_when_redis_unavailable(self, mock_lock_cls):
        manager = JobElasticManager()
        manager._redis_available_now = MagicMock(return_value=False)

        job_meta = JobMeta(
            uuid="job-id-1",
            name="job-a",
            namespace="ns",
            cluster="c1",
        )
        runtime = RuntimeInfo(running_pods=[])
        success, reason = manager._make_elastic_decision(runtime, job_meta)

        self.assertFalse(success)
        self.assertIn("redis unavailable", reason)
        mock_lock_cls.assert_not_called()

    def test_accept_runtime_info_when_redis_unavailable(self):
        manager = JobElasticManager()
        manager._redis_available_now = MagicMock(return_value=False)
        job_metrics = MagicMock()
        job_metrics.job_meta.name = "job-a"

        success = manager.receive_job_runtime_info(job_metrics)
        self.assertTrue(success)
        self.assertIs(manager.report_queue.get_nowait(), job_metrics)

    def test_reject_runtime_info_without_job_meta(self):
        manager = JobElasticManager()
        job_metrics = MagicMock()
        job_metrics.job_meta = None

        result = manager.dispatch_job_runtime_info(job_metrics)

        self.assertFalse(result.success)
        self.assertEqual(result.reason, "job meta is empty")
        self.assertTrue(manager.report_queue.empty())

    def test_process_loop_discards_none_and_balances_queue_tasks(self):
        manager = JobElasticManager()
        valid_metrics = MagicMock()
        valid_metrics.job_meta.uuid = "job-id-1"
        valid_metrics.job_meta.name = "job-a"
        manager.report_queue.put_nowait(None)
        manager.report_queue.put_nowait(valid_metrics)
        manager._stop_event = MagicMock()
        manager._stop_event.is_set.side_effect = [False, False, True]
        job_meta = MagicMock(name="job_meta")
        job_meta.name = "job-a"
        job_meta.uuid = "job-id-1"
        runtime = MagicMock(name="runtime")
        manager._init_job_meta = MagicMock(return_value=job_meta)
        manager._init_job_runtime_info = MagicMock(return_value=runtime)
        manager._pre_check = MagicMock(return_value=True)
        manager._async_save_to_db = MagicMock()
        manager._redis_available_now = MagicMock(return_value=False)

        manager.start_process_loop()

        manager._init_job_meta.assert_called_once_with(
            job_metrics=valid_metrics
        )
        manager._async_save_to_db.assert_called_once_with(valid_metrics)
        self.assertEqual(manager.report_queue.unfinished_tasks, 0)
        self.assertEqual(manager._reports_rejected_total, 1)
        self.assertEqual(manager._processing_errors_total, 1)

    def test_persist_metrics_before_skipping_decision_without_redis(self):
        manager = JobElasticManager()
        job_metrics = MagicMock()
        manager.report_queue.put_nowait(job_metrics)
        manager._stop_event = MagicMock()
        manager._stop_event.is_set.side_effect = [False, True]
        job_meta = MagicMock()
        job_meta.name = "job-a"
        job_meta.uuid = "job-id-1"
        runtime = MagicMock()
        manager._init_job_meta = MagicMock(return_value=job_meta)
        manager._init_job_runtime_info = MagicMock(return_value=runtime)
        manager._pre_check = MagicMock(return_value=True)
        manager._async_save_to_db = MagicMock()
        manager._redis_available_now = MagicMock(return_value=False)
        manager._make_elastic_decision = MagicMock()

        manager.start_process_loop()

        manager._async_save_to_db.assert_called_once_with(job_metrics)
        manager._make_elastic_decision.assert_not_called()

    def test_trigger_elastic_retry_after_failure(self):
        manager = JobElasticManager()
        manager._vertical_elastic_service.record_trigger_result = MagicMock()
        manager._redis_available_now = MagicMock(return_value=True)
        manager.master_client_pool = MagicMock()
        failed_client = MagicMock()
        failed_client.trigger_elastic.side_effect = RuntimeError("stale client")
        ok_client = MagicMock()
        manager.master_client_pool.get_master_client.side_effect = [
            failed_client,
            ok_client,
        ]

        job_meta = JobMeta(
            uuid="job-id-1",
            name="job-a",
            namespace="ns",
            cluster="c1",
        )
        success = manager.trigger_elastic("vertical", "ADD_GPUS", job_meta)
        self.assertTrue(success)
        manager.master_client_pool.invalidate_local_client.assert_called_once_with(
            "job-id-1"
        )
        manager._vertical_elastic_service.record_trigger_result.assert_called_once_with(
            "ADD_GPUS", "retry_success"
        )
        self.assertEqual(manager._elastic_triggers["retry_success"], 1)

    def test_skip_trigger_when_redis_unavailable(self):
        manager = JobElasticManager()
        manager._vertical_elastic_service.record_trigger_result = MagicMock()
        manager._redis_available_now = MagicMock(return_value=False)
        manager.master_client_pool = MagicMock()
        job_meta = JobMeta(
            uuid="job-id-1",
            name="job-a",
            namespace="ns",
            cluster="c1",
        )

        success = manager.trigger_elastic("vertical", "ADD_GPUS", job_meta)

        self.assertFalse(success)
        manager.master_client_pool.get_master_client.assert_not_called()
        manager._vertical_elastic_service.record_trigger_result.assert_called_once_with(
            "ADD_GPUS", "redis_unavailable"
        )
        self.assertEqual(manager._elastic_triggers["redis_unavailable"], 1)

    def test_metrics_snapshot_has_fixed_multi_instance_results(self):
        manager = JobElasticManager()
        manager._increment_metric(manager._distributed_locks, "contended")
        manager._increment_metric(manager._elastic_decisions, "lock_contended")

        snapshot = manager.metrics_snapshot()

        self.assertEqual(snapshot["distributed_locks"]["contended"], 1)
        self.assertEqual(snapshot["elastic_decisions"]["lock_contended"], 1)
        self.assertEqual(snapshot["processor_ready"], 0)
        self.assertIn("owner_allocator_instance_registered", snapshot)
        self.assertIn("owner_allocator_active_instances", snapshot)
        self.assertIn("owner_allocator_quarantined_instances", snapshot)

    def test_not_ready_before_owner_allocator_membership_registration(self):
        owner_manager = MagicMock(enabled=True)
        owner_manager.is_instance_registered.return_value = False
        manager = JobElasticManager(job_owner_manager=owner_manager)
        manager._process_thread = MagicMock()
        manager._process_thread.is_alive.return_value = True
        manager._redis_available_now = MagicMock(return_value=True)

        self.assertFalse(manager.is_ready())
        manager._redis_available_now.assert_not_called()

    @patch("dlrover.brain.python.jobmanagement.job_elastic_manager._redis_client")
    def test_redis_health_check_updates_metrics_without_runtime_reports(
        self, mock_redis_client
    ):
        client = MagicMock()
        mock_redis_client.return_value = client
        manager = JobElasticManager()

        self.assertTrue(manager._redis_available_now())
        snapshot = manager.metrics_snapshot()

        self.assertEqual(snapshot["redis_available"], 1)
        self.assertEqual(snapshot["redis_checks"]["success"], 1)
        self.assertGreater(snapshot["redis_last_check_timestamp"], 0)


if __name__ == "__main__":
    unittest.main()
