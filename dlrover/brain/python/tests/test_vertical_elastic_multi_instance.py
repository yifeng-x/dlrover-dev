import json
import unittest
from unittest.mock import MagicMock, patch

from dlrover.brain.python.jobmanagement.elastic.vertical_elastic import (
    REDIS_GPU_TIME_WINDOW_KEY_PREFIX,
    VerticalElasticService,
)
from dlrover.python.common.comm import GPUStats
from dlrover.python.common.constants import WorkerResourceScaleAction


class TestVerticalElasticMultiInstance(unittest.TestCase):
    def _build_service(self):
        service = VerticalElasticService()
        service.optimize_add_gpus_utilization_threshold = 60
        service.optimize_reduce_gpus_utilization_threshold = 10
        service.optimize_gpu_emergency_high_utilization_threshold = 95
        service.trigger_resource_elastic_time_window = 60
        service.min_samples_in_window = 2
        service.max_buffer_size = 100
        return service

    @patch("dlrover.brain.python.jobmanagement.elastic.vertical_elastic.time.time")
    def test_use_shared_store_history_for_multi_instance(self, mock_time):
        service = self._build_service()
        mock_time.return_value = 2.0  # 2000ms

        redis_client = MagicMock()
        redis_client.get.return_value = json.dumps(
            [
                {
                    "timestamp": 1000,
                    "max_gpu_utilization": 50.0,
                    "gpu_count": 1,
                    "raw_utils": [50.0],
                }
            ]
        )
        service._redis = redis_client

        action = service.verify_trigger_gpu_resource_elastic(
            job_name="job-a",
            job_id="job-id-1",
            namespace="ns",
            cluster="c1",
            gpu_stats=[GPUStats(gpu_utilization=90.0)],
            pod_name="worker-0",
        )

        self.assertEqual(action, WorkerResourceScaleAction.ADD_GPUS)
        redis_client.set.assert_called_once()
        key = redis_client.set.call_args.args[0]
        payload = redis_client.set.call_args.args[1]
        self.assertEqual(
            key,
            f"{REDIS_GPU_TIME_WINDOW_KEY_PREFIX}:job-id-1:worker-0",
        )
        self.assertEqual(len(json.loads(payload)), 2)

    def test_skip_local_store_when_shared_store_unavailable(self):
        service = self._build_service()

        redis_client = MagicMock()
        redis_client.get.side_effect = RuntimeError("redis unavailable")
        service._redis = redis_client

        history = service._load_history_data("job-id-1", "worker-0")
        self.assertIsNone(history)

    def test_skip_decision_when_redis_unavailable(self):
        service = self._build_service()
        redis_client = MagicMock()
        redis_client.ping.side_effect = RuntimeError("redis unavailable")
        service._redis = redis_client

        with patch(
            "dlrover.brain.python.jobmanagement.elastic.vertical_elastic._redis_client",
            return_value=None,
        ):
            action = service.verify_trigger_gpu_resource_elastic(
                job_name="job-a",
                job_id="job-id-1",
                namespace="ns",
                cluster="c1",
                gpu_stats=[GPUStats(gpu_utilization=99.0)],
                pod_name="worker-0",
            )

        self.assertEqual(action, WorkerResourceScaleAction.NOT_ADJUST)
        redis_client.get.assert_not_called()

    def test_cleanup_redis_state(self):
        service = self._build_service()
        redis_client = MagicMock()
        service._redis = redis_client

        service.cleanup_state("job-id-1", "worker-0")

        redis_client.delete.assert_called_once_with(
            f"{REDIS_GPU_TIME_WINDOW_KEY_PREFIX}:job-id-1:worker-0"
        )

    def test_cleanup_all_job_redis_state(self):
        service = self._build_service()
        redis_client = MagicMock()
        redis_client.scan_iter.return_value = iter(
            [
                f"{REDIS_GPU_TIME_WINDOW_KEY_PREFIX}:job-id-1:worker-0",
                f"{REDIS_GPU_TIME_WINDOW_KEY_PREFIX}:job-id-1:worker-1",
            ]
        )
        service._redis = redis_client

        self.assertTrue(service.cleanup_job_state("job-id-1"))

        redis_client.scan_iter.assert_called_once_with(
            match=f"{REDIS_GPU_TIME_WINDOW_KEY_PREFIX}:job-id-1:*",
            count=100,
        )
        self.assertEqual(redis_client.delete.call_count, 2)
        redis_client.delete.assert_any_call(
            f"{REDIS_GPU_TIME_WINDOW_KEY_PREFIX}:job-id-1:worker-0"
        )
        redis_client.delete.assert_any_call(
            f"{REDIS_GPU_TIME_WINDOW_KEY_PREFIX}:job-id-1:worker-1"
        )

    def test_gpu_aggregation_includes_memory_metrics(self):
        service = self._build_service()

        record = service._aggregate_current_gpu_stats(
            [
                GPUStats(
                    gpu_utilization=75,
                    used_memory_mb=200,
                    total_memory_mb=1000,
                ),
                GPUStats(
                    gpu_utilization=80,
                    used_memory_mb=600,
                    total_memory_mb=1000,
                ),
            ],
            1000,
            "worker-0",
        )

        self.assertEqual(record["max_gpu_utilization"], 80)
        self.assertEqual(record["max_gpu_used_memory_mb"], 600)
        self.assertEqual(record["max_gpu_memory_utilization"], 60)


if __name__ == "__main__":
    unittest.main()
