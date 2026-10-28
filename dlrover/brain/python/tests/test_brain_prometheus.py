import unittest
from unittest.mock import patch

from dlrover.brain.python.server.prometheus import BrainMetricsServer


def _snapshot():
    return {
        "reports_total": 2,
        "reports_rejected_total": 1,
        "reports_processed_total": 2,
        "processing_errors_total": 0,
        "queue_size": 0,
        "queue_capacity": 100,
        "redis_available": 1,
        "redis_last_check_timestamp": 1,
        "processor_ready": 1,
        "owned_jobs": 1,
        "owner_allocator_instance_registered": 1,
        "owner_allocator_active_instances": 2,
        "owner_allocator_quarantined_instances": 0,
        "redis_checks": {"success": 1},
        "distributed_locks": {"acquired": 1},
        "elastic_decisions": {"triggered": 1},
        "elastic_triggers": {"success": 1},
        "job_dispatches": {"local": 1},
    }


class BrainPrometheusTest(unittest.TestCase):
    def setUp(self):
        self.metrics = BrainMetricsServer()
        self.metrics.set_snapshot_provider(_snapshot)

    def test_collects_runtime_and_vertical_metrics_from_one_registry(self):
        self.metrics.vertical_metrics.save_metrics(
            "job-a",
            "job-id-1",
            "cluster-a",
            "namespace-a",
            "worker-0",
            {
                "gpu_count": 1,
                "max_gpu_utilization": 80,
                "max_gpu_used_memory_mb": 1000,
                "max_gpu_memory_utilization": 50,
            },
        )
        body, ready = self.metrics.collect()

        self.assertEqual(ready, 1)
        self.assertIn("dlrover_brain_runtime_reports_total", body)
        self.assertIn(
            "dlrover_brain_vertical_max_gpu_utilization_percent", body
        )
        self.assertIn("python_gc_objects_collected_total", body)

    def test_low_cardinality_counters_have_zero_baselines(self):
        registry = self.metrics.registry
        self.assertEqual(
            registry.get_sample_value(
                "dlrover_brain_vertical_action_decisions_total",
                {"action": "add_gpus", "emergency": "false"},
            ),
            0,
        )
        self.assertEqual(
            registry.get_sample_value(
                "dlrover_brain_vertical_triggers_total",
                {"action": "add_gpus", "result": "success"},
            ),
            0,
        )

    def test_records_low_cardinality_decisions_and_triggers(self):
        persister = self.metrics.vertical_metrics
        persister.save_decision(
            "job-a",
            "job-id-1",
            "cluster-a",
            "namespace-a",
            "worker-0",
            "add_gpus",
            90,
            40,
            5,
            False,
        )
        persister.save_trigger("ADD_GPUS", "success")

        self.assertEqual(
            self.metrics.registry.get_sample_value(
                "dlrover_brain_vertical_action_decisions_total",
                {"action": "add_gpus", "emergency": "false"},
            ),
            1,
        )
        self.assertEqual(
            self.metrics.registry.get_sample_value(
                "dlrover_brain_vertical_triggers_total",
                {"action": "add_gpus", "result": "success"},
            ),
            1,
        )

    def test_job_cleanup_removes_vertical_series(self):
        persister = self.metrics.vertical_metrics
        persister.save_decision(
            "job-a",
            "job-id-1",
            "cluster-a",
            "namespace-a",
            "worker-0",
            "add_gpus",
            90,
            40,
            5,
            False,
        )
        self.assertIn("job-id-1", self.metrics.collect()[0])

        persister.cleanup_job("job-id-1")

        self.assertNotIn("job-id-1", self.metrics.collect()[0])
        self.assertEqual(
            self.metrics.registry.get_sample_value(
                "dlrover_brain_vertical_action_decisions_total",
                {"action": "add_gpus", "emergency": "false"},
            ),
            1,
        )

    @patch(
        "dlrover.brain.python.server.prometheus.start_http_server",
        side_effect=OSError("address in use"),
    )
    def test_metrics_bind_failure_is_fatal(self, _):
        with self.assertRaises(RuntimeError):
            self.metrics.start(9091)


if __name__ == "__main__":
    unittest.main()
