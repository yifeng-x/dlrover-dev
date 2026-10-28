# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from dlrover.python.common.constants import NodeStatus
from dlrover.python.common.metric.prometheus import (
    MetricSample,
    MetricsServer,
    render_prometheus,
)
from dlrover.python.master.monitor.perf_monitor import PerfMonitor
from dlrover.python.master.monitor.prometheus import MasterMetricsCollector
from dlrover.python.master.node.job_context import get_job_context


class PrometheusMetricsTest(unittest.TestCase):
    def setUp(self):
        self.context = get_job_context()
        self.context._job_nodes = {}

    def test_render_escapes_labels_and_declares_metric_once(self):
        output = render_prometheus(
            [
                MetricSample("dlrover_test", 1, {"label": 'a"b'}),
                MetricSample("dlrover_test", 2),
            ]
        )
        self.assertEqual(output.count("# TYPE dlrover_test gauge"), 1)
        self.assertIn('label="a\\"b"', output)

    def test_metrics_server_uses_explicit_registry_and_runtime_metrics(self):
        server = MetricsServer(
            lambda: [MetricSample("dlrover_test", 1)]
        )

        output, ready = server.collect()

        self.assertEqual(ready, 1)
        self.assertIn("dlrover_test 1.0", output)
        self.assertIn("process_start_time_seconds", output)
        self.assertIn("python_info", output)

    def test_runtime_metrics_can_be_disabled_for_agents(self):
        server = MetricsServer(
            lambda: [MetricSample("dlrover_agent_test", 1)],
            include_runtime_metrics=False,
        )

        output, ready = server.collect()

        self.assertEqual(ready, 1)
        self.assertIn("dlrover_agent_test 1.0", output)
        self.assertNotIn("process_start_time_seconds", output)
        self.assertNotIn("python_info", output)

    def test_collection_failure_is_isolated_and_observable(self):
        server = MetricsServer(
            lambda: [
                MetricSample("dlrover_conflict", 1),
                MetricSample(
                    "dlrover_conflict", 2, metric_type="counter"
                ),
            ]
        )

        output, ready = server.collect()

        self.assertEqual(ready, 0)
        self.assertIn("dlrover_metrics_collection_errors_total 1.0", output)
        self.assertIn("dlrover_metrics_collector_ready 0.0", output)

    @patch(
        "dlrover.python.common.metric.prometheus.start_http_server",
        side_effect=OSError("address in use"),
    )
    def test_metrics_bind_failure_is_fatal(self, _):
        server = MetricsServer(
            lambda: [MetricSample("dlrover_test", 1)]
        )

        with self.assertRaises(RuntimeError):
            server.start(9090)

    @patch("dlrover.python.common.metric.prometheus.start_http_server")
    def test_metrics_server_stops_official_http_server(self, mock_start):
        httpd = MagicMock()
        thread = MagicMock()
        mock_start.return_value = (httpd, thread)
        server = MetricsServer(
            lambda: [MetricSample("dlrover_test", 1)]
        )

        server.start(19090)
        server.stop()

        httpd.shutdown.assert_called_once()
        httpd.server_close.assert_called_once()
        thread.join.assert_called_once_with(timeout=5)

    def test_brain_metrics_read_snapshot_without_requests(self):
        args = SimpleNamespace(
            cluster="c", namespace="ns", job_uuid="job",
            distribution_strategy="allreduce",
        )
        for state, failures, last_success in [
            ("disabled", 0, 0), ("backoff", 3, 0),
            ("healthy", 0, 100), ("failed", 1, 100), ("stopped", 0, 100),
        ]:
            with self.subTest(state=state):
                heartbeat = MagicMock()
                heartbeat.snapshot = (state, failures, last_success)
                collector = MasterMetricsCollector(
                    args, self.context, PerfMonitor(), None, heartbeat,
                )
                samples = list(collector.collect())
                states = {
                    sample.labels["state"]: sample.value for sample in samples
                    if sample.name == "dlrover_master_brain_connection_state"
                }
                self.assertEqual(sum(states.values()), 1)
                self.assertEqual(states[state], 1)
                values = {sample.name: sample.value for sample in samples}
                self.assertEqual(
                    values["dlrover_master_brain_consecutive_failures"], failures
                )
                self.assertEqual(
                    values["dlrover_master_brain_last_success_timestamp_seconds"],
                    last_success,
                )
                self.assertEqual(heartbeat.mock_calls, [])

    def test_master_collector_exports_training_and_worker_snapshot(self):
        args = SimpleNamespace(
            cluster="cluster-a",
            namespace="default",
            job_uuid="job-1",
            distribution_strategy="allreduce",
        )
        clock = MagicMock(side_effect=[900, 1000, 1010])
        perf_monitor = PerfMonitor(clock=clock)
        perf_monitor.add_running_worker("worker", 0)
        perf_monitor.set_target_worker_num(2)
        perf_monitor.collect_global_step(10, 100)
        perf_monitor.collect_global_step(20, 110)
        self.context._job_nodes = {
            "worker": {
                0: SimpleNamespace(status=NodeStatus.RUNNING, heartbeat_time=100),
                1: SimpleNamespace(status=NodeStatus.PENDING, heartbeat_time=0),
            }
        }
        self.context.update_job_stage("running")

        samples = list(
            MasterMetricsCollector(args, self.context, perf_monitor, None).collect()
        )
        values = {
            (sample.name, tuple(sorted(sample.labels.items()))): sample.value
            for sample in samples
        }
        labels = tuple(
            sorted(
                {
                    "cluster": "cluster-a",
                    "namespace": "default",
                    "job_uid": "job-1",
                    "strategy": "allreduce",
                }.items()
            )
        )
        self.assertEqual(values[("dlrover_training_global_step", labels)], 20)
        self.assertEqual(
            values[("dlrover_job_last_progress_timestamp_seconds", labels)],
            1010,
        )
        self.assertEqual(
            values[("dlrover_training_speed_steps_per_second", labels)], 1
        )
        self.assertEqual(values[("dlrover_training_running_workers", labels)], 1)
        self.assertEqual(values[("dlrover_training_target_workers", labels)], 2)
        self.assertEqual(values[("dlrover_master_pending_nodes", labels)], 1)
        self.assertEqual(
            values[
                (
                    "dlrover_master_workers",
                    tuple(
                        sorted(
                            {
                                **dict(labels),
                                "role": "worker",
                                "status": NodeStatus.RUNNING,
                            }.items()
                        )
                    ),
                )
            ],
            1,
        )


if __name__ == "__main__":
    unittest.main()
