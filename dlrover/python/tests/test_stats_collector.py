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

import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

from dlrover.python.common.constants import NodeType
from dlrover.python.common.node import Node, NodeResource
from dlrover.python.master.monitor.perf_monitor import PerfMonitor
from dlrover.python.master.stats.job_collector import JobMetricCollector
from dlrover.python.master.stats.reporter import JobMeta, LocalStatsReporter
from dlrover.python.master.stats.training_metrics import RuntimeMetric


class LocalStatsCollectorTest(unittest.TestCase):
    def test_report_resource_usage(self):
        job_meta = JobMeta("1111")
        reporter = LocalStatsReporter.singleton_instance(job_meta)
        reporter._runtime_stats = []
        ps = Node(
            NodeType.PS,
            0,
            config_resource=NodeResource(4, 4096),
            name="ps-0",
        )
        worker = Node(
            NodeType.WORKER, 0, config_resource=NodeResource(6, 4096)
        )
        worker.used_resource = NodeResource(1, 2048)
        ps.used_resource = NodeResource(1.0, 2048)
        for i in range(3):
            nodes = [ps]
            for i in range(3):
                nodes.append(worker)
            step = i * 100 + 1
            ts = i * 1000 + 1
            m = RuntimeMetric(nodes, global_step=step, speed=12, timestamp=ts)
            reporter.report_runtime_stats(m)
        self.assertEqual(len(reporter._runtime_stats), 3)

        for i in range(10):
            nodes = [ps]
            for i in range(4):
                worker.used_resource = NodeResource(1, 2048)
                nodes.append(worker)
            step = i * 100 + 1
            ts = i * 1000 + 1
            m = RuntimeMetric(nodes, global_step=step, speed=12, timestamp=ts)
            reporter.report_runtime_stats(m)
        self.assertEqual(len(reporter._runtime_stats), 8)


class StatsCollectorTest(unittest.TestCase):
    def test_job_metric_collector(self):
        collector = JobMetricCollector(
            "1111", "default", "local", "dlrover", ""
        )
        collector.collect_dataset_metric("test", 1000)
        custom_metric = {"key0": 100}
        collector.collect_custom_data(custom_metric)
        self.assertDictEqual(custom_metric, collector._custom_metric)

        perf_monitor = PerfMonitor()
        t = int(time.time())
        perf_monitor.set_target_worker_num(1)
        perf_monitor.collect_global_step(100, t)
        perf_monitor.collect_global_step(1100, t + 10)
        perf_monitor.add_running_worker(NodeType.WORKER, 0)
        worker = Node(NodeType.WORKER, 0, None)
        collector._stats_reporter._runtime_stats = []
        perf_monitor._start_training_time = 100
        perf_monitor._init_time = 10
        collector._ensure_runtime_report_thread_started = Mock()
        collector.collect_runtime_stats(perf_monitor, [worker])
        collector._report_runtime_stats()
        self.assertEqual(len(collector._runtime_metric.running_nodes), 1)
        self.assertEqual(collector._runtime_metric.speed, 100)
        self.assertEqual(len(collector._stats_reporter._runtime_stats), 1)

    def test_concurrent_start_runtime_resource_report_thread(self):
        collector = JobMetricCollector(
            "1111", "default", "local", "dlrover", ""
        )
        thread_started = threading.Event()
        stop_thread = threading.Event()
        start_count = 0
        start_count_lock = threading.Lock()

        def report_periodically():
            nonlocal start_count
            with start_count_lock:
                start_count += 1
            thread_started.set()
            stop_thread.wait()

        collector.report_runtime_resource_periodically = report_periodically
        perf_monitor = PerfMonitor()
        worker = Node(NodeType.WORKER, 0, None)
        worker.used_resource = NodeResource(1, 2048)
        worker_count = 32
        barrier = threading.Barrier(worker_count)

        def start_report_thread():
            barrier.wait(timeout=2)
            collector.collect_runtime_resource_usage(
                perf_monitor, [worker]
            )

        try:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [
                    executor.submit(start_report_thread)
                    for _ in range(worker_count)
                ]
                for future in futures:
                    future.result()

            self.assertTrue(thread_started.wait(timeout=1))
            self.assertEqual(start_count, 1)
            self.assertIsNotNone(
                collector._report_runtime_resource_thread
            )
            self.assertTrue(
                collector._report_runtime_resource_thread.is_alive()
            )
        finally:
            stop_thread.set()
            thread = collector._report_runtime_resource_thread
            if thread is not None:
                thread.join(timeout=1)

    def test_recreate_stopped_runtime_resource_report_thread(self):
        collector = JobMetricCollector(
            "1111", "default", "local", "dlrover", ""
        )
        first_thread_finished = threading.Event()

        def finish_first_thread():
            first_thread_finished.set()

        collector.report_runtime_resource_periodically = finish_first_thread
        collector._ensure_runtime_resource_report_thread_started()
        first_thread = collector._report_runtime_resource_thread
        self.assertTrue(first_thread_finished.wait(timeout=1))
        first_thread.join(timeout=1)
        self.assertFalse(first_thread.is_alive())

        second_thread_started = threading.Event()
        stop_second_thread = threading.Event()

        def run_second_thread():
            second_thread_started.set()
            stop_second_thread.wait()

        collector.report_runtime_resource_periodically = run_second_thread
        try:
            collector._ensure_runtime_resource_report_thread_started()
            second_thread = collector._report_runtime_resource_thread
            self.assertTrue(second_thread_started.wait(timeout=1))
            self.assertIsNot(first_thread, second_thread)
            self.assertTrue(second_thread.is_alive())
        finally:
            stop_second_thread.set()
            thread = collector._report_runtime_resource_thread
            if thread is not None:
                thread.join(timeout=1)

    def test_runtime_and_resource_metrics_are_isolated(self):
        collector = JobMetricCollector(
            "1111", "default", "local", "dlrover", ""
        )
        collector._ensure_runtime_report_thread_started = Mock()
        collector._ensure_runtime_resource_report_thread_started = Mock()

        perf_monitor = PerfMonitor()
        perf_monitor.set_target_worker_num(1)
        perf_monitor.add_running_worker(NodeType.WORKER, 0)
        now = int(time.time())
        perf_monitor.collect_global_step(100, now)
        perf_monitor.collect_global_step(200, now + 10)

        worker = Node(NodeType.WORKER, 0, None)
        worker.used_resource = NodeResource(1, 2048)

        collector.collect_runtime_resource_usage(perf_monitor, [worker])
        resource_metric = collector._get_runtime_resource_metric_snapshot()
        collector.collect_runtime_stats(perf_monitor, [worker])
        runtime_metric = collector._get_runtime_metric_snapshot()

        self.assertIsNot(
            collector._runtime_metric,
            collector._runtime_resource_metric,
        )
        self.assertEqual(len(resource_metric.running_nodes), 1)
        self.assertEqual(len(runtime_metric.running_nodes), 1)
        self.assertEqual(runtime_metric.global_step, 200)
        self.assertGreater(runtime_metric.speed, 0)
        self.assertEqual(resource_metric.global_step, 0)
