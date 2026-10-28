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

import time
import unittest
from unittest.mock import MagicMock

from dlrover.python.common.constants import NodeType
from dlrover.python.master.monitor.perf_monitor import PerfMonitor


class SpeedMonitorTest(unittest.TestCase):
    def test_monitor_running_workers(self):
        monitor = PerfMonitor()
        monitor.set_target_worker_num(2)
        monitor.add_running_worker(NodeType.WORKER, 0)
        monitor.add_running_worker(NodeType.WORKER, 1)
        monitor.collect_global_step(1, 1)
        monitor.collect_global_step(301, 11)
        monitor.collect_global_step(9001, 301)
        self.assertEqual(monitor.completed_global_step, 9001)
        self.assertTrue(monitor.worker_adjustment_finished())
        self.assertEqual(monitor.running_speed, 30)
        monitor.remove_running_worker(NodeType.WORKER, 1)
        monitor.collect_global_step(18001, 601)
        self.assertFalse(monitor.worker_adjustment_finished())

    def test_monitor_eval_time(self):
        monitor = PerfMonitor()
        monitor.set_worker_start_eval_time(0)
        time.sleep(0.1)
        monitor.update_worker_eval_time(0)
        eval_time = monitor.get_worker_eval_time(0)
        self.assertTrue(eval_time > 0.1)

    def test_prometheus_snapshot_uses_master_progress_time(self):
        clock = MagicMock(side_effect=[990, 1000, 1010, 1020])
        monitor = PerfMonitor(clock=clock)
        monitor.add_running_worker(NodeType.WORKER, 0)
        monitor.set_target_worker_num(2)

        monitor.collect_global_step(100, 0)
        monitor.collect_global_step(200, None)
        monitor.collect_global_step(150, 30)

        snapshot = monitor.get_metrics_snapshot()
        self.assertEqual(snapshot.completed_global_step, 200)
        self.assertEqual(snapshot.last_progress_timestamp, 1010)
        self.assertEqual(snapshot.running_speed, 10)
        self.assertEqual(snapshot.running_worker_count, 1)
        self.assertEqual(snapshot.target_worker_count, 2)

        monitor.reset_running_perf_monitor()
        reset_snapshot = monitor.get_metrics_snapshot()
        self.assertEqual(reset_snapshot.completed_global_step, 200)
        self.assertEqual(reset_snapshot.last_progress_timestamp, 1010)
        self.assertEqual(reset_snapshot.running_speed, 0)
        self.assertEqual(reset_snapshot.running_worker_count, 0)
