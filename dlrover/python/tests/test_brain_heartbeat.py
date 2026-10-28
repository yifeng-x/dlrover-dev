# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import threading
import unittest
from unittest.mock import Mock, patch

from dlrover.brain.python.client.client import BrainRequestError
from dlrover.python.master.brain_heartbeat import BrainHeartbeat


class BrainHeartbeatTest(unittest.TestCase):
    def test_initial_failure_recovers_and_resets_backoff(self):
        register = Mock(side_effect=BrainRequestError("unavailable"))
        heartbeat = Mock(side_effect=[True, BrainRequestError("timeout")])
        reporter = BrainHeartbeat(register, heartbeat)
        with patch("dlrover.python.master.brain_heartbeat.random.uniform",
                   side_effect=lambda low, high: high):
            delay = reporter._attempt(register)
            self.assertEqual(delay, 10)
            self.assertEqual(reporter.snapshot, ("backoff", 1, 0))
            self.assertEqual(reporter._attempt(heartbeat), 10)
            self.assertEqual(reporter.snapshot[:2], ("healthy", 0))
            last_success = reporter.snapshot[2]
            self.assertGreater(last_success, 0)
            self.assertEqual(reporter._attempt(heartbeat), 10)
            self.assertEqual(reporter.snapshot, ("backoff", 1, last_success))

    def test_backoff_is_capped_and_server_delay_is_honored(self):
        report = Mock(side_effect=BrainRequestError("unavailable"))
        reporter = BrainHeartbeat(report, report)
        with patch("dlrover.python.master.brain_heartbeat.random.uniform",
                   side_effect=lambda low, high: high):
            self.assertEqual(
                [reporter._attempt(report) for _ in range(6)],
                [10, 20, 30, 30, 30, 30],
            )
            report.side_effect = BrainRequestError("busy", retry_after=120)
            self.assertEqual(reporter._attempt(report), 120)

    def test_jitter_stays_within_backoff_bounds(self):
        report = Mock(return_value=False)
        reporter = BrainHeartbeat(report, report)
        for cap in [10, 20, 30, 30]:
            delay = reporter._attempt(report)
            self.assertGreaterEqual(delay, cap * 0.8)
            self.assertLessEqual(delay, cap)

    def test_permanent_registration_error_does_not_start_thread(self):
        register = Mock(side_effect=BrainRequestError("unauthorized", False))
        heartbeat = Mock()
        reporter = BrainHeartbeat(register, heartbeat)
        reporter.start()
        reporter.start()
        self.assertEqual(reporter.snapshot[:2], ("failed", 1))
        self.assertIsNone(reporter._thread)
        register.assert_called_once()
        heartbeat.assert_not_called()
        reporter.stop()

    def test_stop_interrupts_backoff_and_prevents_restart(self):
        register = Mock(side_effect=BrainRequestError("busy", retry_after=120))
        heartbeat = Mock()
        reporter = BrainHeartbeat(register, heartbeat)
        reporter.start()
        reporter.start()
        reporter.stop()
        reporter.start()
        self.assertFalse(reporter._thread.is_alive())
        self.assertEqual(reporter.snapshot[0], "stopped")
        register.assert_called_once()
        heartbeat.assert_not_called()

    def test_loop_recovers_then_stops_on_permanent_error(self):
        heartbeat = Mock(side_effect=[True, BrainRequestError("bad job", False)])
        reporter = BrainHeartbeat(Mock(), heartbeat)
        reporter._stop = Mock()
        reporter._stop.is_set.return_value = False
        reporter._stop.wait.return_value = False
        reporter._run(30)
        self.assertEqual(heartbeat.call_count, 2)
        self.assertEqual(reporter.snapshot[0], "failed")
        self.assertEqual(
            [call.args[0] for call in reporter._stop.wait.call_args_list],
            [30, 10],
        )

    def test_stop_during_inflight_heartbeat_prevents_recovery(self):
        entered = threading.Event()
        release = threading.Event()

        def report():
            entered.set()
            release.wait(5)
            return True

        reporter = BrainHeartbeat(Mock(), report)
        thread = threading.Thread(target=reporter._attempt, args=(report,))
        reporter._thread = thread
        thread.start()
        try:
            self.assertTrue(entered.wait(1))
            reporter.stop()
        finally:
            release.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(reporter.snapshot[0], "stopped")

    def test_stop_before_start_does_not_register(self):
        register = Mock()
        reporter = BrainHeartbeat(register, Mock())
        reporter.stop()
        reporter.start()
        register.assert_not_called()


if __name__ == "__main__":
    unittest.main()
