# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import os
import tempfile
import threading
import time
import unittest
from unittest import mock

from dlrover.python.workspace.release_supervisor import (
    run_training,
    wait_for_training,
)
from dlrover.python.workspace.state import (
    STATE_FAILED,
    STATE_RUNNING,
    STATE_STOPPED,
    RunState,
    WorkspaceStateStore,
)


class ReleaseSupervisorTest(unittest.TestCase):
    def test_run_training_records_success(self):
        with tempfile.TemporaryDirectory() as state_dir:
            store = WorkspaceStateStore(state_dir)
            store.save(RunState(state=STATE_RUNNING, launch_id="launch-0"))
            with mock.patch.dict(
                os.environ,
                {
                    "DLROVER_WORKSPACE_USER_COMMAND": "exit 0",
                    "DLROVER_WORKSPACE_LAUNCH_ID": "launch-0",
                },
                clear=True,
            ):
                self.assertEqual(run_training(state_dir), 0)

            state = store.load()
            self.assertEqual(state.state, STATE_STOPPED)
            self.assertEqual(state.exit_code, 0)

    def test_run_training_records_failure(self):
        with tempfile.TemporaryDirectory() as state_dir:
            store = WorkspaceStateStore(state_dir)
            store.save(RunState(state=STATE_RUNNING, launch_id="launch-1"))
            with mock.patch.dict(
                os.environ,
                {
                    "DLROVER_WORKSPACE_USER_COMMAND": "exit 7",
                    "DLROVER_WORKSPACE_LAUNCH_ID": "launch-1",
                },
                clear=True,
            ):
                self.assertEqual(run_training(state_dir), 7)

            state = store.load()
            self.assertEqual(state.state, STATE_FAILED)
            self.assertEqual(state.exit_code, 7)
            self.assertIn("code 7", state.last_error)

    def test_wait_for_training_returns_failure(self):
        with tempfile.TemporaryDirectory() as state_dir:
            store = WorkspaceStateStore(state_dir)
            result = []
            waiter = threading.Thread(
                target=lambda: result.append(
                    wait_for_training(state_dir, poll_interval=0.01)
                )
            )
            waiter.start()
            time.sleep(0.02)
            store.save(
                RunState(
                    state=STATE_FAILED,
                    launch_id="launch-2",
                    exit_code=9,
                )
            )
            waiter.join(timeout=2)

            self.assertFalse(waiter.is_alive())
            self.assertEqual(result, [9])


if __name__ == "__main__":
    unittest.main()
