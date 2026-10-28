# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import os
import shlex
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from io import StringIO
from unittest import mock

from dlrover.python.workspace.process_manager import WorkspaceProcessManager
from dlrover.python.workspace.state import (
    RunState,
    STATE_FAILED,
    STATE_RUNNING,
    STATE_STOPPED,
)


class WorkspaceProcessManagerTest(unittest.TestCase):
    def test_agentctl_passes_directories(self):
        from dlrover.python.workspace import agentctl

        with mock.patch.object(agentctl, "WorkspaceProcessManager") as manager:
            manager.return_value.launch.return_value = 0
            self.assertEqual(agentctl.main([
                "--state-dir", "/state", "--log-dir", "/logs", "launch",
                "--cmd-b64", "ZWNobyBoaQ==", "--launch-id", "launch-1",
            ]), 0)
            manager.assert_called_once_with("/logs", state_dir="/state")
            manager.return_value.launch.assert_called_once_with(
                command="echo hi", launch_id="launch-1", wait_seconds=5,
            )

    def test_launch_exports_workspace_state_file(self):
        with tempfile.TemporaryDirectory() as log_dir:
            manager = WorkspaceProcessManager(
                log_dir, state_dir=os.path.join(log_dir, "state")
            )
            process = mock.MagicMock(pid=12345)
            with mock.patch.dict(
                os.environ,
                {"DLROVER_WORKSPACE_MIRROR_STDOUT": "false"},
            ):
                with mock.patch(
                    "dlrover.python.workspace.process_manager.subprocess.Popen",
                    return_value=process,
                ) as popen:
                    with mock.patch(
                        "dlrover.python.workspace.process_manager.os.getpgid",
                        return_value=12345,
                    ):
                        with mock.patch.object(manager, "status", return_value=""):
                            code = manager.launch("echo hi", "launch-env")

            self.assertEqual(code, 0)
            launch_env = popen.call_args.kwargs["env"]
            self.assertEqual(
                launch_env["DLROVER_WORKSPACE_STATE_FILE"],
                manager.store.state_file,
            )

    def test_release_launch_uses_result_wrapper(self):
        with tempfile.TemporaryDirectory() as root:
            manager = WorkspaceProcessManager(
                os.path.join(root, "logs"),
                state_dir=os.path.join(root, "state"),
            )
            with mock.patch.dict(
                os.environ,
                {
                    "DLROVER_WORKSPACE_EXIT_POLICY": "release",
                    "DLROVER_WORKSPACE_MIRROR_STDOUT": "false",
                },
                clear=True,
            ):
                command = manager._build_runner_command("echo train")

            self.assertIn(
                "dlrover.python.workspace.release_supervisor run", command
            )
            self.assertIn("--state-dir", command)
            self.assertNotIn("echo train", command)

    def test_release_launch_records_real_process_failure(self):
        with tempfile.TemporaryDirectory() as root:
            manager = WorkspaceProcessManager(
                os.path.join(root, "logs"),
                state_dir=os.path.join(root, "state"),
            )
            with mock.patch.dict(
                os.environ,
                {
                    "DLROVER_WORKSPACE_EXIT_POLICY": "release",
                    "DLROVER_WORKSPACE_MIRROR_STDOUT": "false",
                },
                clear=True,
            ):
                with redirect_stdout(StringIO()):
                    self.assertEqual(
                        manager.launch("exit 6", "release-failure", 0), 0
                    )
                deadline = time.time() + 5
                while time.time() < deadline:
                    state = manager.store.load()
                    if state.state == STATE_FAILED:
                        break
                    time.sleep(0.05)

            state = manager.store.load()
            self.assertEqual(state.state, STATE_FAILED)
            self.assertEqual(state.exit_code, 6)

    def test_launch_status_duplicate_and_stop(self):
        with tempfile.TemporaryDirectory() as log_dir:
            mirror_file = os.path.join(log_dir, "container.stdout")
            manager = WorkspaceProcessManager(
                log_dir, state_dir=os.path.join(log_dir, "state")
            )
            script = "import time; print('ready', flush=True); time.sleep(60)"
            command = "%s -c %s" % (
                shlex.quote(sys.executable),
                shlex.quote(script),
            )

            try:
                with mock.patch.dict(
                    os.environ,
                    {"DLROVER_WORKSPACE_CONTAINER_STDOUT": mirror_file},
                ):
                    with redirect_stdout(StringIO()):
                        launch_code = manager.launch(
                            command, "launch-test", wait_seconds=0
                        )
                self.assertEqual(launch_code, 0)
                self._wait_for_text(manager.log_file, "ready")
                self._wait_for_text(mirror_file, "ready")

                state = manager.store.load()
                self.assertEqual(state.state, STATE_RUNNING)
                self.assertEqual(state.launch_id, "launch-test")
                self.assertTrue(os.path.exists(manager.pid_file))
                self.assertTrue(os.path.exists(manager.pgid_file))
                self.assertTrue(os.path.exists(manager.launch_id_file))

                status = manager.status()
                self.assertIn("launch_id: launch-test", status)
                self.assertIn("state: RUNNING", status)
                self.assertIn("dlrover-run:", status)
                self.assertIn("training:", status)

                with redirect_stdout(StringIO()):
                    duplicate_code = manager.launch(
                        command, "launch-test-2", wait_seconds=0
                    )
                self.assertEqual(duplicate_code, 2)
            finally:
                master_client = mock.MagicMock()
                with mock.patch.dict(
                    os.environ,
                    {"DLROVER_WORKSPACE_MODE": "true"},
                ):
                    with mock.patch.object(
                        manager,
                        "_build_master_client",
                        return_value=master_client,
                    ):
                        with redirect_stdout(StringIO()):
                            manager.stop(grace_period=0, kill_timeout=0)
                master_client.report_workspace_training_finished.assert_called_once()

            state = manager.store.load()
            self.assertEqual(state.state, STATE_STOPPED)
            self.assertFalse(os.path.exists(manager.pid_file))
            self.assertFalse(os.path.exists(manager.pgid_file))
            self.assertFalse(os.path.exists(manager.launch_id_file))

    def test_stop_skips_master_report_outside_workspace(self):
        with tempfile.TemporaryDirectory() as log_dir:
            manager = WorkspaceProcessManager(
                log_dir, state_dir=os.path.join(log_dir, "state")
            )

            with mock.patch.dict(os.environ, {}, clear=True):
                with mock.patch.object(
                    manager, "_build_master_client"
                ) as build_client:
                    with redirect_stdout(StringIO()):
                        code = manager.stop(grace_period=0, kill_timeout=0)

            self.assertEqual(code, 0)
            build_client.assert_not_called()

    def test_release_stop_reports_succeeded_exit(self):
        with tempfile.TemporaryDirectory() as log_dir:
            manager = WorkspaceProcessManager(
                log_dir, state_dir=os.path.join(log_dir, "state")
            )
            master_client = mock.MagicMock()
            with mock.patch.dict(
                os.environ,
                {
                    "DLROVER_WORKSPACE_MODE": "true",
                    "DLROVER_WORKSPACE_EXIT_POLICY": "release",
                },
            ):
                with mock.patch.object(
                    manager,
                    "_build_master_client",
                    return_value=master_client,
                ):
                    manager._report_workspace_training_finished()

            master_client.report_succeeded_exited.assert_called_once()
            master_client.report_workspace_training_finished.assert_not_called()

    def test_launch_failure_logs_diagnostic_and_saves_state(self):
        with tempfile.TemporaryDirectory() as log_dir:
            manager = WorkspaceProcessManager(
                log_dir, state_dir=os.path.join(log_dir, "state")
            )

            with mock.patch(
                "dlrover.python.workspace.process_manager.logger"
            ) as manager_logger:
                with mock.patch.object(
                    manager,
                    "_write_launch_marker",
                    side_effect=RuntimeError("marker failed"),
                ):
                    code = manager.launch("echo hi", "launch-failed")

            self.assertEqual(code, 1)
            state = manager.store.load()
            self.assertEqual(state.state, STATE_FAILED)
            self.assertIn("RuntimeError", state.last_error)
            manager_logger.warning.assert_called()
            self._wait_for_text(
                manager.log_file, "workspace process launch failed"
            )
            self._wait_for_text(manager.log_file, "launch_id: launch-failed")

    def test_log_directory_does_not_change_training_identity(self):
        with tempfile.TemporaryDirectory() as root:
            state_dir = os.path.join(root, "state")
            first = WorkspaceProcessManager(os.path.join(root, "logs1"), state_dir)
            first.store.save(RunState(state=STATE_RUNNING, launch_id="active", pid=123))
            second = WorkspaceProcessManager(os.path.join(root, "logs2"), state_dir)
            with mock.patch.object(second, "_is_alive", return_value=True):
                self.assertEqual(second.launch("echo hi", "other"), 2)
            self.assertFalse(os.path.exists(second.log_dir))
            self.assertEqual(first.store.lock_file, second.store.lock_file)
            self.assertEqual(os.path.dirname(second.pid_file), state_dir)

    @mock.patch("dlrover.python.workspace.process_manager.datetime")
    @mock.patch.dict(os.environ, {"NODE_RANK": "2"})
    def test_repeated_launches_preserve_logs_and_record_path(self, clock):
        clock.now.return_value = datetime(2026, 9, 10, 8, 30, 15, 123456, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as root:
            manager = WorkspaceProcessManager(os.path.join(root, "logs"), os.path.join(root, "state"))
            timestamp = "20260910T083015123456Z"
            with mock.patch.object(manager, "_is_alive", return_value=False), \
                 mock.patch.object(manager, "status", return_value=""), \
                 mock.patch("dlrover.python.workspace.process_manager.subprocess.Popen", return_value=mock.Mock(pid=123)), \
                 mock.patch("dlrover.python.workspace.process_manager.os.getpgid", return_value=123):
                self.assertEqual(manager.launch("echo first", "first"), 0)
                first = manager.log_file
                self.assertEqual(manager.launch("echo second", "second"), 0)
            self.assertEqual(os.path.basename(first), "train.rank_2_" + timestamp + ".log")
            self.assertEqual(os.path.basename(manager.log_file), "train.rank_2_" + timestamp + "_1.log")
            self.assertEqual(manager.store.load().log_file, manager.log_file)
            with open(first) as fp:
                self.assertIn("echo first", fp.read())
            self.assertEqual(sorted(os.listdir(manager.log_dir)), sorted([os.path.basename(first), os.path.basename(manager.log_file)]))

    @mock.patch.dict(os.environ, {"NODE_RANK": "0"})
    def test_logs_use_selected_directory_and_follow_selected_file(self):
        with tempfile.TemporaryDirectory() as root:
            state_dir = os.path.join(root, "state")
            manager = WorkspaceProcessManager(root, state_dir)
            recorded = os.path.join(root, "recorded.log")
            manager.store.save(RunState(log_file=recorded))
            names = ["train.rank_0_20260910T083015123456Z.log",
                     "train.rank_0_20260910T093015123456Z.log",
                     "train.rank_1_20260910T103015123456Z.log"]
            for name in names:
                with open(os.path.join(root, name), "w"):
                    pass
            with mock.patch("dlrover.python.workspace.process_manager.log_utils.print_tail") as tail:
                default_manager = WorkspaceProcessManager(state_dir=state_dir)
                self.assertEqual(default_manager.log_dir, "/tmp/dlrover-workspace")
                manager.logs(10, False)
                tail.assert_called_once_with(os.path.join(root, names[1]), 10)
            with mock.patch("dlrover.python.workspace.process_manager.log_utils.follow_file") as follow:
                manager.logs(5, True)
                follow.assert_called_once_with(os.path.join(root, names[1]), 5)

    def _wait_for_text(self, path, text, timeout=5):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with open(path, "r") as fp:
                    if text in fp.read():
                        return
            except FileNotFoundError:
                pass
            time.sleep(0.1)
        self.fail("did not find %r in %s" % (text, path))


if __name__ == "__main__":
    unittest.main()
