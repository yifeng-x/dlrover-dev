# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import os
import re
import signal
import shlex
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import List, Optional

from dlrover.brain.python.common.log import default_logger as logger
from dlrover.python.workspace.constants import (
    DEFAULT_LOG_DIR,
    DEFAULT_STATE_DIR,
)
from dlrover.python.workspace import log_utils
from dlrover.python.workspace.proc_utils import (
    find_pids_by_launch_id,
    get_process_info,
    is_pgid_alive,
    is_pid_alive,
    signal_pgid,
    signal_pids,
)
from dlrover.python.workspace.state import (
    STATE_FAILED,
    STATE_IDLE,
    STATE_RUNNING,
    STATE_STOPPED,
    STATE_STOPPING,
    RunState,
    WorkspaceStateStore,
)


class WorkspaceProcessManager:
    def __init__(
        self,
        log_dir: str = DEFAULT_LOG_DIR,
        state_dir: str = DEFAULT_STATE_DIR,
    ):
        self.log_dir = os.path.abspath(log_dir)
        self.state_dir = os.path.abspath(state_dir)
        self.store = WorkspaceStateStore(self.state_dir)
        self.pid_file = os.path.join(self.state_dir, "train.pid")
        self.pgid_file = os.path.join(self.state_dir, "train.pgid")
        self.launch_id_file = os.path.join(self.state_dir, "train.launch_id")
        self.rank = os.getenv("NODE_RANK", "unknown")
        self.log_file = os.path.join(self.log_dir, "train.rank_%s.log" % self.rank)

    def _create_log_file(self):
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        os.makedirs(self.log_dir, exist_ok=True)
        stem = "train.rank_%s_%s" % (self.rank, timestamp)
        sequence = 0
        while True:
            suffix = "_%d" % sequence if sequence else ""
            path = os.path.join(self.log_dir, stem + suffix + ".log")
            try:
                with open(path, "x"):
                    pass
                self.log_file = path
                return
            except FileExistsError:
                sequence += 1

    def _resolve_log_file(self):
        pattern = re.compile(
            r"train\.rank_%s_(\d{8}T\d{12}Z)(?:_(\d+))?\.log"
            % re.escape(self.rank)
        )
        candidates = []
        if os.path.isdir(self.log_dir):
            for name in os.listdir(self.log_dir):
                match = pattern.fullmatch(name)
                if match and os.path.isfile(os.path.join(self.log_dir, name)):
                    candidates.append((match[1], int(match[2] or 0), name))
        if candidates:
            return os.path.join(self.log_dir, max(candidates)[2])
        # Support logs from the previous, non-timestamped layout.
        return os.path.join(self.log_dir, "train.rank_%s.log" % self.rank)

    def launch(
        self,
        command: str,
        launch_id: str,
        wait_seconds: int = 5,
    ) -> int:
        with self.store.locked():
            state = self._load_state_or_compat()
            state = self._refresh_state(state)
            if self._is_active(state):
                logger.warning(
                    "Workspace training already running log_dir=%s pid=%s "
                    "pgid=%s launch_id=%s.",
                    self.log_dir,
                    state.pid,
                    state.pgid,
                    state.launch_id,
                )
                return 2

            self._create_log_file()
            env = os.environ.copy()
            env["DLROVER_WORKSPACE_LAUNCH_ID"] = launch_id
            env["DLROVER_WORKSPACE_USER_COMMAND"] = command
            # Let the elastic agent distinguish a user-requested workspace
            # stop from an unexpected worker failure.  The stop command sets
            # this state to STOPPING before signalling training workers.
            env["DLROVER_WORKSPACE_STATE_FILE"] = self.store.state_file
            state = RunState(
                state=STATE_RUNNING,
                launch_id=launch_id,
                command=command,
                log_file=self.log_file,
                started_at=time.time(),
            )

            log_fp = None
            try:
                self._write_launch_marker(command, launch_id)

                runner_command = self._build_runner_command(command)

                if not self._mirror_stdout_enabled():
                    log_fp = open(self.log_file, "a")

                # 让 Linux 执行 setsid()，创建全新的会话 + 全新的进程组（PGID），新进程的 PGID = 它自己的 PID
                proc = subprocess.Popen(
                    ["/bin/bash", "-lc", runner_command],
                    stdin=subprocess.DEVNULL,
                    stdout=log_fp or subprocess.DEVNULL,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    close_fds=True,
                    env=env,
                )
                if log_fp:
                    log_fp.close()
                state.pid = proc.pid

                try:
                    state.pgid = os.getpgid(proc.pid)
                except ProcessLookupError:
                    logger.warning("Failed to get process group id for pid=%s", proc.pid)
                    state.pgid = proc.pid
                # agentctl intentionally detaches the training process and
                # exits after recording metadata.
                proc._child_created = False
                self.store.save(state)
                self._write_compat_files(state)
            except Exception as e:
                logger.warning(
                    "Failed to launch workspace training log_dir=%s "
                    "launch_id=%s log_file=%s command=%s.",
                    self.log_dir,
                    launch_id,
                    self.log_file,
                    command,
                    exc_info=e,
                )
                state.state = STATE_FAILED
                state.last_error = self._short_error(e)
                self._safe_save_state(state)
                message = self._format_exception("launch", e, state)
                self._append_diagnostic(message)
                return 1
            finally:
                if log_fp and not log_fp.closed:
                    log_fp.close()

        logger.info(
            "Workspace training started pid=%s pgid=%s launch_id=%s log=%s.",
            state.pid,
            state.pgid,
            state.launch_id,
            state.log_file,
        )
        logger.info(
            "Workspace training status after launch:\n%s",
            self.status(wait_for_workers_seconds=wait_seconds).rstrip(),
        )
        return 0

    def status(self, wait_for_workers_seconds: int = 0) -> str:
        state = RunState()
        try:
            with self.store.locked():
                state = self._refresh_state(self._load_state_or_compat())
                self.store.save(state)

            worker_pids = self._wait_for_worker_pids(
                state.launch_id, max(0, int(wait_for_workers_seconds))
            )
            lines = [
                "  launch_id: %s" % (state.launch_id or "unknown"),
                "  state: %s" % (state.state or STATE_IDLE),
                "  process_group: %s" % (state.pgid or "unknown"),
                "  log_file: %s" % (state.log_file or self.log_file),
            ]
            if state.exit_code is not None:
                lines.append("  exit_code: %s" % state.exit_code)
            if state.last_error:
                lines.append("  last_error: %s" % state.last_error)
            lines.extend(["  dlrover-run:"])
            lines.extend(self._format_process(state.pid, indent="    "))
            lines.extend(["  training:"])
            if worker_pids:
                for index, pid in enumerate(worker_pids):
                    lines.append("    worker[%d]:" % index)
                    lines.extend(self._format_process(pid, indent="      "))
            else:
                lines.append("    pending: true")
            return "\n".join(lines) + "\n"
        except Exception as e:
            logger.warning(
                "Failed to collect workspace status log_dir=%s "
                "log_file=%s.",
                self.log_dir,
                self.log_file,
                exc_info=e,
            )
            message = self._format_exception("status", e, state)
            self._append_diagnostic(message)
            return message

    def stop(self, grace_period: int, kill_timeout: int) -> int:
        grace_period = max(0, int(grace_period))
        kill_timeout = max(0, int(kill_timeout))

        with self.store.locked():
            state = self._refresh_state(self._load_state_or_compat())
            if not self._has_metadata(state):
                self._step(1, "no pid file: %s" % self.pid_file)
                return 0
            state.state = STATE_STOPPING
            self.store.save(state)

        self._step(
            1,
            "loaded stop metadata pid=%s pgid=%s launch_id=%s"
            % (state.pid or "", state.pgid or "", state.launch_id or ""),
        )
        if not state.pgid and state.pid:
            info = get_process_info(state.pid)
            if info:
                state.pgid = info.pgid
        self._step(2, "resolved process group pgid=%s" % (state.pgid or ""))

        self._step(3, "scanning training worker processes")
        worker_pids = find_pids_by_launch_id(
            state.launch_id, training_workers_only=True
        )
        if worker_pids:
            signal_pids(worker_pids, signal.SIGUSR1)
            self._step(
                4,
                "sent SIGUSR1 graceful stop signal to worker pids=%s grace_period=%ss"
                % (" ".join(str(pid) for pid in worker_pids), grace_period),
            )
        else:
            self._step(4, "no torch worker pids found, waiting for launcher exit")

        self._step(5, "waiting for graceful exit")
        self._wait_until_stopped(state, grace_period, stage=5)
        if not self._is_alive(state):
            self._finish_stopped(state, exit_code=0)
            self._report_workspace_training_finished()
            self._step(
                6,
                "gracefully stopped pid=%s pgid=%s" % (state.pid, state.pgid),
            )
            self._step(7, "cleaned workspace stop metadata")
            return 0

        self._step(
            6,
            "graceful stop timed out, terminating pid=%s pgid=%s"
            % (state.pid or "", state.pgid or ""),
        )
        if state.pgid and is_pgid_alive(state.pgid):
            self._step(6, "sending SIGTERM to pgid=%s" % state.pgid)
            signal_pgid(state.pgid, signal.SIGTERM)
        elif state.pid and is_pid_alive(state.pid):
            self._step(6, "sending SIGTERM to pid=%s" % state.pid)
            signal_pids([state.pid], signal.SIGTERM)
        self._wait_until_stopped(state, kill_timeout, stage=6)
        if self._is_alive(state):
            if state.pgid and is_pgid_alive(state.pgid):
                self._step(
                    6,
                    "SIGTERM timed out, sending SIGKILL to pgid=%s"
                    % state.pgid,
                )
                signal_pgid(state.pgid, signal.SIGKILL)
            elif state.pid and is_pid_alive(state.pid):
                self._step(
                    6,
                    "SIGTERM timed out, sending SIGKILL to pid=%s" % state.pid,
                )
                signal_pids([state.pid], signal.SIGKILL)

        if state.launch_id:
            self._step(6, "scanning residual launch processes")
            residual = find_pids_by_launch_id(state.launch_id)
            if residual:
                self._step(
                    6,
                    "terminating residual pids=%s"
                    % " ".join(str(pid) for pid in residual),
                )
                signal_pids(residual, signal.SIGTERM)
                time.sleep(kill_timeout)
                signal_pids(residual, signal.SIGKILL)
                self._step(
                    6,
                    "stopped residual pids=%s"
                    % " ".join(str(pid) for pid in residual),
                )
            else:
                self._step(6, "no residual launch processes found")

        if self._is_alive_after_force_kill(state):
            message = (
                "force stop finished but process is still alive "
                "pid=%s pgid=%s" % (state.pid or "", state.pgid or "")
            )
            logger.error(
                "Workspace force stop failed log_dir=%s pid=%s pgid=%s "
                "launch_id=%s.",
                self.log_dir,
                state.pid,
                state.pgid,
                state.launch_id,
            )
            self._finish_failed(state, message)
            self._step(7, message)
            self._report_workspace_training_finished(succeeded=False)
            return 1

        self._finish_stopped(state, exit_code=0)
        self._report_workspace_training_finished()
        self._step(7, "cleaned workspace stop metadata")
        return 0

    def logs(self, tail: int, follow: bool):
        try:
            self.log_file = self._resolve_log_file()
            if follow:
                log_utils.follow_file(self.log_file, tail)
            else:
                log_utils.print_tail(self.log_file, tail)
            return 0
        except Exception as e:
            logger.warning(
                "Failed to read workspace logs log_dir=%s log_file=%s "
                "tail=%s follow=%s.",
                self.log_dir,
                self.log_file,
                tail,
                follow,
                exc_info=e,
            )
            message = self._format_exception("logs", e, self._load_state_or_compat())
            self._append_diagnostic(message)
            return 1

    def _write_launch_marker(self, command: str, launch_id: str):
        os.makedirs(self.log_dir, exist_ok=True)
        with open(self.log_file, "a") as log_fp:
            log_fp.write(
                "\n[%s] workspace launch_id=%s command=%s\n"
                % (self._now(), launch_id, command)
            )
            log_fp.flush()

    def _build_runner_command(self, command: str) -> str:
        runner_command = command
        if (
            os.getenv("DLROVER_WORKSPACE_EXIT_POLICY", "retain").lower()
            == "release"
        ):
            runner_command = (
                "%s -m dlrover.python.workspace.release_supervisor run "
                "--state-dir %s"
                % (
                    shlex.quote(sys.executable),
                    shlex.quote(self.state_dir),
                )
            )
        if not self._mirror_stdout_enabled():
            return runner_command
        log_file = shlex.quote(self.log_file)
        stdout_file = shlex.quote(self._container_stdout_file())
        return (
            "set -o pipefail; "
            "%s 2>&1 | tee -a %s %s >/dev/null"
            % (runner_command, log_file, stdout_file)
        )

    def _mirror_stdout_enabled(self) -> bool:
        value = os.getenv("DLROVER_WORKSPACE_MIRROR_STDOUT", "true").lower()
        return value not in ("0", "false", "no", "off")

    def _container_stdout_file(self) -> str:
        return os.getenv(
            "DLROVER_WORKSPACE_CONTAINER_STDOUT", "/proc/1/fd/1"
        )

    def _load_state_or_compat(self) -> RunState:
        state = self.store.load()
        if state.log_file:
            self.log_file = state.log_file
        if state.launch_id or state.pid or state.pgid:
            if not state.log_file:
                state.log_file = self.log_file
            return state
        pid = self._read_int_file(self.pid_file)
        pgid = self._read_int_file(self.pgid_file)
        launch_id = self._read_str_file(self.launch_id_file)
        if pid or pgid or launch_id:
            return RunState(
                state=STATE_RUNNING,
                launch_id=launch_id,
                pid=pid,
                pgid=pgid,
                log_file=self.log_file,
                updated_at=time.time(),
            )
        return state

    def _refresh_state(self, state: RunState) -> RunState:
        if not state.log_file:
            state.log_file = self.log_file
        if state.pid and not state.pgid:
            info = get_process_info(state.pid)
            if info:
                state.pgid = info.pgid
        if state.state in (STATE_RUNNING, STATE_STOPPING):
            if not self._is_alive(state):
                state.state = STATE_STOPPED
        return state

    def _is_active(self, state: RunState) -> bool:
        return state.state in (STATE_RUNNING, STATE_STOPPING, STATE_FAILED) and (
            self._is_alive(state)
        )

    def _is_alive(self, state: RunState) -> bool:
        if state.launch_id and find_pids_by_launch_id(state.launch_id):
            return True
        if state.pgid:
            return is_pgid_alive(state.pgid)
        return is_pid_alive(state.pid)

    def _has_metadata(self, state: RunState) -> bool:
        return bool(state.pid or state.pgid or state.launch_id)

    def _wait_for_worker_pids(self, launch_id: str, wait_seconds: int) -> List[int]:
        deadline = time.time() + wait_seconds
        while True:
            pids = find_pids_by_launch_id(launch_id, training_workers_only=True)
            if pids or wait_seconds <= 0 or time.time() >= deadline:
                return pids
            time.sleep(1)

    def _wait_until_stopped(self, state: RunState, timeout: int, stage: int):
        deadline = time.time() + timeout
        start = time.time()
        while self._is_alive(state) and time.time() < deadline:
            time.sleep(2)
            elapsed = int(time.time() - start)
            if elapsed and elapsed % 10 == 0:
                self._step(
                    stage,
                    "waiting elapsed=%ss/%ss pid=%s pgid=%s"
                    % (elapsed, timeout, state.pid or "", state.pgid or ""),
                )

    def _finish_stopped(self, state: RunState, exit_code: Optional[int]):
        with self.store.locked():
            state.state = STATE_STOPPED
            state.exit_code = exit_code
            self.store.save(state)
            self._remove_compat_files()

    def _report_workspace_training_finished(self, succeeded=True):
        if os.getenv("DLROVER_WORKSPACE_MODE", "").lower() not in (
            "true",
            "1",
            "yes",
            "y",
        ):
            return
        try:
            client = self._build_master_client()
            if not client:
                logger.warning(
                    "Skip reporting workspace training finished because "
                    "master client is unavailable."
                )
                return
            if (
                os.getenv("DLROVER_WORKSPACE_EXIT_POLICY", "retain").lower()
                == "release"
            ):
                if succeeded:
                    client.report_succeeded_exited()
                else:
                    client.report_failed_exited()
                self._step(7, "reported released workspace stop to master")
            else:
                client.report_workspace_training_finished()
                self._step(7, "reported workspace training finished to master")
        except Exception as e:
            logger.warning(
                "Failed to report workspace training finished to master.",
                exc_info=e,
            )

    def _build_master_client(self):
        from dlrover.python.elastic_agent.master_client import (
            build_master_client,
        )

        return build_master_client(timeout=5)

    def _finish_failed(self, state: RunState, message: str):
        with self.store.locked():
            state.state = STATE_FAILED
            state.last_error = message
            self.store.save(state)

    def _safe_save_state(self, state: RunState):
        try:
            self.store.save(state)
        except Exception as e:
            logger.error(
                "Failed to save workspace state after error log_dir=%s: %s: %s",
                self.log_dir,
                type(e).__name__,
                e,
            )

    def _is_alive_after_force_kill(self, state: RunState) -> bool:
        deadline = time.time() + 5
        while self._is_alive(state) and time.time() < deadline:
            time.sleep(0.2)
        return self._is_alive(state)

    def _format_process(self, pid: Optional[int], indent: str) -> List[str]:
        lines = [indent + "process:"]
        if not pid:
            lines.append(indent + "  unavailable")
            return lines
        info = get_process_info(pid)
        if not info or info.stat == "Z":
            lines.append(indent + "  not running (pid=%s)" % pid)
            return lines
        lines.extend(
            [
                indent + "  pid: %s" % info.pid,
                indent + "  ppid: %s" % info.ppid,
                indent + "  pgid: %s" % info.pgid,
                indent + "  stat: %s" % info.stat,
                indent + "  cmd: %s" % info.cmd,
            ]
        )
        return lines

    def _write_compat_files(self, state: RunState):
        if state.pid is not None:
            self._write_file(self.pid_file, "%s\n" % state.pid)
        if state.pgid is not None:
            self._write_file(self.pgid_file, "%s\n" % state.pgid)
        if state.launch_id:
            self._write_file(self.launch_id_file, "%s\n" % state.launch_id)

    def _remove_compat_files(self):
        for path in (self.pid_file, self.pgid_file, self.launch_id_file):
            try:
                os.remove(path)
            except FileNotFoundError:
                logger.warning("Compat file not found for removal: %s", path)
                pass

    def _read_int_file(self, path: str) -> Optional[int]:
        value = self._read_str_file(path)
        try:
            return int(value) if value else None
        except ValueError:
            logger.warning("Failed to parse integer from file %s with content: %s", path, value)
            return None

    def _read_str_file(self, path: str) -> str:
        try:
            with open(path, "r") as fp:
                return fp.read().strip()
        except FileNotFoundError:
            logger.warning("File not found: %s", path)
            return ""

    def _write_file(self, path: str, value: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp_path = "%s.%s.tmp" % (path, os.getpid())
        with open(tmp_path, "w") as fp:
            fp.write(value)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp_path, path)

    def _step(self, index: int, message: str):
        logger.info("[%s] [%d/7] %s", self._now(), index, message)

    def _now(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S")

    def _short_error(self, error: Exception):
        return "%s: %s" % (type(error).__name__, error)

    def _format_exception(
        self, operation: str, error: Exception, state: Optional[RunState] = None
    ):
        state = state or RunState()
        lines = [
            "workspace process %s failed:" % operation,
            "  state_dir: %s" % self.state_dir,
            "  log_dir: %s" % self.log_dir,
            "  log_file: %s" % (state.log_file or self.log_file),
            "  pid_file: %s" % self.pid_file,
            "  pgid_file: %s" % self.pgid_file,
            "  launch_id_file: %s" % self.launch_id_file,
            "  state: %s" % (state.state or STATE_IDLE),
            "  launch_id: %s" % (state.launch_id or ""),
            "  pid: %s" % (state.pid or ""),
            "  pgid: %s" % (state.pgid or ""),
            "  error_type: %s" % type(error).__name__,
            "  error: %s" % error,
            "  traceback:",
        ]
        lines.extend(
            "    " + line.rstrip()
            for line in traceback.format_exception(
                type(error), error, error.__traceback__
            )
        )
        return "\n".join(lines) + "\n"

    def _append_diagnostic(self, message: str):
        try:
            os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
            with open(self.log_file, "a") as log_fp:
                log_fp.write("\n[%s] %s\n" % (self._now(), message.rstrip()))
                log_fp.flush()
        except Exception:
            logger.warning(
                "Failed to append workspace diagnostic log_file=%s.",
                self.log_file,
            )
            pass
