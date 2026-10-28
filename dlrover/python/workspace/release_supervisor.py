# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import argparse
import os
import subprocess
import time

from dlrover.python.common.log import default_logger as logger
from dlrover.python.workspace.constants import DEFAULT_STATE_DIR
from dlrover.python.workspace.state import (
    STATE_FAILED,
    STATE_RUNNING,
    STATE_STOPPED,
    WorkspaceStateStore,
)


def _normalize_exit_code(return_code: int) -> int:
    if return_code < 0:
        return min(255, 128 + abs(return_code))
    return min(255, return_code)


def _wait_for_launch_state(
    store: WorkspaceStateStore, launch_id: str, timeout: float = 10
):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(store.state_file):
            state = store.load()
            if state.launch_id == launch_id and state.state == STATE_RUNNING:
                return
        time.sleep(0.05)


def run_training(state_dir: str) -> int:
    command = os.getenv("DLROVER_WORKSPACE_USER_COMMAND", "")
    launch_id = os.getenv("DLROVER_WORKSPACE_LAUNCH_ID", "")
    store = WorkspaceStateStore(state_dir)
    if not command:
        logger.error("DLROVER_WORKSPACE_USER_COMMAND is empty.")
        return 1

    # The detached wrapper can start before agentctl persists RUNNING. Wait so
    # a short-lived command cannot have its terminal state overwritten.
    _wait_for_launch_state(store, launch_id)
    try:
        completed = subprocess.run(["/bin/bash", "-lc", command])
        exit_code = _normalize_exit_code(completed.returncode)
        last_error = ""
        if exit_code != 0:
            last_error = "training exited with code %s" % exit_code
    except Exception as error:
        logger.exception("Failed to run release workspace training command.")
        exit_code = 1
        last_error = "%s: %s" % (type(error).__name__, error)

    with store.locked():
        state = store.load()
        if launch_id and state.launch_id and state.launch_id != launch_id:
            logger.warning(
                "Ignore stale workspace result launch_id=%s current=%s.",
                launch_id,
                state.launch_id,
            )
            return exit_code
        state.state = STATE_STOPPED if exit_code == 0 else STATE_FAILED
        state.exit_code = exit_code
        state.last_error = last_error
        store.save(state)
    return exit_code


def wait_for_training(state_dir: str, poll_interval: float = 0.2) -> int:
    store = WorkspaceStateStore(state_dir)
    logger.info("Release workspace supervisor is waiting for training result.")
    while True:
        if not os.path.exists(store.state_file):
            time.sleep(poll_interval)
            continue
        state = store.load()
        if state.state == STATE_FAILED:
            exit_code = state.exit_code if state.exit_code is not None else 1
            logger.error(
                "Release workspace training failed with code %s.", exit_code
            )
            return _normalize_exit_code(exit_code or 1)
        if state.state == STATE_STOPPED and state.exit_code is not None:
            logger.info(
                "Release workspace training completed with code %s.",
                state.exit_code,
            )
            return _normalize_exit_code(state.exit_code)
        time.sleep(poll_interval)


def build_parser():
    parser = argparse.ArgumentParser("DLRover release workspace supervisor")
    parser.add_argument("command", choices=("run", "wait"))
    parser.add_argument("--state-dir", default=DEFAULT_STATE_DIR)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.command == "run":
        return run_training(args.state_dir)
    return wait_for_training(args.state_dir)


if __name__ == "__main__":
    raise SystemExit(main())
