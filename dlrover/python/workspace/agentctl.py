# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import argparse
import base64
import sys

from dlrover.brain.python.common.log import default_logger as logger
from dlrover.python.workspace.constants import DEFAULT_LOG_DIR, DEFAULT_STATE_DIR
from dlrover.python.workspace.process_manager import WorkspaceProcessManager


def _decode_command(value: str) -> str:
    return base64.b64decode(value.encode("ascii")).decode("utf-8")


def _emit_command_output(text: str):
    if not text:
        return
    sys.stdout.write(text)
    sys.stdout.flush()


def _add_cli_log_args(parser):
    parser.add_argument(
        "--cli-log",
        choices=("on", "off"),
        default=argparse.SUPPRESS,
    )


def _normalize_cli_log_enabled(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).lower() == "on"


def _configure_cli_logging(enabled: bool):
    logger.disabled = not enabled


def build_parser():
    parser = argparse.ArgumentParser("DLRover workspace agent control")
    parser.add_argument(
        "--log-dir", default=DEFAULT_LOG_DIR,
        help="Pod training log directory (default: %(default)s).",
    )
    parser.add_argument("--state-dir", default=DEFAULT_STATE_DIR)
    _add_cli_log_args(parser)
    subparsers = parser.add_subparsers(dest="command", required=True)

    launch = subparsers.add_parser("launch")
    _add_cli_log_args(launch)
    launch.add_argument("--cmd-b64", required=True)
    launch.add_argument("--launch-id", required=True)
    launch.add_argument("--wait-for-workers-seconds", type=int, default=5)

    status = subparsers.add_parser("status")
    _add_cli_log_args(status)
    status.add_argument("--wait-for-workers-seconds", type=int, default=0)

    stop = subparsers.add_parser("stop")
    _add_cli_log_args(stop)
    stop.add_argument("--grace-period", type=int, default=300)
    stop.add_argument("--kill-timeout", type=int, default=30)

    logs = subparsers.add_parser("logs")
    _add_cli_log_args(logs)
    logs.add_argument("--tail", type=int, default=100)
    logs.add_argument("--follow", "-f", action="store_true")
    return parser


def main(argv=None):
    previous_logger_disabled = logger.disabled
    manager = None
    try:
        args = build_parser().parse_args(argv)
        _configure_cli_logging(
            _normalize_cli_log_enabled(getattr(args, "cli_log", None))
        )
        manager = WorkspaceProcessManager(args.log_dir, state_dir=args.state_dir)
        if args.command == "launch":
            return manager.launch(
                command=_decode_command(args.cmd_b64),
                launch_id=args.launch_id,
                wait_seconds=args.wait_for_workers_seconds,
            )
        if args.command == "status":
            _emit_command_output(
                manager.status(
                    wait_for_workers_seconds=args.wait_for_workers_seconds
                )
            )
            return 0
        if args.command == "stop":
            return manager.stop(
                grace_period=args.grace_period,
                kill_timeout=args.kill_timeout,
            )
        if args.command == "logs":
            return manager.logs(tail=args.tail, follow=args.follow)
    except Exception as e:
        logger.error(
            "workspace agentctl failed command=%s log_dir=%s launch_id=%s.",
            args.command,
            args.log_dir,
            getattr(args, "launch_id", ""),
        )
        if manager is not None:
            try:
                status = manager.status()
                logger.error(
                    "workspace status after agentctl failure:\n%s",
                    status.rstrip(),
                )
            except Exception as status_error:
                logger.error(
                    "Failed to collect workspace status after agentctl error "
                    "command=%s log_dir=%s.",
                    args.command,
                    args.log_dir,
                )
                logger.error(
                    "workspace status collection failed: %s: %s",
                    type(status_error).__name__,
                    status_error,
                )
        return 1
    finally:
        logger.disabled = previous_logger_disabled
    return 1


if __name__ == "__main__":
    sys.exit(main())
