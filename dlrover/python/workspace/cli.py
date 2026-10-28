# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import argparse
import os
import re
import threading
import sys
from typing import Optional

from dlrover.brain.python.common.log import default_logger as logger
from dlrover.python.workspace.constants import (
    DEFAULT_LOG_DIR,
    DEFAULT_STATE_DIR,
)
from dlrover.python.workspace.network import (
    NETWORK_MODE_ALL,
    NETWORK_MODE_NONE,
    NETWORK_MODE_RANKS,
    WorkspaceLaunchCommandBuilder,
    WorkspaceNetworkPlanner,
)

DEFAULT_CONTAINER = "main"
DEFAULT_STOP_GRACE_PERIOD = 300
DEFAULT_STOP_KILL_TIMEOUT = 30

WorkspaceLauncher = None


def _parse_ranks(value: Optional[str]):
    if not value:
        return None
    ranks = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        rank = int(item)
        if rank < 0:
            raise ValueError("--ranks only supports non-negative integers.")
        ranks.append(rank)
    duplicated = sorted(
        rank for rank in set(ranks) if ranks.count(rank) > 1
    )
    if duplicated:
        raise ValueError(
            "--ranks contains duplicated ranks: "
            + ",".join(str(rank) for rank in duplicated)
        )
    return ranks


def _add_common_args(parser):
    parser.add_argument("--job-name", default=None)
    parser.add_argument("--namespace", default=None)
    parser.add_argument("--container", default=DEFAULT_CONTAINER)
    parser.add_argument("--ranks", default=None, help="Comma-separated ranks.")
    parser.add_argument(
        "--state-dir", default=DEFAULT_STATE_DIR,
        help="Pod-local directory for workspace state, locks and process metadata.",
    )
    _add_cli_log_args(parser)


def _add_cli_log_args(parser):
    parser.add_argument(
        "--cli-log",
        choices=("on", "off"),
        default=argparse.SUPPRESS,
        help="Control DLRover workspace CLI execution logs.",
    )


def build_parser():
    parser = argparse.ArgumentParser("DLRover workspace launcher")
    _add_cli_log_args(parser)
    subparsers = parser.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser("status")
    _add_common_args(status)
    status.add_argument(
        "--log-dir", default=DEFAULT_LOG_DIR,
        help="Compatibility option; state is located using --state-dir.",
    )

    launch = subparsers.add_parser("launch")
    _add_common_args(launch)
    launch_cmd = launch.add_mutually_exclusive_group(required=True)
    launch_cmd.add_argument("--cmd", help="Command to run on workers.")
    launch_cmd.add_argument(
        "--framework-cmd",
        help=(
            "Training framework command to wrap with dlrover-run, for example "
            "'swift sft --model /nas/model'."
        ),
    )
    launch.add_argument(
        "--dlrover-run-args",
        default="--nnodes=$NODE_NUM --nproc_per_node=1",
        help=(
            "DLRover launch arguments used when --network-mode is not none. "
            "Default: '--nnodes=$NODE_NUM --nproc_per_node=1'."
        ),
    )
    launch.add_argument(
        "--network-mode",
        "--auto-network",
        choices=(NETWORK_MODE_NONE, NETWORK_MODE_ALL, NETWORK_MODE_RANKS),
        default=NETWORK_MODE_NONE,
        help=(
            "Control DLRover automatic networking for launch. "
            "'none' runs the command without wrapping it with dlrover-run; "
            "'all' networks all worker Pods; 'ranks' networks only --ranks. "
            "Default: none."
        ),
    )
    launch.add_argument(
        "--log-dir", default=DEFAULT_LOG_DIR,
        help="Pod directory for timestamped training logs (default: %(default)s).",
    )
    launch.add_argument("--foreground", action="store_true")
    launch.add_argument("--max-workers", type=int, default=32)

    stop = subparsers.add_parser("stop")
    _add_common_args(stop)
    stop.add_argument(
        "--log-dir", default=DEFAULT_LOG_DIR,
        help="Compatibility option; state is located using --state-dir.",
    )
    stop.add_argument("--max-workers", type=int, default=32)
    stop.add_argument("--grace-period", type=int, default=DEFAULT_STOP_GRACE_PERIOD)
    stop.add_argument("--kill-timeout", type=int, default=DEFAULT_STOP_KILL_TIMEOUT)

    logs = subparsers.add_parser("logs")
    _add_common_args(logs)
    logs.add_argument(
        "--log-dir", default=DEFAULT_LOG_DIR,
        help="Pod training log directory (default: %(default)s).",
    )
    logs.add_argument("--tail", type=int, default=100)
    logs.add_argument("--max-workers", type=int, default=32)
    logs.add_argument("--follow", "-f", action="store_true")
    return parser


def _get_workspace_launcher_cls():
    global WorkspaceLauncher
    if WorkspaceLauncher is None:
        from dlrover.python.workspace.launcher import WorkspaceLauncher as cls

        WorkspaceLauncher = cls
    return WorkspaceLauncher


def _default_cli_log_enabled():
    value = os.getenv("DLROVER_WORKSPACE_CLI_LOG", "false").lower()
    return value == "on"


def _scan_cli_log_enabled(argv):
    enabled = _default_cli_log_enabled()
    for index, item in enumerate(argv):
        if item == "--cli-log" and index + 1 < len(argv):
            enabled = argv[index + 1].lower() == "on"
        elif item.startswith("--cli-log="):
            enabled = item.split("=", 1)[1].lower() == "on"
    return enabled


def _normalize_cli_log_enabled(value, default: bool):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).lower() == "on"


def _configure_cli_logging(enabled: bool):
    logger.disabled = not enabled


def _emit_command_output(text: str):
    if not text:
        return
    sys.stdout.write(text)
    sys.stdout.flush()


def _format_result_status(command: str, ok: bool):
    action = (command or "command").upper()
    outcome = "SUCCESS" if ok else "FAILED"
    return f"{action} {outcome}"


def _emit_results(command: str, results):
    failed = False
    for result in results:
        status = _format_result_status(command, result.ok)
        _emit_command_output(
            f"===== rank {result.pod.rank} {result.pod.name} [{status}] =====\n"
        )
        if result.output:
            _emit_command_output(result.output.rstrip() + "\n")
        if not result.ok:
            failed = True
    return 1 if failed else 0


def _emit_stream(pod, output):
    if output:
        _emit_command_output(f"[rank {pod.rank} {pod.name}] {output}")


class _StopProgressReporter:
    _stage_pattern = re.compile(r"\[(\d+)/(\d+)\]")

    def __init__(self, pods):
        self._lock = threading.Lock()
        self._pods = {pod.rank: pod for pod in pods}
        self._stages = {pod.rank: 0 for pod in pods}
        self._total_steps = max(1, 7 * len(pods))
        self._done = False
        self._buffers = {pod.rank: "" for pod in pods}

    def handle_output(self, pod, output):
        if not output:
            return
        with self._lock:
            buffer = self._buffers.get(pod.rank, "") + output
            lines = buffer.splitlines(keepends=True)
            tail = ""
            if lines and not lines[-1].endswith(("\n", "\r")):
                tail = lines.pop()
            self._buffers[pod.rank] = tail
            for line in lines:
                self._handle_line_locked(pod, line.rstrip("\r\n"))

    def finish(self):
        with self._lock:
            for rank, tail in list(self._buffers.items()):
                if not tail:
                    continue
                pod = self._pods.get(rank)
                if pod is not None:
                    self._handle_line_locked(pod, tail.rstrip("\r\n"))
                self._buffers[rank] = ""
            self._done = True
            logger.info(self._format_progress_locked())

    def _handle_line_locked(self, pod, line):
        if not line:
            return
        self._update_stage(pod.rank, line)
        logger.info("[rank %s %s] %s", pod.rank, pod.name, line)
        logger.info(self._format_progress_locked())

    def _update_stage(self, rank, output):
        for match in self._stage_pattern.finditer(output):
            stage = int(match.group(1))
            self._stages[rank] = max(self._stages.get(rank, 0), stage)

    def _format_progress_locked(self):
        completed = sum(self._stages.values())
        if self._done:
            completed = self._total_steps
        progress = completed / self._total_steps
        width = 24
        filled = min(width, int(width * progress))
        bar = "#" * filled + "-" * (width - filled)
        active_rank = max(
            self._stages,
            key=lambda rank: (self._stages[rank], -rank),
            default=-1,
        )
        if active_rank >= 0:
            pod = self._pods[active_rank]
            stage = self._stages[active_rank]
            suffix = f"rank {pod.rank} {pod.name} {stage}/7"
        else:
            suffix = "done" if self._done else "waiting"
        return (
            f"stop [{bar}] {completed}/{self._total_steps} "
            f"{progress:>3.0%} {suffix}"
        )


def main(argv=None):
    previous_logger_disabled = logger.disabled
    try:
        argv = sys.argv[1:] if argv is None else list(argv)
        cli_log_enabled = _scan_cli_log_enabled(argv)
        _configure_cli_logging(cli_log_enabled)
        args = build_parser().parse_args(argv)
        cli_log_enabled = _normalize_cli_log_enabled(
            getattr(args, "cli_log", None), cli_log_enabled
        )
        _configure_cli_logging(cli_log_enabled)
        ranks = _parse_ranks(args.ranks)
        launcher_cls = _get_workspace_launcher_cls()
        launcher = launcher_cls(
            job_name=args.job_name,
            namespace=args.namespace,
            container=args.container,
        )
        release_policy = os.getenv(
            "DLROVER_WORKSPACE_EXIT_POLICY", "retain"
        ).lower() == "release"
        if release_policy and args.command == "launch":
            if ranks is not None:
                raise ValueError(
                    "--ranks is unsupported with workspace exit policy release."
                )
            if args.network_mode != NETWORK_MODE_ALL:
                raise ValueError(
                    "workspace exit policy release requires "
                    "--network-mode all."
                )
            if not args.framework_cmd:
                raise ValueError(
                    "workspace exit policy release requires --framework-cmd."
                )
            if args.foreground:
                raise ValueError(
                    "--foreground is unsupported with workspace exit policy release."
                )
        if release_policy and args.command == "stop" and ranks is not None:
            raise ValueError(
                "--ranks is unsupported with workspace exit policy release."
            )
        if args.command == "status":
            return _emit_results(
                args.command,
                launcher.status(
                    ranks=ranks,
                    log_dir=args.log_dir,
                    state_dir=args.state_dir,
                    show_cli_log=cli_log_enabled,
                )
            )
        if args.command == "launch":
            ready_pods = None
            if release_policy:
                ready_pods = launcher.wait_for_running_pods(
                    log_dir=args.log_dir, state_dir=args.state_dir
                )
            network_plan = WorkspaceNetworkPlanner(launcher).plan(
                getattr(args, "network_mode", NETWORK_MODE_NONE),
                ranks,
                pods=ready_pods,
            )
            command = WorkspaceLaunchCommandBuilder().build(
                cmd=args.cmd,
                framework_cmd=args.framework_cmd,
                dlrover_run_args=args.dlrover_run_args,
                network_config=network_plan.config,
            )
            return _emit_results(
                args.command,
                launcher.launch(
                    command=command,
                    ranks=network_plan.ranks,
                    log_dir=args.log_dir,
                    state_dir=args.state_dir,
                    foreground=args.foreground,
                    max_workers=args.max_workers,
                    show_cli_log=cli_log_enabled,
                )
            )

        if args.command == "stop":
            pods = launcher.list_worker_pods(ranks=ranks)
            reporter = _StopProgressReporter(pods)
            try:
                results = launcher.stop(
                    ranks=ranks,
                    log_dir=args.log_dir,
                    state_dir=args.state_dir,
                    max_workers=args.max_workers,
                    grace_period=args.grace_period,
                    kill_timeout=args.kill_timeout,
                    output_callback=reporter.handle_output,
                    show_cli_log=cli_log_enabled,
                )
            finally:
                reporter.finish()
            return _emit_results(args.command, results)

        if args.command == "logs":
            if args.follow:
                return _emit_results(
                    args.command,
                    launcher.logs(
                        ranks=ranks,
                        log_dir=args.log_dir,
                        state_dir=args.state_dir,
                        tail=args.tail,
                        max_workers=args.max_workers,
                        follow=True,
                        output_callback=_emit_stream,
                        show_cli_log=cli_log_enabled,
                    )
                )
            return _emit_results(
                args.command,
                launcher.logs(
                    ranks=ranks,
                    log_dir=args.log_dir,
                    state_dir=args.state_dir,
                    tail=args.tail,
                    max_workers=args.max_workers,
                    show_cli_log=cli_log_enabled,
                )
            )
    except Exception as e:
        _log_cli_error(args, e)
        return 1
    finally:
        logger.disabled = previous_logger_disabled
    return 1


def _log_cli_error(args, e: Exception):
    logger.error(
        f"DLRover workspace CLI failed,\n"
        f"  command={getattr(args, 'command', '')} \n"
        f"  job_name={getattr(args, 'job_name', None)}\n"
        f"  namespace={getattr(args, 'namespace', None)}\n"
        f"  container={getattr(args, 'container', '')}\n"
        f"  ranks={getattr(args, 'ranks', None)}\n"
        f"  log_dir={getattr(args, 'log_dir', None)}\n"
        f"  max_workers={getattr(args, 'max_workers', None)}.",
        exc_info=e,
    )


if __name__ == "__main__":
    sys.exit(main())
