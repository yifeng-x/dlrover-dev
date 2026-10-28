# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import base64
import os
import shlex
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional

from dlrover.brain.python.common.log import default_logger as logger
from dlrover.python.workspace.constants import (
    DEFAULT_LOG_DIR,
    DEFAULT_STATE_DIR,
)
from dlrover.python.common.constants import ElasticJobLabel, NodeType

try:
    from kubernetes import client, config
    from kubernetes.stream import stream
except ImportError as e:
    logger.warning("Failed to import kubernetes package: %s", e)

    class _MissingKubernetesClient:
        class CoreV1Api:
            def __init__(self):
                raise RuntimeError("kubernetes package is required")

    class _MissingKubernetesConfig:
        def load_incluster_config(self):
            raise RuntimeError("kubernetes package is required")

        def load_kube_config(self):
            raise RuntimeError("kubernetes package is required")

    def stream(*args, **kwargs):
        raise RuntimeError("kubernetes package is required")

    client = _MissingKubernetesClient()
    config = _MissingKubernetesConfig()

DEFAULT_CONTAINER = "main"
DEFAULT_PID_FILE = f"{DEFAULT_STATE_DIR}/train.pid"
DEFAULT_PGID_FILE = f"{DEFAULT_STATE_DIR}/train.pgid"
DEFAULT_LAUNCH_ID_FILE = f"{DEFAULT_STATE_DIR}/train.launch_id"
DEFAULT_LOG_FILE = f"{DEFAULT_LOG_DIR}/train.log"
DEFAULT_STOP_GRACE_PERIOD = 300
DEFAULT_STOP_KILL_TIMEOUT = 30


@dataclass
class WorkspacePod:
    name: str
    namespace: str
    rank: int
    phase: str
    pod_ip: str = ""
    node_name: str = ""
    run_mode: str = ""
    exit_policy: str = "retain"


@dataclass
class ExecResult:
    pod: WorkspacePod
    ok: bool
    output: str


def load_kube_config():
    try:
        config.load_incluster_config()
    except Exception as e:
        logger.warning(f"Failed to load Kubernetes config, use kube_config, error: {e}")
        config.load_kube_config()


class WorkspaceLauncher:
    def __init__(
        self,
        job_name: Optional[str] = None,
        namespace: Optional[str] = None,
        container: str = DEFAULT_CONTAINER,
    ):
        self.job_name = job_name or os.getenv("ELASTIC_JOB_NAME")
        self.namespace = namespace or os.getenv("NAMESPACE") or "default"
        self.container = container
        if not self.job_name:
            raise ValueError(
                "job name is required. Use --job-name or set ELASTIC_JOB_NAME."
            )
        load_kube_config()
        self._core = client.CoreV1Api()


    def list_worker_pods(self, ranks: Optional[Iterable[int]] = None):
        label_selector = ",".join(
            [
                f"{ElasticJobLabel.JOB_KEY}={self.job_name}",
                f"{ElasticJobLabel.REPLICA_TYPE_KEY}={NodeType.WORKER}",
            ]
        )

        pod_list = None
        try:
            pod_list = self._core.list_namespaced_pod(
                namespace=self.namespace, label_selector=label_selector
            )
        except Exception as e:
            logger.error(
                "Failed to list workspace worker pods "
                "job_name=%s namespace=%s label_selector=%s ranks=%s.",
                self.job_name,
                self.namespace,
                label_selector,
                list(ranks) if ranks is not None else None,
                exc_info=e,
            )
            return []

        if pod_list is None:
            logger.warning(
                "No worker pods found for workspace "
                "job_name=%s namespace=%s label_selector=%s ranks=%s.",
                self.job_name,
                self.namespace,
                label_selector,
                list(ranks) if ranks is not None else None,
            )
            return []

        rank_set = set(ranks) if ranks is not None else None
        pods: List[WorkspacePod] = []
        for item in pod_list.items:
            labels = item.metadata.labels or {}
            rank = int(labels.get(ElasticJobLabel.RANK_INDEX_KEY, "-1"))
            if rank < 0:
                continue
            if rank_set is not None and rank not in rank_set:
                continue
            pods.append(
                WorkspacePod(
                    name=item.metadata.name,
                    namespace=item.metadata.namespace,
                    rank=rank,
                    phase=item.status.phase,
                    pod_ip=item.status.pod_ip or "",
                    node_name=item.spec.node_name or "",
                    run_mode=labels.get("dlrover.io/run-mode", ""),
                    exit_policy=labels.get(
                        "dlrover.io/workspace-exit-policy", "retain"
                    ),
                )
            )
        return sorted(pods, key=lambda pod: pod.rank)

    def status(
        self,
        ranks: Optional[Iterable[int]] = None,
        log_dir: str = DEFAULT_LOG_DIR,
        show_cli_log: bool = False,
        state_dir: str = DEFAULT_STATE_DIR,
    ):
        pods = self.list_worker_pods(ranks)
        running_pods = [pod for pod in pods if pod.phase == "Running"]
        pending_pods = [pod for pod in pods if pod.phase != "Running"]
        results: List[ExecResult] = []
        if running_pods:
            shell = self._build_agentctl_shell(
                log_dir,
                "status",
                "--wait-for-workers-seconds",
                "0",
                show_cli_log=show_cli_log,
                state_dir=state_dir,
            )
            results.extend(
                self._maybe_decorate_workspace_results(
                    running_pods,
                    self._exec_on_pods(running_pods, shell),
                    show_cli_log,
                )
            )
        for pod in pending_pods:
            results.append(
                ExecResult(
                    pod=pod,
                    ok=True,
                    output=self._format_non_running_status(pod, show_cli_log),
                )
            )
        # Sort results by pod rank to ensure consistent order, ASC
        return sorted(results, key=lambda item: item.pod.rank)

    def launch(
        self,
        command: str,
        ranks: Optional[Iterable[int]] = None,
        log_dir: str = DEFAULT_LOG_DIR,
        foreground: bool = False,
        max_workers: int = 32,
        show_cli_log: bool = False,
        state_dir: str = DEFAULT_STATE_DIR,
    ):
        pods = self.wait_for_running_pods(
            ranks, log_dir=log_dir, state_dir=state_dir
        )

        launch_id = uuid.uuid4().hex
        export_launch_id = (
            "export DLROVER_WORKSPACE_LAUNCH_ID="
            f"{shlex.quote(launch_id)}; "
        )

        if foreground:
            shell = f"{export_launch_id}{command}"
        else:
            command_b64 = base64.b64encode(command.encode("utf-8")).decode(
                "ascii"
            )
            shell = self._build_agentctl_shell(
                log_dir,
                "launch",
                "--cmd-b64",
                command_b64,
                "--launch-id",
                launch_id,
                "--wait-for-workers-seconds",
                "5",
                show_cli_log=show_cli_log,
                state_dir=state_dir,
            )

        results = self._exec_on_pods(pods, shell, max_workers=max_workers)
        return self._maybe_decorate_workspace_results(
            pods, results, show_cli_log
        )

    def _maybe_decorate_workspace_results(
        self,
        pods: List[WorkspacePod],
        results: List[ExecResult],
        show_cli_log: bool,
    ):
        if show_cli_log:
            return self._decorate_workspace_results(pods, results)
        return sorted(results, key=lambda item: item.pod.rank)

    def _decorate_workspace_results(
        self, pods: List[WorkspacePod], results: List[ExecResult]
    ):
        pod_map = {pod.name: pod for pod in pods}
        decorated = []
        for result in results:
            pod = pod_map.get(result.pod.name, result.pod)
            decorated.append(
                ExecResult(
                    pod=pod,
                    ok=result.ok,
                    output=self._format_workspace_snapshot(
                        pod, result.output
                    ),
                )
            )
        return sorted(decorated, key=lambda item: item.pod.rank)

    def _format_workspace_snapshot(self, pod: WorkspacePod, body: str):
        lines = [
            "[workspace]",
            f"  rank: {pod.rank}",
            f"  pod: {pod.name}",
            f"  namespace: {pod.namespace}",
            f"  phase: {pod.phase}",
            f"  ip: {pod.pod_ip or 'unknown'}",
            f"  node: {pod.node_name or 'unknown'}",
        ]
        if body:
            lines.append(body.rstrip())
        return "\n".join(lines) + "\n"

    def _format_non_running_status(
        self, pod: WorkspacePod, show_cli_log: bool = False
    ):
        body = (
            "  dlrover-run:\n"
            "    unavailable while pod is not running\n"
            "  training:\n"
            "    unavailable while pod is not running\n"
        )
        if show_cli_log:
            return self._format_workspace_snapshot(pod, body)
        return body

    def _build_agentctl_shell(
        self,
        log_dir: str,
        subcommand: str,
        *args: str,
        show_cli_log: bool = False,
        state_dir: str = DEFAULT_STATE_DIR,
    ):
        quoted_args = " ".join(shlex.quote(str(arg)) for arg in args)
        cli_log_value = "on" if show_cli_log else "off"
        command = (
            "python_bin=${DLROVER_WORKSPACE_AGENTCTL_PYTHON:-python}; "
            "exec \"$python_bin\" -m dlrover.python.workspace.agentctl "
            f"--state-dir {shlex.quote(state_dir)} "
            f"--log-dir {shlex.quote(log_dir)} "
            f"--cli-log {cli_log_value} "
            f"{shlex.quote(subcommand)}"
        )
        if quoted_args:
            command = f"{command} {quoted_args}"
        return command

    def stop(
        self,
        ranks: Optional[Iterable[int]] = None,
        log_dir: str = DEFAULT_LOG_DIR,
        max_workers: int = 32,
        grace_period: int = DEFAULT_STOP_GRACE_PERIOD,
        kill_timeout: int = DEFAULT_STOP_KILL_TIMEOUT,
        output_callback: Optional[Callable[[WorkspacePod, str], None]] = None,
        show_cli_log: bool = False,
        state_dir: str = DEFAULT_STATE_DIR,
    ):
        grace_period = max(0, int(grace_period))
        kill_timeout = max(0, int(kill_timeout))

        pods = self.list_worker_pods(ranks)

        shell = self._build_agentctl_shell(
            log_dir,
            "stop",
            "--grace-period",
            str(grace_period),
            "--kill-timeout",
            str(kill_timeout),
            show_cli_log=show_cli_log,
            state_dir=state_dir,
        )

        if output_callback:
            results = self._stream_on_pods(
                pods,
                shell,
                max_workers=max_workers,
                output_callback=output_callback,
            )
        else:
            results = self._exec_on_pods(pods, shell, max_workers=max_workers)
        return self._maybe_decorate_workspace_results(
            pods, results, show_cli_log
        )

    def logs(
        self,
        ranks: Optional[Iterable[int]] = None,
        log_dir: str = DEFAULT_LOG_DIR,
        tail: int = 100,
        max_workers: int = 32,
        follow: bool = False,
        output_callback: Optional[Callable[[WorkspacePod, str], None]] = None,
        show_cli_log: bool = False,
        state_dir: str = DEFAULT_STATE_DIR,
    ):
        if follow:
            return self.follow_logs(
                ranks=ranks,
                log_dir=log_dir,
                tail=tail,
                max_workers=max_workers,
                output_callback=output_callback,
                show_cli_log=show_cli_log,
                state_dir=state_dir,
            )

        pods = self.list_worker_pods(ranks)

        shell = self._build_agentctl_shell(
            log_dir,
            "logs",
            "--tail",
            str(int(tail)),
            show_cli_log=show_cli_log,
            state_dir=state_dir,
        )
        return self._maybe_decorate_workspace_results(
            pods,
            self._exec_on_pods(pods, shell, max_workers=max_workers),
            show_cli_log,
        )

    def follow_logs(
        self,
        ranks: Optional[Iterable[int]] = None,
        log_dir: str = DEFAULT_LOG_DIR,
        tail: int = 100,
        max_workers: int = 32,
        output_callback: Optional[Callable[[WorkspacePod, str], None]] = None,
        show_cli_log: bool = False,
        state_dir: str = DEFAULT_STATE_DIR,
    ):
        pods = self.list_worker_pods(ranks)

        shell = self._build_agentctl_shell(
            log_dir,
            "logs",
            "--tail",
            str(int(tail)),
            "--follow",
            show_cli_log=show_cli_log,
            state_dir=state_dir,
        )
        results = self._stream_on_pods(
            pods,
            shell,
            max_workers=max_workers,
            output_callback=output_callback,
        )
        return self._maybe_decorate_workspace_results(
            pods, results, show_cli_log
        )

    def wait_for_running_pods(
        self,
        ranks: Optional[Iterable[int]] = None,
        log_dir: str = DEFAULT_LOG_DIR,
        state_dir: str = DEFAULT_STATE_DIR,
    ):
        release = os.getenv(
            "DLROVER_WORKSPACE_EXIT_POLICY", "retain"
        ).lower() == "release"
        expected = int(os.getenv("NODE_NUM", "0") or 0) if release else 0
        timeout = int(
            os.getenv("DLROVER_WORKSPACE_READY_TIMEOUT_SECONDS", "300")
        )
        deadline = time.time() + max(0, timeout)
        ready = False
        while True:
            pods = self.list_worker_pods(ranks)
            terminal_pods = [
                pod
                for pod in pods
                if pod.phase in ("Failed", "Succeeded")
            ]
            if release and terminal_pods:
                detail = ", ".join(
                    f"{pod.name}:{pod.phase}" for pod in terminal_pods
                )
                raise RuntimeError(
                    "worker pod exited before workspace launch: " + detail
                )
            ready = pods and all(pod.phase == "Running" for pod in pods)
            if release and expected:
                ready = ready and len(pods) == expected and {
                    pod.rank for pod in pods
                } == set(range(expected))
            if release and ready:
                status_shell = self._build_agentctl_shell(
                    log_dir,
                    "status",
                    show_cli_log=False,
                    state_dir=state_dir,
                )
                debug_mode = os.getenv(
                    "DLROVER_WORKSPACE_DEBUG_MODE", "false"
                ).lower() in ("true", "1", "yes", "y")
                if debug_mode:
                    ready_file = os.getenv(
                        "DLROVER_WORKSPACE_DEBUG_READY_FILE",
                        "/tmp/dlrover-workspace-debug-ready",
                    )
                    status_shell = (
                        f"test -f {shlex.quote(ready_file)} || exit 1; "
                        f"{status_shell}"
                    )
                statuses = self._exec_on_pods(pods, status_shell)
                ready = all(
                    result.ok
                    and any(
                        "state: %s" % state in result.output
                        for state in ("IDLE", "STOPPED")
                    )
                    for result in statuses
                )
            if ready or time.time() >= deadline or not release:
                break
            time.sleep(2)
        if not pods:
            raise RuntimeError("no worker pods found")
        if release and ranks is not None:
            raise ValueError("--ranks is unsupported with exit policy release")
        if release and any(pod.exit_policy != "release" for pod in pods):
            raise RuntimeError("worker pods have inconsistent exit policies")
        if release and expected and len(pods) != expected:
            raise RuntimeError(
                "worker pods are not ready: expected=%s actual=%s"
                % (expected, len(pods))
            )
        if release and not ready:
            raise RuntimeError(
                "worker pods are not ready for launch before timeout"
            )
        if ranks is not None:
            requested = set(ranks)
            found = {pod.rank for pod in pods}
            missing = sorted(requested - found)
            if missing:
                raise RuntimeError(
                    "worker pods not found for ranks: "
                    + ",".join(str(rank) for rank in missing)
                )
        not_running = [pod for pod in pods if pod.phase != "Running"]
        if not_running:
            detail = ", ".join(
                f"{pod.name}:{pod.phase}" for pod in not_running
            )
            raise RuntimeError(f"worker pods are not all running: {detail}")
        return pods

    # Keep the private alias for callers and tests written against the
    # original launcher implementation.
    def _require_running_pods(self, ranks: Optional[Iterable[int]]):
        return self.wait_for_running_pods(ranks)

    def _exec_on_pods(
        self,
        pods: List[WorkspacePod],
        shell: str,
        max_workers: int = 32,
    ):
        results: List[ExecResult] = []
        with ThreadPoolExecutor(max_workers=min(max_workers, len(pods) or 1)) as ex:
            futures = [ex.submit(self._exec, pod, shell) for pod in pods]
            for future in as_completed(futures):
                results.append(future.result())

        return sorted(results, key=lambda item: item.pod.rank)

    def _exec(self, pod: WorkspacePod, shell: str):
        """
            Execute the shell command on the pod and return the output.
            Used to enter Pod containers and execute shell commands (similar to kubectl exec).
        """
        try:
            output = stream(
                self._core.connect_get_namespaced_pod_exec,
                pod.name,
                pod.namespace,
                command=["/bin/bash", "-lc", shell],
                container=self.container,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )
            return ExecResult(pod=pod, ok=True, output=output or "")
        except Exception as e:
            logger.warning(
                "Workspace pod exec failed pod=%s namespace=%s rank=%s "
                "container=%s command=%s.",
                pod.name,
                pod.namespace,
                pod.rank,
                self.container,
                self._redact_shell(shell),
                exc_info=e,
            )
            return ExecResult(
                pod=pod,
                ok=False,
                output=self._format_exec_error(pod, shell, e),
            )

    def _stream_on_pods(
        self,
        pods: List[WorkspacePod],
        shell: str,
        max_workers: int = 32,
        output_callback: Optional[Callable[[WorkspacePod, str], None]] = None,
    ):
        results: List[ExecResult] = []
        stop_event = threading.Event()
        with ThreadPoolExecutor(max_workers=min(max_workers, len(pods) or 1)) as ex:
            futures = [
                ex.submit(
                    self._stream_exec,
                    pod,
                    shell,
                    output_callback,
                    stop_event,
                )
                for pod in pods
            ]
            try:
                for future in as_completed(futures):
                    results.append(future.result())
            except KeyboardInterrupt:
                logger.warning("Workspace streaming exec interrupted.")
                stop_event.set()
                for future in futures:
                    future.cancel()
                raise

        return sorted(results, key=lambda item: item.pod.rank)

    def _stream_exec(
        self,
        pod: WorkspacePod,
        shell: str,
        output_callback: Optional[Callable[[WorkspacePod, str], None]],
        stop_event: threading.Event,
    ):
        resp = None
        try:
            resp = stream(
                self._core.connect_get_namespaced_pod_exec,
                pod.name,
                pod.namespace,
                command=["/bin/bash", "-lc", shell],
                container=self.container,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
                _preload_content=False,
            )
            while not stop_event.is_set() and resp.is_open():
                resp.update(timeout=1)
                if resp.peek_stdout():
                    output = resp.read_stdout()
                    if output_callback:
                        output_callback(pod, output)
                if resp.peek_stderr():
                    output = resp.read_stderr()
                    if output_callback:
                        output_callback(pod, output)
            return ExecResult(pod=pod, ok=True, output="")
        except Exception as e:
            logger.warning(
                "Workspace streaming pod exec failed pod=%s namespace=%s "
                "rank=%s container=%s command=%s.",
                pod.name,
                pod.namespace,
                pod.rank,
                self.container,
                self._redact_shell(shell),
                exc_info=e,
            )
            return ExecResult(
                pod=pod,
                ok=False,
                output=self._format_exec_error(pod, shell, e),
            )
        finally:
            if resp is not None:
                resp.close()

    def _format_exec_error(self, pod: WorkspacePod, shell: str, error: Exception):
        lines = [
            "workspace exec failed:",
            f"  pod: {pod.name}",
            f"  namespace: {pod.namespace}",
            f"  rank: {pod.rank}",
            f"  phase: {pod.phase}",
            f"  container: {self.container}",
            f"  command: {self._redact_shell(shell)}",
            f"  error_type: {type(error).__name__}",
            f"  error: {error}",
        ]
        for attr in ("status", "reason", "body"):
            value = getattr(error, attr, None)
            if value:
                lines.append(f"  {attr}: {value}")
        lines.append("  traceback:")
        lines.extend(
            "    " + line
            for line in traceback.format_exception(
                type(error), error, error.__traceback__
            )
        )
        return "".join(line if line.endswith("\n") else line + "\n" for line in lines)

    def _redact_shell(self, shell: str):
        try:
            parts = shlex.split(shell)
        except Exception as e:
            logger.warning("Failed to split shell command.", exc_info=e)
            return shell

        for index, part in enumerate(parts):
            if part == "--cmd-b64" and index + 1 < len(parts):
                parts[index + 1] = "<redacted>"
        return " ".join(shlex.quote(part) for part in parts)

    def _exec_on_pods_with_streaming_output(
        self,
        pods: List[WorkspacePod],
        shell: str,
        max_workers: int = 32,
        output_callback: Optional[Callable[[WorkspacePod, str], None]] = None,
    ):
        return self._stream_on_pods(
            pods,
            shell,
            max_workers=max_workers,
            output_callback=output_callback,
        )
