# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest import mock

from dlrover.python.common.constants import ElasticJobLabel, NodeType
from dlrover.python.workspace import cli
from dlrover.python.workspace.launcher import (
    ExecResult,
    WorkspaceLauncher,
    WorkspacePod,
)
from dlrover.python.workspace.network import (
    NETWORK_MODE_NONE,
    NETWORK_MODE_RANKS,
    WorkspaceLaunchCommandBuilder,
    WorkspaceNetworkConfig,
    WorkspaceNetworkPlanner,
)


class _Meta:
    def __init__(self, name, namespace, labels):
        self.name = name
        self.namespace = namespace
        self.labels = labels


class _Status:
    def __init__(self, phase="Running", pod_ip="10.0.0.1"):
        self.phase = phase
        self.pod_ip = pod_ip


class _Spec:
    def __init__(self, node_name="node-0"):
        self.node_name = node_name


class _Pod:
    def __init__(self, name, rank, phase="Running", exit_policy="retain"):
        labels = {
            ElasticJobLabel.JOB_KEY: "job",
            ElasticJobLabel.REPLICA_TYPE_KEY: NodeType.WORKER,
            ElasticJobLabel.RANK_INDEX_KEY: str(rank),
            "dlrover.io/run-mode": "workspace",
            "dlrover.io/workspace-exit-policy": exit_policy,
        }
        self.metadata = _Meta(name, "default", labels)
        self.status = _Status(phase=phase)
        self.spec = _Spec()


class _PodList:
    def __init__(self, items):
        self.items = items


class WorkspaceLauncherTest(unittest.TestCase):
    @mock.patch("dlrover.python.workspace.launcher.client.CoreV1Api")
    @mock.patch("dlrover.python.workspace.launcher.load_kube_config")
    def test_list_worker_pods_sorted_by_rank(self, *_):
        launcher = WorkspaceLauncher(job_name="job", namespace="default")
        launcher._core.list_namespaced_pod.return_value = _PodList(
            [_Pod("worker-2", 2), _Pod("worker-0", 0)]
        )

        pods = launcher.list_worker_pods()

        self.assertEqual([pod.rank for pod in pods], [0, 2])
        launcher._core.list_namespaced_pod.assert_called_once()

    @mock.patch("dlrover.python.workspace.launcher.client.CoreV1Api")
    @mock.patch("dlrover.python.workspace.launcher.load_kube_config")
    def test_launch_requires_running_workers(self, *_):
        launcher = WorkspaceLauncher(job_name="job", namespace="default")
        launcher._core.list_namespaced_pod.return_value = _PodList(
            [_Pod("worker-0", 0), _Pod("worker-1", 1, phase="Pending")]
        )

        with self.assertRaisesRegex(RuntimeError, "not all running"):
            launcher.launch("echo hi")

    @mock.patch("dlrover.python.workspace.launcher.client.CoreV1Api")
    @mock.patch("dlrover.python.workspace.launcher.load_kube_config")
    def test_release_launch_requires_all_expected_workers(self, *_):
        launcher = WorkspaceLauncher(job_name="job", namespace="default")
        launcher._core.list_namespaced_pod.return_value = _PodList(
            [_Pod("worker-0", 0, exit_policy="release")]
        )
        with mock.patch.dict(
            "os.environ",
            {
                "DLROVER_WORKSPACE_EXIT_POLICY": "release",
                "DLROVER_WORKSPACE_READY_TIMEOUT_SECONDS": "0",
                "NODE_NUM": "2",
            },
        ):
            with self.assertRaisesRegex(RuntimeError, "expected=2"):
                launcher.launch("echo hi")

    @mock.patch("dlrover.python.workspace.launcher.time.sleep")
    @mock.patch("dlrover.python.workspace.launcher.client.CoreV1Api")
    @mock.patch("dlrover.python.workspace.launcher.load_kube_config")
    def test_release_waits_for_all_workers_before_network_plan(
        self, _, __, sleep
    ):
        launcher = WorkspaceLauncher(job_name="job", namespace="default")
        launcher._core.list_namespaced_pod.side_effect = [
            _PodList([_Pod("worker-0", 0, exit_policy="release")]),
            _PodList(
                [
                    _Pod("worker-0", 0, exit_policy="release"),
                    _Pod("worker-1", 1, exit_policy="release"),
                ]
            ),
        ]
        launcher._exec_on_pods = mock.Mock(
            side_effect=lambda pods, *_args, **_kwargs: [
                ExecResult(pod=pod, ok=True, output="  state: IDLE\n")
                for pod in pods
            ]
        )

        with mock.patch.dict(
            "os.environ",
            {
                "DLROVER_WORKSPACE_EXIT_POLICY": "release",
                "DLROVER_WORKSPACE_DEBUG_MODE": "true",
                "DLROVER_WORKSPACE_DEBUG_READY_FILE": "/tmp/debug-ready",
                "NODE_NUM": "2",
            },
        ):
            pods = launcher.wait_for_running_pods()
            plan = WorkspaceNetworkPlanner(launcher).plan(
                "all", pods=pods
            )

        self.assertEqual([pod.rank for pod in pods], [0, 1])
        self.assertEqual(plan.config.node_num, 2)
        sleep.assert_called_once_with(2)
        self.assertIn(
            "test -f /tmp/debug-ready || exit 1",
            launcher._exec_on_pods.call_args.args[1],
        )

    @mock.patch("dlrover.python.workspace.launcher.stream")
    @mock.patch("dlrover.python.workspace.launcher.client.CoreV1Api")
    @mock.patch("dlrover.python.workspace.launcher.load_kube_config")
    def test_launch_execs_on_workers(self, _, core_api, stream):
        core = core_api.return_value
        core.list_namespaced_pod.return_value = _PodList([_Pod("worker-0", 0)])
        stream.return_value = "started pid=123"
        launcher = WorkspaceLauncher(job_name="job", namespace="default")

        results = launcher.launch("echo hi")

        self.assertTrue(results[0].ok)
        self.assertIn("started", results[0].output)
        self.assertNotIn("[workspace]", results[0].output)
        stream.assert_called_once()
        kwargs = stream.call_args.kwargs
        self.assertEqual(kwargs["container"], "main")
        self.assertEqual(kwargs["command"][:2], ["/bin/bash", "-lc"])
        self.assertIn("dlrover.python.workspace.agentctl", kwargs["command"][2])
        self.assertIn("launch", kwargs["command"][2])
        self.assertIn("--cmd-b64", kwargs["command"][2])
        self.assertIn("--launch-id", kwargs["command"][2])
        self.assertIn("--wait-for-workers-seconds 5", kwargs["command"][2])
        self.assertIn("--cli-log off", kwargs["command"][2])

    @mock.patch("dlrover.python.workspace.launcher.load_kube_config")
    def test_launch_passes_state_dir(self, _):
        launcher = WorkspaceLauncher(job_name="job", namespace="default")
        pods = [mock.Mock(), mock.Mock()]
        with mock.patch.object(launcher, "wait_for_running_pods", return_value=pods) as ready, \
             mock.patch.object(launcher, "_exec_on_pods", return_value=[]) as execute:
            launcher.launch("echo hi", log_dir="/logs", state_dir="/state custom")
        ready.assert_called_once_with(None, log_dir="/logs", state_dir="/state custom")
        self.assertEqual(execute.call_args.args[0], pods)
        shell = execute.call_args.args[1]
        self.assertIn("--state-dir '/state custom'", shell)

    @mock.patch("dlrover.python.workspace.launcher.stream")
    @mock.patch("dlrover.python.workspace.launcher.client.CoreV1Api")
    @mock.patch("dlrover.python.workspace.launcher.load_kube_config")
    def test_launch_can_show_agentctl_cli_log(self, _, core_api, stream):
        core = core_api.return_value
        core.list_namespaced_pod.return_value = _PodList([_Pod("worker-0", 0)])
        stream.return_value = "started pid=123"
        launcher = WorkspaceLauncher(job_name="job", namespace="default")

        results = launcher.launch("echo hi", show_cli_log=True)

        self.assertIn("[workspace]", results[0].output)
        self.assertIn("pod: worker-0", results[0].output)
        kwargs = stream.call_args.kwargs
        self.assertIn("--cli-log on", kwargs["command"][2])

    @mock.patch("dlrover.python.workspace.launcher.stream")
    @mock.patch("dlrover.python.workspace.launcher.client.CoreV1Api")
    @mock.patch("dlrover.python.workspace.launcher.load_kube_config")
    def test_exec_error_has_workspace_context(self, _, core_api, stream):
        core = core_api.return_value
        core.list_namespaced_pod.return_value = _PodList([_Pod("worker-0", 0)])
        stream.side_effect = RuntimeError("exec denied")
        launcher = WorkspaceLauncher(job_name="job", namespace="default")

        results = launcher.launch("echo hi")

        self.assertFalse(results[0].ok)
        self.assertIn("workspace exec failed", results[0].output)
        self.assertIn("pod: worker-0", results[0].output)
        self.assertIn("container: main", results[0].output)
        self.assertIn("error_type: RuntimeError", results[0].output)
        self.assertIn("--cmd-b64 '<redacted>'", results[0].output)

    @mock.patch("dlrover.python.workspace.launcher.client.CoreV1Api")
    @mock.patch("dlrover.python.workspace.launcher.load_kube_config")
    def test_status_hides_workspace_snapshot_by_default(self, *_):
        launcher = WorkspaceLauncher(job_name="job", namespace="default")
        launcher._core.list_namespaced_pod.return_value = _PodList(
            [_Pod("worker-0", 0)]
        )
        launcher._exec_on_pods = mock.Mock(
            return_value=[
                ExecResult(
                    pod=WorkspacePod(
                        name="worker-0",
                        namespace="default",
                        rank=0,
                        phase="Running",
                        pod_ip="10.0.0.1",
                        node_name="node-0",
                    ),
                    ok=True,
                    output=(
                        "  launch_id: abc\n"
                        "  process_group: 123\n"
                        "  dlrover-run:\n"
                        "    process:\n"
                        "      pid: 1\n"
                        "      ppid: 0\n"
                        "      pgid: 123\n"
                        "      stat: S\n"
                        "      cmd: dlrover-run\n"
                        "  training:\n"
                        "    worker[0]:\n"
                        "      process:\n"
                        "      pid: 2\n"
                    ),
                )
            ]
        )

        results = launcher.status()

        self.assertTrue(results[0].ok)
        self.assertNotIn("[workspace]", results[0].output)
        self.assertNotIn("pod: worker-0", results[0].output)
        self.assertIn("launch_id:", results[0].output)
        self.assertIn("process_group:", results[0].output)
        self.assertIn("dlrover-run:", results[0].output)
        self.assertIn("training:", results[0].output)

    @mock.patch("dlrover.python.workspace.launcher.client.CoreV1Api")
    @mock.patch("dlrover.python.workspace.launcher.load_kube_config")
    def test_status_shows_workspace_snapshot_with_cli_log(self, *_):
        launcher = WorkspaceLauncher(job_name="job", namespace="default")
        launcher._core.list_namespaced_pod.return_value = _PodList(
            [_Pod("worker-0", 0)]
        )
        launcher._exec_on_pods = mock.Mock(
            return_value=[
                ExecResult(
                    pod=WorkspacePod(
                        name="worker-0",
                        namespace="default",
                        rank=0,
                        phase="Running",
                        pod_ip="10.0.0.1",
                        node_name="node-0",
                    ),
                    ok=True,
                    output="  launch_id: abc\n",
                )
            ]
        )

        results = launcher.status(show_cli_log=True)

        self.assertTrue(results[0].ok)
        self.assertIn("[workspace]", results[0].output)
        self.assertIn("rank: 0", results[0].output)
        self.assertIn("pod: worker-0", results[0].output)
        self.assertIn("namespace: default", results[0].output)
        self.assertIn("phase: Running", results[0].output)
        self.assertIn("ip: 10.0.0.1", results[0].output)
        self.assertIn("node: node-0", results[0].output)

    @mock.patch("dlrover.python.workspace.launcher.client.CoreV1Api")
    @mock.patch("dlrover.python.workspace.launcher.load_kube_config")
    def test_launch_requires_all_selected_ranks_found(self, *_):
        launcher = WorkspaceLauncher(job_name="job", namespace="default")
        launcher._core.list_namespaced_pod.return_value = _PodList(
            [_Pod("worker-0", 0)]
        )

        with self.assertRaisesRegex(RuntimeError, "ranks: 1"):
            launcher.launch("echo hi", ranks=[0, 1])

    @mock.patch("dlrover.python.workspace.launcher.stream")
    @mock.patch("dlrover.python.workspace.launcher.client.CoreV1Api")
    @mock.patch("dlrover.python.workspace.launcher.load_kube_config")
    def test_stop_kills_process_group(self, _, core_api, stream):
        core = core_api.return_value
        core.list_namespaced_pod.return_value = _PodList([_Pod("worker-0", 0)])
        stream.return_value = "stopped pgid=123 pid=123"
        launcher = WorkspaceLauncher(job_name="job", namespace="default")

        results = launcher.stop()

        self.assertTrue(results[0].ok)
        self.assertNotIn("[workspace]", results[0].output)
        kwargs = stream.call_args.kwargs
        self.assertEqual(kwargs["container"], "main")
        self.assertIn("dlrover.python.workspace.agentctl", kwargs["command"][2])
        self.assertIn("stop", kwargs["command"][2])
        self.assertIn("--grace-period 300", kwargs["command"][2])
        self.assertIn("--kill-timeout 30", kwargs["command"][2])

    @mock.patch("dlrover.python.workspace.launcher.stream")
    @mock.patch("dlrover.python.workspace.launcher.client.CoreV1Api")
    @mock.patch("dlrover.python.workspace.launcher.load_kube_config")
    def test_stop_shows_workspace_snapshot_with_cli_log(self, _, core_api, stream):
        core = core_api.return_value
        core.list_namespaced_pod.return_value = _PodList([_Pod("worker-0", 0)])
        stream.return_value = "stopped pgid=123 pid=123"
        launcher = WorkspaceLauncher(job_name="job", namespace="default")

        results = launcher.stop(show_cli_log=True)

        self.assertTrue(results[0].ok)
        self.assertIn("[workspace]", results[0].output)
        self.assertIn("pod: worker-0", results[0].output)

    @mock.patch("dlrover.python.workspace.launcher.stream")
    @mock.patch("dlrover.python.workspace.launcher.client.CoreV1Api")
    @mock.patch("dlrover.python.workspace.launcher.load_kube_config")
    def test_stop_streams_progress(self, _, core_api, stream):
        class _Resp:
            def __init__(self):
                self._closed = False
                self._stdout = [
                    "[2026-05-27 10:00:00] [1/7] loaded stop metadata pid=1 pgid=1 launch_id=x\n",
                    "[2026-05-27 10:00:00] [2/7] resolved process group pgid=1\n",
                ]

            def is_open(self):
                return not self._closed

            def update(self, timeout=1):
                if not self._stdout:
                    self._closed = True

            def peek_stdout(self):
                return bool(self._stdout)

            def read_stdout(self):
                return self._stdout.pop(0)

            def peek_stderr(self):
                return False

            def close(self):
                self._closed = True

        core = core_api.return_value
        core.list_namespaced_pod.return_value = _PodList([_Pod("worker-0", 0)])
        stream.return_value = _Resp()
        launcher = WorkspaceLauncher(job_name="job", namespace="default")
        outputs = []

        results = launcher.stop(output_callback=lambda pod, output: outputs.append(output))

        self.assertTrue(results[0].ok)
        self.assertGreaterEqual(len(outputs), 1)
        kwargs = stream.call_args.kwargs
        self.assertFalse(kwargs["_preload_content"])

    @mock.patch("dlrover.python.workspace.launcher.stream")
    @mock.patch("dlrover.python.workspace.launcher.client.CoreV1Api")
    @mock.patch("dlrover.python.workspace.launcher.load_kube_config")
    def test_follow_logs_uses_streaming_exec(self, _, core_api, stream):
        class _Resp:
            def __init__(self):
                self._closed = False

            def is_open(self):
                return not self._closed

            def update(self, timeout=1):
                self._closed = True

            def peek_stdout(self):
                return True

            def read_stdout(self):
                return "line-1\n"

            def peek_stderr(self):
                return False

            def close(self):
                self._closed = True

        core = core_api.return_value
        core.list_namespaced_pod.return_value = _PodList([_Pod("worker-0", 0)])
        stream.return_value = _Resp()
        launcher = WorkspaceLauncher(job_name="job", namespace="default")
        outputs = []

        results = launcher.logs(
            follow=True,
            output_callback=lambda pod, output: outputs.append((pod.rank, output)),
        )

        self.assertTrue(results[0].ok)
        self.assertEqual(outputs, [(0, "line-1\n")])
        kwargs = stream.call_args.kwargs
        self.assertFalse(kwargs["_preload_content"])
        self.assertTrue(kwargs["stdout"])
        self.assertIn("dlrover.python.workspace.agentctl", kwargs["command"][2])
        self.assertIn("logs", kwargs["command"][2])
        self.assertIn("--follow", kwargs["command"][2])

    @mock.patch("dlrover.python.workspace.launcher.stream")
    @mock.patch("dlrover.python.workspace.launcher.client.CoreV1Api")
    @mock.patch("dlrover.python.workspace.launcher.load_kube_config")
    def test_logs_shows_workspace_snapshot_with_cli_log(self, _, core_api, stream):
        core = core_api.return_value
        core.list_namespaced_pod.return_value = _PodList([_Pod("worker-0", 0)])
        stream.return_value = "line-1\n"
        launcher = WorkspaceLauncher(job_name="job", namespace="default")

        results = launcher.logs(show_cli_log=True)

        self.assertTrue(results[0].ok)
        self.assertIn("[workspace]", results[0].output)
        self.assertIn("pod: worker-0", results[0].output)


class WorkspaceCliTest(unittest.TestCase):
    @mock.patch("dlrover.python.workspace.cli.WorkspaceLauncher")
    def test_custom_state_dir_reaches_all_commands(self, launcher_cls):
        launcher = launcher_cls.return_value
        launcher.list_worker_pods.return_value = []
        for command in ("launch", "status", "stop", "logs"):
            with self.subTest(command=command):
                method = getattr(launcher, command)
                method.return_value = []
                argv = [command, "--job-name", "job", "--state-dir", "/state"]
                if command == "launch":
                    argv += ["--cmd", "echo hi", "--log-dir", "/logs"]
                self.assertEqual(cli.main(argv), 0)
                self.assertEqual(method.call_args.kwargs["state_dir"], "/state")
                if command == "logs":
                    self.assertEqual(
                        method.call_args.kwargs["log_dir"],
                        "/tmp/dlrover-workspace",
                    )

    @mock.patch("dlrover.python.workspace.cli.WorkspaceLauncher")
    def test_cli_launch(self, launcher_cls):
        launcher = launcher_cls.return_value
        launcher.launch.return_value = []

        code = cli.main(["launch", "--job-name", "job", "--cmd", "echo hi"])

        self.assertEqual(code, 0)
        launcher.launch.assert_called_once()
        self.assertEqual(launcher.launch.call_args.kwargs["command"], "echo hi")
        self.assertFalse(launcher.launch.call_args.kwargs["show_cli_log"])

    @mock.patch("dlrover.python.workspace.cli.WorkspaceLauncher")
    def test_cli_launch_formats_result_status(self, launcher_cls):
        launcher = launcher_cls.return_value
        pod = WorkspacePod("worker-0", "default", 0, "Running")
        launcher.launch.return_value = [
            ExecResult(pod=pod, ok=True, output="started\n"),
            ExecResult(pod=pod, ok=False, output="failed\n"),
        ]

        output = StringIO()
        with redirect_stdout(output):
            code = cli.main(["launch", "--job-name", "job", "--cmd", "echo hi"])

        self.assertEqual(code, 1)
        self.assertIn(
            "===== rank 0 worker-0 [LAUNCH SUCCESS] =====",
            output.getvalue(),
        )
        self.assertIn(
            "===== rank 0 worker-0 [LAUNCH FAILED] =====",
            output.getvalue(),
        )

    @mock.patch("dlrover.python.workspace.cli.WorkspaceLauncher")
    def test_cli_launch_can_show_cli_log(self, launcher_cls):
        launcher = launcher_cls.return_value
        launcher.launch.return_value = []

        code = cli.main(
            [
                "launch",
                "--job-name",
                "job",
                "--cmd",
                "echo hi",
                "--cli-log",
                "on",
            ]
        )

        self.assertEqual(code, 0)
        launcher.launch.assert_called_once()
        self.assertTrue(launcher.launch.call_args.kwargs["show_cli_log"])

    @mock.patch("dlrover.python.workspace.cli.WorkspaceLauncher")
    def test_cli_launch_does_not_network_framework_cmd_by_default(
        self, launcher_cls
    ):
        launcher = launcher_cls.return_value
        launcher.launch.return_value = []

        code = cli.main(
            [
                "launch",
                "--job-name",
                "job",
                "--framework-cmd",
                "swift sft --model /nas/model",
            ]
        )

        self.assertEqual(code, 0)
        launcher.launch.assert_called_once()
        self.assertEqual(
            launcher.launch.call_args.kwargs["command"],
            "swift sft --model /nas/model",
        )

    @mock.patch("dlrover.python.workspace.cli.WorkspaceLauncher")
    def test_cli_launch_networks_all_workers(
        self, launcher_cls
    ):
        launcher = launcher_cls.return_value
        launcher.launch.return_value = []
        launcher.list_worker_pods.return_value = [
            WorkspacePod("worker-0", "default", 0, "Running"),
            WorkspacePod("worker-1", "default", 1, "Running"),
        ]

        code = cli.main(
            [
                "launch",
                "--job-name",
                "job",
                "--framework-cmd",
                "swift sft --model /nas/model",
                "--network-mode",
                "all",
                "--dlrover-run-args",
                "--nnodes=$NODE_NUM --nproc_per_node=2 --max_restarts=1",
            ]
        )

        self.assertEqual(code, 0)
        launcher.list_worker_pods.assert_called_once_with(ranks=None)
        launcher.launch.assert_called_once()
        self.assertEqual(
            launcher.launch.call_args.kwargs["command"],
            "export NODE_NUM=2; "
            "dlrover-run --nnodes=$NODE_NUM --nproc_per_node=2 --max_restarts=1 "
            "swift sft --model /nas/model",
        )

    @mock.patch("dlrover.python.workspace.cli.WorkspaceLauncher")
    def test_cli_launch_networks_selected_workers_with_contiguous_ranks(
        self, launcher_cls
    ):
        launcher = launcher_cls.return_value
        launcher.launch.return_value = []
        launcher.list_worker_pods.return_value = [
            WorkspacePod("worker-2", "default", 2, "Running"),
            WorkspacePod("worker-5", "default", 5, "Running"),
        ]

        code = cli.main(
            [
                "launch",
                "--job-name",
                "job",
                "--ranks",
                "2,5",
                "--network-mode",
                "ranks",
                "--framework-cmd",
                "train.py",
            ]
        )

        self.assertEqual(code, 0)
        launcher.list_worker_pods.assert_called_once_with(ranks=[2, 5])
        launcher.launch.assert_called_once()
        self.assertEqual(
            launcher.launch.call_args.kwargs["command"],
            "export NODE_NUM=2; "
            'case "${NODE_RANK:-}" in '
            "2) export NODE_RANK=0 ;; "
            "5) export NODE_RANK=1 ;; "
            '*) echo "NODE_RANK ${NODE_RANK:-} is not selected for '
            'workspace networking"; exit 1 ;; esac; '
            "dlrover-run --nnodes=$NODE_NUM --nproc_per_node=1 train.py",
        )
        self.assertEqual(launcher.launch.call_args.kwargs["ranks"], [2, 5])

    @mock.patch("dlrover.python.workspace.cli.WorkspaceLauncher")
    def test_cli_launch_network_ranks_rejects_missing_rank(self, launcher_cls):
        launcher = launcher_cls.return_value
        launcher.launch.return_value = []
        launcher.list_worker_pods.return_value = [
            WorkspacePod("worker-2", "default", 2, "Running"),
        ]

        code = cli.main(
            [
                "launch",
                "--job-name",
                "job",
                "--ranks",
                "2,5",
                "--network-mode",
                "ranks",
                "--framework-cmd",
                "train.py",
            ]
        )

        self.assertEqual(code, 1)
        launcher.launch.assert_not_called()

    @mock.patch("dlrover.python.workspace.cli.WorkspaceLauncher")
    def test_cli_rejects_duplicated_ranks(self, launcher_cls):
        code = cli.main(
            [
                "launch",
                "--job-name",
                "job",
                "--ranks",
                "2,2",
                "--network-mode",
                "ranks",
                "--framework-cmd",
                "train.py",
            ]
        )

        self.assertEqual(code, 1)
        launcher_cls.assert_not_called()

    @mock.patch("dlrover.python.workspace.cli.WorkspaceLauncher")
    def test_cli_launch_network_ranks_requires_ranks(self, launcher_cls):
        code = cli.main(
            [
                "launch",
                "--job-name",
                "job",
                "--network-mode",
                "ranks",
                "--framework-cmd",
                "train.py",
            ]
        )

        self.assertEqual(code, 1)
        launcher_cls.return_value.launch.assert_not_called()

    @mock.patch("dlrover.python.workspace.cli.WorkspaceLauncher")
    def test_cli_launch_network_all_rejects_ranks(self, launcher_cls):
        code = cli.main(
            [
                "launch",
                "--job-name",
                "job",
                "--ranks",
                "0",
                "--network-mode",
                "all",
                "--framework-cmd",
                "train.py",
            ]
        )

        self.assertEqual(code, 1)
        launcher_cls.return_value.launch.assert_not_called()

    @mock.patch("dlrover.python.workspace.cli.WorkspaceLauncher")
    def test_cli_release_requires_full_framework_launch(self, launcher_cls):
        with mock.patch.dict(
            "os.environ",
            {"DLROVER_WORKSPACE_EXIT_POLICY": "release"},
        ):
            code = cli.main(
                ["launch", "--job-name", "job", "--cmd", "echo hi"]
            )
        self.assertEqual(code, 1)
        launcher_cls.return_value.launch.assert_not_called()

        launcher = launcher_cls.return_value
        ready_pods = [
            WorkspacePod("worker-0", "default", 0, "Running")
        ]
        launcher.wait_for_running_pods.return_value = ready_pods
        launcher.list_worker_pods.return_value = ready_pods
        launcher.launch.return_value = []
        with mock.patch.dict(
            "os.environ",
            {"DLROVER_WORKSPACE_EXIT_POLICY": "release"},
        ):
            code = cli.main(
                [
                    "launch",
                    "--job-name",
                    "job",
                    "--network-mode",
                    "all",
                    "--framework-cmd",
                    "train.py",
                ]
            )
        self.assertEqual(code, 0)
        launcher.wait_for_running_pods.assert_called_once_with(
            log_dir="/tmp/dlrover-workspace",
            state_dir="/tmp/dlrover-workspace-state",
        )
        launcher.launch.assert_called_once()

    @mock.patch("dlrover.python.workspace.cli.WorkspaceLauncher")
    def test_cli_logs_follow(self, launcher_cls):
        launcher = launcher_cls.return_value
        launcher.logs.return_value = []

        code = cli.main(["logs", "--job-name", "job", "--follow"])

        self.assertEqual(code, 0)
        launcher.logs.assert_called_once()
        self.assertTrue(launcher.logs.call_args.kwargs["follow"])

    @mock.patch("dlrover.python.workspace.cli.WorkspaceLauncher")
    def test_cli_stop_grace_period(self, launcher_cls):
        launcher = launcher_cls.return_value
        launcher.stop.return_value = []

        code = cli.main(
            [
                "stop",
                "--job-name",
                "job",
                "--grace-period",
                "120",
                "--kill-timeout",
                "10",
            ]
        )

        self.assertEqual(code, 0)
        launcher.stop.assert_called_once()
        self.assertEqual(launcher.stop.call_args.kwargs["grace_period"], 120)
        self.assertEqual(launcher.stop.call_args.kwargs["kill_timeout"], 10)
        self.assertIsNotNone(launcher.stop.call_args.kwargs["output_callback"])

    @mock.patch("dlrover.python.workspace.cli._StopProgressReporter")
    @mock.patch("dlrover.python.workspace.cli.WorkspaceLauncher")
    def test_cli_stop_uses_progress_reporter(self, launcher_cls, reporter_cls):
        launcher = launcher_cls.return_value
        launcher.list_worker_pods.return_value = []
        launcher.stop.return_value = []

        code = cli.main(["stop", "--job-name", "job"])

        self.assertEqual(code, 0)
        reporter_cls.assert_called_once()
        launcher.stop.assert_called_once()
        self.assertIsNotNone(launcher.stop.call_args.kwargs["output_callback"])

    @mock.patch("dlrover.python.workspace.cli.logger")
    @mock.patch("dlrover.python.workspace.cli.WorkspaceLauncher")
    def test_cli_logs_context_on_error(self, launcher_cls, cli_logger):
        launcher_cls.side_effect = RuntimeError("missing kube config")

        code = cli.main(["status", "--job-name", "job", "--cli-log", "on"])

        self.assertEqual(code, 1)
        cli_logger.error.assert_called_once()
        self.assertIn("DLRover workspace CLI failed", cli_logger.error.call_args[0][0])


class WorkspaceNetworkTest(unittest.TestCase):
    def test_planner_keeps_none_mode_unmanaged(self):
        launcher = mock.Mock()

        plan = WorkspaceNetworkPlanner(launcher).plan(
            NETWORK_MODE_NONE, ranks=[1, 3]
        )

        self.assertEqual(plan.ranks, [1, 3])
        self.assertIsNone(plan.config)
        launcher.list_worker_pods.assert_not_called()

    def test_planner_remaps_selected_ranks_to_contiguous_workspace_ranks(self):
        launcher = mock.Mock()
        launcher.list_worker_pods.return_value = [
            WorkspacePod("worker-2", "default", 2, "Running"),
            WorkspacePod("worker-5", "default", 5, "Running"),
        ]

        plan = WorkspaceNetworkPlanner(launcher).plan(
            NETWORK_MODE_RANKS, ranks=[2, 5]
        )

        self.assertEqual(plan.ranks, [2, 5])
        self.assertEqual(plan.config.node_num, 2)
        self.assertEqual(plan.config.rank_map, {2: 0, 5: 1})
        launcher.list_worker_pods.assert_called_once_with(ranks=[2, 5])

    def test_command_builder_does_not_wrap_raw_cmd(self):
        command = WorkspaceLaunchCommandBuilder().build(
            cmd="echo hi",
            framework_cmd=None,
            dlrover_run_args="--nnodes=$NODE_NUM",
            network_config=WorkspaceNetworkConfig(node_num=2),
        )

        self.assertEqual(command, "export NODE_NUM=2; echo hi")


if __name__ == "__main__":
    unittest.main()
