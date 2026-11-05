# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import contextlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from dlrover.python.master.node.workspace_task import WorkspaceTaskCoordinator
from dlrover.python.workspace import cli, role_control, role_runtime
from dlrover.python.workspace.frameworks.script import parse_input
from dlrover.python.workspace.launcher import WorkspacePod
from dlrover.python.workspace.process_manager import WorkspaceProcessManager
from dlrover.python.workspace.role_plan import build_plan, enabled

TRAINER = "ASCEND_RT_VISIBLE_DEVICES=1,2 NPROC_PER_NODE=2 swift rlhf --rlhf_type grpo --model /models/qwen --model_type qwen3 --use_vllm true --vllm_mode server --vllm_server_host 127.0.0.1 --vllm_server_port 8991 --max_steps 4"
ROLLOUT = "ASCEND_RT_VISIBLE_DEVICES=0 swift rollout --model /models/qwen --model_type qwen3 --port 8991"


def args(command=TRAINER, *extra):
    return cli.build_parser().parse_args(
        [
            "launch",
            "--framework-cmd",
            command,
            "--framework",
            "swift",
            "--network-mode",
            "all",
            *extra,
        ]
    )


def pods(*ranks):
    return [
        WorkspacePod(
            "worker-%s" % r,
            "default",
            r,
            "Running",
            pod_ip="10.0.0.%s" % (r + 1),
            run_mode="workspace",
            uid="uid-%s" % r,
        )
        for r in ranks
    ]


class ScriptTests(unittest.TestCase):
    def test_constants_quotes_environment_and_lines(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "train.sh")
            path.write_text(
                '# comment\nMODEL="/models/a b"\nexport TOKEN=secret\ncd /training\nCUDA_VISIBLE_DEVICES=0 swift rlhf \\\n --model "$MODEL" --literal \'$NO_EXPAND\'\n'
            )
            (command,) = parse_input("bash " + str(path))
            self.assertEqual(command.line, 5)
            self.assertEqual(
                command.environment,
                {"TOKEN": "secret", "CUDA_VISIBLE_DEVICES": "0"},
            )
            self.assertEqual(command.cwd, "/training")
            self.assertEqual(
                command.argv[-4:],
                ["--model", "/models/a b", "--literal", "$NO_EXPAND"],
            )

    def test_no_shell_execution(self):
        for command in [
            "swift rlhf | tee out",
            "source setup.sh",
            "swift rlhf $(id)",
            "swift rlhf `id`",
            "if true; then swift rlhf; fi",
            "swift rlhf $MISSING",
            "swift rlhf &amp; echo ok",
        ]:
            with self.subTest(command=command):
                with self.assertRaises(ValueError):
                    # parse_input preserves unknown simple commands; the adapter rejects them.
                    build_plan(args(command), pods(0))

    def test_shell_variables_are_not_implicitly_exported(self):
        result = parse_input("NPROC_PER_NODE=8\nswift rlhf")
        self.assertNotIn("NPROC_PER_NODE", result[0].environment)


class PlanTests(unittest.TestCase):
    def test_opt_in_only(self):
        old = cli.build_parser().parse_args(
            ["launch", "--framework-cmd", TRAINER]
        )
        self.assertFalse(enabled(old))
        self.assertEqual(
            old.dlrover_run_args, "--nnodes=$NODE_NUM --nproc_per_node=1"
        )

    def test_managed_same_pods(self):
        plan = build_plan(
            args(ROLLOUT + "\n" + TRAINER, "--rollout-management", "managed"),
            pods(2, 0),
        )
        self.assertEqual(plan.deployment, "disaggregated")
        trainers = [r for r in plan.roles if r.name == "trainer"]
        self.assertEqual([r.rank for r in trainers], [0, 2])
        self.assertEqual([r.node_rank for r in trainers], [0, 1])
        self.assertEqual(trainers[0].command, trainers[1].command)
        self.assertIn("10.0.0.1", trainers[0].command)
        self.assertIn("10.0.0.3", trainers[0].command)
        self.assertNotIn("127.0.0.1", trainers[0].command)
        self.assertNotIn("NPROC_PER_NODE", trainers[0].environment)
        self.assertIn("--nproc_per_node=2", trainers[0].shell)
        self.assertIn("export NODE_NUM=2", trainers[0].shell)
        self.assertNotIn("swift rlhf", trainers[0].shell)

    def test_roles_on_different_pods(self):
        plan = build_plan(
            args(
                ROLLOUT + "\n" + TRAINER,
                "--rollout-management",
                "managed",
                "--trainer-ranks",
                "2,4",
                "--rollout-ranks",
                "0",
            ),
            pods(4, 0, 2),
        )
        trainers = [r for r in plan.roles if r.name == "trainer"]
        self.assertEqual([r.node_rank for r in trainers], [0, 1])
        self.assertIn("export NODE_NUM=2", trainers[0].shell)

    def test_external_and_colocate_have_no_service(self):
        for command in [
            TRAINER,
            TRAINER.replace("vllm_mode server", "vllm_mode colocate"),
        ]:
            plan = build_plan(args(command), pods(0))
            self.assertEqual([r.name for r in plan.roles], ["trainer"])

    def test_external_requires_explicit_managed_for_service_script(self):
        with self.assertRaisesRegex(ValueError, "explicitly"):
            build_plan(args(ROLLOUT + "\n" + TRAINER), pods(0))

    def test_invalid_layouts_and_modes(self):
        cases = [
            ("--trainer-ranks", "0"),
            ("--trainer-ranks", "0,0", "--rollout-ranks", "2"),
            ("--trainer-ranks", "0", "--rollout-ranks", "4"),
            ("--trainer-devices", "0"),
            ("--rollout-ready-timeout", "nan"),
            ("--rollout-ready-timeout", "0"),
        ]
        for extra in cases:
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                build_plan(
                    args(
                        ROLLOUT + "\n" + TRAINER,
                        "--rollout-management",
                        "managed",
                        *extra
                    ),
                    pods(0, 2),
                )
        for extra in [
            ("--foreground",),
            ("--framework", "megatron"),
            ("--deployment-mode", "colocate"),
            ("--network-mode", "none"),
        ]:
            with self.subTest(extra=extra), self.assertRaises(ValueError):
                build_plan(args(TRAINER, *extra), pods(0, 2))

    def test_parallel_capacity_is_checked_before_launch(self):
        with self.assertRaises(ValueError):
            build_plan(
                args(
                    TRAINER.replace("vllm_mode server", "vllm_mode colocate")
                    + " --vllm_tensor_parallel_size 3"
                ),
                pods(0),
            )
        with self.assertRaises(ValueError):
            build_plan(
                args(
                    ROLLOUT
                    + " --vllm_tensor_parallel_size -1 --vllm_data_parallel_size -1\n"
                    + TRAINER,
                    "--rollout-management",
                    "managed",
                ),
                pods(0),
            )

    def test_model_mismatch_and_abbreviations(self):
        for trainer in [
            TRAINER.replace("/models/qwen", "/models/other"),
            TRAINER.replace("qwen3", "qwen3_5"),
            TRAINER.replace("--max_steps", "--max_step"),
        ]:
            with self.assertRaises(ValueError):
                build_plan(
                    args(
                        ROLLOUT + "\n" + trainer,
                        "--rollout-management",
                        "managed",
                    ),
                    pods(0),
                )

    def test_explicit_nproc_precedence_and_mapping(self):
        plan = build_plan(
            args(
                TRAINER,
                "--trainer-devices",
                "1,2,3",
                "--dlrover-run-args=--nnodes=$NODE_NUM --nproc_per_node=1",
            ),
            pods(0),
            True,
        )
        self.assertEqual(plan.roles[0].nproc, 1)
        self.assertEqual(plan.sources["nproc"]["local_rank_devices"], [1])
        plan = build_plan(args(TRAINER, "--trainer-devices", "1"), pods(0))
        self.assertEqual(plan.roles[0].nproc, 1)
        for value in [
            "--nnodes=3",
            "--nproc_per_node=8",
            "--max_restarts=1",
            "--node_rank=7",
        ]:
            with self.assertRaises(ValueError):
                build_plan(
                    args(TRAINER, "--dlrover-run-args=" + value), pods(0), True
                )

    def test_dry_run_no_mutation_and_redaction(self):
        value = "HF_TOKEN=secret " + TRAINER + " --hub_token=private"
        plan = build_plan(args(value), pods(0))
        text = plan.describe()
        self.assertNotIn("secret", text)
        self.assertNotIn("private", text)
        with mock.patch.object(cli, "_get_workspace_launcher_cls") as factory:
            launcher = factory.return_value.return_value
            launcher.list_worker_pods.return_value = pods(0)
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(
                    cli.main(
                        [
                            "launch",
                            "--framework-cmd",
                            TRAINER,
                            "--framework",
                            "swift",
                            "--network-mode",
                            "all",
                            "--dry-run",
                        ]
                    ),
                    0,
                )
            launcher.launch.assert_not_called()
            launcher._exec.assert_not_called()
            launcher.wait_for_running_pods.assert_not_called()

    def test_local_keeps_native_launcher(self):
        plan = build_plan(args(TRAINER, "--network-mode", "none"), pods(0))
        self.assertEqual(plan.roles[0].environment["NPROC_PER_NODE"], "2")
        self.assertEqual(plan.roles[0].command[:2], ["swift", "rlhf"])
        self.assertEqual(plan.roles[0].shell, "")


class SubmissionTests(unittest.TestCase):
    def test_submit_and_partial_launch_compensation(self):
        from dlrover.python.workspace import role_launcher

        for fail in (False, True):
            selected = pods(0, 2)
            arguments = args(
                ROLLOUT + "\n" + TRAINER, "--rollout-management", "managed"
            )
            plan = build_plan(arguments, selected)
            launcher = mock.Mock()
            launcher.wait_for_running_pods.return_value = selected
            launcher.list_worker_pods.return_value = selected
            calls = []

            def call(launcher, pod, action, payload, args):
                calls.append((pod.rank, action, payload))
                if action == "preflight":
                    return {
                        "versions": {"ms-swift": "4.0.0"},
                        "models": {"trainer": "same"},
                    }
                if action == "register":
                    self.assertNotIn("plan", payload)
                    return {"epoch": "epoch"}
                if action == "launch":
                    self.assertEqual(
                        {r["rank"] for r in payload["plan"]["roles"]},
                        {pod.rank},
                    )
                    self.assertEqual(payload["selected_ranks"], [0, 2])
                    if fail and pod.rank == 2:
                        raise RuntimeError("lost launch acknowledgement")
                return {"state": "RUNNING"}

            with mock.patch.object(role_launcher, "call", side_effect=call):
                if fail:
                    with self.assertRaisesRegex(RuntimeError, "lost launch"):
                        role_launcher.launch(launcher, plan, arguments)
                    self.assertEqual(
                        [
                            r
                            for r, action, _ in calls
                            if action == "unlaunched"
                        ],
                        [0, 2],
                    )
                    self.assertEqual(
                        sum(action == "cancel" for _, action, _ in calls), 1
                    )
                else:
                    self.assertEqual(
                        role_launcher.launch(launcher, plan, arguments)[
                            "state"
                        ],
                        "RUNNING",
                    )


class CoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.now = 0
        self.c = WorkspaceTaskCoordinator(lambda: self.now)
        self.members = [
            {
                "rank": r,
                "uid": "u%s" % r,
                "launch_id": "l%s" % r,
                "roles": ["trainer", "rollout"],
            }
            for r in (0, 2)
        ]
        self.c.exchange(
            "register",
            "run",
            "",
            {"members": self.members, "ready_timeout": 100},
        )

    def heartbeat(self, rank, **kwargs):
        member = dict(next(m for m in self.members if m["rank"] == rank))
        member.update(kwargs)
        return self.c.exchange("heartbeat", "run", self.c.epoch, member)

    def test_start_ready_first_step_and_cleanup_barrier(self):
        self.assertEqual(self.heartbeat(0)["state"], "PREPARING")
        self.assertEqual(self.heartbeat(2)["state"], "SERVICES_STARTING")
        self.heartbeat(0, services_ready=True)
        self.assertEqual(
            self.heartbeat(2, services_ready=True)["state"], "TRAINER_STARTING"
        )
        self.heartbeat(0, initialized=True)
        self.assertEqual(
            self.heartbeat(2, initialized=True)["state"], "RUNNING"
        )
        self.heartbeat(0, initialized=True, trainer_done=True)
        self.assertEqual(
            self.heartbeat(2, initialized=True, trainer_done=True)["state"],
            "STOPPING",
        )
        self.assertTrue(self.c.active)
        self.heartbeat(0, cleaned=True)
        self.assertEqual(self.heartbeat(2, cleaned=True)["state"], "SUCCEEDED")
        self.assertFalse(self.c.active)

    def test_fence_and_single_task(self):
        result = self.c.exchange("heartbeat", "run", "old", {})
        self.assertEqual(result["state"], "FENCED")
        self.assertEqual(self.heartbeat(0, uid="replaced")["state"], "FENCED")
        with self.assertRaises(ValueError):
            self.c.exchange("register", "other", "", {"members": self.members})
        self.assertEqual(self.c.task["reports"], {})

    def test_timeout_still_holds_occupancy_until_cleaned(self):
        self.heartbeat(0)
        self.now = 61
        self.assertEqual(self.heartbeat(2)["state"], "STOPPING")
        self.assertEqual(self.c.task["outcome"], "FAILED")
        self.heartbeat(0, cleaned=True)
        self.assertTrue(self.c.active)
        self.heartbeat(2, cleaned=True)
        self.assertFalse(self.c.active)

    def test_error_overrides_success_during_cleanup(self):
        self.c.exchange("stop", "run", self.c.epoch, {"outcome": "SUCCEEDED"})
        self.heartbeat(0, error="cannot clean", cleaned=False)
        self.assertEqual(self.c.task["outcome"], "FAILED")
        self.assertTrue(self.c.active)

    def test_failure_does_not_release_external_resources(self):
        self.c.exchange("stop", "run", self.c.epoch, {"outcome": "CANCELLED"})
        self.heartbeat(0, cleaned=True)
        result = self.heartbeat(2, cleaned=True)
        self.assertEqual(result["state"], "CANCELLED")


class RuntimeTests(unittest.TestCase):
    def test_stop_refuses_partial_scope(self):
        data = {
            "plan": {"roles": [{"rank": 0}, {"rank": 2}]},
            "members": [{"rank": 0}, {"rank": 2}],
            "active": True,
        }
        with mock.patch.object(role_runtime, "exchange") as exchange:
            with self.assertRaisesRegex(ValueError, "partial"):
                role_control.stop(data, [0], 0, 0, "/unused")
            exchange.assert_not_called()

    def test_legacy_launch_blocked_by_task_reservation(self):
        with tempfile.TemporaryDirectory() as directory:
            role_runtime.write_json(
                Path(directory, role_runtime.INDEX), {"active": True}
            )
            manager = WorkspaceProcessManager(directory, directory)
            with self.assertRaisesRegex(RuntimeError, "owns"):
                manager.launch("echo should-not-run", "legacy", 0)

    def test_failed_spawn_reconciles_reservation_without_claiming_live_cleanup(
        self,
    ):
        for alive in (False, True):
            with self.subTest(
                alive=alive
            ), tempfile.TemporaryDirectory() as directory:
                data = {
                    "run_id": "new",
                    "launch_id": "new.0",
                    "uid": "u",
                    "rank": 0,
                    "active": True,
                }
                role_runtime.write_json(
                    Path(directory, role_runtime.INDEX), data
                )
                manager = WorkspaceProcessManager(directory, directory)
                with mock.patch.object(
                    role_runtime,
                    "find_pids_by_launch_id",
                    return_value=[123] if alive else [],
                ), mock.patch.object(role_runtime, "exchange") as exchange:
                    role_control._failed_launch(data, manager)
                    self.assertEqual(
                        exchange.call_args.args[2]["cleaned"], not alive
                    )
                    self.assertEqual(
                        role_runtime.read_index(directory)["active"], alive
                    )

    def test_cleanup_owns_only_task_process(self):
        owned = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            env=dict(
                os.environ, DLROVER_WORKSPACE_LAUNCH_ID="workspace-unit-owned"
            ),
            start_new_session=True,
        )
        external = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            start_new_session=True,
        )
        try:
            self.assertTrue(
                role_runtime.cleanup(["workspace-unit-owned"], grace=1)
            )
            owned.wait(timeout=5)
            self.assertIsNone(external.poll())
        finally:
            for proc in (owned, external):
                if proc.poll() is None:
                    proc.kill()
                proc.wait()

    def test_listener_ownership(self):
        proc = subprocess.Popen(
            [
                sys.executable,
                "-u",
                "-c",
                'import socket,time; s=socket.socket(); s.bind(("127.0.0.1",0)); s.listen(); print(s.getsockname()[1], flush=True); time.sleep(60)',
            ],
            stdout=subprocess.PIPE,
            text=True,
            env=dict(
                os.environ,
                DLROVER_WORKSPACE_LAUNCH_ID="workspace-unit-listener",
            ),
            start_new_session=True,
        )
        try:
            port = int(proc.stdout.readline())
            self.assertTrue(
                role_runtime.owns_listener("workspace-unit-listener", port)
            )
            self.assertFalse(
                role_runtime.owns_listener("different-task", port)
            )
        finally:
            proc.kill()
            proc.wait()
            proc.stdout.close()


class SupervisorIntegrationTests(unittest.TestCase):
    """Real local child processes and HTTP, with an in-process Master."""

    def test_managed_completion_cleans_service(self):
        self._managed()

    def test_failed_trainer_cleans_service(self):
        self._managed(trainer_code="raise SystemExit(7)", expected=1)

    def test_failed_service_never_starts_trainer(self):
        self._managed(fail_service=True, expected=1)

    def _managed(self, trainer_code=None, fail_service=False, expected=0):
        import socket
        from dlrover.python.workspace.state import (
            RunState,
            WorkspaceStateStore,
        )

        with tempfile.TemporaryDirectory() as directory:
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            server = Path(directory, "service.py")
            server.write_text("""import json, sys
from http.server import BaseHTTPRequestHandler, HTTPServer
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({'status':'ok'} if self.path == '/health/' else {'world_size':1}).encode()
        self.send_response(200)
        self.end_headers()
        self.wfile.write(body)
HTTPServer(('127.0.0.1', int(sys.argv[1])), Handler).serve_forever()
""")
            trainer = 'import os; from pathlib import Path; p=Path(os.environ["DLROVER_WORKSPACE_INIT_DIR"]); p.mkdir(exist_ok=True); (p/"rank-0").touch()'
            if trainer_code is not None:
                trainer = trainer_code
            if fail_service:
                server.write_text("raise SystemExit(8)")
            roles = [
                {
                    "name": "rollout",
                    "rank": 0,
                    "command": [sys.executable, str(server), str(port)],
                    "shell": "",
                    "environment": {},
                    "cwd": "",
                    "devices": [0],
                    "nproc": 1,
                    "endpoint": {"host": "127.0.0.1", "port": port},
                },
                {
                    "name": "trainer",
                    "rank": 0,
                    "command": [],
                    "shell": "exec "
                    + __import__("shlex").join(
                        [sys.executable, "-c", trainer]
                    ),
                    "environment": {},
                    "cwd": "",
                    "devices": [1],
                    "nproc": 1,
                    "endpoint": {},
                },
            ]
            data = {
                "run_id": "integration",
                "launch_id": "integration.0",
                "uid": "u0",
                "rank": 0,
                "active": True,
                "log_dir": directory,
                "plan": {
                    "framework": "swift",
                    "roles": roles,
                    "management": "managed",
                },
            }
            coordinator = WorkspaceTaskCoordinator()
            result = coordinator.exchange(
                "register",
                data["run_id"],
                "",
                {
                    "members": [
                        {
                            "rank": 0,
                            "uid": "u0",
                            "launch_id": "integration.0",
                            "roles": ["trainer", "rollout"],
                        }
                    ],
                    "ready_timeout": 30,
                },
            )
            data["epoch"] = result["epoch"]
            role_runtime.write_json(Path(directory, role_runtime.INDEX), data)
            WorkspaceStateStore(directory).save(
                RunState(state="RUNNING", launch_id="integration.0")
            )
            original_popen = subprocess.Popen

            def popen(argv, **kwargs):
                if "guard" in argv:
                    return mock.Mock()
                return original_popen(argv, **kwargs)

            def exchange(data, operation, payload=None):
                return coordinator.exchange(
                    operation, data["run_id"], data["epoch"], payload or {}
                )

            try:
                with mock.patch.object(
                    role_runtime, "exchange", side_effect=exchange
                ), mock.patch.object(
                    role_runtime.subprocess, "Popen", side_effect=popen
                ):
                    self.assertEqual(role_runtime.run(directory), expected)
                index = role_runtime.read_index(directory)
                self.assertFalse(index["active"])
                self.assertEqual(
                    index["state"], "SUCCEEDED" if expected == 0 else "FAILED"
                )
                self.assertFalse(
                    role_runtime.find_pids_by_launch_id("integration.rollout")
                )
                self.assertEqual(
                    WorkspaceStateStore(directory).load().exit_code, expected
                )
            finally:
                role_runtime.cleanup(
                    ["integration.rollout", "integration.trainer"], grace=1
                )


if __name__ == "__main__":
    unittest.main()
