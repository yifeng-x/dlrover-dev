# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

"""Pod-local role supervision, bounded control leases and ownership cleanup."""

import argparse
from functools import lru_cache
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

from dlrover.python.workspace.frameworks import get_adapter
from dlrover.python.workspace.proc_utils import (
    find_pids_by_launch_id,
    signal_pids,
    is_pid_alive,
    read_environ,
)
from dlrover.python.workspace.role_plan import LAUNCH_ENV
from dlrover.python.workspace.state import (
    WorkspaceStateStore,
    STATE_FAILED,
    STATE_STOPPED,
)

INDEX = "role-task.json"


def write_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".%s.tmp" % os.getpid())
    with open(
        temp, "w", opener=lambda p, flags: os.open(p, flags, 0o600)
    ) as fp:
        json.dump(data, fp)
        fp.flush()
        os.fsync(fp.fileno())
    os.replace(temp, path)


def read_index(state_dir):
    try:
        return json.loads(Path(state_dir, INDEX).read_text())
    except FileNotFoundError:
        return None


@lru_cache(maxsize=1)
def client():
    from dlrover.python.elastic_agent.master_client import build_master_client

    result = build_master_client(timeout=5)
    if result is None:
        raise RuntimeError("adapted workspace requires a reachable Master")
    return result


def exchange(data, operation, payload=None):
    return client().workspace_task(
        operation, data["run_id"], data.get("epoch", ""), payload
    )


def preflight(plan, rank, state_dir):
    from dlrover.python.workspace.process_manager import (
        WorkspaceProcessManager,
    )

    manager = WorkspaceProcessManager(state_dir=state_dir)
    with manager.store.locked():
        index = read_index(state_dir)
        if index and index.get("active"):
            raise RuntimeError(
                "an adapted task still owns this Pod; stop/reconcile it first"
            )
        if manager._is_active(
            manager._refresh_state(manager._load_state_or_compat())
        ):
            raise RuntimeError(
                "workspace already has an active legacy command"
            )
    roles = [r for r in plan["roles"] if r["rank"] == rank]
    return get_adapter(plan["framework"], []).validate_worker(roles)


def owns_listener(launch_id, port):
    inodes = set()
    for table in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            for line in Path(table).read_text().splitlines()[1:]:
                fields = line.split()
                if (
                    fields[3] == "0A"
                    and int(fields[1].rsplit(":", 1)[1], 16) == port
                ):
                    inodes.add("socket:[%s]" % fields[9])
        except FileNotFoundError:
            continue
    for pid in find_pids_by_launch_id(launch_id):
        try:
            for fd in Path("/proc/%s/fd" % pid).iterdir():
                try:
                    if os.readlink(fd) in inodes:
                        return True
                except OSError:
                    continue
        except OSError:
            continue
    return False


def cleanup(launch_ids, grace=10):
    # Handles children which escaped their original process group as well.
    for sig, seconds in ((signal.SIGTERM, grace), (signal.SIGKILL, 5)):
        deadline = time.monotonic() + seconds
        while True:
            pids = [
                p
                for launch_id in launch_ids
                for p in find_pids_by_launch_id(launch_id)
            ]
            if not pids:
                return True
            signal_pids(pids, sig)
            if time.monotonic() >= deadline:
                break
            time.sleep(0.1)
    return not any(find_pids_by_launch_id(i) for i in launch_ids)


def run(state_dir):
    data = read_index(state_dir)
    from dlrover.python.workspace.release_supervisor import (
        _wait_for_launch_state,
    )

    _wait_for_launch_state(WorkspaceStateStore(state_dir), data["launch_id"])
    plan, rank = data["plan"], data["rank"]
    adapter = get_adapter(plan["framework"], [])
    root = Path(state_dir, "runs", data["run_id"])
    root.mkdir(parents=True, exist_ok=True)
    own = [r for r in plan["roles"] if r["rank"] == rank]
    processes, statuses = {}, {}
    launch_ids = {r["name"]: data["run_id"] + "." + r["name"] for r in own}
    for r in own:
        directory = root / r["name"]
        directory.mkdir(exist_ok=True)
        statuses[r["name"]] = {
            "state": "PENDING",
            "devices": r["devices"],
            "launch_id": launch_ids[r["name"]],
            "log_file": str(
                Path(
                    data["log_dir"],
                    data["run_id"],
                    r["name"],
                    "rank-%s.log" % rank,
                )
            ),
        }
    data["roles"] = statuses
    data["supervisor_pid"] = os.getpid()
    write_json(Path(state_dir, INDEX), data)
    # Guardian survives an unexpected supervisor death and cleans owned roles.
    guard = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "dlrover.python.workspace.role_runtime",
            "guard",
            "--state-dir",
            state_dir,
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    previous_signals = {
        sig: signal.signal(sig, stop)
        for sig in (signal.SIGTERM, signal.SIGINT)
    }
    last_control, error = time.monotonic(), ""
    report = {
        "rank": rank,
        "uid": data["uid"],
        "launch_id": data["launch_id"],
        "services_ready": False,
        "initialized": not any(r["name"] == "trainer" for r in own),
        "trainer_done": False,
        "cleaned": False,
    }
    result = {"state": "PREPARING"}
    external_endpoints, external_ready, probe_index = [], set(), 0
    if plan["management"] == "external" and any(
        r["name"] == "trainer" for r in own
    ):
        trainer = next(r for r in own if r["name"] == "trainer")
        external_endpoints = adapter.external_endpoints(trainer)
    try:
        while True:
            try:
                result = exchange(
                    data,
                    "stop" if stopping else "heartbeat",
                    (
                        {"reason": "user stop"}
                        if stopping
                        else dict(report, error=error)
                    ),
                )
                last_control = time.monotonic()
            except Exception as exc:
                if time.monotonic() - last_control >= 60:
                    error = "Master control lease expired: " + str(exc)
                    break
                time.sleep(2)
                continue
            state = result["state"]
            data.update(state=state, reason=result.get("reason", ""))
            write_json(Path(state_dir, INDEX), data)
            if state in (
                "FENCED",
                "STOPPING",
                "FAILED",
                "CANCELLED",
                "SUCCEEDED",
            ):
                break
            for role in own:
                name = role["name"]
                allowed = (
                    state
                    in ("SERVICES_STARTING", "TRAINER_STARTING", "RUNNING")
                    if name == "rollout"
                    else state in ("TRAINER_STARTING", "RUNNING")
                )
                if allowed and name not in processes:
                    env = os.environ.copy()
                    for key in LAUNCH_ENV:
                        env.pop(key, None)
                    for key in list(env):
                        if key.startswith("TORCHELASTIC_"):
                            env.pop(key)
                    env.update(role["environment"])
                    env.update(
                        DLROVER_WORKSPACE_LAUNCH_ID=launch_ids[name],
                        DLROVER_WORKSPACE_ROLE_TASK="1",
                        DLROVER_WORKSPACE_MODE="true",
                        DLROVER_WORKSPACE_EXIT_POLICY="retain",
                    )
                    if name == "rollout":
                        # Bind before spawning to reject an already occupied endpoint.
                        with socket.socket(
                            socket.AF_INET6
                            if ":" in role["endpoint"]["host"]
                            else socket.AF_INET
                        ) as sock:
                            sock.bind(
                                (
                                    role["endpoint"]["host"],
                                    role["endpoint"]["port"],
                                )
                            )
                    else:
                        env["NODE_RANK"] = str(rank) if role["shell"] else "0"
                        env["DLROVER_WORKSPACE_INIT_DIR"] = str(
                            root / "initialized"
                        )
                    argv = role["command"]
                    if role["shell"]:
                        argv = ["/bin/bash", "-c", role["shell"]]
                    elif name == "trainer":
                        argv = adapter.prepare_local_command(argv)
                    logfile = Path(statuses[name]["log_file"])
                    logfile.parent.mkdir(parents=True, exist_ok=True)
                    with logfile.open("ab") as log:
                        processes[name] = subprocess.Popen(
                            argv,
                            env=env,
                            cwd=role["cwd"] or None,
                            stdin=subprocess.DEVNULL,
                            stdout=log,
                            stderr=subprocess.STDOUT,
                            start_new_session=True,
                        )
                    statuses[name].update(
                        state="STARTING", pid=processes[name].pid
                    )
                if name in processes:
                    rc = processes[name].poll()
                    if name == "rollout":
                        if rc is not None:
                            raise RuntimeError("rollout exited: %s" % rc)
                        try:
                            if not owns_listener(
                                launch_ids[name], role["endpoint"]["port"]
                            ):
                                raise RuntimeError(
                                    "rollout listener is not owned by this task"
                                )
                            adapter.readiness(role["endpoint"])
                            statuses[name]["state"] = "READY"
                        except Exception:
                            if statuses[name]["state"] == "READY":
                                raise RuntimeError("rollout health lost")
                    else:
                        report["initialized"] = all(
                            (root / "initialized" / ("rank-%s" % i)).exists()
                            for i in range(role["nproc"])
                        )
                        if report["initialized"]:
                            statuses[name]["state"] = "RUNNING"
                        if rc is not None:
                            if rc != 0 or not report["initialized"]:
                                raise RuntimeError(
                                    "trainer exited before successful training: %s"
                                    % rc
                                )
                            report["trainer_done"] = True
                            statuses[name]["state"] = "SUCCEEDED"
            report["services_ready"] = all(
                s["state"] == "READY"
                for n, s in statuses.items()
                if n == "rollout"
            )
            if external_endpoints:
                # One probe per lease tick keeps control RPCs bounded.
                try:
                    adapter.readiness(external_endpoints[probe_index])
                    external_ready.add(probe_index)
                except Exception:
                    external_ready.discard(probe_index)
                    if state in ("TRAINER_STARTING", "RUNNING"):
                        raise RuntimeError("external rollout health lost")
                probe_index = (probe_index + 1) % len(external_endpoints)
                report["services_ready"] = len(external_ready) == len(
                    external_endpoints
                )
            time.sleep(2)
    except BaseException as exc:
        error = "%s: %s" % (type(exc).__name__, exc)
    finally:
        try:
            if error:
                exchange(data, "stop", {"outcome": "FAILED", "reason": error})
        except Exception:
            pass
        cleaned = True
        for name in ("trainer", "rollout"):
            if name in launch_ids:
                clean = cleanup([launch_ids[name]])
                cleaned = clean and cleaned
                statuses[name]["state"] = (
                    "STOPPED" if clean else "CLEANUP_FAILED"
                )
        for process in processes.values():
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                cleaned = False
        report.update(cleaned=cleaned, error=error)
        if cleaned:
            # A terminal result is published only after every member cleaned.
            deadline = time.monotonic() + 70
            while time.monotonic() < deadline:
                try:
                    result = exchange(data, "heartbeat", report)
                    if result["state"] in (
                        "SUCCEEDED",
                        "FAILED",
                        "CANCELLED",
                        "FENCED",
                    ):
                        break
                except Exception:
                    pass
                time.sleep(2)
        data.update(
            active=not (
                cleaned
                and result.get("state")
                in ("SUCCEEDED", "FAILED", "CANCELLED", "FENCED")
            ),
            cleaned=cleaned,
            state=result.get("state", "FAILED"),
            reason=error or result.get("reason", ""),
        )
        if data["state"] not in ("SUCCEEDED", "FAILED", "CANCELLED"):
            data["state"] = "FAILED"
        write_json(Path(state_dir, INDEX), data)
        store = WorkspaceStateStore(state_dir)
        with store.locked():
            current = store.load()
            if current.launch_id == data["launch_id"]:
                current.state = (
                    STATE_STOPPED
                    if data["state"] == "SUCCEEDED"
                    else STATE_FAILED
                )
                current.exit_code = 0 if data["state"] == "SUCCEEDED" else 1
                current.last_error = data["reason"]
                store.save(current)
        if cleaned:
            guard.terminate()
            guard.wait(timeout=5)
    for sig, handler in previous_signals.items():
        signal.signal(sig, handler)
    return 0 if data["state"] == "SUCCEEDED" else 1


def supervisor_alive(data):
    pid = data.get("supervisor_pid")
    return bool(
        pid
        and is_pid_alive(pid)
        and read_environ(pid).get("DLROVER_WORKSPACE_LAUNCH_ID")
        == data["launch_id"]
    )


def guard(state_dir):
    data = read_index(state_dir)
    while supervisor_alive(data):
        time.sleep(2)
    current = read_index(state_dir)
    if current["run_id"] != data["run_id"] or not current.get("active"):
        return
    cleaned = cleanup([r["launch_id"] for r in current["roles"].values()])
    current.update(
        cleaned=cleaned,
        state="FAILED",
        reason="role supervisor exited unexpectedly",
    )
    try:
        exchange(
            data, "stop", {"outcome": "FAILED", "reason": current["reason"]}
        )
        result = exchange(
            data,
            "heartbeat",
            {
                "rank": data["rank"],
                "uid": data["uid"],
                "launch_id": data["launch_id"],
                "cleaned": cleaned,
                "error": current["reason"],
            },
        )
    except Exception:
        result = {}
    current["active"] = not (
        cleaned
        and result.get("state")
        in ("SUCCEEDED", "FAILED", "CANCELLED", "FENCED")
    )
    write_json(Path(state_dir, INDEX), current)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("run", "guard"))
    parser.add_argument("--state-dir", required=True)
    args = parser.parse_args()
    return (
        run(args.state_dir) if args.action == "run" else guard(args.state_dir)
    )


if __name__ == "__main__":
    raise SystemExit(main())
