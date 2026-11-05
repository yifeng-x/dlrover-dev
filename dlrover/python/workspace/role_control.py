# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import json
from pathlib import Path
import shlex
import sys
import time

from dlrover.python.workspace import role_runtime as runtime


def control(action, data, manager):
    if action == "preflight":
        return runtime.preflight(data["plan"], data["rank"], manager.state_dir)
    if action == "register":
        return runtime.exchange(
            data,
            "register",
            {
                "members": data["members"],
                "ready_timeout": data["ready_timeout"],
                "reconciled": True,
            },
        )
    if action == "launch":
        data.update(active=True, state="PREPARING", roles={})
        command = shlex.join(
            [
                sys.executable,
                "-m",
                "dlrover.python.workspace.role_runtime",
                "run",
                "--state-dir",
                manager.state_dir,
            ]
        )
        try:
            rc = manager.launch(
                command, data["launch_id"], wait_seconds=0, role_task=data
            )
        except Exception:
            _failed_launch(data, manager)
            raise
        if rc:
            _failed_launch(data, manager)
            raise RuntimeError("Pod task launch failed with code %s" % rc)
        return {"started": True}
    if action == "query":
        return runtime.exchange(data, "status")
    if action == "cancel":
        return runtime.exchange(
            data,
            "stop",
            {"outcome": "FAILED", "reason": "launch interrupted/failed"},
        )
    if action == "unlaunched":
        with manager.store.locked():
            current = runtime.read_index(manager.state_dir)
            if (
                current
                and current["run_id"] == data["run_id"]
                and current.get("active")
            ):
                return {"cleanup_pending": True}
            return runtime.exchange(
                data, "heartbeat", _report(data, cleaned=True)
            )
    raise ValueError("unknown role task action")


def _report(data, **updates):
    return dict(
        {key: data[key] for key in ("rank", "uid", "launch_id")}, **updates
    )


def _failed_launch(data, manager):
    # A lost write/ack after Popen must never acknowledge a live execution as
    # cleaned. Conversely a failed log-directory creation owns no process.
    with manager.store.locked():
        cleaned = not runtime.find_pids_by_launch_id(data["launch_id"])
        current = runtime.read_index(manager.state_dir)
        if cleaned and current and current["run_id"] == data["run_id"]:
            current.update(
                active=False,
                cleaned=True,
                state="FAILED",
                reason="Pod launch rejected",
            )
            runtime.write_json(Path(manager.state_dir, runtime.INDEX), current)
        runtime.exchange(
            data,
            "heartbeat",
            _report(data, cleaned=cleaned, error="Pod launch rejected"),
        )


def status(data):
    public = {
        k: data.get(k)
        for k in (
            "run_id",
            "state",
            "active",
            "reason",
            "rank",
            "uid",
            "roles",
        )
    }
    public["deployment"] = data["plan"]["deployment"]
    public["management"] = data["plan"]["management"]
    return json.dumps(public, indent=2) + "\n"


def stop(data, ranks, grace, kill_timeout, state_dir):
    selected = set(
        data.get("selected_ranks", [r["rank"] for r in data["plan"]["roles"]])
    )
    if (
        ranks is not None
        and set(ranks) != selected
        and set(ranks) != {m["rank"] for m in data["members"]}
    ):
        raise ValueError(
            "partial stop is unsupported; stop the complete adapted task"
        )
    if not data.get("active"):
        return 0
    result = runtime.exchange(
        data, "stop", {"outcome": "CANCELLED", "reason": "user stop"}
    )
    deadline = time.monotonic() + max(grace + kill_timeout, 90)
    while time.monotonic() < deadline:
        current = runtime.read_index(state_dir)
        if not current.get("active"):
            return 0
        if current.get("supervisor_pid") and not runtime.supervisor_alive(
            current
        ):
            cleaned = True
            for name in ("trainer", "rollout"):
                role = current.get("roles", {}).get(name)
                if role:
                    cleaned = runtime.cleanup([role["launch_id"]]) and cleaned
            result = runtime.exchange(
                current,
                "heartbeat",
                {
                    "rank": current["rank"],
                    "uid": current["uid"],
                    "launch_id": current["launch_id"],
                    "cleaned": cleaned,
                },
            )
            if cleaned and result["state"] in (
                "SUCCEEDED",
                "FAILED",
                "CANCELLED",
                "FENCED",
            ):
                current.update(
                    active=False, cleaned=True, state=result["state"]
                )
                runtime.write_json(Path(state_dir, runtime.INDEX), current)
                return 0
        if result["state"] == "FENCED":
            # Supervisor will detect fencing and clean its owned role processes.
            pass
        time.sleep(1)
    raise RuntimeError(
        "task stop timed out; inspect role status for residual processes"
    )


def logs(data, role, tail, follow):
    # Keep the existing log reader's semantics for each selected role.
    paths = [
        (name, item["log_file"])
        for name, item in data.get("roles", {}).items()
        if role is None or role == name
    ]
    if follow:
        # GNU tail preserves filenames when following multiple role logs.
        import subprocess

        existing = [path for _, path in paths if Path(path).exists()]
        if not existing:
            return 0
        return subprocess.call(["tail", "-n", str(tail), "-F", *existing])
    for name, path in paths:
        print("===== role %s =====" % name)
        if Path(path).exists():
            from collections import deque

            with open(path, errors="replace") as fp:
                print("".join(deque(fp, maxlen=max(0, tail))), end="")
    return 0
