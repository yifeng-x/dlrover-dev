# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

"""Submit a fixed role plan through the existing pods-exec/agentctl channel."""

import base64
import json
import time


def encode(data):
    return base64.b64encode(json.dumps(data).encode()).decode()


def call(launcher, pod, action, payload, args):
    shell = launcher._build_agentctl_shell(
        args.log_dir,
        "role-task",
        "--action",
        action,
        "--payload-b64",
        encode(payload),
        state_dir=args.state_dir,
    )
    # Streaming transport exposes the remote exit status; a nonempty stdout
    # alone cannot establish successful agentctl execution.
    result = launcher._exec_checked(pod, shell)
    if not result.ok:
        raise RuntimeError(
            "%s on %s failed: %s" % (action, pod.name, result.output)
        )
    lines = [
        line[len("WORKSPACE_TASK=") :]
        for line in result.output.splitlines()
        if line.startswith("WORKSPACE_TASK=")
    ]
    if len(lines) != 1:
        raise RuntimeError(
            "missing structured workspace task response from " + pod.name
        )
    response = json.loads(lines[0])
    if response.get("error"):
        raise RuntimeError(response["error"])
    return response


def launch(launcher, plan, args):
    # Reserve every workspace Pod, including idle ones, using the legacy
    # process lock. This excludes concurrent legacy launches on other ranks.
    pods = launcher.wait_for_running_pods()
    snapshot = {p.rank: p for p in pods}
    for selected in plan.pods:
        current = snapshot.get(selected["rank"])
        if (
            not current
            or current.uid != selected["uid"]
            or current.pod_ip != selected["pod_ip"]
        ):
            raise RuntimeError("Pod identity changed while planning")
    if any(
        p.run_mode != "workspace" or p.exit_policy != "retain" or not p.uid
        for p in pods
    ):
        raise RuntimeError("all workers must be retain workspace Pods")
    data = plan.to_dict()
    local_plans = {
        p.rank: dict(
            data, roles=[r for r in data["roles"] if r["rank"] == p.rank]
        )
        for p in pods
    }
    checks = [
        call(
            launcher,
            p,
            "preflight",
            {"plan": local_plans[p.rank], "rank": p.rank},
            args,
        )
        for p in pods
    ]
    versions = [c["versions"] for c in checks if c["versions"]]
    if any(v != versions[0] for v in versions[1:]):
        raise RuntimeError(
            "framework/runtime versions differ across role Pods"
        )
    models = [
        fingerprint for c in checks for fingerprint in c["models"].values()
    ]
    if len(set(models)) != 1:
        raise RuntimeError("model metadata differs across roles/Pods")
    members = [
        {
            "rank": p.rank,
            "uid": p.uid,
            "ip": p.pod_ip,
            "launch_id": plan.run_id + "." + str(p.rank),
            "roles": [r.name for r in plan.roles if r.rank == p.rank],
        }
        for p in pods
    ]
    common = {
        "run_id": plan.run_id,
        "selected_ranks": sorted({r.rank for r in plan.roles}),
        "members": members,
        "log_dir": args.log_dir,
        "ready_timeout": plan.ready_timeout,
    }
    registration = call(launcher, pods[0], "register", common, args)
    common["epoch"] = registration["epoch"]
    try:
        for pod, member in zip(pods, members):
            call(
                launcher,
                pod,
                "launch",
                dict(common, plan=local_plans[pod.rank], **member),
                args,
            )
        deadline = time.monotonic() + 2 * plan.ready_timeout + 60
        while time.monotonic() < deadline:
            current = {p.rank: p for p in launcher.list_worker_pods()}
            if any(
                p.rank not in current
                or current[p.rank].uid != p.uid
                or current[p.rank].pod_ip != p.pod_ip
                for p in pods
            ):
                raise RuntimeError("Pod identity changed during task startup")
            result = call(launcher, pods[0], "query", common, args)
            if result["state"] in ("RUNNING", "SUCCEEDED"):
                return result
            if result["state"] in (
                "STOPPING",
                "FAILED",
                "CANCELLED",
                "FENCED",
            ):
                raise RuntimeError(
                    "task startup failed: "
                    + result.get("reason", result["state"])
                )
            time.sleep(2)
        raise RuntimeError("task startup timed out")
    except BaseException:
        try:
            call(launcher, pods[0], "cancel", common, args)
            # Unlaunched members own no processes and can acknowledge cleanup.
            for pod, member in zip(pods, members):
                call(launcher, pod, "unlaunched", dict(common, **member), args)
        except Exception:
            # Every started supervisor has an independent bounded control lease.
            pass
        raise
