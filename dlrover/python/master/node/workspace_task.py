# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

"""In-memory, fail-closed coordinator for explicitly adapted workspace tasks."""

import threading
import math
import time
import uuid

TERMINAL = ("SUCCEEDED", "FAILED", "CANCELLED")


class WorkspaceTaskCoordinator:
    def __init__(self, clock=time.monotonic):
        self.epoch = uuid.uuid4().hex
        self.clock = clock
        self.lock = threading.Lock()
        self.task = None

    @property
    def active(self):
        task = self.task
        return bool(task and task["state"] not in TERMINAL)

    def observe_pod(self, rank, uid, ip, running):
        with self.lock:
            if not self.active:
                return
            member = self.task["members"].get(str(rank))
            if (
                not member
                or not running
                or member["uid"] != uid
                or member.get("ip") != ip
            ):
                self._stop("FAILED", "workspace Pod membership/UID/IP changed")

    def fail(self, reason):
        with self.lock:
            if self.active:
                self._stop("FAILED", reason)

    def _stop(self, outcome, reason):
        if outcome not in TERMINAL:
            raise ValueError("invalid task outcome")
        task = self.task
        if task["state"] not in TERMINAL:
            if task["state"] != "STOPPING" or outcome == "FAILED":
                task.update(state="STOPPING", outcome=outcome, reason=reason)

    def exchange(self, operation, run_id, epoch, payload):
        with self.lock:
            now = self.clock()
            task = self.task
            if operation == "register":
                if self.active:
                    self._advance(now)
                    # The submitting CLI has checked every Pod under its process
                    # lock. An expired task with no remaining local ownership
                    # can be reconciled; live tasks can never be replaced.
                    expired = (
                        now
                        > max([task["deadline"], *task["seen"].values()]) + 60
                    )
                    if not (
                        task["state"] == "STOPPING"
                        and expired
                        and payload.get("reconciled")
                    ):
                        raise ValueError(
                            "workspace already has an active adapted task"
                        )
                members = payload["members"]
                if not members or len({m["rank"] for m in members}) != len(
                    members
                ):
                    raise ValueError("invalid task membership")
                if not any("trainer" in m["roles"] for m in members):
                    raise ValueError("task requires trainer members")
                self.task = task = {
                    "run_id": run_id,
                    "state": "PREPARING",
                    "outcome": "",
                    "reason": "",
                    "members": {str(m["rank"]): m for m in members},
                    "reports": {},
                    "seen": {},
                    "deadline": now + 60,
                    "ready_timeout": float(payload["ready_timeout"]),
                    "first_done": None,
                }
                if (
                    not math.isfinite(task["ready_timeout"])
                    or task["ready_timeout"] <= 0
                ):
                    self.task = None
                    raise ValueError("invalid readiness timeout")
            elif not task or task["run_id"] != run_id or epoch != self.epoch:
                return {
                    "epoch": self.epoch,
                    "state": "FENCED",
                    "reason": "stale execution or restarted Master",
                }
            elif operation == "stop":
                self._stop(
                    payload.get("outcome", "CANCELLED"),
                    payload.get("reason", "user stop"),
                )
            elif operation == "heartbeat":
                rank = str(payload["rank"])
                expected = task["members"].get(rank)
                if not expected or any(
                    payload.get(k) != expected[k] for k in ("uid", "launch_id")
                ):
                    return {
                        "epoch": self.epoch,
                        "state": "FENCED",
                        "reason": "role instance identity mismatch",
                    }
                task["reports"][rank] = payload
                task["seen"][rank] = now
                if payload.get("error"):
                    self._stop("FAILED", payload["error"])
            elif operation != "status":
                raise ValueError("unknown workspace task operation")
            if task["state"] not in TERMINAL:
                self._advance(now)
            return {
                "epoch": self.epoch,
                "run_id": run_id,
                "state": task["state"],
                "outcome": task["outcome"],
                "reason": task["reason"],
            }

    def _advance(self, now):
        t = self.task
        members, reports = t["members"], t["reports"]
        if any(now - seen > 60 for seen in t["seen"].values()):
            self._stop("FAILED", "worker control heartbeat expired")
        if t["state"] == "PREPARING":
            if set(reports) == set(members):
                t.update(
                    state="SERVICES_STARTING",
                    deadline=now + t["ready_timeout"],
                )
        if t["state"] == "SERVICES_STARTING":
            if all(reports.get(r, {}).get("services_ready") for r in members):
                t.update(
                    state="TRAINER_STARTING", deadline=now + t["ready_timeout"]
                )
        trainers = [r for r, m in members.items() if "trainer" in m["roles"]]
        if t["state"] == "TRAINER_STARTING":
            if all(reports.get(r, {}).get("initialized") for r in trainers):
                t["state"] = "RUNNING"
        if t["state"] in ("TRAINER_STARTING", "RUNNING"):
            done = [
                r for r in trainers if reports.get(r, {}).get("trainer_done")
            ]
            if done and t["first_done"] is None:
                t["first_done"] = now
            if len(done) == len(trainers):
                self._stop("SUCCEEDED", "all trainers completed")
            elif t["first_done"] is not None and now - t["first_done"] > 60:
                self._stop("FAILED", "trainer completion window expired")
        if (
            t["state"]
            in ("PREPARING", "SERVICES_STARTING", "TRAINER_STARTING")
            and now > t["deadline"]
        ):
            self._stop("FAILED", "startup/initialization timeout")
        if t["state"] == "STOPPING" and all(
            reports.get(r, {}).get("cleaned") for r in members
        ):
            t["state"] = t["outcome"]
