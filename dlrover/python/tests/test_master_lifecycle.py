# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

from types import SimpleNamespace
import threading

from dlrover.python.master.monitor.job_lifecycle import MasterLifecycle


def args(created="1970-01-01T00:00:10Z"):
    return SimpleNamespace(job_uuid="u", job_name="job", cluster="c",
                           namespace="n", raw_job={"metadata": {
                               "uid": "u", "creationTimestamp": created}})


URL = "http://localhost:8080/api/v1/elastic/job-snapshots"


def test_disabled_does_not_start(monkeypatch):
    monkeypatch.delenv("DLROVER_MASTER_LIFECYCLE_ENABLED", raising=False)
    assert MasterLifecycle.start_if_enabled(args()) is None


def test_retries_preserve_snapshot_and_terminal_order():
    delivered = []
    started = threading.Event()

    def send(snapshot):
        delivered.append(snapshot)
        started.set()
        if len(delivered) == 1:
            raise OSError("temporary failure")

    reporter = MasterLifecycle(args(), URL, clock=lambda: 100, send=send)
    reporter._thread.start()
    assert started.wait(1)
    reporter.finish("completed")
    assert not reporter._thread.is_alive()
    assert delivered[0] == delivered[1]
    assert delivered[-1]["status"] == "completed"
    assert delivered[-1]["submitted_at"] == 10
    assert delivered[-1]["completed_at"] == 100
    assert delivered[-1]["lifecycle_observed_at"] > delivered[0]["lifecycle_observed_at"]


def test_interrupt_does_not_report_success():
    delivered = []
    ready = threading.Event()
    def send(snapshot):
        delivered.append(snapshot)
        ready.set()
    reporter = MasterLifecycle(args("invalid"), URL, send=send)
    reporter._thread.start()
    assert ready.wait(1)
    reporter.finish()
    assert all(s["status"] == "running" for s in delivered)
    assert delivered[0]["submitted_at"] is None


def test_failure_has_no_completion_time():
    delivered = []
    reporter = MasterLifecycle(args(), URL, send=delivered.append)
    reporter._thread.start()
    reporter.finish("failed")
    assert delivered[-1]["status"] == "failed"
    assert delivered[-1]["completed_at"] is None
