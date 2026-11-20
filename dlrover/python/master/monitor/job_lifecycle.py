# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

"""Temporary, opt-in lifecycle snapshots for integration testing only.

One writer per job is required. CR creation to Master-confirmed completion is
not platform submission-to-completion JCT. Delivery is bounded best effort.
"""

import json
import os
import threading
import time
from datetime import datetime
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from dlrover.python.common.job_classification import MASTER_JOB_TYPE, classify_job
from dlrover.python.common.log import default_logger as logger


class MasterLifecycle:
    def __init__(self, args, url, clock=time.time, send=None):
        parsed = urlparse(url)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname
                or parsed.username or parsed.password or parsed.query
                or parsed.fragment
                or parsed.path != "/api/v1/elastic/job-snapshots"):
            raise ValueError("Invalid lifecycle endpoint")
        metadata = (getattr(args, "raw_job", None) or {}).get("metadata", {})
        if not args.job_uuid or metadata.get("uid") != args.job_uuid:
            raise ValueError("Lifecycle requires matching ElasticJob UID")
        submitted = None
        try:
            value = datetime.fromisoformat(metadata["creationTimestamp"].replace("Z", "+00:00"))
            if value.tzinfo is not None and 0 <= value.timestamp() <= clock():
                submitted = value.timestamp()
        except (KeyError, TypeError, ValueError, AttributeError):
            pass
        self.identity = dict(job_uid=args.job_uuid, job_name=args.job_name,
                             cluster=args.cluster, namespace=args.namespace,
                             submitted_at=submitted)
        master_job = dict(getattr(args, "raw_job", None) or {}, kind=MASTER_JOB_TYPE)
        self.identity.update(classify_job(master_job,
                             getattr(args, "resource_monitor_switch", None),
                             getattr(args, "configmap_manual_scale_switch", None)))
        self.url, self.clock = url, clock
        self.send = send or self._send
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._terminal = False
        self._closed = False
        self._snapshot = self._make("running", None)
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="temporary-job-lifecycle")

    @classmethod
    def start_if_enabled(cls, args):
        if os.getenv("DLROVER_MASTER_LIFECYCLE_ENABLED", "false").strip().lower() != "true":
            return None
        try:
            reporter = cls(args, os.getenv("DLROVER_MASTER_LIFECYCLE_URL", ""))
            reporter._thread.start()
            logger.warning("Temporary Master lifecycle enabled: CR-to-Master timing; disable platform writer for these jobs")
            return reporter
        except Exception:
            logger.warning("Temporary Master lifecycle unavailable; training continues")
            return None

    def _make(self, status, completed):
        return dict(self.identity, status=status, completed_at=completed,
                    lifecycle_observed_at=self.clock())

    def _send(self, snapshot):
        request = Request(self.url, data=json.dumps(snapshot).encode(),
                          headers={"Content-Type": "application/json"})
        with urlopen(request, timeout=1) as response:
            body = response.read(4097)
            if len(body) > 4096 or json.loads(body).get("recorded") is not True:
                raise ValueError("Invalid lifecycle acknowledgement")

    def _run(self):
        while True:
            with self._lock:
                if self._closed:
                    return
                snapshot = dict(self._snapshot)
                terminal = self._terminal
            # Retries preserve the original observation time and payload.
            for _ in range(2):
                try:
                    self.send(snapshot)
                    break
                except Exception:
                    logger.warning("Temporary lifecycle delivery failed")
            if terminal:
                return
            self._wake.wait(60)
            self._wake.clear()

    def finish(self, status=None):
        """Unknown/interrupted exits stop reporting without inventing success."""
        with self._lock:
            if self._terminal or self._closed:
                return
            if status in ("completed", "failed"):
                snapshot = self._make(status, self.clock() if status == "completed" else None)
                # Keep snapshots ordered even on low-resolution clocks.
                snapshot["lifecycle_observed_at"] = max(
                    snapshot["lifecycle_observed_at"],
                    self._snapshot["lifecycle_observed_at"] + 0.000001)
                self._snapshot = snapshot
                self._terminal = True
            else:
                self._closed = True
        self._wake.set()
        self._thread.join(timeout=2)
