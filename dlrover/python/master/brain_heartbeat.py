# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
"""Bounded Master registration and recoverable Brain heartbeats."""

import random
import threading
import time

from dlrover.brain.python.client.client import BrainRequestError
from dlrover.python.common.log import default_logger as logger


class BrainHeartbeat:
    INTERVAL = 10
    MAX_BACKOFF = 30

    def __init__(self, register, heartbeat):
        self._register = register
        self._heartbeat = heartbeat
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread = None
        self._started = False
        # Replace this tuple atomically so metric collection never does I/O.
        self._snapshot = ("disabled", 0, 0.0)

    @property
    def snapshot(self):
        if self._stop.is_set():
            return ("stopped", *self._snapshot[1:])
        return self._snapshot

    def start(self):
        with self._lock:
            if self._started or self._stop.is_set():
                return
            self._started = True
        delay = self._attempt(self._register)
        with self._lock:
            if delay is None or self._stop.is_set():
                return
            self._thread = threading.Thread(
                target=self._run,
                args=(delay,),
                name="masterservice_heartbeat_report",
                daemon=True,
            )
            self._thread.start()

    def _attempt(self, report):
        if self._stop.is_set():
            return None
        state, failures, last_success = self._snapshot
        try:
            if not report():
                raise BrainRequestError("Brain returned no successful response")
        except BrainRequestError as exc:
            if self._stop.is_set():
                return None
            failures += 1
            if not exc.retryable:
                self._snapshot = ("failed", failures, last_success)
                logger.error("Stop Brain heartbeat retries: %s", exc)
                return None
            self._snapshot = ("backoff", failures, last_success)
            if state != "backoff":
                logger.warning(
                    "Brain registration/heartbeat unavailable; "
                    "retrying with backoff: %s", exc,
                )
            delay = min(
                self.INTERVAL * 2 ** min(failures - 1, 2),
                self.MAX_BACKOFF,
            )
            # Jitter stays within the cap; honor a longer server retry delay.
            return max(random.uniform(0.8 * delay, delay), exc.retry_after or 0)
        except Exception:
            if self._stop.is_set():
                return None
            self._snapshot = ("failed", failures + 1, last_success)
            logger.exception("Stop Brain heartbeats after unexpected error")
            return None
        if self._stop.is_set():
            return None
        self._snapshot = ("healthy", 0, time.time())
        if state != "healthy":
            logger.info("Brain registration/heartbeat connection ready")
        return self.INTERVAL

    def _run(self, delay):
        while not self._stop.wait(delay):
            # Brain treats a heartbeat with an address as registration when
            # the previous owner has expired, including initial failures.
            delay = self._attempt(self._heartbeat)
            if delay is None:
                break

    def stop(self):
        self._stop.set()
        with self._lock:
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            # An in-flight HTTP request has its own connect/read timeouts.
            thread.join(timeout=1)
        _, failures, last_success = self._snapshot
        self._snapshot = ("stopped", failures, last_success)
