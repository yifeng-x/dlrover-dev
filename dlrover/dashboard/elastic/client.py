# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
"""Opt-in Master -> API event sink. Original logs remain the replay source."""

import json
import queue
import threading
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


class HTTPEventSink:
    def __init__(self, url, timeout=1):
        parsed = urlparse(url)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname
                or parsed.username or parsed.password or parsed.query or parsed.fragment
                or parsed.path != "/api/v1/elastic/events"):
            raise ValueError("Configure an events endpoint")
        self.url, self.timeout = url, timeout

    def __call__(self, event):
        payload = json.dumps(event, allow_nan=False, ensure_ascii=False).encode("utf-8")
        self.send_payload(payload)

    def send_payload(self, payload):
        request = Request(self.url, data=payload, headers={
            "Content-Type": "application/json"})
        # Reuse the event ID on retry: a timeout may follow a committed write.
        for attempt in range(2):
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    body = response.read(4097)
                    if len(body) > 4096 or json.loads(body).get("recorded") is not True:
                        raise ValueError("Invalid event acknowledgement")
                return
            except HTTPError as exc:
                if attempt or exc.code not in (408, 429, 500, 502, 503, 504):
                    raise RuntimeError("Event service rejected delivery") from None
            except (URLError, TimeoutError, OSError):
                if attempt:
                    raise RuntimeError("Event service unavailable") from None


class AsyncHTTPEventSink:
    """Bounded best-effort delivery; controller threads never wait on network I/O.

    Logs are the replay source. Full queues and oversized events are dropped
    from HTTP delivery. A stuck DNS/socket call occupies only this daemon,
    never creates additional threads, and cannot delay process termination.
    """

    def __init__(self, url, capacity=32):
        if capacity <= 0:
            raise ValueError("Event queue capacity must be positive")
        self._client = HTTPEventSink(url)
        self._queue = queue.Queue(maxsize=capacity)
        self.dropped = 0
        self.failed = 0
        self._worker = threading.Thread(
            target=self._run, name="elastic-event-delivery", daemon=True)
        self._worker.start()

    def __call__(self, event):
        payload = json.dumps(event, allow_nan=False, ensure_ascii=False).encode("utf-8")
        if len(payload) > 1024 * 1024:
            self.dropped += 1
            return
        try:
            self._queue.put_nowait(payload)
        except queue.Full:
            self.dropped += 1

    def _run(self):
        while True:
            payload = self._queue.get()
            try:
                self._client.send_payload(payload)
            except Exception:
                # Never propagate delivery failures to control/recovery paths.
                self.failed += 1
            finally:
                self._queue.task_done()
