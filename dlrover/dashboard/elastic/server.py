# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Small standalone API. Put user/tenant authorization in the same-origin gateway."""

import json
import sqlite3
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

from dlrover.python.common.log import default_logger as logger

from .baselines import preview
from .store import StoreUnavailable


class ElasticAPI(ThreadingHTTPServer):
    # server_close waits for bounded in-flight requests during SIGTERM draining.
    daemon_threads = False

    def __init__(self, address, service):
        self.service = service
        self.draining = False
        super().__init__(address, Handler)


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        self.connection.settimeout(10)

    def log_message(self, *_):
        # Avoid logging identity-bearing paths.
        pass

    def send_json(self, status, body):
        elapsed_ms = (time.monotonic() - self.request_started) * 1000
        if status >= 400:
            logger.warning(
                "Elastic API %s %s status=%d reason=%s elapsed_ms=%.1f",
                self.command, self.operation, status,
                body.get("code", body.get("status")), elapsed_ms,
            )
        elif "inserted" in body:
            logger.info(
                "Elastic API %s %s status=%d inserted=%s elapsed_ms=%.1f",
                self.command, self.operation, status, body["inserted"], elapsed_ms,
            )
        elif self.operation not in ("healthz", "readyz"):
            logger.info(
                "Elastic API %s %s status=%d elapsed_ms=%.1f",
                self.command, self.operation, status, elapsed_ms,
            )
        payload = json.dumps(body, ensure_ascii=False, allow_nan=False).encode(
            "utf-8"
        )
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        self.request_started = time.monotonic()
        self.operation = "unknown"
        path = urlsplit(self.path).path
        if path == "/healthz":
            self.operation = "healthz"
            return self.send_json(200, {"status": "alive"})
        if path == "/readyz":
            self.operation = "readyz"
            try:
                if self.server.draining:
                    raise StoreUnavailable("Draining")
                self.server.service.store.health()
                return self.send_json(200, {"status": "ready"})
            except (StoreUnavailable, sqlite3.Error):
                return self.send_json(503, {"status": "not_ready"})
        try:
            if path == "/api/v1/elastic/baselines":
                self.operation = "baselines"
                query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
                items = self.server.service.store.baselines(
                    query.get("cluster", [""])[0], query.get("kind", [None])[0],
                    query.get("resource_pool", [None])[0],
                )
                return self.send_json(200, {"items": items})
            if path == "/api/v1/elastic/jobs":
                self.operation = "jobs"
                return self.send_json(200, self.server.service.jobs())
            if path == "/api/v1/elastic/jct-statistics":
                self.operation = "jct-statistics"
                query = parse_qs(
                    urlsplit(self.path).query, keep_blank_values=True
                )
                if any(
                    key not in ("cluster", "namespace", "submitted_from", "submitted_to", "elasticity_enabled")
                    or len(values) != 1
                    or not values[0]
                    or len(values[0]) > 512
                    for key, values in query.items()
                ):
                    return self.send_json(422, {"code": "INVALID_FILTER"})
                try:
                    filters = {key: values[0] for key, values in query.items()}
                    for key in ("submitted_from", "submitted_to"):
                        if key in filters:
                            filters[key] = float(filters[key])
                    result = self.server.service.jct_statistics(**filters)
                except ValueError:
                    return self.send_json(422, {"code": "INVALID_FILTER"})
                return self.send_json(200, result)
            prefix, suffix = "/api/v1/elastic/jobs/", "/dashboard"
            if path.startswith(prefix) and path.endswith(suffix):
                self.operation = "job-dashboard"
                uid = unquote(
                    path[len(prefix) : -len(suffix)], errors="strict"
                )
                if not uid or len(uid) > 512:
                    return self.send_json(422, {"code": "INVALID_JOB_UID"})
                return self.send_json(200, self.server.service.dashboard(uid))
            self.send_json(404, {"code": "NOT_FOUND"})
        except KeyError:
            self.send_json(404, {"code": "JOB_NOT_FOUND"})
        except (ValueError, sqlite3.Error, StoreUnavailable) as exc:
            logger.warning("Dashboard read failed operation=%s error=%s", self.operation, type(exc).__name__)
            self.send_json(503, {"code": "DATA_UNAVAILABLE"})

    def do_POST(self):
        # Internal component endpoint for Master and platform collectors.
        self.request_started = time.monotonic()
        self.operation = "unknown"
        path = urlsplit(self.path).path
        if path not in (
            "/api/v1/elastic/baselines",
            "/api/v1/elastic/baselines/preview",
            "/api/v1/elastic/events",
            "/api/v1/elastic/job-snapshots",
        ):
            return self.send_json(404, {"code": "NOT_FOUND"})
        self.operation = path.rsplit("/", 1)[1]
        uid = None
        try:
            if self.headers.get("Transfer-Encoding"):
                return self.send_json(400, {"code": "CONTENT_LENGTH_REQUIRED"})
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 1024 * 1024:
                return self.send_json(413, {"code": "INVALID_BODY_SIZE"})
            self.connection.settimeout(5)
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise ValueError("Incomplete body")
            value = json.loads(raw)
            if path.startswith("/api/v1/elastic/baselines"):
                try:
                    snapshot = preview(self.server.service, value)
                except ValueError as exc:
                    return self.send_json(422, {"code": "INVALID_BASELINE", "message": str(exc)})
                if path.endswith("/preview"):
                    return self.send_json(200, snapshot)
                if value.get("preview_id") != snapshot["id"]:
                    return self.send_json(409, {"message": "数据已变化，请重新计算后保存"})
                return self.send_json(200, self.server.service.store.save_baseline(snapshot))
            if isinstance(value, dict):
                identity = value.get("job") if path.endswith("/events") else value
                if isinstance(identity, dict) and isinstance(identity.get("job_uid"), str):
                    uid = identity["job_uid"][:512]
            store = self.server.service.store
            inserted = (
                store.append(value)
                if path.endswith("/events")
                else store.register(value)
            )
            log = logger.info if inserted else logger.debug
            log("Dashboard ingest operation=%s job_uid=%r changed=%s", self.operation, uid, inserted)
            self.send_json(200, {"recorded": True, "inserted": inserted})
        except (ValueError, TypeError, UnicodeError) as exc:
            logger.warning("Dashboard rejected record operation=%s job_uid=%r error=%s",
                           self.operation, uid, type(exc).__name__)
            self.send_json(422, {"code": "INVALID_RECORD"})
        except (sqlite3.Error, StoreUnavailable) as exc:
            logger.warning("Dashboard write failed operation=%s job_uid=%r error=%s",
                           self.operation, uid, type(exc).__name__)
            self.send_json(503, {"code": "STORE_UNAVAILABLE"})
