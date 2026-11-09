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
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlsplit

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
        path = urlsplit(self.path).path
        if path == "/healthz":
            return self.send_json(200, {"status": "alive"})
        if path == "/readyz":
            try:
                if self.server.draining:
                    raise StoreUnavailable("Draining")
                self.server.service.store.health()
                return self.send_json(200, {"status": "ready"})
            except (StoreUnavailable, sqlite3.Error):
                return self.send_json(503, {"status": "not_ready"})
        try:
            if path == "/api/v1/elastic/jobs":
                return self.send_json(200, self.server.service.jobs())
            if path == "/api/v1/elastic/jct-statistics":
                query = parse_qs(
                    urlsplit(self.path).query, keep_blank_values=True
                )
                if any(
                    key not in ("cluster", "namespace")
                    or len(values) != 1
                    or not values[0]
                    or len(values[0]) > 512
                    for key, values in query.items()
                ):
                    return self.send_json(422, {"code": "INVALID_FILTER"})
                return self.send_json(
                    200,
                    self.server.service.jct_statistics(
                        **{key: values[0] for key, values in query.items()}
                    ),
                )
            prefix, suffix = "/api/v1/elastic/jobs/", "/dashboard"
            if path.startswith(prefix) and path.endswith(suffix):
                uid = unquote(
                    path[len(prefix) : -len(suffix)], errors="strict"
                )
                if not uid or len(uid) > 512:
                    return self.send_json(422, {"code": "INVALID_JOB_UID"})
                return self.send_json(200, self.server.service.dashboard(uid))
            self.send_json(404, {"code": "NOT_FOUND"})
        except KeyError:
            self.send_json(404, {"code": "JOB_NOT_FOUND"})
        except (ValueError, sqlite3.Error, StoreUnavailable):
            self.send_json(503, {"code": "DATA_UNAVAILABLE"})

    def do_POST(self):
        # Internal component endpoint for Master and platform collectors.
        path = urlsplit(self.path).path
        if path not in (
            "/api/v1/elastic/events",
            "/api/v1/elastic/job-snapshots",
        ):
            return self.send_json(404, {"code": "NOT_FOUND"})
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
            store = self.server.service.store
            inserted = (
                store.append(value)
                if path.endswith("/events")
                else store.register(value)
            )
            self.send_json(200, {"recorded": True, "inserted": inserted})
        except (ValueError, TypeError, UnicodeError):
            self.send_json(422, {"code": "INVALID_RECORD"})
        except (sqlite3.Error, StoreUnavailable):
            self.send_json(503, {"code": "STORE_UNAVAILABLE"})
