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

"""Start the internal elastic dashboard API backed by MySQL."""

import argparse
import os
import signal
import threading
from contextlib import ExitStack

from dlrover.python.common.log import default_logger as logger

from .aggregate import DashboardService
from .prometheus import PrometheusBenefits
from .server import ElasticAPI


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="DLRover Elastic Dashboard Server"
    )
    parser.add_argument("--host", default="0.0.0.0", help="HTTP bind address")
    parser.add_argument(
        "--port", type=int, default=8080, help="HTTP listen port"
    )
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    return args


def main():
    args = parse_args()
    from .mysql_store import MySQLEventStore

    with ExitStack() as cleanup:
        store = MySQLEventStore.from_env()
        cleanup.callback(store.close)
        prometheus_url = os.getenv("DLROVER_PROMETHEUS_URL")
        prometheus = (
            PrometheusBenefits(
                prometheus_url, os.getenv("DLROVER_PROMETHEUS_TOKEN")
            )
            if prometheus_url
            else None
        )
        service = DashboardService(store, prometheus)
        with ElasticAPI((args.host, args.port), service) as server:

            def stop(*_):
                server.draining = True
                # shutdown cannot run in the serve_forever thread.
                threading.Thread(target=server.shutdown, daemon=True).start()

            signal.signal(signal.SIGTERM, stop)
            logger.info(
                "Elastic dashboard API listening on %s:%d",
                *server.server_address[:2],
            )
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass
        logger.info("Elastic dashboard API stopped")


if __name__ == "__main__":
    main()
