# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
"""python -m dlrover.dashboard.elastic --help"""

import argparse
import json
import os
import signal
import sqlite3
import sys
import threading
from pathlib import Path

from .aggregate import DashboardService
from .prometheus import PrometheusBenefits
from .server import ElasticAPI
from .store import create_store


def import_events(store, stream):
    inserted = skipped = 0
    for line_number, line in enumerate(stream, 1):
        if not line.strip():
            continue
        if "VerticalElasticEvent " in line:
            line = line.split("VerticalElasticEvent ", 1)[1]
        elif not line.lstrip().startswith("{"):
            skipped += 1
            continue
        try:
            inserted += store.append(json.loads(line))
        except (ValueError, TypeError) as exc:
            raise ValueError("Invalid event on line %d" % line_number) from exc
    return {"inserted": inserted, "unrelated_lines": skipped}


def export_events(path, stream):
    """Stream original SQLite events read-only for migration to MySQL."""
    db = sqlite3.connect(Path(path).absolute().as_uri() + "?mode=ro", uri=True)
    count = 0
    try:
        for (payload,) in db.execute("SELECT payload FROM events ORDER BY seq"):
            stream.write(payload + "\n")
            count += 1
    finally:
        db.close()
    return count


def main():
    parser = argparse.ArgumentParser(description="Durable elastic dashboard API and log importer")
    parser.add_argument("--storage", choices=("mysql", "sqlite"),
                        default=os.getenv("DLROVER_ELASTIC_STORAGE"))
    parser.add_argument("--db", help="Local SQLite path (development compatibility)")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init-db", help="Create MySQL tables before starting API replicas")
    serve = commands.add_parser("serve")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    serve.add_argument("--prometheus-url", default=os.getenv("DLROVER_PROMETHEUS_URL"))
    imp = commands.add_parser("import-events")
    imp.add_argument("file", help="Master log / JSONL, or - for stdin")
    snapshot = commands.add_parser("import-job")
    snapshot.add_argument("file", help="Platform lifecycle snapshot JSON")
    export = commands.add_parser("export-events", help="Read-only SQLite export for MySQL migration")
    export.add_argument("file", help="New JSONL output file; existing files are never overwritten")
    args = parser.parse_args()
    storage = args.storage or ("sqlite" if args.db else "mysql")
    if args.command == "export-events":
        if storage != "sqlite" or not args.db or not Path(args.db).is_file():
            parser.error("export-events requires an existing SQLite --db")
        with open(args.file, "x", encoding="utf-8") as stream:
            count = export_events(args.db, stream)
        print(json.dumps({"exported": count}))
        return
    store = create_store(storage, args.db)
    if args.command == "init-db":
        if storage == "mysql":
            store.initialize()
        store.health()
        print("Elastic event schema is ready")
    elif args.command == "import-events":
        if args.file == "-":
            result = import_events(store, sys.stdin)
        else:
            with open(args.file, encoding="utf-8") as stream:
                result = import_events(store, stream)
        print(json.dumps(result))
    elif args.command == "import-job":
        with open(args.file, encoding="utf-8") as stream:
            print(json.dumps({"updated": store.register(json.load(stream))}))
    else:
        prometheus = PrometheusBenefits(args.prometheus_url, os.getenv("DLROVER_PROMETHEUS_TOKEN")) if args.prometheus_url else None
        service = DashboardService(store, prometheus)
        with ElasticAPI((args.host, args.port), service) as server:
            def stop(*_):
                server.draining = True
                # shutdown cannot run in the serve_forever thread.
                threading.Thread(target=server.shutdown, daemon=True).start()
            signal.signal(signal.SIGTERM, stop)
            print("Elastic dashboard API listening on %s:%d" % server.server_address[:2], flush=True)
            try:
                server.serve_forever()
            except KeyboardInterrupt:
                pass


if __name__ == "__main__":
    main()
