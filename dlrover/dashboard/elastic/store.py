# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
"""Local SQLite event ledger. Never share this database over NFS."""

import json
import math
import sqlite3
from contextlib import contextmanager
from pathlib import Path


def finite(value):
    return (
        isinstance(value, (int, float)) and not isinstance(value, bool)
        and math.isfinite(value)
    )


def text(value, name):
    if not isinstance(value, str) or not value.strip() or len(value) > 512:
        raise ValueError("Invalid " + name)
    return value


class StoreUnavailable(RuntimeError):
    """Transient storage failure, safe to translate to an HTTP 503."""


def event_values(event):
    if not isinstance(event, dict) or event.get("schema_version") != 1:
        raise ValueError("Invalid event schema")
    event_id = text(event.get("event_id"), "event_id")
    text(event.get("event_type"), "event_type")
    at = event.get("occurred_at")
    if not finite(at) or at < 0 or not isinstance(event.get("job"), dict):
        raise ValueError("Invalid event timestamp/job")
    for key in ("plan_id", "request_id", "phase", "action", "trigger_source",
                "execution_status", "recovery_status", "reason_code"):
        if event.get(key) is not None:
            text(event[key], key)
    payload = json.dumps(event, sort_keys=True, ensure_ascii=False, allow_nan=False)
    if len(payload.encode("utf-8")) > 1024 * 1024:
        raise ValueError("Event exceeds 1 MiB")
    return event_id, at, payload


def lifecycle_values(job):
    if not isinstance(job, dict):
        raise ValueError("Invalid job")
    status = job.get("status", "unknown")
    if status not in ("running", "completed", "failed", "unknown"):
        raise ValueError("Invalid lifecycle status")
    submitted, completed, observed = (job.get(k) for k in (
        "submitted_at", "completed_at", "lifecycle_observed_at"))
    for value in (submitted, completed, observed):
        if value is not None and (not finite(value) or value < 0):
            raise ValueError("Invalid lifecycle timestamp")
    if observed is None:
        raise ValueError("lifecycle_observed_at is required")
    if completed is not None and (status != "completed" or
                                  submitted is not None and completed < submitted):
        raise ValueError("Invalid completed lifecycle")
    return status, submitted, completed, observed


class EventStore:
    def __init__(self, path):
        self.path = str(Path(path).expanduser().absolute())
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                    uid TEXT PRIMARY KEY, name TEXT NOT NULL,
                    cluster TEXT NOT NULL, namespace TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'unknown',
                    submitted_at REAL, completed_at REAL,
                    lifecycle_observed_at REAL
                );
                CREATE TABLE IF NOT EXISTS events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    job_uid TEXT NOT NULL REFERENCES jobs(uid),
                    occurred_at REAL NOT NULL, payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS event_job_time
                    ON events(job_uid, occurred_at, seq);
            """)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=1)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _job(db, job):
        uid = text(job.get("job_uid"), "job_uid")
        cluster = text(job.get("cluster") or "default", "cluster")
        namespace = text(job.get("namespace") or "default", "namespace")
        name = text(job.get("job_name") or uid, "job_name")
        old = db.execute("SELECT * FROM jobs WHERE uid=?", (uid,)).fetchone()
        if old and (old["cluster"], old["namespace"]) != (cluster, namespace):
            raise ValueError("Job UID collision across cluster/namespace")
        db.execute(
            "INSERT OR IGNORE INTO jobs(uid,name,cluster,namespace) VALUES(?,?,?,?)",
            (uid, name, cluster, namespace),
        )
        if name != uid:
            db.execute("UPDATE jobs SET name=? WHERE uid=?", (name, uid))
        return uid

    def append(self, event):
        """An identical replay is a no-op; conflicting IDs fail, not overwrite."""
        event_id, at, payload = event_values(event)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            old = db.execute("SELECT payload FROM events WHERE event_id=?", (event_id,)).fetchone()
            if old:
                if old["payload"] != payload:
                    raise ValueError("Conflicting event_id")
                return False
            uid = self._job(db, event["job"])
            db.execute(
                "INSERT INTO events(event_id,job_uid,occurred_at,payload) VALUES(?,?,?,?)",
                (event_id, uid, at, payload),
            )
        return True

    def register(self, job):
        """Import an explicit platform lifecycle snapshot, never infer from plans."""
        status, submitted, completed, observed = lifecycle_values(job)
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            uid = self._job(db, job)
            old = db.execute("SELECT * FROM jobs WHERE uid=?", (uid,)).fetchone()
            if old["lifecycle_observed_at"] is not None:
                if observed < old["lifecycle_observed_at"]:
                    return False
                if observed == old["lifecycle_observed_at"]:
                    if (status, submitted, completed) != (old["status"], old["submitted_at"], old["completed_at"]):
                        raise ValueError("Conflicting lifecycle snapshot")
                    return False
            db.execute(
                "UPDATE jobs SET status=?,submitted_at=?,completed_at=?,"
                "lifecycle_observed_at=? WHERE uid=?",
                (status, submitted, completed, observed, uid),
            )
        return True

    def jobs(self):
        with self.connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM jobs ORDER BY name,uid")]

    def read(self, uid):
        with self.connect() as db:
            job = db.execute("SELECT * FROM jobs WHERE uid=?", (uid,)).fetchone()
            if not job:
                raise KeyError(uid)
            rows = db.execute(
                "SELECT payload FROM events WHERE job_uid=? ORDER BY occurred_at,seq LIMIT 10001",
                (uid,),
            ).fetchall()
            if len(rows) > 10000:
                raise ValueError("Job exceeds 10000 events; pagination is required")
            return dict(job), [json.loads(row[0]) for row in rows]

    def health(self):
        with self.connect() as db:
            db.execute("SELECT uid FROM jobs LIMIT 1").fetchall()
            db.execute("SELECT seq FROM events LIMIT 1").fetchall()


def create_store(storage="mysql", path=None, environ=None):
    if storage == "mysql":
        from .mysql_store import MySQLEventStore
        return MySQLEventStore.from_env(environ)
    if storage == "sqlite" and path:
        return EventStore(path)
    raise ValueError("SQLite requires --db; storage must be mysql or sqlite")
