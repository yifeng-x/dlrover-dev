# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
"""MySQL ledger: per-request transactions, shared safely by multiple API Pods."""

import json
import os
from contextlib import contextmanager
from pathlib import Path

from .store import StoreUnavailable, event_values, lifecycle_values, text


def identity(value, name):
    result = text(value, name)
    if result != result.strip():
        raise ValueError("Whitespace is not allowed around " + name)
    return result


class MySQLEventStore:
    def __init__(self, config):
        try:
            import pymysql
        except ImportError as exc:
            raise RuntimeError("Install elastic dashboard requirements for MySQL") from exc
        self.driver = pymysql
        self._config = dict(config)

    @classmethod
    def from_env(cls, environ=None):
        env = os.environ if environ is None else environ
        def required(name):
            value = env.get("DLROVER_MYSQL_" + name)
            if not value:
                raise ValueError("Missing DLROVER_MYSQL_" + name)
            return value
        config = dict(host=required("HOST"), user=required("USER"),
                      password=required("PASSWORD"), database=required("DATABASE"),
                      port=int(env.get("DLROVER_MYSQL_PORT", "3306")),
                      connect_timeout=3, read_timeout=5, write_timeout=5,
                      charset="utf8mb4", autocommit=False,
                      init_command="SET SESSION innodb_lock_wait_timeout=3")
        if not 1 <= config["port"] <= 65535:
            raise ValueError("Invalid MySQL port")
        ca = env.get("DLROVER_MYSQL_SSL_CA")
        if ca:
            config.update(ssl_ca=ca, ssl_verify_cert=True, ssl_verify_identity=True)
        return cls(config)

    @contextmanager
    def connect(self):
        connection = None
        try:
            connection = self.driver.connect(
                **self._config, cursorclass=self.driver.cursors.DictCursor)
            yield connection
            connection.commit()
        except (self.driver.MySQLError, OSError):
            if connection:
                try:
                    connection.rollback()
                except self.driver.MySQLError:
                    pass
            # Do not expose connection strings, credentials or SQL in API/logs.
            raise StoreUnavailable("MySQL is unavailable") from None
        except BaseException:
            if connection:
                connection.rollback()
            raise
        finally:
            if connection:
                try:
                    connection.close()
                except self.driver.MySQLError:
                    pass

    def initialize(self):
        sql = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
        sql = "\n".join(line for line in sql.splitlines() if not line.startswith("--"))
        with self.connect() as db, db.cursor() as cursor:
            for statement in sql.split(";"):
                if statement.strip():
                    cursor.execute(statement)

    @staticmethod
    def _job(cursor, job):
        uid = identity(job.get("job_uid"), "job_uid")
        cluster = identity(job.get("cluster") or "default", "cluster")
        namespace = identity(job.get("namespace") or "default", "namespace")
        name = text(job.get("job_name") or uid, "job_name")
        # The upsert obtains the job row lock even on the first concurrent event.
        cursor.execute(
            "INSERT INTO elastic_jobs(uid,name,cluster,namespace) VALUES(%s,%s,%s,%s) "
            "ON DUPLICATE KEY UPDATE uid=elastic_jobs.uid", (uid, name, cluster, namespace))
        cursor.execute("SELECT * FROM elastic_jobs WHERE uid=%s FOR UPDATE", (uid,))
        old = cursor.fetchone()
        if (old["cluster"], old["namespace"]) != (cluster, namespace):
            raise ValueError("Job UID collision across cluster/namespace")
        if name != uid:
            cursor.execute("UPDATE elastic_jobs SET name=%s WHERE uid=%s", (name, uid))
        return uid, old

    def append(self, event):
        event_id, at, payload = event_values(event)
        identity(event_id, "event_id")
        with self.connect() as db, db.cursor() as cursor:
            uid, _ = self._job(cursor, event["job"])
            try:
                cursor.execute(
                    "INSERT INTO elastic_events(event_id,job_uid,occurred_at,payload) VALUES(%s,%s,%s,%s)",
                    (event_id, uid, at, payload))
            except self.driver.IntegrityError as exc:
                if exc.args[0] != 1062:
                    raise
                cursor.execute("SELECT payload FROM elastic_events WHERE event_id=%s FOR UPDATE", (event_id,))
                old = cursor.fetchone()
                if not old or old["payload"] != payload:
                    raise ValueError("Conflicting event_id") from None
                return False
        return True

    def register(self, job):
        status, submitted, completed, observed = lifecycle_values(job)
        with self.connect() as db, db.cursor() as cursor:
            uid, old = self._job(cursor, job)
            previous = old["lifecycle_observed_at"]
            if previous is not None:
                if observed < previous:
                    return False
                if observed == previous:
                    if (status, submitted, completed) != (old["status"], old["submitted_at"], old["completed_at"]):
                        raise ValueError("Conflicting lifecycle snapshot")
                    return False
            cursor.execute(
                "UPDATE elastic_jobs SET status=%s,submitted_at=%s,completed_at=%s,"
                "lifecycle_observed_at=%s WHERE uid=%s",
                (status, submitted, completed, observed, uid))
        return True

    def jobs(self):
        with self.connect() as db, db.cursor() as cursor:
            cursor.execute("SELECT * FROM elastic_jobs ORDER BY name,uid")
            return list(cursor.fetchall())

    def read(self, uid):
        with self.connect() as db, db.cursor() as cursor:
            cursor.execute("SELECT * FROM elastic_jobs WHERE uid=%s", (uid,))
            job = cursor.fetchone()
            if not job:
                raise KeyError(uid)
            cursor.execute(
                "SELECT payload FROM elastic_events WHERE job_uid=%s ORDER BY occurred_at,seq LIMIT 10001", (uid,))
            rows = cursor.fetchall()
            if len(rows) > 10000:
                raise ValueError("Job exceeds 10000 events; pagination is required")
            return job, [json.loads(row["payload"]) for row in rows]

    def health(self):
        with self.connect() as db, db.cursor() as cursor:
            # Verify connectivity and schema, not just a TCP connection.
            cursor.execute("SELECT uid FROM elastic_jobs LIMIT 1")
            cursor.fetchall()
            cursor.execute("SELECT seq FROM elastic_events LIMIT 1")
            cursor.fetchall()
