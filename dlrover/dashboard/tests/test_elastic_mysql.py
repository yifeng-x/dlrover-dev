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

"""Set DLROVER_TEST_MYSQL=1 and DLROVER_MYSQL_* for real MySQL integration."""

import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

import pytest
from sqlalchemy import delete, event, text
from sqlalchemy.exc import OperationalError, TimeoutError

from dlrover.dashboard.elastic.db import Database
from dlrover.dashboard.elastic.models import ElasticEvent, ElasticJob
from dlrover.dashboard.elastic.mysql_store import MySQLEventStore
from dlrover.dashboard.elastic.store import StoreUnavailable, create_store


def config(**changes):
    return dict(
        DLROVER_MYSQL_HOST="db",
        DLROVER_MYSQL_DATABASE="elastic_test",
        DLROVER_MYSQL_USER="test",
        DLROVER_MYSQL_PASSWORD="test-only",
        **changes,
    )


def test_mysql_configuration_requires_credentials_and_verifies_tls(
    monkeypatch,
):
    create = MagicMock()
    monkeypatch.setattr("dlrover.dashboard.elastic.db.create_engine", create)
    secret = "test@:/?#only"
    env = config(DLROVER_MYSQL_SSL_CA="/ca.crt")
    env["DLROVER_MYSQL_PASSWORD"] = secret
    database = Database.from_env(env)
    url = create.call_args.args[0]
    options = create.call_args.kwargs
    assert url.password == secret
    assert secret not in str(url)
    assert options["pool_size"] == 20
    assert options["max_overflow"] == 10
    assert options["pool_timeout"] == 3
    assert options["pool_recycle"] == 200
    assert options["pool_pre_ping"] is True
    assert options["hide_parameters"] is True
    assert options["connect_args"]["ssl_verify_identity"] is True
    assert options["connect_args"]["ssl_verify_cert"] is True
    assert options["connect_args"]["connect_timeout"] == 3
    assert database.session_factory.kw["autoflush"] is False
    with pytest.raises(ValueError, match="PASSWORD"):
        Database.from_env(
            {"DLROVER_MYSQL_HOST": "db", "DLROVER_MYSQL_USER": "u"}
        )
    with pytest.raises(ValueError, match="port"):
        Database.from_env(config(DLROVER_MYSQL_PORT="99999"))


def test_mysql_engine_is_lazy_and_does_not_connect_at_startup(monkeypatch):
    import pymysql

    connect = MagicMock(side_effect=AssertionError("Unexpected startup I/O"))
    monkeypatch.setattr(pymysql, "connect", connect)
    store = MySQLEventStore.from_env(config())
    try:
        assert store.database.engine.pool.checkedout() == 0
        connect.assert_not_called()
    finally:
        store.close()


@pytest.mark.parametrize(
    "failure",
    [
        OperationalError("private SQL", {}, Exception("private connection")),
        TimeoutError("pool exhausted"),
    ],
)
def test_mysql_unavailable_is_sanitized(monkeypatch, tmp_path, failure):
    database = Database("sqlite:///" + str(tmp_path / "pool.db"))
    store = MySQLEventStore(database)
    monkeypatch.setattr(
        database.engine, "connect", MagicMock(side_effect=failure)
    )
    with pytest.raises(StoreUnavailable) as error:
        store.health()
    assert str(error.value) == "MySQL is unavailable"
    assert error.value.__suppress_context__
    store.close()


def test_transactions_rollback_and_return_connections_to_pool(tmp_path):
    database = Database("sqlite:///" + str(tmp_path / "pool.db"))
    store = MySQLEventStore(database)
    opened = []
    event.listen(database.engine, "connect", lambda *args: opened.append(1))
    try:
        with store.connect() as session:
            session.execute(
                text("CREATE TABLE records (uid INTEGER PRIMARY KEY)")
            )
        with store.connect() as session:
            session.execute(text("INSERT INTO records VALUES (1)"))
        with pytest.raises(ValueError):
            with store.connect() as session:
                session.execute(text("INSERT INTO records VALUES (2)"))
                raise ValueError("Conflicting event_id")
        with store.connect() as session:
            assert session.execute(
                text("SELECT uid FROM records")
            ).scalars().all() == [1]
        assert len(opened) == 1
        assert database.engine.pool.checkedout() == 0
    finally:
        store.close()


def test_storage_factory_keeps_explicit_sqlite_compatibility(tmp_path):
    store = create_store("sqlite", str(tmp_path / "legacy.db"))
    store.health()
    assert store.jobs() == []
    with pytest.raises(ValueError):
        create_store("sqlite")


@pytest.fixture
def mysql():
    if os.getenv("DLROVER_TEST_MYSQL") != "1":
        pytest.skip(
            "Real MySQL: set DLROVER_TEST_MYSQL=1 and connection variables"
        )
    # A requested integration run fails, rather than silently skipping a missing driver/DB.
    store = MySQLEventStore.from_env()
    store.initialize()
    yield store
    store.close()


@pytest.fixture
def mysql_job(mysql):
    uid = "test-" + uuid.uuid4().hex
    yield dict(
        job_uid=uid, job_name="中文训练", cluster="test", namespace="test"
    )
    with mysql.connect() as session:
        session.execute(
            delete(ElasticEvent).where(ElasticEvent.job_uid == uid)
        )
        session.execute(delete(ElasticJob).where(ElasticJob.uid == uid))


def record(job, event_id):
    return dict(
        schema_version=1,
        event_id=event_id,
        occurred_at=100,
        event_type="phase_changed",
        plan_id="plan",
        job=job,
    )


def test_real_mysql_two_replicas_deduplicate_concurrently(mysql, mysql_job):
    second = MySQLEventStore.from_env()
    value = record(mysql_job, "event-" + uuid.uuid4().hex)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(
            pool.map(
                lambda index: (mysql if index % 2 else second).append(value),
                range(16),
            )
        )
    assert sum(results) == 1
    assert second.read(mysql_job["job_uid"])[1] == [value]
    with pytest.raises(ValueError, match="Conflicting"):
        second.append({**value, "occurred_at": 101})
    second.close()


def test_real_mysql_lifecycle_is_monotonic_under_concurrency(mysql, mysql_job):
    def update(index):
        return mysql.register(
            dict(
                mysql_job,
                status="completed" if index == 15 else "running",
                submitted_at=100,
                completed_at=200 if index == 15 else None,
                lifecycle_observed_at=200 + index,
            )
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(update, range(16)))
    stored, _ = mysql.read(mysql_job["job_uid"])
    assert stored["status"] == "completed"
    assert stored["lifecycle_observed_at"] == 215


def test_real_mysql_duplicate_id_does_not_create_orphan_job(mysql, mysql_job):
    value = record(mysql_job, "event-" + uuid.uuid4().hex)
    mysql.append(value)
    other = dict(mysql_job, job_uid="uncommitted-" + uuid.uuid4().hex)
    with pytest.raises(ValueError, match="Conflicting"):
        mysql.append({**value, "job": other})
    with pytest.raises(KeyError):
        mysql.read(other["job_uid"])


def test_real_mysql_case_sensitive_ids_and_ordering(mysql, mysql_job):
    event_id = uuid.uuid4().hex
    mysql.append(record(mysql_job, event_id + "A"))
    mysql.append({**record(mysql_job, event_id + "a"), "occurred_at": 90})
    records = mysql.read(mysql_job["job_uid"])[1]
    assert [r["occurred_at"] for r in records] == [90, 100]
    with pytest.raises(ValueError, match="collision"):
        mysql.append(
            {**record(dict(mysql_job, namespace="other"), uuid.uuid4().hex)}
        )


def test_orm_models_compile_to_existing_mysql_schema():
    from sqlalchemy.dialects import mysql
    from sqlalchemy.schema import CreateTable

    from dlrover.dashboard.elastic.models import ElasticEvent, ElasticJob

    dialect = mysql.dialect()
    job_ddl = str(CreateTable(ElasticJob.__table__).compile(dialect=dialect))
    event_ddl = str(
        CreateTable(ElasticEvent.__table__).compile(dialect=dialect)
    )
    assert "DEFAULT 'unknown'" in job_ddl
    assert "utf8mb4_bin" in job_ddl
    assert "BIGINT UNSIGNED NOT NULL AUTO_INCREMENT" in event_ddl
    assert "LONGTEXT NOT NULL" in event_ddl
    assert "uq_elastic_event_id" in event_ddl
    assert "fk_elastic_event_job" in event_ddl

    session = MagicMock()
    session.scalars.return_value.one.return_value = ElasticJob(
        uid="job",
        name="job",
        cluster="c",
        namespace="n",
    )
    MySQLEventStore._job(
        session, dict(job_uid="job", cluster="c", namespace="n")
    )
    upsert = str(session.execute.call_args.args[0].compile(dialect=dialect))
    query = str(session.scalars.call_args.args[0].compile(dialect=dialect))
    assert "ON DUPLICATE KEY UPDATE uid = elastic_jobs.uid" in upsert
    assert "FOR UPDATE" in query


def test_orm_round_trip_updates_ordering_and_rollback(tmp_path, monkeypatch):
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    from dlrover.dashboard.elastic.models import ElasticJob

    # Only adapt the dialect-specific upsert. These SQLite tests verify ORM
    # behavior, not MySQL locking (covered by opt-in integration tests below).
    class SQLiteUpsert:
        def __init__(self, model):
            self.statement = sqlite_insert(model)

        def values(self, **values):
            self.statement = self.statement.values(**values)
            return self

        def on_duplicate_key_update(self, **values):
            return self.statement.on_conflict_do_update(
                index_elements=["uid"],
                set_=values,
            )

    monkeypatch.setattr(
        "dlrover.dashboard.elastic.mysql_store.insert", SQLiteUpsert
    )
    store = MySQLEventStore(Database("sqlite:///" + str(tmp_path / "orm.db")))
    try:
        store.initialize()
        store.health()
        job = dict(
            job_uid="job", job_name="中文训练", cluster="c", namespace="n"
        )
        first = record(job, "first")
        second = dict(record(job, "second"), occurred_at=90)
        assert store.append(first)
        assert store.append(second)
        assert store.read("job")[1] == [second, first]
        snapshot = dict(
            job,
            status="completed",
            submitted_at=10,
            completed_at=120,
            lifecycle_observed_at=121,
        )
        assert store.register(snapshot)
        assert not store.register(snapshot)
        assert not store.register(
            dict(
                snapshot,
                status="running",
                completed_at=None,
                lifecycle_observed_at=119,
            )
        )
        with pytest.raises(ValueError, match="Conflicting lifecycle"):
            store.register(
                dict(snapshot, job_name="should roll back", completed_at=119)
            )
        stored = store.jobs()[0]
        assert stored["name"] == "中文训练"
        assert stored["completed_at"] == 120
        with pytest.raises(ValueError, match="collision"):
            store.append(record(dict(job, namespace="other"), "rejected"))
        assert len(store.read("job")[1]) == 2
        with store.connect() as session:
            assert session.get(ElasticJob, "job").status == "completed"
        assert store.database.engine.pool.checkedout() == 0
    finally:
        store.close()
