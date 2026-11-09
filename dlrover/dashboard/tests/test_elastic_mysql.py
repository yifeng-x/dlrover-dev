# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
"""Set DLROVER_TEST_MYSQL=1 and DLROVER_MYSQL_* for real MySQL integration."""

import os
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock
from types import SimpleNamespace

import pytest

from dlrover.dashboard.elastic.mysql_store import MySQLEventStore
from dlrover.dashboard.elastic.store import StoreUnavailable, create_store


def config(**changes):
    return dict(DLROVER_MYSQL_HOST="db", DLROVER_MYSQL_DATABASE="elastic_test",
                DLROVER_MYSQL_USER="test", DLROVER_MYSQL_PASSWORD="test-only", **changes)


@pytest.fixture
def driver_stub(monkeypatch):
    """Unit tests isolate connection behavior; only mysql fixture uses a real driver."""
    class DriverError(Exception):
        pass
    driver = SimpleNamespace(MySQLError=DriverError, OperationalError=DriverError,
                             connect=MagicMock(), cursors=SimpleNamespace(DictCursor=object))
    monkeypatch.setitem(sys.modules, "pymysql", driver)
    return driver


def test_mysql_configuration_requires_credentials_and_verifies_tls(driver_stub):
    store = MySQLEventStore.from_env(config(DLROVER_MYSQL_SSL_CA="/ca.crt"))
    assert store._config["ssl_verify_identity"] is True
    assert store._config["ssl_verify_cert"] is True
    assert store._config["autocommit"] is False
    assert store._config["connect_timeout"] == 3
    with pytest.raises(ValueError, match="PASSWORD"):
        MySQLEventStore.from_env({"DLROVER_MYSQL_HOST": "db", "DLROVER_MYSQL_USER": "u"})
    with pytest.raises(ValueError, match="port"):
        MySQLEventStore.from_env(config(DLROVER_MYSQL_PORT="99999"))


def test_mysql_unavailable_is_sanitized(monkeypatch, driver_stub):
    driver = driver_stub
    store = MySQLEventStore.from_env(config())
    monkeypatch.setattr(driver, "connect", MagicMock(side_effect=driver.OperationalError(2003, "sensitive connection detail")))
    with pytest.raises(StoreUnavailable) as error:
        store.health()
    assert str(error.value) == "MySQL is unavailable"
    assert error.value.__suppress_context__


def test_mysql_transaction_rolls_back_on_conflict(monkeypatch, driver_stub):
    driver = driver_stub
    store = MySQLEventStore.from_env(config())
    connection = MagicMock()
    monkeypatch.setattr(driver, "connect", MagicMock(return_value=connection))
    with pytest.raises(ValueError):
        with store.connect():
            raise ValueError("Conflicting event_id")
    connection.rollback.assert_called_once()
    connection.commit.assert_not_called()
    connection.close.assert_called_once()


def test_storage_factory_keeps_explicit_sqlite_compatibility(tmp_path):
    store = create_store("sqlite", str(tmp_path / "legacy.db"))
    store.health()
    assert store.jobs() == []
    with pytest.raises(ValueError):
        create_store("sqlite")


@pytest.fixture
def mysql():
    if os.getenv("DLROVER_TEST_MYSQL") != "1":
        pytest.skip("Real MySQL: set DLROVER_TEST_MYSQL=1 and connection variables")
    # A requested integration run fails, rather than silently skipping a missing driver/DB.
    store = MySQLEventStore.from_env()
    store.initialize()
    return store


@pytest.fixture
def mysql_job(mysql):
    uid = "test-" + uuid.uuid4().hex
    yield dict(job_uid=uid, job_name="中文训练", cluster="test", namespace="test")
    with mysql.connect() as db, db.cursor() as cursor:
        cursor.execute("DELETE FROM elastic_events WHERE job_uid=%s", (uid,))
        cursor.execute("DELETE FROM elastic_jobs WHERE uid=%s", (uid,))


def record(job, event_id):
    return dict(schema_version=1, event_id=event_id, occurred_at=100,
                event_type="phase_changed", plan_id="plan", job=job)


def test_real_mysql_two_replicas_deduplicate_concurrently(mysql, mysql_job):
    second = MySQLEventStore.from_env()
    value = record(mysql_job, "event-" + uuid.uuid4().hex)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda index: (mysql if index % 2 else second).append(value), range(16)))
    assert sum(results) == 1
    assert second.read(mysql_job["job_uid"])[1] == [value]
    with pytest.raises(ValueError, match="Conflicting"):
        second.append({**value, "occurred_at": 101})


def test_real_mysql_lifecycle_is_monotonic_under_concurrency(mysql, mysql_job):
    def update(index):
        return mysql.register(dict(mysql_job, status="completed" if index == 15 else "running",
                                   submitted_at=100, completed_at=200 if index == 15 else None,
                                   lifecycle_observed_at=200 + index))
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(update, range(16)))
    stored, _ = MySQLEventStore.from_env().read(mysql_job["job_uid"])
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
        mysql.append({**record(dict(mysql_job, namespace="other"), uuid.uuid4().hex)})
