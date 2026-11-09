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

"""MySQL event ledger using a shared engine and isolated transactions."""

import json
from contextlib import contextmanager

from sqlalchemy import select
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from .db import Database
from .models import Base, ElasticEvent, ElasticJob
from .store import StoreUnavailable, event_values, lifecycle_values, text


def identity(value, name):
    result = text(value, name)
    if result != result.strip():
        raise ValueError("Whitespace is not allowed around " + name)
    return result


class MySQLEventStore:
    def __init__(self, database):
        self.database = database

    @classmethod
    def from_env(cls, environ=None):
        return cls(Database.from_env(environ))

    @contextmanager
    def connect(self):
        try:
            with self.database.get_db_session() as session:
                with session.begin():
                    yield session
        except (SQLAlchemyError, OSError):
            # Session contexts roll back and return connections to the pool.
            raise StoreUnavailable("MySQL is unavailable") from None

    def close(self):
        self.database.close()

    def initialize(self):
        """Explicit test/setup helper; never called on service startup."""
        try:
            Base.metadata.create_all(self.database.engine)
        except (SQLAlchemyError, OSError):
            raise StoreUnavailable("MySQL is unavailable") from None

    @staticmethod
    def _job(session, job):
        uid = identity(job.get("job_uid"), "job_uid")
        cluster = identity(job.get("cluster") or "default", "cluster")
        namespace = identity(job.get("namespace") or "default", "namespace")
        name = text(job.get("job_name") or uid, "job_name")
        # Atomic create-or-lock, including concurrent first events for a job.
        statement = insert(ElasticJob).values(
            uid=uid,
            name=name,
            cluster=cluster,
            namespace=namespace,
        )
        session.execute(statement.on_duplicate_key_update(uid=ElasticJob.uid))
        old = session.scalars(
            select(ElasticJob).where(ElasticJob.uid == uid).with_for_update()
        ).one()
        if (old.cluster, old.namespace) != (cluster, namespace):
            raise ValueError("Job UID collision across cluster/namespace")
        if name != uid:
            old.name = name
        return uid, old

    def append(self, event):
        event_id, at, payload = event_values(event)
        identity(event_id, "event_id")
        with self.connect() as session:
            uid, _ = self._job(session, event["job"])
            try:
                # A duplicate rolls back only this insert; the job lock remains
                # held by the outer transaction while comparing the payload.
                with session.begin_nested():
                    session.add(
                        ElasticEvent(
                            event_id=event_id,
                            job_uid=uid,
                            occurred_at=at,
                            payload=payload,
                        )
                    )
                    # Force INSERT inside the savepoint so duplicates are
                    # handled here instead of during outer transaction commit.
                    session.flush()
            except IntegrityError as exc:
                if not exc.orig.args or exc.orig.args[0] != 1062:
                    raise
                old = session.scalars(
                    select(ElasticEvent)
                    .where(ElasticEvent.event_id == event_id)
                    .with_for_update()
                ).first()
                if old is None or old.payload != payload:
                    raise ValueError("Conflicting event_id") from None
                return False
        return True

    def register(self, job):
        status, submitted, completed, observed = lifecycle_values(job)
        with self.connect() as session:
            uid, old = self._job(session, job)
            previous = old.lifecycle_observed_at
            if previous is not None:
                if observed < previous:
                    return False
                if observed == previous:
                    if (status, submitted, completed) != (
                        old.status,
                        old.submitted_at,
                        old.completed_at,
                    ):
                        raise ValueError("Conflicting lifecycle snapshot")
                    return False
            old.status = status
            old.submitted_at = submitted
            old.completed_at = completed
            old.lifecycle_observed_at = observed
        return True

    def jobs(self):
        with self.connect() as session:
            jobs = session.scalars(
                select(ElasticJob).order_by(
                    ElasticJob.name,
                    ElasticJob.uid,
                )
            )
            return [job.to_dict() for job in jobs]

    def read(self, uid):
        with self.connect() as session:
            job = session.get(ElasticJob, uid)
            if job is None:
                raise KeyError(uid)
            payloads = session.scalars(
                select(ElasticEvent.payload)
                .where(ElasticEvent.job_uid == uid)
                .order_by(ElasticEvent.occurred_at, ElasticEvent.seq)
                .limit(10001)
            ).all()
            if len(payloads) > 10000:
                raise ValueError(
                    "Job exceeds 10000 events; pagination is required"
                )
            return job.to_dict(), [json.loads(payload) for payload in payloads]

    def health(self):
        with self.connect() as session:
            session.execute(select(ElasticJob.uid).limit(1)).all()
            session.execute(select(ElasticEvent.seq).limit(1)).all()
