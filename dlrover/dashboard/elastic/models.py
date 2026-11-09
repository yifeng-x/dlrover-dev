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

"""ORM mappings for the pre-created elastic dashboard MySQL tables."""

from sqlalchemy import (
    BigInteger,
    Column,
    Double,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects import mysql
from sqlalchemy.orm import declarative_base

Base = declarative_base()


def identifier():
    return String(512).with_variant(
        mysql.VARCHAR(512, collation="utf8mb4_bin"), "mysql"
    )


class ElasticJob(Base):
    __tablename__ = "elastic_jobs"
    __table_args__ = {
        "mysql_engine": "InnoDB",
        "mysql_charset": "utf8mb4",
        "mysql_collate": "utf8mb4_bin",
    }

    uid = Column(identifier(), primary_key=True)
    name = Column(String(512), nullable=False)
    cluster = Column(identifier(), nullable=False)
    namespace = Column(identifier(), nullable=False)
    status = Column(String(16), nullable=False, server_default="unknown")
    submitted_at = Column(Double)
    completed_at = Column(Double)
    lifecycle_observed_at = Column(Double)

    def to_dict(self):
        return {
            column.name: getattr(self, column.name)
            for column in self.__table__.columns
        }


class ElasticEvent(Base):
    __tablename__ = "elastic_events"
    __table_args__ = (
        UniqueConstraint("event_id", name="uq_elastic_event_id"),
        Index("ix_elastic_event_job_time", "job_uid", "occurred_at", "seq"),
        {
            "mysql_engine": "InnoDB",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_bin",
        },
    )

    seq = Column(
        BigInteger()
        .with_variant(mysql.BIGINT(unsigned=True), "mysql")
        .with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    event_id = Column(identifier(), nullable=False)
    job_uid = Column(
        identifier(),
        ForeignKey("elastic_jobs.uid", name="fk_elastic_event_job"),
        nullable=False,
    )
    occurred_at = Column(Double, nullable=False)
    payload = Column(
        Text().with_variant(mysql.LONGTEXT(), "mysql"), nullable=False
    )
