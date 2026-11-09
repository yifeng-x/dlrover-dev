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

"""Lazy SQLAlchemy engine and per-request sessions for the aggregation Pod."""

import os

from sqlalchemy import URL, create_engine
from sqlalchemy.orm import sessionmaker


class Database:
    def __init__(self, url, connect_args=None):
        self.engine = create_engine(
            url,
            pool_size=20,
            max_overflow=10,
            pool_recycle=200,
            pool_pre_ping=True,
            pool_timeout=3,
            echo=False,
            hide_parameters=True,
            connect_args=connect_args or {},
        )
        self.session_factory = sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=True,
        )

    def get_db_session(self):
        return self.session_factory()

    def close(self):
        self.engine.dispose()

    @classmethod
    def from_env(cls, environ=None):
        env = os.environ if environ is None else environ

        def required(name):
            value = env.get("DLROVER_MYSQL_" + name)
            if not value:
                raise ValueError("Missing DLROVER_MYSQL_" + name)
            return value

        host, user, password, database = (
            required(key) for key in ("HOST", "USER", "PASSWORD", "DATABASE")
        )
        port = int(env.get("DLROVER_MYSQL_PORT", "3306"))
        if not 1 <= port <= 65535:
            raise ValueError("Invalid MySQL port")
        # URL.create handles reserved characters in credentials without quoting.
        url = URL.create(
            "mysql+pymysql",
            username=user,
            password=password,
            host=host,
            port=port,
            database=database,
            query={"charset": "utf8mb4"},
        )
        connect_args = {
            "connect_timeout": 3,
            "read_timeout": 5,
            "write_timeout": 5,
            "init_command": "SET SESSION innodb_lock_wait_timeout=3",
        }
        ca = env.get("DLROVER_MYSQL_SSL_CA")
        if ca:
            connect_args.update(
                ssl_ca=ca,
                ssl_verify_cert=True,
                ssl_verify_identity=True,
            )
        return cls(url, connect_args)
