import os

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

_DB_USERNAME_KEY = "DB_USERNAME"
_DB_PASSWORD_KEY = "DB_PWD"
_DB_PORT_KEY = "DB_PORT"
_DB_HOST_KEY = "DB_HOST"

_DEFALUT_MYSQL_HOST = "mysql.elastic-training.svc.cluster.local"
_DEFALUT_MYSQL_PORT = "3306"
_DEFALUT_MYSQL_USERNAME = "root"
_DEFALUT_MYSQL_PWD = "root"

HOST = os.getenv(_DB_HOST_KEY, _DEFALUT_MYSQL_HOST)
USERNAME = os.getenv(_DB_USERNAME_KEY, _DEFALUT_MYSQL_USERNAME)
PWD = os.getenv(_DB_PASSWORD_KEY, _DEFALUT_MYSQL_PWD)
PORT = os.getenv(_DB_PORT_KEY, _DEFALUT_MYSQL_PORT)

DB_URL = f"mysql+pymysql://{USERNAME}:{PWD}@{HOST}:{PORT}/dlrover?charset=utf8mb4"

engine = create_engine(
    DB_URL, 
    pool_size=20,
    max_overflow=10,
    pool_recycle=200, 
    pool_pre_ping=True,
    echo=False  # 生产环境务必关闭 SQL 日志
)

SessionFactory = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=True,
)

Base = declarative_base()

# 获取会话
def get_db_session():
    return SessionFactory()
