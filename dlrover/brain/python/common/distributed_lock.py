import os
import socket
from statistics import mode
import threading
import uuid
from typing import Optional
from dlrover.brain.python import client

from dlrover.python.common.log import default_logger as logger

try:
    import redis
    from redis.cluster import RedisCluster, ClusterNode
except ImportError:
    redis = None
    RedisCluster = None

class RedisDistributedLock:
    """
    A lightweight Redis lock based on SET NX EX.
    It is safe to release with value check to avoid deleting others' lock.
    """

    _UNLOCK_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

    _RENEW_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("expire", KEYS[1], tonumber(ARGV[2]))
else
    return 0
end
"""

    def __init__(self, key: str, ttl_seconds: int = 30):
        self._key = key
        self._ttl_seconds = ttl_seconds
        self._token = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex}"
        self._client = _redis_client()
        self._acquired = False
        self._renew_stop_event = threading.Event()
        self._renew_thread = None

    def acquire(self) -> bool:
        if self._client is None:
            return False
        try:
            self._acquired = bool(
                self._client.set(
                    self._key,
                    self._token,
                    nx=True,
                    ex=self._ttl_seconds,
                )
            )
            if self._acquired:
                self._start_renew_thread()
            return self._acquired
        except Exception as e:
            logger.warning(
                f"Acquire redis distributed lock failed for key={self._key}: {e}"
            )
            return False

    def release(self):
        if not self._acquired or self._client is None:
            return
        try:
            self._stop_renew_thread()
            self._client.eval(
                self._UNLOCK_SCRIPT,
                1,
                self._key,
                self._token,
            )
        except Exception as e:
            logger.warning(
                f"Release redis distributed lock failed for key={self._key}: {e}"
            )
        finally:
            self._acquired = False

    @property
    def available(self) -> bool:
        return self._client is not None

    @property
    def last_error(self):
        return self._last_error
    
    @property
    def is_acquired(self) -> bool:
        """Whether this process still owns the lock according to renewal state."""
        return self._acquired

    def _start_renew_thread(self):
        if self._renew_thread and self._renew_thread.is_alive():
            return
        self._renew_stop_event.clear()
        self._renew_thread = threading.Thread(
            target=self._renew_loop,
            name=f"redis_lock_renew_{self._key}",
            daemon=True,
        )
        self._renew_thread.start()

    def _stop_renew_thread(self):
        self._renew_stop_event.set()
        if self._renew_thread and self._renew_thread.is_alive():
            self._renew_thread.join(timeout=0.2)

    def _renew_loop(self):
        interval = max(1, int(self._ttl_seconds * 2 / 3))
        while not self._renew_stop_event.wait(interval):
            try:
                renewed = self._client.eval(
                    self._RENEW_SCRIPT,
                    1,
                    self._key,
                    self._token,
                    str(self._ttl_seconds),
                )
                if int(renewed) != 1:
                    logger.warning(
                        f"Redis lock renew failed or ownership lost for key={self._key}"
                    )
                    self._acquired = False
                    self._renew_stop_event.set()
                    return
            except Exception as e:
                logger.warning(
                    f"Redis lock renew error for key={self._key}: {e}"
                )
                self._last_error = e
                self._acquired = False
                self._renew_stop_event.set()
                return

_CLIENT = None
_CLIENT_LOCK = threading.Lock()

# redis unavailable policy
_DEFAULT_REDIS_UNAVAILABLE_POLICY = "fail_closed"
REDIS_UNAVAILABLE_POLICYS = {
    "fail_closed": _DEFAULT_REDIS_UNAVAILABLE_POLICY,
}

def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int = 0) -> int:
    value = os.getenv(name, "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid integer env %s=%s, use default=%s.", name, value, default)
        return default

def redis_required_for_brain() -> bool:
    """Whether brain must use Redis instead of local process state."""
    if _env_bool("DLROVER_BRAIN_MULTI_INSTANCE"):
        return True
    if _env_int("DLROVER_BRAIN_REPLICAS", 1) > 1:
        return True
    return False

def redis_configured_for_brain() -> bool:
    return bool(
        os.getenv("DLROVER_BRAIN_REDIS_URL", "").strip()
        or os.getenv("DLROVER_BRAIN_REDIS_CLUSTER_NODES", "").strip()
    )

def redis_unavailable_policy() -> str:
    policy = os.getenv(
        "DLROVER_BRAIN_REDIS_UNAVAILABLE_POLICY",
        _DEFAULT_REDIS_UNAVAILABLE_POLICY,
    ).strip().lower()
    
    normalized = REDIS_UNAVAILABLE_POLICYS.get(policy)
    if normalized:
        return normalized
    logger.warning(
        "Invalid DLROVER_BRAIN_REDIS_UNAVAILABLE_POLICY=%s, use %s.",
        policy,
        _DEFAULT_REDIS_UNAVAILABLE_POLICY,
    )
    return _DEFAULT_REDIS_UNAVAILABLE_POLICY

def redis_fail_closed_for_brain(stage: str) -> bool:
    if not redis_available_now() and redis_unavailable_policy() == _DEFAULT_REDIS_UNAVAILABLE_POLICY:
        logger.warning("Redis unavailable, fail closed elastic decision during %s stage for brain.", stage)
        return True
    return False

def redis_available_now() -> bool:
    client = _redis_client()
    if client is None:
        return False
    try:
        client.ping()
        return True
    except Exception as e:
        logger.warning(f"Redis health check failed: {e}")
        return False

def brain_requires_shared_state() -> bool:
    return redis_required_for_brain() or redis_configured_for_brain()

def brain_instance_id() -> str:
    return (
        os.getenv("DLROVER_BRAIN_INSTANCE_ID", "").strip()
        or os.getenv("HOSTNAME", "").strip()
        or socket.gethostname()
    )

def _create_client() -> Optional["redis.Redis"]:
    with _CLIENT_LOCK:
        global _CLIENT
        if _CLIENT is not None:
            return _CLIENT
        _CLIENT = _create_cluster_client()
        if _CLIENT is None:
            _CLIENT = _create_single_client()
        return _CLIENT


def _parse_cluster_nodes(nodes: str):
    parsed_nodes = []
    for item in nodes.split(","):
        host_port = item.strip()
        if not host_port:
            continue
        host, sep, port = host_port.partition(":")
        if not sep or not host.strip() or not port.strip():
            raise ValueError(
                f"Invalid redis cluster node '{host_port}', expected host:port."
            )
        parsed_nodes.append((host.strip(), int(port.strip())))
    return parsed_nodes


def _create_single_client() -> Optional["redis.Redis"]:
    redis_url = os.getenv("DLROVER_BRAIN_REDIS_URL", "").strip()
    redis_pw = os.getenv("DLROVER_BRAIN_REDIS_PW", "").strip()
    if not redis_url or not redis_pw:
        logger.warning(
            "Redis disabled: DLROVER_BRAIN_REDIS_URL is empty in single mode."
        )
        return None
    client = redis.Redis.from_url(
        redis_url,
        socket_connect_timeout=1,
        socket_timeout=1,
        decode_responses=True,
        password=redis_pw,
    )
    client.ping()
    logger.info("Redis client for brain is enabled in single mode.")
    return client


def _create_cluster_client() -> Optional[object]:
    nodes_str = os.getenv("DLROVER_BRAIN_REDIS_CLUSTER_NODES", "").strip()
    if not nodes_str:
        logger.warning(
            "Redis cluster disabled: DLROVER_BRAIN_REDIS_CLUSTER_NODES is empty."
        )
        return None
    if redis is None or not hasattr(redis, "cluster"):
        logger.warning("Redis cluster client is unavailable in current redis package.")
        return None

    parsed_nodes = _parse_cluster_nodes(nodes_str)
    if not parsed_nodes:
        logger.warning("Redis cluster disabled: no valid startup nodes parsed.")
        return None

    startup_nodes = [ClusterNode(host, port) for host, port in parsed_nodes]
    redis_pw = os.getenv("DLROVER_BRAIN_REDIS_PW", "").strip()
    client = RedisCluster(
        startup_nodes=startup_nodes,
        socket_connect_timeout=1,
        socket_timeout=1,
        decode_responses=True,
        password=redis_pw,
    )
    client.ping()
    logger.info("Redis client for brain is enabled in cluster mode.")
    return client

def _redis_client() -> Optional["redis.Redis"]:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    if redis is None:
        logger.warning("Redis disabled: python package 'redis' is not installed.")
        return None

    with _CLIENT_LOCK:
        if _CLIENT is not None:
            return _CLIENT

        redis_mode = os.getenv("DLROVER_BRAIN_REDIS_MODE", "single").strip().lower()
        try:
            if redis_mode == "cluster":
                _CLIENT = _create_cluster_client()
            else:
                _CLIENT = _create_single_client()
            logger.info("Redis client for brain connected successfully.")
            return _CLIENT
        except Exception as e:
            logger.warning(f"Init redis client failed in mode={redis_mode}: {e}")
            return None

