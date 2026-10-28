import time
import threading
import os
from typing import Dict, Optional
from dlrover.brain.python.jobmanagement.master_client import build_master_client
from dlrover.brain.python.common.distributed_lock import _redis_client
from dlrover.python.common.log import default_logger as logger

class MasterClientPool:
    _instance = None
    _lock = threading.Lock() # class level lock
    _DEFAULT_SHARED_INDEX_KEY = "dlrover:brain:master_client:index"
    _DEFAULT_JOB_NAME_TO_UID_KEY = "dlrover:brain:job_name_to_uid"
    _DEFAULT_HANDLED_CONFIGMAP_STORE_KEY = "dlrover:brain:handled_configmap_version"

    _DELETE_MASTER_IF_UNCHANGED_SCRIPT = """
if redis.call("exists", KEYS[1]) == 0 then
    return 1
end
local heartbeat = redis.call("hget", KEYS[1], "heartbeat_ts")
if (not heartbeat and ARGV[1] == "") or heartbeat == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0
"""
    _DELETE_STALE_INDEX_SCRIPT = """
local score = redis.call("zscore", KEYS[1], ARGV[1])
if not score then
    return 1
end
if tonumber(score) <= tonumber(ARGV[2]) then
    return redis.call("zrem", KEYS[1], ARGV[1])
end
return 2
"""

    def __new__(cls):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
                    # MasterClient is a process-local connection object. All
                    # business state (address and heartbeat) lives in Redis.
                    cls._instance.master_clients: Dict[str, object] = {}
                    
                    cls._instance._redis = _redis_client()
                    cls._instance._shared_store_ttl = int(
                        os.getenv("DLROVER_BRAIN_MASTER_ADDR_TTL_SECONDS", "3600")
                    )
                    cls._instance._shared_store_index_key = os.getenv(
                        "DLROVER_BRAIN_MASTER_ADDR_INDEX",
                        cls._DEFAULT_SHARED_INDEX_KEY
                    )
                    cls._instance._job_name_to_uid_key = os.getenv(
                        "DLROVER_BRAIN_JOB_NAME_TO_UID",
                        cls._DEFAULT_JOB_NAME_TO_UID_KEY
                    )
                    cls._instance._handled_configmap_key = os.getenv(
                        "DLROVER_BRAIN_HANDLED_CONFIGMAP_STORE_KEY",
                        cls._DEFAULT_HANDLED_CONFIGMAP_STORE_KEY
                    )
                    cls._instance._cleanup_stop_event = threading.Event()
                    cls._instance._cleanup_thread = None
                    cls._instance._job_cleanup_callback = None
        return cls._instance

    def register(self, job_name: str, job_id: str, master_addr: str) -> bool:
        """
        Auto register master_addr to master_clients
        """
        with self._lock:
            if not self._ensure_store_available("register", job_name, job_id):
                return False

            if not self._save_to_shared_store(job_name, job_id, master_addr):
                logger.error(
                    f"[Brain] Redis is unavailable, "
                    f"reject master register for {job_name}/{job_id}."
                )
                return False

            client = self.master_clients.get(job_id)
            client_addr = getattr(client, "_master_addr", None)
            if client_addr and client_addr != master_addr:
                logger.warning(
                    f"[Brain] Master address conflict for {job_name}/{job_id}, "
                    f"cached={client_addr}, new={master_addr}. Use new address."
                )
                self._close_local_client_unlocked(job_id)
            self._save_job_name_to_uid_mapping(job_name, job_id)
        logger.info(
            f"[Brain] Register job master: {job_name}/{job_id} "
            f"to master {master_addr}"
        )
        return True

    def get_master_client(
        self, job_name: str, 
        job_id: str, 
        refresh_from_shared_store: bool = False,
    ):
        """
        Get master client by job name.

        ``refresh_from_shared_store`` is retained for API compatibility. Redis
        is always queried before a cached connection can be reused.
        """
        with self._lock:
            if not self._ensure_store_available(
                "get master client", job_name, job_id
            ):
                return None

            loaded, master_addr = self._load_master_addr_from_shared_store(job_id)
            if not loaded:
                logger.error(
                    f"[Brain] Redis is unavailable, reject get master client "
                    f"for {job_name}/{job_id}."
                )
                return None
            if not master_addr:
                self._close_local_client_unlocked(job_id)
                logger.warning(
                    f"[Brain] No master addrs found for job "
                    f"{job_name}/{job_id}, job not registered."
                )
                return None

            master_client = self.master_clients.get(job_id)
            if (
                master_client
                and getattr(master_client, "_master_addr", None) == master_addr
            ):
                return master_client

            if master_client:
                self._close_local_client_unlocked(job_id)

            master_client = build_master_client(
                master_addr=master_addr,
                job_name=job_name,
                job_id=job_id,
            )
            self.master_clients[job_id] = master_client
            return master_client

    def heartbeat(self, job_name: str, job_id: str, master_addr: str = "") -> bool:
        with self._lock:
            if not self._ensure_store_available("heartbeat", job_name, job_id):
                return False

            if not self._refresh_heartbeat_in_shared_store(
                job_name, job_id, master_addr
            ):
                logger.error(
                    f"[Brain] Redis is unavailable, "
                    f"reject heartbeat for {job_name}/{job_id}."
                )
                return False

            if master_addr:
                client = self.master_clients.get(job_id)
                client_addr = getattr(client, "_master_addr", None)
                if client_addr and client_addr != master_addr:
                    logger.info(
                        f"[Brain] Refresh master addr from heartbeat for "
                        f"{job_name}/{job_id}: {master_addr}"
                    )
                    self._close_local_client_unlocked(job_id)
            return True

    def clean_timeout_jobs(self, timeout=60):
        if not self._redis_available():
            logger.error("[Brain] Redis is unavailable, skip timeout cleanup.")
            return []
        return self._clean_timeout_jobs_from_shared_store(timeout)

    def set_job_cleanup_callback(self, callback):
        """Register the lifecycle coordinator invoked for stale jobs."""
        self._job_cleanup_callback = callback

    def cleanup_job_state(
        self,
        job_id: str,
        expected_heartbeat_ts: str,
        expire_before: float,
    ) -> bool:
        """Conditionally delete Redis state and the local connection cache.

        The heartbeat comparison fences a stale cleanup from deleting a job
        that registered or sent a heartbeat after the timeout scan.
        """
        if not self._redis_available():
            return False
        try:
            deleted = self._redis.eval(
                self._DELETE_MASTER_IF_UNCHANGED_SCRIPT,
                1,
                self._shared_store_key(job_id),
                expected_heartbeat_ts,
            )
            if int(deleted) != 1:
                logger.info(
                    "Skip cleanup for job %s because its heartbeat changed.",
                    job_id,
                )
                return False
            with self._lock:
                self._clean_client_unlocked(job_id)
            return True
        except Exception as e:
            logger.warning(
                f"[Brain] Conditional cleanup for {job_id} failed: {e}"
            )
            return False

    def finish_job_cleanup(self, job_id: str, expire_before: float) -> bool:
        """Remove only a still-stale index entry after all state is cleaned."""
        if not self._redis_available():
            return False
        try:
            result = self._redis.eval(
                self._DELETE_STALE_INDEX_SCRIPT,
                1,
                self._shared_store_index_key,
                job_id,
                str(expire_before),
            )
            # 1: absent/removed, 2: a fresh heartbeat recreated the index.
            return int(result) in (1, 2)
        except Exception as e:
            logger.warning(
                f"[Brain] Finish cleanup index for {job_id} failed: {e}"
            )
            return False
    
    def invalidate_local_client(self, job_id: str):
        """
        Invalidate only local cached client and keep shared store data.
        Used for reconnect/retry scenarios.
        """
        with self._lock:
            self._close_local_client_unlocked(job_id)
    
    def _clean_client_unlocked(self, job_id: str):
        """
        No lock, for internal use only
        """
        if job_id is None:
            logger.warning(f"No job id found for job {job_id}, job not registered.")
            return
        
        try:
            self._close_local_client_unlocked(job_id)
        except Exception as e:
            logger.error(f"Failed to clean master client for job {job_id}: {e}")

    def _close_local_client_unlocked(self, job_id: str):
        client = self.master_clients.get(job_id)
        if not client:
            return
        try:
            client._close_grpc_channel()
        except Exception as e:
            logger.warning(f"Failed to close grpc channel for {job_id}: {e}")
        finally:
            self.master_clients.pop(job_id, None)

    def start_auto_clean_thread(self, timeout = 60):
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            return
        self._cleanup_stop_event.clear()

        self._cleanup_thread = threading.Thread(
            target=self.async_clean_timeout_master_client,
            args=(timeout,),
            name="periodic_clean_master_client",
            daemon=True
        )
        self._cleanup_thread.start()
        if self._cleanup_thread.is_alive():
            logger.info("Periodic to Clean timeout job master_client initialized successfully")
    
    def stop(self):
        self._cleanup_stop_event.set()
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=2)
        with self._lock:
            for job_id in list(self.master_clients):
                self._close_local_client_unlocked(job_id)

    def async_clean_timeout_master_client(self, timeout = 60):
        logger.info("Periodic to Clean timeout job master_client initializing...")
        while not self._cleanup_stop_event.is_set():
            try:
                expire_master_clients = self.clean_timeout_jobs(timeout)
                if len(expire_master_clients) > 0:
                    logger.info(
                        "Periodic to clean timeout dlrover master_clients "
                        f"is: {expire_master_clients}"
                    )
            except Exception as e:
                logger.error(
                    f"Periodic to clean timeout job master_client error: {e}"
                )
            self._cleanup_stop_event.wait(10)
    
    def get_job_name(self, job_id: str) -> str:
        if not self._redis_available():
            return None
        try:
            job_name = self._redis.hget(
                self._shared_store_key(job_id), "job_name"
            )
            return job_name if job_name else None
        except Exception as e:
            logger.warning("[Brain] Load job name from redis failed for %s: %s",
                job_id,
                e,
            )
            return None

    def _shared_store_key(self, job_id: str) -> str:
        return f"dlrover:brain:master_client:{job_id}"

    def _shared_tw_store_key(self, job_id: str) -> str:
        return f"dlrover:brain:vertical_elastic:gpu_tw:{job_id}"

    def _ensure_store_available(
        self, operation: str, job_name: str, job_id: str
    ) -> bool:
        if self._redis_available():
            return True
        logger.error(
            f"[Brain] Redis is unavailable, reject {operation} "
            f"for {job_name}/{job_id}."
        )
        return False
    
    def _redis_available(self) -> bool:
        if self._redis is None:
            self._redis = _redis_client()
        return self._redis is not None

    def _save_to_shared_store(
        self, job_name: str, job_id: str, master_addr: str
    ) -> bool:
        if not self._redis:
            return False
        try:
            heartbeat_ts = time.time()
            self._redis.hset(
                self._shared_store_key(job_id),
                mapping={
                    "job_name": job_name,
                    "job_id": job_id,
                    "master_addr": master_addr,
                    "heartbeat_ts": str(heartbeat_ts),
                },
            )
            self._redis.expire(
                self._shared_store_key(job_id), self._shared_store_ttl
            )
            self._redis.zadd(self._shared_store_index_key, {job_id: heartbeat_ts})
            return True
        except Exception as e:
            logger.warning(f"[Brain] Save master address to redis failed: {e}")
            return False

    def _load_master_addr_from_shared_store(
        self, job_id: str
    ) -> tuple[bool, Optional[str]]:
        if not self._redis:
            return False, None
        try:
            master_addr = self._redis.hget(
                self._shared_store_key(job_id), "master_addr"
            )
            return True, master_addr if master_addr else None
        except Exception as e:
            logger.warning(f"[Brain] Load master address from redis failed: {e}")
            return False, None

    def _refresh_heartbeat_in_shared_store(
        self, job_name: str, job_id: str, master_addr: str = ""
    ) -> bool:
        if not self._redis:
            return False
        try:
            key = self._shared_store_key(job_id)
            heartbeat_ts = time.time()
            mapping = {
                "job_name": job_name,
                "job_id": job_id,
                "heartbeat_ts": str(heartbeat_ts),
            }
            if master_addr:
                mapping["master_addr"] = master_addr
            self._redis.hset(
                key,
                mapping=mapping,
            )
            self._redis.expire(key, self._shared_store_ttl)
            self._redis.zadd(self._shared_store_index_key, {job_id: heartbeat_ts})
            return True
        except Exception as e:
            logger.warning(f"[Brain] Refresh heartbeat in redis failed: {e}")
            return False

    def _delete_from_shared_store(self, job_id: str, job_name: str):
        if not self._redis:
            return
        try:
            self._redis.delete(self._job_name_to_uid_store_key(job_name))
            self._redis.delete(self._shared_store_key(job_id))
            self._redis.delete(self._shared_tw_store_key(job_id))
            self._redis.zrem(self._shared_store_index_key, job_id)
        except Exception as e:
            logger.warning(f"[Brain] Delete master address from redis failed: {e}")

    def _delete_job_name_to_uid_mapping(self, job_name: str):
        """
        删除 job_name -> job_uid 的映射关系

        Args:
            job_name: 作业名称
        """
        if not self._redis:
            return
        try:
            mapping_key = self._job_name_to_uid_store_key(job_name)
            self._redis.delete(mapping_key)
            logger.info(f"[Brain] Deleted redis job_name -> job_uid mapping for: {job_name}")
        except Exception as e:
            logger.warning(f"[Brain] Delete job_name to job_uid mapping failed: {e}")

    def _clean_timeout_jobs_from_local_store(self, timeout=60):
        now = time.time()
        expired = []
        with self._lock:
            for job_id in list(self.last_heartbeat_time.keys()):
                if now - self.last_heartbeat_time[job_id] > timeout:
                    job_name = self.job_mapping.get(job_id, "Unknown")
                    expired.append(f"{job_name}/{job_id}")
                    self._clean_client_unlocked(job_id)
                    self.clean_job_tmp_file(job_id)
        return expired

    def _clean_timeout_jobs_from_shared_store(self, timeout=60):
        now = time.time()
        expire_before = now - timeout
        expired = []
        try:
            stale_job_ids = self._redis.zrangebyscore(
                self._shared_store_index_key,
                min="-inf",
                max=expire_before,
            )
            for job_id in stale_job_ids:
                data = self._redis.hgetall(self._shared_store_key(job_id)) or {}
                heartbeat_raw = data.get("heartbeat_ts", "")
                heartbeat_ts = float(heartbeat_raw or "0")
                # If heartbeat has been refreshed after index query, keep this job.
                if heartbeat_ts > 0 and now - heartbeat_ts <= timeout:
                    self._redis.zadd(
                        self._shared_store_index_key, {job_id: heartbeat_ts}
                    )
                    continue
                if not data:
                    self._run_job_cleanup(
                        "Unknown", job_id, "", expire_before
                    )
                    continue
                job_id = data.get("job_id", job_id)
                job_name = data.get("job_name", "Unknown")
                if self._run_job_cleanup(
                    job_name, job_id, heartbeat_raw, expire_before
                ):
                    expired.append(f"{job_name}/{job_id}")
        except Exception as e:
            logger.warning(f"[Brain] Shared-store timeout cleanup failed: {e}")
        self._reconcile_local_clients()
        return expired

    def _reconcile_local_clients(self):
        """Close cached connections whose Redis address is gone or changed."""
        with self._lock:
            cached = list(self.master_clients.items())
        for job_id, client in cached:
            try:
                master_addr = self._redis.hget(
                    self._shared_store_key(job_id), "master_addr"
                )
            except Exception as e:
                logger.warning(
                    "[Brain] Reconcile local master clients failed: %s", e
                )
                return
            if not master_addr or getattr(client, "_master_addr", None) != master_addr:
                with self._lock:
                    # Do not close a connection replaced after the snapshot.
                    if self.master_clients.get(job_id) is client:
                        self._close_local_client_unlocked(job_id)

    def _run_job_cleanup(
        self,
        job_name: str,
        job_id: str,
        expected_heartbeat_ts: str,
        expire_before: float,
    ) -> bool:
        callback = self._job_cleanup_callback
        if callback:
            return bool(
                callback(
                    job_name,
                    job_id,
                    expected_heartbeat_ts,
                    expire_before,
                )
            )
        if not self.cleanup_job_state(
            job_id, expected_heartbeat_ts, expire_before
        ):
            return False
        return self.finish_job_cleanup(job_id, expire_before)

    def _job_name_to_uid_store_key(self, job_name: str) -> str:
        return f"{self._job_name_to_uid_key}:{job_name}"

    def _handled_configmap_store_key(self, job_uid: str) -> str:
        return f"{self._handled_configmap_key}:{job_uid}"

    def _save_job_name_to_uid_mapping(self, job_name: str, job_id: str):
        if not self._redis:
            return
        try:
            mapping_key = self._job_name_to_uid_store_key(job_name)
            old_job_id = self._redis.get(mapping_key)

            # 如果存在旧的 job_uid 且与新的不同，需要删除旧 job_uid 的元数据
            if old_job_id and old_job_id != job_id:
                old_job_id_str = old_job_id.decode('utf-8') if isinstance(old_job_id, bytes) else old_job_id
                logger.info(
                    f"[Brain] Job name '{job_name}' already mapped to old job_id '{old_job_id_str}', "
                    f"overwriting with new job_id '{job_id}'"
                )
                # delete old shared info
                self._delete_from_shared_store(old_job_id_str, job_name)
                # record latest handled configmap version to new job_id
                handled_configmap_version = self._redis.get(self._handled_configmap_store_key(old_job_id_str))
                if handled_configmap_version is not None:
                    self._redis.set(self._handled_configmap_store_key(job_id), handled_configmap_version, ex=self._shared_store_ttl)
                    logger.info(
                        f"[Brain] job:{job_name} record old job_id:{old_job_id_str} handled configmap version to new job_id:{job_id},handled_configmap_version:{handled_configmap_version}")

            # save new key:job_uid
            self._redis.set(mapping_key, job_id, ex=self._shared_store_ttl)
            logger.info(f"[Brain] Saved job_name -> job_uid mapping: {job_name} -> {job_id}")
        except Exception as e:
            logger.info(f"[Brain] Save job_name to job_uid mapping failed: {e}")

    def get_job_id_by_name(self, job_name: str) -> str:
        if not self._redis:
            logger.info("[Brain] Redis not available, cannot get job_id by job_name")
            return None
        try:
            mapping_key = self._job_name_to_uid_store_key(job_name)
            job_id = self._redis.get(mapping_key)
            if job_id:
                return job_id.decode('utf-8') if isinstance(job_id, bytes) else job_id
            logger.info(f"[Brain] No job_id found for job_name: {job_name}")
            return None
        except Exception as e:
            logger.info(f"[Brain] Get job_id by job_name failed: {e}")
            return None

    def get_master_client_by_job_name(self, job_name: str, refresh_from_shared_store: bool = False):
        job_id = self.get_job_id_by_name(job_name)
        if not job_id:
            logger.warning(f"[Brain] Cannot get master client: no job_id found for job_name '{job_name}'")
            return None
        return self.get_master_client(job_name, job_id, refresh_from_shared_store)
