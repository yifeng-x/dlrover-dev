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

"""Sticky per-job ownership for multi-instance Brain deployments."""

from dataclasses import dataclass
import json
import os
import threading
import time
import uuid
from typing import Dict, Optional, Tuple

from dlrover.brain.python.common.distributed_lock import (
    _redis_client,
    brain_instance_id,
    redis_required_for_brain,
)
from dlrover.python.common.log import default_logger as logger


OWNER_KEY_PREFIX = "dlrover:brain:vertical_elastic:owner"
REPORT_DEDUP_KEY_PREFIX = "dlrover:brain:vertical_elastic:report"
BRAIN_INSTANCE_INDEX_KEY = "dlrover:brain:owner_allocator:{registry}:instances"
BRAIN_INSTANCE_DATA_KEY = "dlrover:brain:owner_allocator:{registry}:data"
BRAIN_ROUND_ROBIN_KEY = "dlrover:brain:owner_allocator:{registry}:sequence"
BRAIN_FORWARD_FAILURE_KEY = (
    "dlrover:brain:owner_allocator:{registry}:forward_failures"
)
BRAIN_QUARANTINE_KEY = (
    "dlrover:brain:owner_allocator:{registry}:quarantine"
)
OWNER_ALLOCATION_LOCK_PREFIX = "dlrover:brain:owner_allocator:lock"


class JobOwnerStoreError(RuntimeError):
    """Raised when ownership cannot be established safely."""


@dataclass(frozen=True)
class JobOwner:
    instance_id: str
    endpoint: str
    token: str


@dataclass(frozen=True)
class JobCleanupLease:
    job_id: str
    cleanup_value: str
    previous_owner_value: str
    previous_ttl_ms: int


@dataclass
class _LocalOwnership:
    owner: JobOwner
    raw_value: str
    last_seen: float


class JobOwnerManager:
    """Assign active jobs round-robin across live Brain instances in Redis."""

    _REGISTER_INSTANCE_SCRIPT = """
local redis_time = redis.call("time")
local now = tonumber(redis_time[1]) + tonumber(redis_time[2]) / 1000000
local previous = redis.call("hget", KEYS[2], ARGV[1])
if not previous or previous ~= ARGV[2] then
    redis.call("hdel", KEYS[3], ARGV[1])
    redis.call("zrem", KEYS[4], ARGV[1])
end
redis.call("zadd", KEYS[1], now, ARGV[1])
redis.call("hset", KEYS[2], ARGV[1], ARGV[2])
return 1
"""

    _RELEASE_INSTANCE_SCRIPT = """
if redis.call("hget", KEYS[2], ARGV[1]) == ARGV[2] then
    redis.call("zrem", KEYS[1], ARGV[1])
    redis.call("hdel", KEYS[2], ARGV[1])
    redis.call("hdel", KEYS[3], ARGV[1])
    redis.call("zrem", KEYS[4], ARGV[1])
    return 1
end
return 0
"""

    _SELECT_INSTANCE_SCRIPT = """
local redis_time = redis.call("time")
local now = tonumber(redis_time[1]) + tonumber(redis_time[2]) / 1000000
local cutoff = now - tonumber(ARGV[1])
local expired = redis.call("zrangebyscore", KEYS[1], "-inf", cutoff)
for _, instance_id in ipairs(expired) do
    redis.call("hdel", KEYS[2], instance_id)
    redis.call("hdel", KEYS[4], instance_id)
    redis.call("zrem", KEYS[5], instance_id)
end
redis.call("zremrangebyscore", KEYS[1], "-inf", cutoff)
redis.call("zremrangebyscore", KEYS[5], "-inf", now)
local instances = redis.call("zrange", KEYS[1], 0, -1)
table.sort(instances)
local available = {}
for _, instance_id in ipairs(instances) do
    if not redis.call("zscore", KEYS[5], instance_id) then
        table.insert(available, instance_id)
    end
end
if #available == 0 then
    return {}
end
local sequence = redis.call("incr", KEYS[3])
local start_index = ((sequence - 1) % #available) + 1
for offset = 0, #available - 1 do
    local index = ((start_index - 1 + offset) % #available) + 1
    local instance_id = available[index]
    local raw = redis.call("hget", KEYS[2], instance_id)
    if raw then
        local ok, data = pcall(cjson.decode, raw)
        if ok and data["endpoint"] and data["endpoint"] ~= "" then
            return {instance_id, data["endpoint"]}
        end
    end
    redis.call("zrem", KEYS[1], instance_id)
    redis.call("hdel", KEYS[2], instance_id)
end
return {}
"""

    _RECORD_FORWARD_FAILURE_SCRIPT = """
local raw = redis.call("hget", KEYS[1], ARGV[1])
if not raw then
    return {0, 0}
end
local ok, instance = pcall(cjson.decode, raw)
if not ok or instance["endpoint"] ~= ARGV[2] then
    return {0, 0}
end
local redis_time = redis.call("time")
local now = tonumber(redis_time[1]) + tonumber(redis_time[2]) / 1000000
local count = 1
local previous = redis.call("hget", KEYS[2], ARGV[1])
if previous then
    local state_ok, state = pcall(cjson.decode, previous)
    if state_ok and now - tonumber(state["last_failure"] or 0) <= tonumber(ARGV[4]) then
        count = tonumber(state["count"] or 0) + 1
    end
end
redis.call("hset", KEYS[2], ARGV[1], cjson.encode({
    count = count,
    last_failure = now
}))
if count >= tonumber(ARGV[3]) then
    redis.call("zadd", KEYS[3], now + tonumber(ARGV[5]), ARGV[1])
    return {count, 1}
end
return {count, 0}
"""

    _CLEAR_FORWARD_FAILURE_SCRIPT = """
local raw = redis.call("hget", KEYS[1], ARGV[1])
if not raw then
    return 0
end
local ok, instance = pcall(cjson.decode, raw)
if not ok or instance["endpoint"] ~= ARGV[2] then
    return 0
end
redis.call("hdel", KEYS[2], ARGV[1])
redis.call("zrem", KEYS[3], ARGV[1])
return 1
"""

    _RENEW_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("expire", KEYS[1], tonumber(ARGV[2]))
else
    return 0
end
"""

    _RELEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

    _COMMIT_REGISTRATION_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    redis.call("set", KEYS[1], ARGV[2], "EX", tonumber(ARGV[3]))
    return 1
else
    return 0
end
"""

    _BEGIN_CLEANUP_SCRIPT = """
local current = redis.call("get", KEYS[1])
if (not current and ARGV[1] == "") or current == ARGV[1] then
    redis.call("set", KEYS[1], ARGV[2], "EX", tonumber(ARGV[3]))
    return 1
else
    return 0
end
"""

    _FINISH_CLEANUP_SCRIPT = """
if redis.call("get", KEYS[1]) ~= ARGV[1] then
    return 0
end
if ARGV[2] == "" then
    return redis.call("del", KEYS[1])
end
redis.call("set", KEYS[1], ARGV[2], "PX", tonumber(ARGV[3]))
return 1
"""

    def __init__(self):
        self.enabled = redis_required_for_brain()
        self.instance_id = brain_instance_id()
        self.endpoint = self._instance_endpoint()
        self.lease_ttl_seconds = self._env_int(
            "DLROVER_BRAIN_JOB_OWNER_TTL_SECONDS", 30, minimum=10
        )
        default_renew = max(1, self.lease_ttl_seconds // 3)
        self.renew_interval_seconds = self._env_int(
            "DLROVER_BRAIN_JOB_OWNER_RENEW_INTERVAL_SECONDS",
            default_renew,
            minimum=1,
        )
        # Never renew at or after lease expiry.
        self.renew_interval_seconds = min(
            self.renew_interval_seconds,
            max(1, self.lease_ttl_seconds // 2),
        )
        self.idle_timeout_seconds = self._env_int(
            "DLROVER_BRAIN_JOB_IDLE_TIMEOUT_SECONDS", 180, minimum=1
        )
        self.report_dedup_ttl_seconds = self._env_int(
            "DLROVER_BRAIN_REPORT_DEDUP_TTL_SECONDS", 300, minimum=1
        )
        self.cleanup_ttl_seconds = self._env_int(
            "DLROVER_BRAIN_JOB_CLEANUP_TTL_SECONDS", 60, minimum=10
        )
        self.instance_ttl_seconds = self._env_int(
            "DLROVER_BRAIN_INSTANCE_TTL_SECONDS",
            max(30, self.renew_interval_seconds * 3),
            minimum=10,
        )
        self.instance_ttl_seconds = max(
            self.instance_ttl_seconds,
            self.renew_interval_seconds * 2,
        )
        self.forward_failure_threshold = self._env_int(
            "DLROVER_BRAIN_FORWARD_FAILURE_THRESHOLD", 3, minimum=1
        )
        self.forward_failure_window_seconds = self._env_int(
            "DLROVER_BRAIN_FORWARD_FAILURE_WINDOW_SECONDS", 30, minimum=1
        )
        self.forward_quarantine_seconds = self._env_int(
            "DLROVER_BRAIN_FORWARD_QUARANTINE_SECONDS", 30, minimum=1
        )
        self._instance_token = uuid.uuid4().hex
        self._instance_value = self._encode_instance()
        self._instance_registered = False
        self._local_owners: Dict[str, _LocalOwnership] = {}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._renew_thread = None
        self._stopped = False

    def start(self):
        if not self.enabled:
            return
        if self._renew_thread and self._renew_thread.is_alive():
            return
        self._stopped = False
        self._stop_event.clear()
        self._refresh_instance_registration()
        self._renew_thread = threading.Thread(
            target=self._renew_loop,
            name="brain_job_owner_renew",
            daemon=True,
        )
        self._renew_thread.start()
        logger.info(
            "Brain job owner manager started: instance=%s endpoint=%s ttl=%ss.",
            self.instance_id,
            self.endpoint,
            self.lease_ttl_seconds,
        )

    def stop(self):
        self._stopped = True
        self._stop_event.set()
        if self._renew_thread and self._renew_thread.is_alive():
            self._renew_thread.join(timeout=2)

        with self._lock:
            owned = list(self._local_owners.items())
            self._local_owners.clear()
        client = _redis_client()
        if client is None:
            self._instance_registered = False
            return
        for job_id, state in owned:
            try:
                client.eval(
                    self._RELEASE_SCRIPT,
                    1,
                    self._owner_key(job_id),
                    state.raw_value,
                )
            except Exception as e:
                logger.warning("Release job owner %s failed: %s", job_id, e)
        self._release_instance_registration(client)

    def claim_for_registration(self, job_id: str) -> Tuple[JobOwner, bool]:
        """Claim ownership during Master registration.

        Returns the current owner and whether this call created its lease.
        The boolean lets the registration path roll back only its own claim
        when persisting the Master address fails.
        """
        if not self.enabled:
            return (
                JobOwner(self.instance_id, self.endpoint, "single-instance"),
                False,
            )
        if not job_id:
            raise JobOwnerStoreError("job uuid is empty")
        if self._stopped:
            raise JobOwnerStoreError("Brain instance is stopping")

        client = self._client_or_raise()
        key = self._owner_key(job_id)
        allocation_token = uuid.uuid4().hex
        allocation_lock_key = self._allocation_lock_key(job_id)
        try:
            raw = client.get(key)
            if raw:
                if self._is_cleanup_value(raw):
                    raise JobOwnerStoreError(
                        f"job {job_id} is being cleaned up"
                    )
                owner = self._decode_owner(raw)
                if owner.instance_id == self.instance_id:
                    self._remember_local(job_id, owner, raw)
                return owner, False

            # Serialize duplicate registrations for the same job so they do
            # not consume multiple positions in the global round-robin.
            locked = client.set(
                allocation_lock_key,
                allocation_token,
                nx=True,
                ex=5,
            )
            if not locked:
                raise JobOwnerStoreError(
                    f"job owner allocation is busy for {job_id}"
                )
            try:
                raw = client.get(key)
                if raw:
                    if self._is_cleanup_value(raw):
                        raise JobOwnerStoreError(
                            f"job {job_id} is being cleaned up"
                        )
                    owner = self._decode_owner(raw)
                    if owner.instance_id == self.instance_id:
                        self._remember_local(job_id, owner, raw)
                    return owner, False

                if not self._refresh_instance_registration(client):
                    raise JobOwnerStoreError(
                        "cannot refresh Brain instance registration"
                    )
                owner = self._allocate_owner(client)
                raw = self._encode_owner(owner, state="registering")
                if not client.set(
                    key,
                    raw,
                    nx=True,
                    ex=self.lease_ttl_seconds,
                ):
                    raise JobOwnerStoreError(
                        f"job owner changed during allocation for {job_id}"
                    )
                if owner.instance_id == self.instance_id:
                    self._remember_local(job_id, owner, raw)
                logger.info(
                    "Round-robin assigned job %s to Brain %s at %s.",
                    job_id,
                    owner.instance_id,
                    owner.endpoint,
                )
                return owner, True
            finally:
                client.eval(
                    self._RELEASE_SCRIPT,
                    1,
                    allocation_lock_key,
                    allocation_token,
                )
        except JobOwnerStoreError:
            raise
        except Exception as e:
            raise JobOwnerStoreError(
                f"access Redis owner for {job_id} failed: {e}"
            ) from e

    def get_owner(self, job_id: str) -> Optional[JobOwner]:
        """Read active ownership without creating a lease."""
        if not self.enabled:
            return JobOwner(self.instance_id, self.endpoint, "single-instance")
        if not job_id:
            raise JobOwnerStoreError("job uuid is empty")
        client = self._client_or_raise()
        try:
            raw = client.get(self._owner_key(job_id))
            if not raw:
                return None
            if self._is_cleanup_value(raw):
                raise JobOwnerStoreError(f"job {job_id} is being cleaned up")
            if self._is_registering_value(raw):
                return None
            owner = self._decode_owner(raw)
            if owner.instance_id == self.instance_id:
                self._remember_local(job_id, owner, raw)
            return owner
        except JobOwnerStoreError:
            raise
        except Exception as e:
            raise JobOwnerStoreError(
                f"read Redis owner for {job_id} failed: {e}"
            ) from e

    def commit_registration(self, job_id: str, token: str) -> bool:
        """Promote a token-matched REGISTERING lease to ACTIVE."""
        if not self.enabled:
            return True
        if not job_id or not token:
            return False
        client = self._client_or_raise()
        try:
            raw = client.get(self._owner_key(job_id))
            if not raw or self._is_cleanup_value(raw):
                return False
            owner = self._decode_owner(raw)
            if owner.instance_id != self.instance_id or owner.token != token:
                return False
            if not self._is_registering_value(raw):
                self._remember_local(job_id, owner, raw)
                return True
            active_raw = self._encode_owner(owner, state="active")
            committed = client.eval(
                self._COMMIT_REGISTRATION_SCRIPT,
                1,
                self._owner_key(job_id),
                raw,
                active_raw,
                str(self.lease_ttl_seconds),
            )
            if int(committed) != 1:
                return False
            self._remember_local(job_id, owner, active_raw)
            return True
        except JobOwnerStoreError:
            raise
        except Exception as e:
            raise JobOwnerStoreError(
                f"commit registration owner for {job_id} failed: {e}"
            ) from e

    def release_claim(self, job_id: str, token: str) -> bool:
        """Release a REGISTERING claim only if its token still owns it.

        The ingress Brain may roll back a remote allocation when forwarding
        fails, so this operation intentionally does not require local owner
        identity. The random token and REGISTERING-state check provide fencing.
        """
        if not self.enabled:
            return True
        if not job_id or not token:
            return False
        client = self._client_or_raise()
        try:
            raw = client.get(self._owner_key(job_id))
            if (
                not raw
                or self._is_cleanup_value(raw)
                or not self._is_registering_value(raw)
            ):
                return False
            owner = self._decode_owner(raw)
            if owner.token != token:
                return False
            released = client.eval(
                self._RELEASE_SCRIPT,
                1,
                self._owner_key(job_id),
                raw,
            )
            if int(released) != 1:
                return False
            with self._lock:
                state = self._local_owners.get(job_id)
                if state and state.owner.token == token:
                    self._local_owners.pop(job_id, None)
            return True
        except JobOwnerStoreError:
            raise
        except Exception as e:
            raise JobOwnerStoreError(
                f"release registration owner for {job_id} failed: {e}"
            ) from e

    def accept_forwarded(self, job_id: str, token: str) -> bool:
        """Validate that a forwarded request reached its current owner."""
        if not self.enabled:
            return True
        if not token:
            return False
        client = self._client_or_raise()
        try:
            raw = client.get(self._owner_key(job_id))
            if not raw:
                return False
            if self._is_cleanup_value(raw):
                return False
            owner = self._decode_owner(raw)
            if owner.instance_id != self.instance_id or owner.token != token:
                return False
            renewed = client.eval(
                self._RENEW_SCRIPT,
                1,
                self._owner_key(job_id),
                raw,
                str(self.lease_ttl_seconds),
            )
            if int(renewed) != 1:
                return False
            self._remember_local(job_id, owner, raw)
            return True
        except JobOwnerStoreError:
            raise
        except Exception as e:
            raise JobOwnerStoreError(
                f"validate forwarded owner for {job_id} failed: {e}"
            ) from e

    def is_current_owner(self, job_id: str, token: str) -> bool:
        if not self.enabled:
            return True
        if not token:
            return False
        client = self._client_or_raise()
        try:
            raw = client.get(self._owner_key(job_id))
            if not raw:
                return False
            if self._is_cleanup_value(raw) or self._is_registering_value(raw):
                return False
            owner = self._decode_owner(raw)
            return (
                owner.instance_id == self.instance_id and owner.token == token
            )
        except JobOwnerStoreError:
            raise
        except Exception as e:
            raise JobOwnerStoreError(
                f"validate owner for {job_id} failed: {e}"
            ) from e

    def mark_report_once(self, job_id: str, report_id: str) -> bool:
        """Return True once for a report, and False for safe HTTP retries."""
        if not self.enabled or not report_id:
            return True
        client = self._client_or_raise()
        try:
            return bool(
                client.set(
                    self._dedup_key(job_id, report_id),
                    self.instance_id,
                    nx=True,
                    ex=self.report_dedup_ttl_seconds,
                )
            )
        except Exception as e:
            raise JobOwnerStoreError(
                f"deduplicate report for {job_id} failed: {e}"
            ) from e

    def forget_report(self, job_id: str, report_id: str):
        """Rollback a dedup reservation when the local queue rejects it."""
        if not self.enabled or not report_id:
            return
        client = self._client_or_raise()
        try:
            client.eval(
                self._RELEASE_SCRIPT,
                1,
                self._dedup_key(job_id, report_id),
                self.instance_id,
            )
        except Exception as e:
            raise JobOwnerStoreError(
                f"rollback report dedup for {job_id} failed: {e}"
            ) from e

    def local_owner_count(self) -> int:
        with self._lock:
            return len(self._local_owners)

    def is_instance_registered(self) -> bool:
        """Return whether the latest Redis membership heartbeat succeeded."""
        return not self.enabled or self._instance_registered

    def active_instance_count(self) -> int:
        """Return the number of non-expired Brain allocator members."""
        if not self.enabled:
            return 1
        client = _redis_client()
        if client is None:
            return 0
        try:
            redis_time = client.time()
            redis_now = float(redis_time[0]) + float(redis_time[1]) / 1000000
            cutoff = redis_now - self.instance_ttl_seconds
            return int(
                client.zcount(BRAIN_INSTANCE_INDEX_KEY, cutoff, "+inf")
            )
        except Exception as e:
            logger.warning("Count active Brain instances failed: %s", e)
            return 0

    def quarantined_instance_count(self) -> int:
        """Return the number of instances currently excluded from allocation."""
        if not self.enabled:
            return 0
        client = _redis_client()
        if client is None:
            return 0
        try:
            redis_time = client.time()
            redis_now = (
                float(redis_time[0]) + float(redis_time[1]) / 1000000
            )
            return int(client.zcount(BRAIN_QUARANTINE_KEY, redis_now, "+inf"))
        except Exception as e:
            logger.warning("Count quarantined Brain instances failed: %s", e)
            return 0

    def record_forward_failure(self, owner: JobOwner) -> bool:
        """Record an unreachable owner and quarantine it at the threshold.

        The current membership endpoint must still match, which prevents a
        delayed failure from quarantining a restarted instance that reused the
        same instance id.
        """
        if not self.enabled or owner.instance_id == self.instance_id:
            return False
        client = self._client_or_raise()
        try:
            result = client.eval(
                self._RECORD_FORWARD_FAILURE_SCRIPT,
                3,
                BRAIN_INSTANCE_DATA_KEY,
                BRAIN_FORWARD_FAILURE_KEY,
                BRAIN_QUARANTINE_KEY,
                owner.instance_id,
                owner.endpoint,
                str(self.forward_failure_threshold),
                str(self.forward_failure_window_seconds),
                str(self.forward_quarantine_seconds),
            )
            count = int(result[0]) if result else 0
            quarantined = bool(result and int(result[1]) == 1)
            if quarantined:
                logger.error(
                    "Quarantined Brain instance %s for %ss after %s "
                    "consecutive forwarding failures.",
                    owner.instance_id,
                    self.forward_quarantine_seconds,
                    count,
                )
            return quarantined
        except Exception as e:
            raise JobOwnerStoreError(
                f"record forward failure for {owner.instance_id} failed: {e}"
            ) from e

    def record_forward_success(self, owner: JobOwner) -> bool:
        """Clear failure/quarantine state after proving HTTP reachability."""
        if not self.enabled or owner.instance_id == self.instance_id:
            return False
        client = self._client_or_raise()
        try:
            result = client.eval(
                self._CLEAR_FORWARD_FAILURE_SCRIPT,
                3,
                BRAIN_INSTANCE_DATA_KEY,
                BRAIN_FORWARD_FAILURE_KEY,
                BRAIN_QUARANTINE_KEY,
                owner.instance_id,
                owner.endpoint,
            )
            return int(result) == 1
        except Exception as e:
            raise JobOwnerStoreError(
                f"clear forward failure for {owner.instance_id} failed: {e}"
            ) from e
    
    def begin_cleanup(self, job_id: str) -> Optional[JobCleanupLease]:
        """Fence a job with a tokenized cleanup marker.

        Replacing the owner value makes any in-flight renewal fail and blocks
        new claims until ``finish_cleanup`` conditionally removes the marker.
        """
        client = self._client_or_raise()
        key = self._owner_key(job_id)
        try:
            previous = client.get(key) or ""
            if previous and self._is_cleanup_value(previous):
                return None
            previous_ttl_ms = int(client.pttl(key)) if previous else 0
            cleanup_value = json.dumps(
                {"state": "cleaning", "token": uuid.uuid4().hex},
                separators=(",", ":"),
                sort_keys=True,
            )
            acquired = client.eval(
                self._BEGIN_CLEANUP_SCRIPT,
                1,
                key,
                previous,
                cleanup_value,
                str(self.cleanup_ttl_seconds),
            )
            if int(acquired) != 1:
                return None
            with self._lock:
                self._local_owners.pop(job_id, None)
            return JobCleanupLease(
                job_id=job_id,
                cleanup_value=cleanup_value,
                previous_owner_value=previous,
                previous_ttl_ms=max(1000, previous_ttl_ms),
            )
        except Exception as e:
            raise JobOwnerStoreError(
                f"begin cleanup for {job_id} failed: {e}"
            ) from e

    def finish_cleanup(
        self, lease: JobCleanupLease, restore_previous_owner: bool = False
    ) -> bool:
        """Release a cleanup marker, optionally restoring the fenced owner."""
        client = self._client_or_raise()
        previous = (
            lease.previous_owner_value if restore_previous_owner else ""
        )
        try:
            result = client.eval(
                self._FINISH_CLEANUP_SCRIPT,
                1,
                self._owner_key(lease.job_id),
                lease.cleanup_value,
                previous,
                str(lease.previous_ttl_ms),
            )
            return int(result) == 1
        except Exception as e:
            raise JobOwnerStoreError(
                f"finish cleanup for {lease.job_id} failed: {e}"
            ) from e

    def cleanup_report_state(self, job_id: str) -> bool:
        """Idempotently remove all report deduplication keys for a job."""
        client = self._client_or_raise()
        pattern = f"{REPORT_DEDUP_KEY_PREFIX}:{self._glob_escape(job_id)}:*"
        try:
            # Delete one key at a time to support Redis Cluster keys that do
            # not share a hash slot.
            for key in client.scan_iter(match=pattern, count=100):
                client.delete(key)
            return True
        except Exception as e:
            logger.warning(
                "Cleanup report dedup state for %s failed: %s", job_id, e
            )
            return False

    def _remember_local(self, job_id: str, owner: JobOwner, raw: str):
        with self._lock:
            self._local_owners[job_id] = _LocalOwnership(
                owner=owner,
                raw_value=raw,
                last_seen=time.monotonic(),
            )

    def _renew_loop(self):
        while not self._stop_event.wait(self.renew_interval_seconds):
            now = time.monotonic()
            with self._lock:
                owned = list(self._local_owners.items())

            client = _redis_client()
            if client is None:
                self._instance_registered = False
                self._drop_all_local_owners()
                continue

            self._refresh_instance_registration(client)

            for job_id, state in owned:
                if now - state.last_seen > self.idle_timeout_seconds:
                    with self._lock:
                        self._local_owners.pop(job_id, None)
                    continue
                try:
                    renewed = client.eval(
                        self._RENEW_SCRIPT,
                        1,
                        self._owner_key(job_id),
                        state.raw_value,
                        str(self.lease_ttl_seconds),
                    )
                    if int(renewed) != 1:
                        with self._lock:
                            self._local_owners.pop(job_id, None)
                        logger.warning("Lost ownership of job %s.", job_id)
                except Exception as e:
                    # Fail closed immediately. A queued item will revalidate
                    # against Redis before changing elastic state.
                    with self._lock:
                        self._local_owners.pop(job_id, None)
                    logger.warning("Renew job owner %s failed: %s", job_id, e)

    def _refresh_instance_registration(self, client=None) -> bool:
        if not self.enabled:
            return True
        if self._stopped:
            self._instance_registered = False
            return False
        client = client or _redis_client()
        if client is None:
            self._instance_registered = False
            return False
        try:
            result = client.eval(
                self._REGISTER_INSTANCE_SCRIPT,
                4,
                BRAIN_INSTANCE_INDEX_KEY,
                BRAIN_INSTANCE_DATA_KEY,
                BRAIN_FORWARD_FAILURE_KEY,
                BRAIN_QUARANTINE_KEY,
                self.instance_id,
                self._instance_value,
            )
            self._instance_registered = int(result) == 1
        except Exception as e:
            self._instance_registered = False
            logger.warning(
                "Register Brain instance %s failed: %s", self.instance_id, e
            )
        return self._instance_registered

    def _release_instance_registration(self, client) -> None:
        try:
            client.eval(
                self._RELEASE_INSTANCE_SCRIPT,
                4,
                BRAIN_INSTANCE_INDEX_KEY,
                BRAIN_INSTANCE_DATA_KEY,
                BRAIN_FORWARD_FAILURE_KEY,
                BRAIN_QUARANTINE_KEY,
                self.instance_id,
                self._instance_value,
            )
        except Exception as e:
            logger.warning(
                "Release Brain instance %s failed: %s", self.instance_id, e
            )
        finally:
            self._instance_registered = False

    def _allocate_owner(self, client) -> JobOwner:
        result = client.eval(
            self._SELECT_INSTANCE_SCRIPT,
            5,
            BRAIN_INSTANCE_INDEX_KEY,
            BRAIN_INSTANCE_DATA_KEY,
            BRAIN_ROUND_ROBIN_KEY,
            BRAIN_FORWARD_FAILURE_KEY,
            BRAIN_QUARANTINE_KEY,
            str(self.instance_ttl_seconds),
        )
        if not result or len(result) != 2:
            raise JobOwnerStoreError("no active Brain instance is registered")
        instance_id = self._decode_redis_text(result[0])
        endpoint = self._decode_redis_text(result[1])
        if not instance_id or not endpoint:
            raise JobOwnerStoreError("invalid Brain instance allocation")
        return JobOwner(instance_id, endpoint, uuid.uuid4().hex)

    def _drop_all_local_owners(self):
        with self._lock:
            self._local_owners.clear()

    def _client_or_raise(self):
        client = _redis_client()
        if client is None:
            raise JobOwnerStoreError("Redis is unavailable")
        return client

    def _instance_endpoint(self) -> str:
        configured = os.getenv("DLROVER_BRAIN_INTERNAL_ADDR", "").strip()
        if configured:
            if not configured.startswith(("http://", "https://")):
                configured = f"http://{configured}"
            return configured.rstrip("/")
        host = (
            os.getenv("POD_IP", "").strip()
            or os.getenv("DLROVER_BRAIN_INSTANCE_HOST", "").strip()
            or self.instance_id
        )
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        port = min(
            65535,
            self._env_int(
                "DLROVER_BRAIN_INTERNAL_PORT", 50002, minimum=1
            ),
        )
        return f"http://{host}:{port}"

    def _encode_instance(self) -> str:
        return json.dumps(
            {"endpoint": self.endpoint, "token": self._instance_token},
            separators=(",", ":"),
            sort_keys=True,
        )

    @staticmethod
    def _decode_redis_text(value) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)

    @staticmethod
    def _encode_owner(owner: JobOwner, state: str = "active") -> str:
        owner_payload = {
            "instance_id": owner.instance_id,
            "endpoint": owner.endpoint,
            "token": owner.token,
        }
        if state == "registering":
            # Keep registration fields nested so an older Brain version that
            # does not understand REGISTERING fails closed instead of treating
            # the incomplete registration as an active owner.
            payload = {"state": state, "owner": owner_payload}
        else:
            payload = owner_payload
            if state:
                payload["state"] = state
        return json.dumps(
            payload,
            separators=(",", ":"),
            sort_keys=True,
        )

    @staticmethod
    def _is_cleanup_value(raw: str) -> bool:
        try:
            return json.loads(raw).get("state") == "cleaning"
        except (TypeError, ValueError, AttributeError):
            return False

    @staticmethod
    def _is_registering_value(raw: str) -> bool:
        try:
            return json.loads(raw).get("state") == "registering"
        except (TypeError, ValueError, AttributeError):
            return False

    @staticmethod
    def _glob_escape(value: str) -> str:
        escaped = value.replace("\\", "\\\\")
        for char in ("*", "?", "["):
            escaped = escaped.replace(char, f"\\{char}")
        return escaped

    @staticmethod
    def _decode_owner(raw: str) -> JobOwner:
        try:
            data = json.loads(raw)
            if data.get("state") == "registering":
                data = data["owner"]
            owner = JobOwner(
                instance_id=str(data["instance_id"]),
                endpoint=str(data["endpoint"]),
                token=str(data["token"]),
            )
        except Exception as e:
            raise JobOwnerStoreError(f"invalid job owner payload: {e}") from e
        if not owner.instance_id or not owner.endpoint or not owner.token:
            raise JobOwnerStoreError("job owner payload contains empty fields")
        return owner

    @staticmethod
    def _owner_key(job_id: str) -> str:
        return f"{OWNER_KEY_PREFIX}:{job_id}"

    @staticmethod
    def _allocation_lock_key(job_id: str) -> str:
        return f"{OWNER_ALLOCATION_LOCK_PREFIX}:{job_id}"

    @staticmethod
    def _dedup_key(job_id: str, report_id: str) -> str:
        return f"{REPORT_DEDUP_KEY_PREFIX}:{job_id}:{report_id}"

    @staticmethod
    def _env_int(name: str, default: int, minimum: int) -> int:
        value = os.getenv(name, "").strip()
        try:
            parsed = int(value) if value else default
        except ValueError:
            logger.warning("Invalid %s=%s, use %s.", name, value, default)
            parsed = default
        return max(minimum, parsed)
