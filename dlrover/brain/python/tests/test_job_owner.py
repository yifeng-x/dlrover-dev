import json
import os
import time
import unittest
from fnmatch import fnmatch
from unittest.mock import patch

from dlrover.brain.python.jobmanagement.job_owner import (
    BRAIN_FORWARD_FAILURE_KEY,
    BRAIN_INSTANCE_INDEX_KEY,
    BRAIN_QUARANTINE_KEY,
    JobOwner,
    JobOwnerManager,
    JobOwnerStoreError,
)


class _FakeRedis:
    def __init__(self):
        self.values = {}
        self.hashes = {}
        self.zsets = {}
        self.counters = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    def eval(self, script, key_count, *params):
        keys = params[:key_count]
        argv = params[key_count:]
        if 'last_failure = now' in script:
            data_key, failure_key, quarantine_key = keys
            instance_id, endpoint, threshold, window, quarantine = argv
            raw = self.hashes.setdefault(data_key, {}).get(instance_id)
            if not raw or json.loads(raw).get("endpoint") != endpoint:
                return [0, 0]
            now = time.time()
            previous_raw = self.hashes.setdefault(failure_key, {}).get(
                instance_id
            )
            count = 1
            if previous_raw:
                previous = json.loads(previous_raw)
                if now - previous["last_failure"] <= float(window):
                    count = previous["count"] + 1
            self.hashes[failure_key][instance_id] = json.dumps(
                {"count": count, "last_failure": now}
            )
            quarantined = count >= int(threshold)
            if quarantined:
                self.zsets.setdefault(quarantine_key, {})[
                    instance_id
                ] = now + float(quarantine)
            return [count, int(quarantined)]
        if 'redis.call("hdel", KEYS[2], ARGV[1])' in script and (
            'redis.call("zrem", KEYS[3], ARGV[1])' in script
        ):
            data_key, failure_key, quarantine_key = keys
            instance_id, endpoint = argv
            raw = self.hashes.setdefault(data_key, {}).get(instance_id)
            if not raw or json.loads(raw).get("endpoint") != endpoint:
                return 0
            self.hashes.setdefault(failure_key, {}).pop(instance_id, None)
            self.zsets.setdefault(quarantine_key, {}).pop(instance_id, None)
            return 1
        if 'redis.call("zadd"' in script:
            index_key, data_key, failure_key, quarantine_key = keys
            instance_id, instance_value = argv
            previous = self.hashes.setdefault(data_key, {}).get(instance_id)
            if not previous or previous != instance_value:
                self.hashes.setdefault(failure_key, {}).pop(
                    instance_id, None
                )
                self.zsets.setdefault(quarantine_key, {}).pop(
                    instance_id, None
                )
            self.zsets.setdefault(index_key, {})[instance_id] = time.time()
            self.hashes[data_key][instance_id] = instance_value
            return 1
        if 'zremrangebyscore' in script:
            (
                index_key,
                data_key,
                sequence_key,
                failure_key,
                quarantine_key,
            ) = keys
            now = time.time()
            cutoff = now - float(argv[0])
            zset = self.zsets.setdefault(index_key, {})
            for member, score in list(zset.items()):
                if score <= cutoff:
                    zset.pop(member, None)
                    self.hashes.setdefault(data_key, {}).pop(member, None)
                    self.hashes.setdefault(failure_key, {}).pop(member, None)
                    self.zsets.setdefault(quarantine_key, {}).pop(
                        member, None
                    )
            quarantine = self.zsets.setdefault(quarantine_key, {})
            for member, score in list(quarantine.items()):
                if score <= now:
                    quarantine.pop(member, None)
            instances = [
                member for member in sorted(zset) if member not in quarantine
            ]
            if not instances:
                return []
            sequence = self.counters.get(sequence_key, 0) + 1
            self.counters[sequence_key] = sequence
            data = self.hashes.setdefault(data_key, {})
            for offset in range(len(instances)):
                index = (sequence - 1 + offset) % len(instances)
                instance_id = instances[index]
                instance = json.loads(data[instance_id])
                endpoint = instance.get("endpoint", "")
                if endpoint:
                    return [instance_id, endpoint]
            return []
        if 'redis.call("hget", KEYS[2]' in script:
            index_key, data_key, failure_key, quarantine_key = keys
            instance_id, expected_value = argv
            data = self.hashes.setdefault(data_key, {})
            if data.get(instance_id) != expected_value:
                return 0
            self.zsets.setdefault(index_key, {}).pop(instance_id, None)
            data.pop(instance_id, None)
            self.hashes.setdefault(failure_key, {}).pop(instance_id, None)
            self.zsets.setdefault(quarantine_key, {}).pop(instance_id, None)
            return 1

        key = keys[0]
        expected = argv[0]
        args = argv[1:]
        if '"PX"' in script:
            previous, ttl_ms = args
            if self.values.get(key) != expected:
                return 0
            if previous:
                self.values[key] = previous
            else:
                self.values.pop(key, None)
            return 1
        if '"EX"' in script and len(args) == 2:
            cleanup_value, ttl_seconds = args
            current = self.values.get(key, "")
            if current != expected:
                return 0
            self.values[key] = cleanup_value
            return 1
        if self.values.get(key) != expected:
            return 0
        if "expire" in script:
            return 1
        self.values.pop(key, None)
        return 1

    def pttl(self, key):
        return 30000 if key in self.values else -2

    def scan_iter(self, match=None, count=None):
        for key in list(self.values):
            if match is None or fnmatch(key, match):
                yield key

    def delete(self, *keys):
        deleted = 0
        for key in keys:
            if key in self.values:
                deleted += 1
                self.values.pop(key, None)
        return deleted

    def zcount(self, key, minimum, maximum):
        minimum = float(minimum)
        return sum(
            1
            for score in self.zsets.get(key, {}).values()
            if score >= minimum
        )

    def time(self):
        now = time.time()
        seconds = int(now)
        return seconds, int((now - seconds) * 1000000)


class TestJobOwnerManager(unittest.TestCase):
    def setUp(self):
        self.redis = _FakeRedis()
        self.env = patch.dict(
            os.environ,
            {
                "DLROVER_BRAIN_MULTI_INSTANCE": "true",
                "DLROVER_BRAIN_INSTANCE_ID": "brain-0",
                "POD_IP": "10.0.0.10",
                "DLROVER_BRAIN_INTERNAL_PORT": "50002",
            },
            clear=False,
        )
        self.env.start()
        self.redis_patch = patch(
            "dlrover.brain.python.jobmanagement.job_owner._redis_client",
            return_value=self.redis,
        )
        self.redis_patch.start()

    def tearDown(self):
        self.redis_patch.stop()
        self.env.stop()

    def test_first_instance_claims_and_keeps_job(self):
        manager = JobOwnerManager()

        owner1, claimed1 = manager.claim_for_registration("job-1")
        owner2, claimed2 = manager.claim_for_registration("job-1")

        self.assertEqual(owner1, owner2)
        self.assertTrue(claimed1)
        self.assertFalse(claimed2)
        self.assertEqual(owner1.instance_id, "brain-0")
        self.assertEqual(owner1.endpoint, "http://10.0.0.10:50002")
        registering = json.loads(self.redis.values[manager_key("job-1")])
        self.assertEqual(registering["state"], "registering")
        self.assertNotIn("instance_id", registering)
        self.assertEqual(registering["owner"]["instance_id"], "brain-0")
        self.assertIsNone(manager.get_owner("job-1"))
        self.assertTrue(manager.accept_forwarded("job-1", owner1.token))
        self.assertTrue(manager.commit_registration("job-1", owner1.token))
        self.assertEqual(manager.get_owner("job-1"), owner1)
        self.assertTrue(manager.is_current_owner("job-1", owner1.token))

    def test_different_jobs_are_allocated_round_robin(self):
        manager0 = JobOwnerManager()
        manager0._refresh_instance_registration(self.redis)
        with patch.dict(
            os.environ,
            {
                "DLROVER_BRAIN_INSTANCE_ID": "brain-1",
                "POD_IP": "10.0.0.11",
            },
        ):
            manager1 = JobOwnerManager()
        manager1._refresh_instance_registration(self.redis)

        self.assertEqual(manager0.active_instance_count(), 2)

        owner1, _ = manager0.claim_for_registration("job-1")
        owner2, _ = manager0.claim_for_registration("job-2")
        owner3, _ = manager0.claim_for_registration("job-3")

        self.assertEqual(owner1.instance_id, "brain-0")
        self.assertEqual(owner2.instance_id, "brain-1")
        self.assertEqual(owner3.instance_id, "brain-0")
        self.assertEqual(owner2.endpoint, "http://10.0.0.11:50002")

    def test_duplicate_job_does_not_advance_round_robin(self):
        manager0 = JobOwnerManager()
        manager0._refresh_instance_registration(self.redis)
        with patch.dict(
            os.environ,
            {
                "DLROVER_BRAIN_INSTANCE_ID": "brain-1",
                "POD_IP": "10.0.0.11",
            },
        ):
            manager1 = JobOwnerManager()
        manager1._refresh_instance_registration(self.redis)

        first, first_claimed = manager0.claim_for_registration("job-1")
        duplicate, duplicate_claimed = manager1.claim_for_registration(
            "job-1"
        )
        second, _ = manager0.claim_for_registration("job-2")

        self.assertTrue(first_claimed)
        self.assertFalse(duplicate_claimed)
        self.assertEqual(duplicate, first)
        self.assertEqual(second.instance_id, "brain-1")

    def test_ingress_can_token_fence_remote_registration_rollback(self):
        manager0 = JobOwnerManager()
        manager0._refresh_instance_registration(self.redis)
        with patch.dict(
            os.environ,
            {
                "DLROVER_BRAIN_INSTANCE_ID": "brain-1",
                "POD_IP": "10.0.0.11",
            },
        ):
            manager1 = JobOwnerManager()
        manager1._refresh_instance_registration(self.redis)
        manager0.claim_for_registration("job-1")
        remote_owner, claimed = manager0.claim_for_registration("job-2")

        self.assertTrue(claimed)
        self.assertEqual(remote_owner.instance_id, "brain-1")
        self.assertTrue(
            manager0.release_claim("job-2", remote_owner.token)
        )
        self.assertIsNone(manager0.get_owner("job-2"))

    def test_allocator_excludes_expired_instance(self):
        manager0 = JobOwnerManager()
        manager0._refresh_instance_registration(self.redis)
        with patch.dict(
            os.environ,
            {
                "DLROVER_BRAIN_INSTANCE_ID": "brain-1",
                "POD_IP": "10.0.0.11",
            },
        ):
            manager1 = JobOwnerManager()
        manager1._refresh_instance_registration(self.redis)
        self.redis.zsets[BRAIN_INSTANCE_INDEX_KEY]["brain-0"] = 0

        owner, _ = manager1.claim_for_registration("job-1")

        self.assertEqual(owner.instance_id, "brain-1")

    def test_consecutive_forward_failures_quarantine_instance(self):
        manager0 = JobOwnerManager()
        manager0._refresh_instance_registration(self.redis)
        with patch.dict(
            os.environ,
            {
                "DLROVER_BRAIN_INSTANCE_ID": "brain-1",
                "POD_IP": "10.0.0.11",
            },
        ):
            manager1 = JobOwnerManager()
        manager1._refresh_instance_registration(self.redis)
        remote = JobOwner(
            manager1.instance_id, manager1.endpoint, "owner-token"
        )

        self.assertFalse(manager0.record_forward_failure(remote))
        self.assertFalse(manager0.record_forward_failure(remote))
        self.assertTrue(manager0.record_forward_failure(remote))
        self.assertEqual(manager0.quarantined_instance_count(), 1)

        owner1, _ = manager0.claim_for_registration("job-1")
        owner2, _ = manager0.claim_for_registration("job-2")
        self.assertEqual(owner1.instance_id, "brain-0")
        self.assertEqual(owner2.instance_id, "brain-0")

    def test_forward_success_clears_failure_and_quarantine(self):
        manager0 = JobOwnerManager()
        manager0._refresh_instance_registration(self.redis)
        with patch.dict(
            os.environ,
            {
                "DLROVER_BRAIN_INSTANCE_ID": "brain-1",
                "POD_IP": "10.0.0.11",
                "DLROVER_BRAIN_FORWARD_FAILURE_THRESHOLD": "1",
            },
        ):
            manager1 = JobOwnerManager()
        manager1._refresh_instance_registration(self.redis)
        remote = JobOwner(
            manager1.instance_id, manager1.endpoint, "owner-token"
        )

        # The reporting ingress owns the threshold, so set it explicitly for
        # this focused one-failure test.
        manager0.forward_failure_threshold = 1
        self.assertTrue(manager0.record_forward_failure(remote))
        self.assertIn(
            remote.instance_id,
            self.redis.hashes[BRAIN_FORWARD_FAILURE_KEY],
        )

        self.assertTrue(manager0.record_forward_success(remote))
        self.assertEqual(manager0.quarantined_instance_count(), 0)
        self.assertNotIn(
            remote.instance_id,
            self.redis.hashes[BRAIN_FORWARD_FAILURE_KEY],
        )

    def test_stale_endpoint_cannot_quarantine_restarted_instance(self):
        manager0 = JobOwnerManager()
        manager0._refresh_instance_registration(self.redis)
        with patch.dict(
            os.environ,
            {
                "DLROVER_BRAIN_INSTANCE_ID": "brain-1",
                "POD_IP": "10.0.0.11",
            },
        ):
            manager1 = JobOwnerManager()
        manager1._refresh_instance_registration(self.redis)
        stale_owner = JobOwner(
            manager1.instance_id, "http://10.0.0.99:50002", "old-token"
        )
        manager0.forward_failure_threshold = 1

        self.assertFalse(manager0.record_forward_failure(stale_owner))
        self.assertEqual(manager0.quarantined_instance_count(), 0)
        self.assertNotIn(
            manager1.instance_id,
            self.redis.hashes.get(BRAIN_FORWARD_FAILURE_KEY, {}),
        )

    def test_stop_removes_only_matching_instance_registration(self):
        manager = JobOwnerManager()
        manager._refresh_instance_registration(self.redis)

        manager.stop()

        self.assertFalse(manager.is_instance_registered())
        self.assertNotIn(
            "brain-0",
            self.redis.zsets[BRAIN_INSTANCE_INDEX_KEY],
        )
        with self.assertRaisesRegex(JobOwnerStoreError, "stopping"):
            manager.claim_for_registration("job-after-stop")

    def test_get_owner_does_not_create_lease(self):
        manager = JobOwnerManager()

        self.assertIsNone(manager.get_owner("job-1"))
        self.assertNotIn(manager_key("job-1"), self.redis.values)

    def test_release_registration_claim_is_token_fenced(self):
        manager = JobOwnerManager()
        owner, claimed = manager.claim_for_registration("job-1")

        self.assertTrue(claimed)
        self.assertFalse(manager.release_claim("job-1", "stale-token"))
        current, claimed_again = manager.claim_for_registration("job-1")
        self.assertEqual(current, owner)
        self.assertFalse(claimed_again)
        self.assertTrue(manager.release_claim("job-1", owner.token))
        self.assertIsNone(manager.get_owner("job-1"))

    def test_existing_remote_owner_is_returned(self):
        remote = JobOwner("brain-1", "http://10.0.0.11:50002", "token-1")
        self.redis.values[manager_key("job-1")] = json.dumps(
            {
                "instance_id": remote.instance_id,
                "endpoint": remote.endpoint,
                "token": remote.token,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        manager = JobOwnerManager()

        owner, _ = manager.claim_for_registration("job-1")

        self.assertEqual(owner, remote)
        self.assertFalse(manager.accept_forwarded("job-1", remote.token))

    def test_old_token_is_fenced_after_takeover(self):
        manager = JobOwnerManager()
        old_owner, _ = manager.claim_for_registration("job-1")
        new_owner = JobOwner(
            "brain-1", "http://10.0.0.11:50002", "new-token"
        )
        self.redis.values[manager_key("job-1")] = manager._encode_owner(
            new_owner
        )

        self.assertFalse(
            manager.is_current_owner("job-1", old_owner.token)
        )
        self.assertFalse(manager.accept_forwarded("job-1", old_owner.token))

    def test_report_deduplication(self):
        manager = JobOwnerManager()

        self.assertTrue(manager.mark_report_once("job-1", "1000"))
        self.assertFalse(manager.mark_report_once("job-1", "1000"))

    def test_redis_unavailable_fails_closed(self):
        with patch(
            "dlrover.brain.python.jobmanagement.job_owner._redis_client",
            return_value=None,
        ):
            manager = JobOwnerManager()
            with self.assertRaises(JobOwnerStoreError):
                manager.claim_for_registration("job-1")

    def test_cleanup_marker_fences_claim_and_can_restore_owner(self):
        manager = JobOwnerManager()
        owner, _ = manager.claim_for_registration("job-1")

        lease = manager.begin_cleanup("job-1")

        self.assertIsNotNone(lease)
        self.assertFalse(manager.is_current_owner("job-1", owner.token))
        with self.assertRaises(JobOwnerStoreError):
            manager.claim_for_registration("job-1")

        self.assertTrue(
            manager.finish_cleanup(lease, restore_previous_owner=True)
        )
        restored, claimed = manager.claim_for_registration("job-1")
        self.assertEqual(restored, owner)
        self.assertFalse(claimed)

    def test_cleanup_report_state_is_idempotent(self):
        manager = JobOwnerManager()
        self.redis.values[
            "dlrover:brain:vertical_elastic:report:job-1:r1"
        ] = "brain-0"
        self.redis.values[
            "dlrover:brain:vertical_elastic:report:job-2:r1"
        ] = "brain-0"

        self.assertTrue(manager.cleanup_report_state("job-1"))
        self.assertTrue(manager.cleanup_report_state("job-1"))
        self.assertNotIn(
            "dlrover:brain:vertical_elastic:report:job-1:r1",
            self.redis.values,
        )
        self.assertIn(
            "dlrover:brain:vertical_elastic:report:job-2:r1",
            self.redis.values,
        )


def manager_key(job_id):
    return f"dlrover:brain:vertical_elastic:owner:{job_id}"


if __name__ == "__main__":
    unittest.main()
