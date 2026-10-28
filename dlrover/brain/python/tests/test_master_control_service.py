import unittest
from unittest.mock import MagicMock

import requests

from dlrover.brain.python.common.job import JobMeta
from dlrover.brain.python.jobmanagement.dispatch_result import DispatchResult
from dlrover.brain.python.jobmanagement.job_owner import JobOwner
from dlrover.brain.python.jobmanagement.master_control_service import (
    MasterControlService,
)


class TestMasterControlService(unittest.TestCase):
    def setUp(self):
        self.pool = MagicMock()
        self.owner_manager = MagicMock(enabled=True, instance_id="brain-0")
        self.service = MasterControlService(self.pool, self.owner_manager)
        self.job_meta = JobMeta(uuid="job-id-1", name="job-a")

    def tearDown(self):
        self.service.close()

    def _local_claim(self):
        owner = JobOwner(
            "brain-0", "http://10.0.0.10:50002", "token-0"
        )
        self.owner_manager.claim_for_registration.return_value = (owner, True)
        self.owner_manager.accept_forwarded.return_value = True
        return owner

    def test_master_registration_claims_local_owner(self):
        self._local_claim()
        self.pool.register.return_value = True
        self.owner_manager.commit_registration.return_value = True

        result = self.service.register_master(
            self.job_meta, "master-svc:50001"
        )

        self.assertTrue(result.success)
        self.pool.register.assert_called_once_with(
            "job-a", "job-id-1", "master-svc:50001"
        )
        self.owner_manager.commit_registration.assert_called_once_with(
            "job-id-1", "token-0"
        )
        self.owner_manager.release_claim.assert_not_called()

    def test_registration_rolls_back_new_claim_on_store_failure(self):
        self._local_claim()
        self.pool.register.return_value = False

        result = self.service.register_master(
            self.job_meta, "master-svc:50001"
        )

        self.assertFalse(result.success)
        self.assertTrue(result.retryable)
        self.owner_manager.release_claim.assert_called_once_with(
            "job-id-1", "token-0"
        )

    def test_registration_rolls_back_when_claim_validation_fails(self):
        self._local_claim()
        self.owner_manager.accept_forwarded.return_value = False

        result = self.service.register_master(
            self.job_meta, "master-svc:50001"
        )

        self.assertFalse(result.success)
        self.pool.register.assert_not_called()
        self.owner_manager.release_claim.assert_called_once_with(
            "job-id-1", "token-0"
        )

    def test_registration_rolls_back_when_commit_fails(self):
        self._local_claim()
        self.pool.register.return_value = True
        self.owner_manager.commit_registration.return_value = False

        result = self.service.register_master(
            self.job_meta, "master-svc:50001"
        )

        self.assertFalse(result.success)
        self.assertEqual(result.reason, "owner registration commit failed")
        self.owner_manager.release_claim.assert_called_once_with(
            "job-id-1", "token-0"
        )

    def test_remote_owner_receives_forwarded_registration(self):
        owner = JobOwner(
            "brain-1", "http://10.0.0.11:50002", "token-1"
        )
        self.owner_manager.claim_for_registration.return_value = (owner, False)
        self.service._forward_master_control = MagicMock(
            return_value=DispatchResult(True, "forwarded")
        )

        result = self.service.register_master(
            self.job_meta, "master-svc:50001"
        )

        self.assertTrue(result.success)
        self.service._forward_master_control.assert_called_once_with(
            "/internal/register_master_client",
            self.job_meta,
            "master-svc:50001",
            owner,
            "register",
        )

    def test_failed_remote_forward_rolls_back_new_allocation(self):
        owner = JobOwner(
            "brain-1", "http://10.0.0.11:50002", "token-1"
        )
        self.owner_manager.claim_for_registration.return_value = (owner, True)
        self.service._forward_master_control = MagicMock(
            return_value=DispatchResult(
                False, "connection refused", retryable=True
            )
        )

        result = self.service.register_master(
            self.job_meta, "master-svc:50001"
        )

        self.assertFalse(result.success)
        self.owner_manager.release_claim.assert_called_once_with(
            "job-id-1", "token-1"
        )

    def test_connection_failure_is_recorded_for_circuit_breaker(self):
        owner = JobOwner(
            "brain-1", "http://10.0.0.11:50002", "token-1"
        )
        self.owner_manager.claim_for_registration.return_value = (owner, True)
        self.service._forward_session.post = MagicMock(
            side_effect=requests.ConnectionError("connection refused")
        )

        result = self.service.register_master(
            self.job_meta, "master-svc:50001"
        )

        self.assertFalse(result.success)
        self.owner_manager.record_forward_failure.assert_called_once_with(
            owner
        )
        self.owner_manager.release_claim.assert_called_once_with(
            "job-id-1", "token-1"
        )

    def test_business_rejection_clears_infrastructure_failures(self):
        owner = JobOwner(
            "brain-1", "http://10.0.0.11:50002", "token-1"
        )
        self.owner_manager.claim_for_registration.return_value = (
            owner,
            False,
        )
        response = MagicMock(status_code=409, text="owner changed")
        self.service._forward_session.post = MagicMock(return_value=response)

        result = self.service.register_master(
            self.job_meta, "master-svc:50001"
        )

        self.assertFalse(result.success)
        self.owner_manager.record_forward_success.assert_called_once_with(
            owner
        )
        self.owner_manager.record_forward_failure.assert_not_called()

    def test_heartbeat_recovers_missing_owner_as_registration(self):
        self.owner_manager.get_owner.return_value = None
        self.service.register_master = MagicMock(
            return_value=DispatchResult(True, "register success")
        )

        result = self.service.heartbeat_master(
            self.job_meta, "master-svc:50001"
        )

        self.assertTrue(result.success)
        self.service.register_master.assert_called_once_with(
            self.job_meta, "master-svc:50001"
        )

    def test_redis_owner_error_is_retryable(self):
        from dlrover.brain.python.jobmanagement.job_owner import (
            JobOwnerStoreError,
        )

        self.owner_manager.claim_for_registration.side_effect = (
            JobOwnerStoreError("redis unavailable")
        )

        result = self.service.register_master(
            self.job_meta, "master-svc:50001"
        )

        self.assertFalse(result.success)
        self.assertTrue(result.retryable)


if __name__ == "__main__":
    unittest.main()
