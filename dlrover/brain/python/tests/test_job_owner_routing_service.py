# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest
from unittest.mock import MagicMock

import requests

from dlrover.brain.python.jobmanagement.job_owner import JobOwner
from dlrover.brain.python.jobmanagement.job_owner_routing_service import (
    JobOwnerRoutingError,
    JobOwnerRoutingService,
)


class TestJobOwnerRoutingService(unittest.TestCase):
    def setUp(self):
        self.owner_manager = MagicMock(
            enabled=True,
            instance_id="brain-0",
            endpoint="http://10.0.0.10:50002",
        )
        self.session = MagicMock()
        self.service = JobOwnerRoutingService(
            self.owner_manager, session=self.session
        )

    def test_resolve_existing_remote_owner_without_claiming(self):
        owner = JobOwner(
            "brain-1", "http://10.0.0.11:50002", "token-1"
        )
        self.owner_manager.get_owner.return_value = owner

        route = self.service.resolve_existing_owner("job-1")

        self.assertFalse(route.local)
        self.assertEqual(route.owner, owner)
        self.owner_manager.claim_for_registration.assert_not_called()
        self.owner_manager.accept_forwarded.assert_not_called()

    def test_missing_owner_is_retryable_and_never_claimed(self):
        self.owner_manager.get_owner.return_value = None

        with self.assertRaisesRegex(
            JobOwnerRoutingError, "owner is not registered"
        ) as context:
            self.service.resolve_existing_owner("job-1")

        self.assertTrue(context.exception.retryable)
        self.owner_manager.claim_for_registration.assert_not_called()

    def test_forwarded_request_requires_matching_token(self):
        self.owner_manager.accept_forwarded.return_value = False

        with self.assertRaisesRegex(JobOwnerRoutingError, "owner changed"):
            self.service.resolve_existing_owner(
                "job-1", forwarded=True, owner_token="stale-token"
            )

    def test_local_owner_is_fenced_before_execution(self):
        owner = JobOwner(
            "brain-0", "http://10.0.0.10:50002", "token-0"
        )
        self.owner_manager.get_owner.return_value = owner
        self.owner_manager.accept_forwarded.return_value = True

        route = self.service.resolve_existing_owner("job-1")

        self.assertTrue(route.local)
        self.owner_manager.accept_forwarded.assert_called_once_with(
            "job-1", "token-0"
        )

    def test_computed_result_is_fenced_before_return(self):
        self.owner_manager.is_current_owner.return_value = False

        with self.assertRaisesRegex(JobOwnerRoutingError, "owner changed"):
            self.service.validate_local_owner("job-1", "stale-token")

        self.owner_manager.is_current_owner.assert_called_once_with(
            "job-1", "stale-token"
        )

    def test_connection_failure_updates_shared_circuit(self):
        owner = JobOwner(
            "brain-1", "http://10.0.0.11:50002", "token-1"
        )
        self.session.post.side_effect = requests.ConnectionError(
            "connection refused"
        )

        with self.assertRaisesRegex(
            JobOwnerRoutingError, "connection refused"
        ):
            self.service.forward_json(
                owner, "/internal/optimize", {"job": {}}, "optimize"
            )

        self.owner_manager.record_forward_failure.assert_called_once_with(
            owner
        )

    def test_successful_forward_clears_shared_circuit(self):
        owner = JobOwner(
            "brain-1", "http://10.0.0.11:50002", "token-1"
        )
        response = MagicMock(status_code=200)
        response.json.return_value = {"job_opt_plan": {}}
        self.session.post.return_value = response

        body = self.service.forward_json(
            owner, "/internal/optimize", {"job": {}}, "optimize"
        )

        self.assertEqual(body, {"job_opt_plan": {}})
        self.owner_manager.record_forward_success.assert_called_once_with(
            owner
        )

    def test_single_instance_routes_locally_without_redis(self):
        self.owner_manager.enabled = False

        route = self.service.resolve_existing_owner("job-1")

        self.assertTrue(route.local)
        self.owner_manager.get_owner.assert_not_called()


if __name__ == "__main__":
    unittest.main()
