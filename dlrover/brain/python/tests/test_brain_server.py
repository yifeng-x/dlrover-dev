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

import unittest
from unittest.mock import MagicMock
from fastapi.testclient import TestClient
from dlrover.brain.python.server.server import BrainServer
from dlrover.brain.python.jobmanagement.job_elastic_manager import (
    DispatchResult,
)
from dlrover.brain.python.jobmanagement.job_owner import JobOwner
from dlrover.brain.python.jobmanagement.job_owner_routing_service import (
    JobOwnerRoute,
    JobOwnerRoutingError,
)
from dlrover.brain.python.common.job import (
    NodeResource,
    NodeGroupResource,
    JobResource,
    JobOptimizePlan,
)


class TestBrainServer(unittest.TestCase):
    def setUp(self):
        self.brain_server = BrainServer()

        self.brain_server._optimizer_manager = MagicMock()
        self.brain_server._config_manager = MagicMock()

        self.client = TestClient(self.brain_server._server)

    def test_optimize_endpoint(self):
        """Test POST /optimize parses nested payloads and returns a rich JobOptimizePlan."""
        mock_plan = JobOptimizePlan(
            timestamp=1690000000,
            job_resource=JobResource(
                node_group_resources={
                    "worker": NodeGroupResource(
                        count=4,
                        resource=NodeResource(
                            memory=8192, cpu=8.0, gpu=1.0, gpu_type="A100"
                        ),
                    )
                }
            ),
        )
        self.brain_server._optimizer_manager.optimize.return_value = mock_plan

        payload = {
            "type": "training",
            "config": {
                "optimizer": "adam",
                "customized_config": {"learning_rate": "0.01"},
            },
            "job": {
                "uuid": "job-123",
                "cluster": "prod-cluster",
                "namespace": "dl-jobs",
                "user": "alice",
                "app": "training-app",
            },
        }

        response = self.client.post("/optimize", json=payload)

        self.assertEqual(response.status_code, 201)
        response_data = response.json()

        self.assertIn("job_opt_plan", response_data)
        plan_data = response_data["job_opt_plan"]

        self.assertEqual(plan_data["timestamp"], 1690000000)

        worker_group = plan_data["job_resource"]["node_group_resources"][
            "worker"
        ]
        self.assertEqual(worker_group["count"], 4)
        self.assertEqual(worker_group["resource"]["gpu_type"], "A100")
        self.assertEqual(worker_group["resource"]["cpu"], 8.0)

    def test_optimize_is_forwarded_to_existing_remote_owner(self):
        owner = JobOwner(
            "brain-1", "http://10.0.0.11:50002", "token-1"
        )
        self.brain_server._job_owner_routing_service = MagicMock()
        router = self.brain_server._job_owner_routing_service
        router.resolve_existing_owner.return_value = JobOwnerRoute(
            owner, local=False
        )
        remote_plan = JobOptimizePlan(timestamp=1690000001)
        router.forward_json.return_value = {
            "job_opt_plan": remote_plan.model_dump()
        }

        response = self.client.post(
            "/optimize",
            json={
                "job": {"uuid": "job-123", "name": "job-a"},
                "config": {"optimizer_name": "BaseOptimizer"},
            },
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(
            response.json()["job_opt_plan"]["timestamp"], 1690000001
        )
        self.brain_server._optimizer_manager.optimize.assert_not_called()
        router.forward_json.assert_called_once()
        call = router.forward_json.call_args
        self.assertEqual(call.args[0], owner)
        self.assertEqual(call.args[1], "/internal/optimize")

    def test_optimize_returns_503_when_owner_is_not_registered(self):
        self.brain_server._job_owner_routing_service = MagicMock()
        router = self.brain_server._job_owner_routing_service
        router.resolve_existing_owner.side_effect = JobOwnerRoutingError(
            "job owner is not registered"
        )

        response = self.client.post(
            "/optimize", json={"job": {"uuid": "job-123"}}
        )

        self.assertEqual(response.status_code, 503)
        self.brain_server._optimizer_manager.optimize.assert_not_called()

    def test_internal_optimize_passes_owner_token_and_computes_locally(self):
        owner = JobOwner(
            "brain-0", "http://10.0.0.10:50002", "token-0"
        )
        self.brain_server._job_owner_routing_service = MagicMock()
        router = self.brain_server._job_owner_routing_service
        router.resolve_existing_owner.return_value = JobOwnerRoute(
            owner, local=True
        )
        self.brain_server._optimizer_manager.optimize.return_value = (
            JobOptimizePlan(timestamp=1690000002)
        )

        response = self.client.post(
            "/internal/optimize",
            headers={"X-DLRover-Owner-Token": "token-0"},
            json={"job": {"uuid": "job-123"}},
        )

        self.assertEqual(response.status_code, 200)
        router.resolve_existing_owner.assert_called_once_with(
            "job-123", forwarded=True, owner_token="token-0"
        )
        self.brain_server._optimizer_manager.optimize.assert_called_once()

    def test_job_config_endpoint_success(self):
        """Test POST /job_config successfully returns configuration dictionary."""
        fake_config = MagicMock()
        expected_configs = {"memory": "16G", "cpu": "8"}

        # UPDATED: We now mock get_config_values instead of convert_to_dict
        fake_config.get_config_values.return_value = expected_configs

        self.brain_server._config_manager.get_job_config.return_value = (
            fake_config
        )

        payload = {
            "type": "inference",
            "job": {"uuid": "job-456", "user": "bob"},
        }

        response = self.client.post("/job_config", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["job_configs"], expected_configs)

    def test_job_config_endpoint_not_found(self):
        """Test POST /job_config safely handles a None return and outputs {}."""
        self.brain_server._config_manager.get_job_config.return_value = None

        payload = {"type": "test", "job": {"uuid": "unknown"}}

        response = self.client.post("/job_config", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["job_configs"], {})

    def test_metrics_collector(self):
        body, ready = self.brain_server._metrics_server.collect()

        self.assertEqual(ready, 1)
        self.assertIn("dlrover_component_ready", body)
        self.assertIn("dlrover_brain_report_queue_capacity", body)
        self.assertIn("dlrover_brain_redis_checks_total", body)
        self.assertIn("dlrover_brain_distributed_lock_total", body)
        self.assertIn("dlrover_brain_elastic_decisions_total", body)
        self.assertIn("dlrover_brain_runtime_processor_ready", body)
        self.assertIn("dlrover_brain_job_dispatch_total", body)
        self.assertIn(
            "dlrover_brain_owner_allocator_instance_registered",
            body,
        )
        self.assertIn(
            "dlrover_brain_owner_allocator_active_instances",
            body,
        )
        self.assertIn(
            "dlrover_brain_owner_allocator_quarantined_instances",
            body,
        )

    def test_business_port_does_not_expose_metrics(self):
        response = self.client.get("/metrics")

        self.assertEqual(response.status_code, 404)

    def test_runtime_report_returns_503_when_owner_routing_fails(self):
        self.brain_server._elastic_manager.dispatch_job_runtime_info = (
            MagicMock(
                return_value=DispatchResult(
                    False, "job owner unavailable", retryable=True
                )
            )
        )

        response = self.client.post(
            "/report_runtime_node_stats",
            json={"job_metrics": {"job_meta": {"uuid": "job-1"}}},
        )

        self.assertEqual(response.status_code, 503)

    def test_internal_runtime_report_passes_owner_token(self):
        self.brain_server._elastic_manager.dispatch_job_runtime_info = (
            MagicMock(return_value=DispatchResult(True, "success"))
        )

        response = self.client.post(
            "/internal/report_runtime_node_stats",
            headers={"X-DLRover-Owner-Token": "token-1"},
            json={"job_metrics": {"job_meta": {"uuid": "job-1"}}},
        )

        self.assertEqual(response.status_code, 200)
        call = self.brain_server._elastic_manager.dispatch_job_runtime_info.call_args
        self.assertTrue(call.kwargs["forwarded"])
        self.assertEqual(call.kwargs["owner_token"], "token-1")

    def test_master_register_returns_503_when_owner_claim_fails(self):
        self.brain_server._master_control_service.register_master = MagicMock(
            return_value=DispatchResult(
                False, "Redis is unavailable", retryable=True
            )
        )

        response = self.client.post(
            "/register_master_client",
            json={
                "job": {"uuid": "job-1", "name": "job-a"},
                "master_addr": "master-svc:50001",
            },
        )

        self.assertEqual(response.status_code, 503)

    def test_internal_master_register_passes_owner_token(self):
        self.brain_server._master_control_service.register_master = MagicMock(
            return_value=DispatchResult(True, "register success")
        )

        response = self.client.post(
            "/internal/register_master_client",
            headers={"X-DLRover-Owner-Token": "token-1"},
            json={
                "job": {"uuid": "job-1", "name": "job-a"},
                "master_addr": "master-svc:50001",
            },
        )

        self.assertEqual(response.status_code, 200)
        call = (
            self.brain_server._master_control_service.register_master.call_args
        )
        self.assertTrue(call.kwargs["forwarded"])
        self.assertEqual(call.kwargs["owner_token"], "token-1")

    def test_internal_master_heartbeat_passes_owner_token(self):
        self.brain_server._master_control_service.heartbeat_master = MagicMock(
            return_value=DispatchResult(True, "heartbeat success")
        )

        response = self.client.post(
            "/internal/report_heart_beat",
            headers={"X-DLRover-Owner-Token": "token-1"},
            json={
                "job": {"uuid": "job-1", "name": "job-a"},
                "master_addr": "master-svc:50001",
            },
        )

        self.assertEqual(response.status_code, 200)
        call = (
            self.brain_server._master_control_service.heartbeat_master.call_args
        )
        self.assertTrue(call.kwargs["forwarded"])
        self.assertEqual(call.kwargs["owner_token"], "token-1")


if __name__ == "__main__":
    unittest.main()
