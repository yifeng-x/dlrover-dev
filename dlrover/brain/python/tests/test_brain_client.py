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
import requests
from dlrover.brain.python.client.client import BrainClient
from dlrover.brain.python.common.http_schemas import (
    OptimizeRequest,
    OptimizeResponse,
    Response,
    JobConfigRequest,
    JobConfigResponse,
    JobMetricsRequest,
    JobMetricsResponse,
)
from dlrover.brain.python.common.job_metric import JobMetrics
from dlrover.brain.python.common.job import (
    JobMeta,
    JobOptimizePlan,
)


class TestBrainClient(unittest.TestCase):
    def setUp(self):
        self.base_url = "http://api.brain.local:8000"
        self.client = BrainClient(self.base_url)

        self.mock_session = MagicMock()
        self.client.session = self.mock_session

    def test_init_creates_robust_session(self):
        """Verify the session is configured with retries."""
        real_client = BrainClient("http://test.com")

        adapter = real_client.session.get_adapter("http://")
        self.assertIsNotNone(adapter)
        self.assertEqual(adapter.max_retries.total, 3)
        self.assertEqual(adapter.max_retries.backoff_factor, 1)

    def test_optimize_success(self):
        job_uid = "job-uuid"
        job = JobMeta(uuid=job_uid)
        req_data = OptimizeRequest(job=job)

        opt_response = OptimizeResponse(
            response=Response(success=True),
            job_opt_plan=JobOptimizePlan(
                job_meta=job,
            ),
        )

        expected_resp_json = opt_response.model_dump()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = expected_resp_json
        self.mock_session.post.return_value = mock_response

        result = self.client.optimize(req_data)

        self.assertIsInstance(result, OptimizeResponse)
        self.assertEqual(result.job_opt_plan.job_meta.uuid, job_uid)

        self.mock_session.post.assert_called_once_with(
            "http://api.brain.local:8000/optimize",
            json=req_data.model_dump(),
        )

    def test_optimize_error(self):
        job_uid = "job-uuid"
        job = JobMeta(uuid=job_uid)
        req_data = OptimizeRequest(job=job)

        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_response.text = "Internal Server Error"

        error = requests.exceptions.HTTPError(
            "503 Error", response=mock_response
        )
        mock_response.raise_for_status.side_effect = error

        self.mock_session.post.return_value = mock_response
        resp = self.client.optimize(req_data)
        self.assertIsNone(resp)

        self.mock_session.post.side_effect = (
            requests.exceptions.ConnectionError("Connection refused")
        )
        resp = self.client.optimize(req_data)
        self.assertIsNone(resp)

    def test_optimize_validation_error(self):
        job_uid = "job-uuid"
        job = JobMeta(uuid=job_uid)
        req_data = OptimizeRequest(job=job)

        # Server returns valid JSON, but missing required field 'plan_id'
        bad_json = {"success": True}  # Missing 'plan_id'

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = bad_json
        self.mock_session.post.return_value = mock_response

        self.mock_session.post.side_effect = Exception("Exception!!!")
        resp = self.client.optimize(req_data)
        self.assertIsNone(resp)

    def test_get_config_success(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        # The JSON the fake server returns
        expected_json = {
            "response": {"success": True, "reason": ""},
            "job_configs": {"memory": "16G", "cpu": "8"},
        }
        mock_response.json.return_value = expected_json

        self.client.session.post.return_value = mock_response

        req = JobConfigRequest(type="test", job=JobMeta(uuid="job-1"))

        result = self.client.get_config(req)

        self.assertIsInstance(result, JobConfigResponse)
        self.assertEqual(result.job_configs, {"memory": "16G", "cpu": "8"})
        self.assertTrue(result.response.success)

        self.client.session.post.assert_called_once_with(
            "http://api.brain.local:8000/job_config", json=req.model_dump()
        )

    def test_get_config_http_error(self):
        """Test how the client handles a 4xx or 5xx HTTP error."""
        mock_err_response = MagicMock()
        mock_err_response.status_code = 503
        mock_err_response.text = "Internal Server Error"

        http_error = requests.exceptions.HTTPError(response=mock_err_response)

        self.client.session.post.side_effect = http_error

        req = JobConfigRequest()
        result = self.client.get_config(req)

        self.assertIsNone(result)

    def test_get_config_connection_error(self):
        """Test how the client handles a complete inability to connect."""
        conn_error = requests.exceptions.ConnectionError("Connection refused")
        self.client.session.post.side_effect = conn_error

        req = JobConfigRequest()
        result = self.client.get_config(req)

        self.assertIsNone(result)

    def test_get_config_unexpected_exception(self):
        """Test how the client handles completely random Python crashes."""
        generic_error = ValueError("Something completely unexpected broke")
        self.client.session.post.side_effect = generic_error

        req = JobConfigRequest()
        result = self.client.get_config(req)

        self.assertIsNone(result)

    def test_report_runtime_node_stats_success(self):
        request = JobMetricsRequest(
            job_metrics=JobMetrics(job_meta=JobMeta(uuid="job-1"))
        )
        expected = JobMetricsResponse(
            response=Response(success=True, reason="success")
        )
        mock_response = MagicMock()
        mock_response.json.return_value = expected.model_dump()
        self.mock_session.post.return_value = mock_response

        result = self.client.report_runtime_node_stats(request)

        self.assertTrue(result.response.success)
        self.mock_session.post.assert_called_once_with(
            "http://api.brain.local:8000/report_runtime_node_stats",
            json=request.model_dump(),
        )




class TestMasterControlClient(unittest.TestCase):
    def setUp(self):
        from dlrover.brain.python.common.http_schemas import MasterHeartBeatRequest

        self.client = BrainClient("http://api.brain.local:8000")
        self.session = MagicMock()
        self.client._control_session = self.session
        self.response = self.session.post.return_value.__enter__.return_value
        self.response.status_code = 200
        self.response.json.return_value = {
            "response": {"success": True, "reason": "ok"}
        }
        self.request = MasterHeartBeatRequest()

    def test_control_requests_have_timeouts_and_no_adapter_retries(self):
        client = BrainClient("http://api.brain.local:8000")
        self.assertEqual(
            client._control_session.get_adapter("http://").max_retries.total, 0
        )
        self.assertEqual(client.session.get_adapter("http://").max_retries.total, 3)
        for method, path in [
            (self.client.register_master_client, "register_master_client"),
            (self.client.report_heart_beat, "report_heart_beat"),
        ]:
            self.assertTrue(method(self.request).response.success)
            self.session.post.assert_called_with(
                f"http://api.brain.local:8000/{path}",
                json=self.request.model_dump(), timeout=(2, 3),
                allow_redirects=False,
            )

    def test_http_error_classification(self):
        from dlrover.brain.python.client.client import BrainRequestError

        self.response.headers = {"Retry-After": "90"}
        for code, retryable in [
            (408, True), (429, True), (500, True), (503, True),
            (400, False), (401, False), (403, False), (404, False), (302, False),
        ]:
            with self.subTest(code=code):
                self.response.status_code = code
                with self.assertRaises(BrainRequestError) as caught:
                    self.client.report_heart_beat(self.request)
                self.assertEqual(caught.exception.retryable, retryable)
                self.assertEqual(caught.exception.retry_after, 90)

    def test_network_and_response_errors_are_retryable(self):
        from dlrover.brain.python.client.client import BrainRequestError

        for error in [requests.ConnectionError("offline"), requests.Timeout("slow")]:
            self.session.post.side_effect = error
            with self.assertRaises(BrainRequestError) as caught:
                self.client.report_heart_beat(self.request)
            self.assertTrue(caught.exception.retryable)
        self.session.post.side_effect = None
        self.response.json.return_value = {"response": {"success": "invalid"}}
        with self.assertRaises(BrainRequestError) as caught:
            self.client.report_heart_beat(self.request)
        self.assertTrue(caught.exception.retryable)
        self.response.json.return_value = {}
        with self.assertRaises(BrainRequestError) as caught:
            self.client.report_heart_beat(self.request)
        self.assertTrue(caught.exception.retryable)
        self.response.json.side_effect = ValueError("invalid JSON")
        with self.assertRaises(BrainRequestError) as caught:
            self.client.report_heart_beat(self.request)
        self.assertTrue(caught.exception.retryable)

    def test_business_rejection_and_invalid_url_are_not_retryable(self):
        from dlrover.brain.python.client.client import BrainRequestError

        self.response.json.return_value = {
            "response": {"success": False, "reason": "invalid job"}
        }
        with self.assertRaises(BrainRequestError) as caught:
            self.client.report_heart_beat(self.request)
        self.assertFalse(caught.exception.retryable)
        self.session.post.side_effect = requests.exceptions.InvalidURL("bad port")
        with self.assertRaises(BrainRequestError) as caught:
            self.client.report_heart_beat(self.request)
        self.assertFalse(caught.exception.retryable)

    def test_retry_after_date_and_invalid_value(self):
        from datetime import datetime, timedelta, timezone
        from email.utils import format_datetime
        from dlrover.brain.python.client.client import _retry_after_seconds

        date = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=90))
        self.assertTrue(88 <= _retry_after_seconds(date) <= 90)
        self.assertEqual(_retry_after_seconds("-10"), 0)
        self.assertIsNone(_retry_after_seconds("invalid"))
        self.assertIsNone(_retry_after_seconds(None))


if __name__ == "__main__":
    unittest.main()
