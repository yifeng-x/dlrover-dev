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

import requests
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dlrover.brain.python.common.http_schemas import (
    MasterHeartBeatRequest,
    MasterHeartBeatResponse,
    JobMetricsRequest,
    JobMetricsResponse,
    OptimizeRequest,
    OptimizeResponse,
    JobConfigRequest,
    JobConfigResponse,
    RegisterMasterClientRequest,
    RegisterMasterClientResponse,
)
from dlrover.brain.python.common.log import default_logger as logger
from typing import Optional


class BrainRequestError(Exception):
    """Failure of Master registration or heartbeat, with retry policy."""

    def __init__(self, reason, retryable=True, retry_after=None):
        super().__init__(reason)
        self.retryable = retryable
        self.retry_after = retry_after


def _retry_after_seconds(value):
    if not value:
        return None
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        try:
            retry_at = parsedate_to_datetime(value)
            return max(0, (retry_at - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


class BrainClient:
    """
    An HTTP client for interacting with the DLRover Brain Server.

    This client provides robust communication with the Brain Server by
    utilizing connection pooling and automatic retries for transient
    network or server-side infrastructure errors (e.g., 502, 503, 504).
    """

    def __init__(self, base_url: str):
        if not base_url.startswith(('http://', 'https://')):
            base_url = f"http://{base_url}"
        self.base_url = base_url.rstrip("/")
        self.session = self._create_robust_session()
        # The Master heartbeat loop owns retries for these two endpoints.
        self._control_session = requests.Session()

    def _create_robust_session(self) -> requests.Session:
        """
        Creates a session with automatic retries for transient errors.
        """
        session = requests.Session()

        # Retry strategy:
        # - total=3: Try 3 times total
        # - backoff_factor=1: Wait 1s, then 2s, then 4s between retries
        # - status_forcelist: Retry on 502, 503, 504 (Server errors)
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def optimize(self, request: OptimizeRequest) -> Optional[OptimizeResponse]:
        """
        Get an optimize plan for a training job from the Brain.
        """
        url = f"{self.base_url}/optimize"

        try:
            response = self.session.post(url, json=request.model_dump())
            response.raise_for_status()

            return OptimizeResponse(**response.json())
        except requests.exceptions.HTTPError as e:
            logger.error(
                f"HTTP Error: {e.response.status_code} - {e.response.text}"
            )
            return None
        except requests.exceptions.ConnectionError:
            logger.info("Failed to connect to the Brain server.")
            return None
        except Exception as e:
            logger.info(f"An unexpected error occurred: {e}")
            return None

    def get_config(
        self, request: JobConfigRequest
    ) -> Optional[JobConfigResponse]:
        """
        Retrieves the job configuration from the Brain.
        """
        url = f"{self.base_url}/job_config"

        try:
            response = self.session.post(url, json=request.model_dump())
            response.raise_for_status()

            return JobConfigResponse(**response.json())
        except requests.exceptions.HTTPError as e:
            logger.error(
                f"HTTP Error fetching config: {e.response.status_code} - {e.response.text}"
            )
            return None
        except requests.exceptions.ConnectionError:
            logger.info(
                "Failed to connect to the Brain server to fetch config."
            )
            return None
        except Exception as e:
            logger.info(f"An unexpected error occurred fetching config: {e}")
            return None

    def report_runtime_node_stats(self, request: JobMetricsRequest) -> Optional[JobMetricsResponse]:
        """
        Report runtime node stats to brain.
        """
        url = f"{self.base_url}/report_runtime_node_stats"

        try:
            response = self.session.post(url, json=request.model_dump())
            response.raise_for_status()

            return JobMetricsResponse(**response.json())
        except requests.exceptions.HTTPError as e:
            logger.error(
                f"HTTP Error: {e.response.status_code} - {e.response.text}"
            )
            return None
        except requests.exceptions.ConnectionError:
            logger.info("Failed to connect to the Brain server.")
            return None
        except Exception as e:
            logger.info(f"An unexpected error occurred: {e}")
            return None

    def _post_master_control(self, path, request, response_type):
        try:
            with self._control_session.post(
                f"{self.base_url}/{path}",
                json=request.model_dump(),
                timeout=(2, 3),
                allow_redirects=False,
            ) as response:
                if response.status_code != 200:
                    code = response.status_code
                    raise BrainRequestError(
                        f"{path} returned HTTP {code}",
                        retryable=code in (408, 429) or code >= 500,
                        retry_after=_retry_after_seconds(
                            response.headers.get("Retry-After")
                        ),
                    )
                data = response.json()
                if (
                    not isinstance(data, dict)
                    or not isinstance(data.get("response"), dict)
                    or "success" not in data["response"]
                ):
                    raise ValueError(f"{path} returned an invalid response")
                result = response_type(**data)
                if not result.response.success:
                    # Retryable business failures are HTTP 503 on Brain.
                    raise BrainRequestError(
                        result.response.reason or f"{path} rejected",
                        retryable=False,
                    )
                return result
        except BrainRequestError:
            raise
        except (
            requests.exceptions.InvalidURL,
            requests.exceptions.InvalidSchema,
            requests.exceptions.MissingSchema,
        ) as exc:
            raise BrainRequestError(str(exc), retryable=False) from exc
        except (requests.exceptions.RequestException, ValueError) as exc:
            raise BrainRequestError(str(exc)) from exc

    def register_master_client(
        self, request: RegisterMasterClientRequest
    ) -> RegisterMasterClientResponse:
        return self._post_master_control(
            "register_master_client", request, RegisterMasterClientResponse
        )

    def report_heart_beat(
        self, request: MasterHeartBeatRequest
    ) -> MasterHeartBeatResponse:
        return self._post_master_control(
            "report_heart_beat", request, MasterHeartBeatResponse
        )
