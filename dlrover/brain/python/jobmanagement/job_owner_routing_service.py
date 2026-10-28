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

"""Reusable routing for job-scoped requests in multi-instance Brain."""

from dataclasses import dataclass
import os
from typing import Any, Dict, Optional

import requests

from dlrover.brain.python.jobmanagement.job_owner import (
    JobOwner,
    JobOwnerManager,
    JobOwnerStoreError,
)
from dlrover.python.common.log import default_logger as logger


class JobOwnerRoutingError(RuntimeError):
    """Raised when a job-scoped request cannot be routed safely."""

    def __init__(self, reason: str, retryable: bool = True):
        super().__init__(reason)
        self.reason = reason
        self.retryable = retryable


@dataclass(frozen=True)
class JobOwnerRoute:
    owner: JobOwner
    local: bool


class JobOwnerRoutingService:
    """Resolve existing owners and forward opaque JSON with fencing.

    This service never creates an owner. Owner creation remains exclusively
    tied to successful Master registration.
    """

    def __init__(
        self,
        job_owner_manager: JobOwnerManager,
        session: Optional[requests.Session] = None,
    ):
        self._job_owner_manager = job_owner_manager
        self._session = (
            session if session is not None else requests.Session()
        )
        self._owns_session = session is None
        try:
            timeout = float(
                os.getenv("DLROVER_BRAIN_FORWARD_TIMEOUT_SECONDS", "3")
            )
        except ValueError:
            timeout = 3.0
        self._forward_timeout_seconds = max(0.1, timeout)

    def resolve_existing_owner(
        self,
        job_id: str,
        forwarded: bool = False,
        owner_token: str = "",
    ) -> JobOwnerRoute:
        """Resolve and fence an existing ACTIVE owner without claiming one."""
        if not job_id:
            raise JobOwnerRoutingError("job uuid is empty", retryable=False)

        manager = self._job_owner_manager
        if not manager.enabled:
            return JobOwnerRoute(
                JobOwner(
                    manager.instance_id,
                    manager.endpoint,
                    "single-instance",
                ),
                local=True,
            )

        try:
            if forwarded:
                if not manager.accept_forwarded(job_id, owner_token):
                    raise JobOwnerRoutingError("job owner changed")
                return JobOwnerRoute(
                    JobOwner(
                        manager.instance_id,
                        manager.endpoint,
                        owner_token,
                    ),
                    local=True,
                )

            owner = manager.get_owner(job_id)
            if owner is None:
                raise JobOwnerRoutingError("job owner is not registered")
            if owner.instance_id != manager.instance_id:
                return JobOwnerRoute(owner, local=False)
            if not manager.accept_forwarded(job_id, owner.token):
                raise JobOwnerRoutingError("job owner changed")
            return JobOwnerRoute(owner, local=True)
        except JobOwnerRoutingError:
            raise
        except JobOwnerStoreError as e:
            raise JobOwnerRoutingError(str(e)) from e

    def validate_local_owner(self, job_id: str, owner_token: str) -> None:
        """Fence a result before returning it from a local computation."""
        if not self._job_owner_manager.enabled:
            return
        try:
            if not self._job_owner_manager.is_current_owner(
                job_id, owner_token
            ):
                raise JobOwnerRoutingError("job owner changed")
        except JobOwnerRoutingError:
            raise
        except JobOwnerStoreError as e:
            raise JobOwnerRoutingError(str(e)) from e

    def forward_json(
        self,
        owner: JobOwner,
        path: str,
        payload: Dict[str, Any],
        operation: str,
    ) -> Dict[str, Any]:
        """Forward JSON and update the shared instance circuit state."""
        url = f"{owner.endpoint}{path}"
        try:
            response = self._session.post(
                url,
                json=payload,
                headers={"X-DLRover-Owner-Token": owner.token},
                timeout=self._forward_timeout_seconds,
            )
        except requests.RequestException as e:
            self._record_forward_failure(owner)
            raise JobOwnerRoutingError(str(e)) from e
        except Exception as e:
            # Local serialization/programming errors do not prove that the
            # target instance is unhealthy.
            raise JobOwnerRoutingError(str(e)) from e

        if response.status_code >= 500:
            self._record_forward_failure(owner)
            raise JobOwnerRoutingError(
                f"owner returned HTTP {response.status_code}: "
                f"{response.text[:256]}"
            )
        if response.status_code < 200 or response.status_code >= 300:
            self._record_forward_success(owner)
            raise JobOwnerRoutingError(
                f"owner returned HTTP {response.status_code}: "
                f"{response.text[:256]}"
            )

        try:
            body = response.json()
            if not isinstance(body, dict):
                raise TypeError("owner response is not a JSON object")
        except (TypeError, ValueError) as e:
            self._record_forward_failure(owner)
            raise JobOwnerRoutingError(
                f"invalid {operation} response from job owner: {e}"
            ) from e

        self._record_forward_success(owner)
        return body

    def _record_forward_failure(self, owner: JobOwner) -> None:
        try:
            self._job_owner_manager.record_forward_failure(owner)
        except JobOwnerStoreError as e:
            logger.warning(
                "Record forwarding failure for Brain %s failed: %s",
                owner.instance_id,
                e,
            )

    def _record_forward_success(self, owner: JobOwner) -> None:
        try:
            self._job_owner_manager.record_forward_success(owner)
        except JobOwnerStoreError as e:
            logger.warning(
                "Clear forwarding failure for Brain %s failed: %s",
                owner.instance_id,
                e,
            )

    def close(self) -> None:
        if self._owns_session:
            self._session.close()
