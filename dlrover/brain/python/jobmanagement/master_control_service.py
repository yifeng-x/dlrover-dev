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

"""Master registration and heartbeat orchestration for a Brain instance."""

from __future__ import annotations

import os
import threading
from typing import TYPE_CHECKING

import requests

from dlrover.brain.python.common.job import JobMeta
from dlrover.brain.python.jobmanagement.dispatch_result import DispatchResult
from dlrover.brain.python.jobmanagement.job_owner import (
    JobOwner,
    JobOwnerManager,
    JobOwnerStoreError,
)
from dlrover.python.common.log import default_logger as logger

if TYPE_CHECKING:
    from dlrover.brain.python.jobmanagement.master_client_pool import (
        MasterClientPool,
    )


class MasterControlService:
    """Route Master control requests through the job's sticky Brain owner.

    This service owns orchestration only. Master address/heartbeat state remains
    in ``MasterClientPool`` and ownership/fencing remains in
    ``JobOwnerManager``.
    """

    def __init__(
        self,
        master_client_pool: MasterClientPool,
        job_owner_manager: JobOwnerManager,
    ):
        self._master_client_pool = master_client_pool
        self._job_owner_manager = job_owner_manager
        self._forward_session = requests.Session()
        try:
            timeout = float(
                os.getenv("DLROVER_BRAIN_FORWARD_TIMEOUT_SECONDS", "3")
            )
        except ValueError:
            timeout = 3.0
        self._forward_timeout_seconds = max(0.1, timeout)
        self._dispatch_locks = [threading.Lock() for _ in range(64)]

    def register_master(
        self,
        job_meta: JobMeta,
        master_addr: str,
        forwarded: bool = False,
        owner_token: str = "",
    ) -> DispatchResult:
        """Register a Master and establish the owner on first success."""
        validation_error = self._validate_master_control(
            job_meta, master_addr, require_address=True
        )
        if validation_error:
            return DispatchResult(False, validation_error)

        job_id = job_meta.uuid
        if not self._job_owner_manager.enabled:
            success = self._master_client_pool.register(
                job_meta.name, job_id, master_addr
            )
            return DispatchResult(
                success,
                "register success" if success else "register failed",
                retryable=not success,
            )

        try:
            if forwarded:
                return self._register_on_local_owner(
                    job_meta, master_addr, owner_token, claimed=False
                )

            owner, claimed = self._job_owner_manager.claim_for_registration(
                job_id
            )
            if owner.instance_id == self._job_owner_manager.instance_id:
                return self._register_on_local_owner(
                    job_meta, master_addr, owner.token, claimed=claimed
                )
            result = self._forward_master_control(
                "/internal/register_master_client",
                job_meta,
                master_addr,
                owner,
                "register",
            )
            if claimed and not result.success:
                self._rollback_registration_claim(job_meta, owner.token)
            return result
        except JobOwnerStoreError as e:
            logger.error(
                "Register Master owner for %s/%s failed: %s",
                job_meta.name,
                job_id,
                e,
            )
            return DispatchResult(False, str(e), retryable=True)

    def heartbeat_master(
        self,
        job_meta: JobMeta,
        master_addr: str,
        forwarded: bool = False,
        owner_token: str = "",
    ) -> DispatchResult:
        """Refresh Master state and the current owner's activity lease."""
        validation_error = self._validate_master_control(
            job_meta, master_addr, require_address=False
        )
        if validation_error:
            return DispatchResult(False, validation_error)

        job_id = job_meta.uuid
        if not self._job_owner_manager.enabled:
            success = self._master_client_pool.heartbeat(
                job_meta.name, job_id, master_addr
            )
            return DispatchResult(
                success,
                "heartbeat success" if success else "heartbeat failed",
                retryable=not success,
            )

        try:
            if forwarded:
                if not self._job_owner_manager.accept_forwarded(
                    job_id, owner_token
                ):
                    return DispatchResult(
                        False, "job owner changed", retryable=True
                    )
                return self._heartbeat_on_local_owner(job_meta, master_addr)

            owner = self._job_owner_manager.get_owner(job_id)
            if owner is None:
                if not master_addr:
                    return DispatchResult(
                        False,
                        "job owner is not registered",
                        retryable=True,
                    )
                return self.register_master(job_meta, master_addr)
            if owner.instance_id == self._job_owner_manager.instance_id:
                if not self._job_owner_manager.accept_forwarded(
                    job_id, owner.token
                ):
                    return DispatchResult(
                        False, "job owner changed", retryable=True
                    )
                return self._heartbeat_on_local_owner(job_meta, master_addr)
            return self._forward_master_control(
                "/internal/report_heart_beat",
                job_meta,
                master_addr,
                owner,
                "heartbeat",
            )
        except JobOwnerStoreError as e:
            logger.error(
                "Refresh Master owner for %s/%s failed: %s",
                job_meta.name,
                job_id,
                e,
            )
            return DispatchResult(False, str(e), retryable=True)

    def _register_on_local_owner(
        self,
        job_meta: JobMeta,
        master_addr: str,
        owner_token: str,
        claimed: bool,
    ) -> DispatchResult:
        job_id = job_meta.uuid
        dispatch_lock = self._dispatch_locks[
            hash(job_id) % len(self._dispatch_locks)
        ]
        with dispatch_lock:
            try:
                accepted = self._job_owner_manager.accept_forwarded(
                    job_id, owner_token
                )
                if not accepted:
                    if claimed:
                        self._rollback_registration_claim(
                            job_meta, owner_token
                        )
                    return DispatchResult(
                        False, "job owner changed", retryable=True
                    )
                success = self._master_client_pool.register(
                    job_meta.name, job_id, master_addr
                )
            except Exception as e:
                if claimed:
                    self._rollback_registration_claim(job_meta, owner_token)
                logger.warning(
                    "Register Master state for %s/%s failed: %s",
                    job_meta.name,
                    job_id,
                    e,
                )
                return DispatchResult(False, str(e), retryable=True)

            if success:
                try:
                    committed = self._job_owner_manager.commit_registration(
                        job_id, owner_token
                    )
                except JobOwnerStoreError as e:
                    committed = False
                    logger.warning(
                        "Commit owner registration for %s/%s failed: %s",
                        job_meta.name,
                        job_id,
                        e,
                    )
                if not committed:
                    if claimed:
                        self._rollback_registration_claim(
                            job_meta, owner_token
                        )
                    return DispatchResult(
                        False,
                        "owner registration commit failed",
                        retryable=True,
                    )
                return DispatchResult(True, "register success")

            if claimed:
                self._rollback_registration_claim(job_meta, owner_token)
            return DispatchResult(False, "register failed", retryable=True)

    def _rollback_registration_claim(
        self, job_meta: JobMeta, owner_token: str
    ) -> None:
        try:
            released = self._job_owner_manager.release_claim(
                job_meta.uuid, owner_token
            )
            if not released:
                logger.warning(
                    "Owner claim for %s/%s changed before rollback.",
                    job_meta.name,
                    job_meta.uuid,
                )
        except JobOwnerStoreError as e:
            logger.warning(
                "Rollback owner claim for %s/%s failed: %s",
                job_meta.name,
                job_meta.uuid,
                e,
            )

    def _heartbeat_on_local_owner(
        self, job_meta: JobMeta, master_addr: str
    ) -> DispatchResult:
        success = self._master_client_pool.heartbeat(
            job_meta.name, job_meta.uuid, master_addr
        )
        if success:
            return DispatchResult(True, "heartbeat success")
        return DispatchResult(False, "heartbeat failed", retryable=True)

    def _forward_master_control(
        self,
        path: str,
        job_meta: JobMeta,
        master_addr: str,
        owner: JobOwner,
        operation: str,
    ) -> DispatchResult:
        url = f"{owner.endpoint}{path}"
        try:
            response = self._forward_session.post(
                url,
                json={
                    "job": job_meta.model_dump(),
                    "master_addr": master_addr,
                },
                headers={"X-DLRover-Owner-Token": owner.token},
                timeout=self._forward_timeout_seconds,
            )
        except requests.RequestException as e:
            self._record_forward_failure(owner)
            logger.warning(
                "Forward Master %s for %s/%s to %s failed: %s",
                operation,
                job_meta.name,
                job_meta.uuid,
                owner.instance_id,
                e,
            )
            return DispatchResult(False, str(e), retryable=True)
        except Exception as e:
            # Local serialization/programming errors do not prove the target
            # instance unhealthy and therefore must not affect its circuit.
            logger.warning(
                "Prepare Master %s forwarding for %s/%s failed: %s",
                operation,
                job_meta.name,
                job_meta.uuid,
                e,
            )
            return DispatchResult(False, str(e), retryable=True)

        if response.status_code >= 500:
            self._record_forward_failure(owner)
            reason = (
                f"owner returned HTTP {response.status_code}: "
                f"{response.text[:256]}"
            )
            logger.warning(
                "Forward Master %s for %s/%s to %s failed: %s",
                operation,
                job_meta.name,
                job_meta.uuid,
                owner.instance_id,
                reason,
            )
            return DispatchResult(False, reason, retryable=True)

        if response.status_code != 200:
            # A non-5xx response proves that the target instance is reachable;
            # do not trip the infrastructure circuit for a business rejection.
            self._record_forward_success(owner)
            reason = (
                f"owner returned HTTP {response.status_code}: "
                f"{response.text[:256]}"
            )
            return DispatchResult(False, reason, retryable=True)

        try:
            result = response.json().get("response", {})
        except (TypeError, ValueError, AttributeError) as e:
            self._record_forward_failure(owner)
            return DispatchResult(False, str(e), retryable=True)

        self._record_forward_success(owner)
        if not bool(result.get("success")):
            return DispatchResult(
                False,
                result.get("reason", f"{operation} failed"),
                retryable=True,
            )
        return DispatchResult(True, f"{operation} forwarded to job owner")

    def _record_forward_failure(self, owner: JobOwner) -> None:
        try:
            self._job_owner_manager.record_forward_failure(owner)
        except JobOwnerStoreError as e:
            # Routing already failed. Preserve that result and surface the
            # circuit state failure separately instead of masking it.
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

    @staticmethod
    def _validate_master_control(
        job_meta: JobMeta, master_addr: str, require_address: bool
    ) -> str:
        if job_meta is None:
            return "job meta is empty"
        if not getattr(job_meta, "uuid", ""):
            return "job uuid is empty"
        if require_address and not master_addr:
            return "master address is empty"
        return ""

    def close(self) -> None:
        self._forward_session.close()
