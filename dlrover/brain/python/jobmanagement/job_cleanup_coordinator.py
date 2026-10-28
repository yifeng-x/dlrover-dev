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

"""Token-fenced cleanup coordination for all per-job Brain state."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dlrover.brain.python.jobmanagement.job_owner import (
    JobOwnerManager,
    JobOwnerStoreError,
)
from dlrover.python.common.log import default_logger as logger

if TYPE_CHECKING:
    from dlrover.brain.python.jobmanagement.elastic.vertical_elastic import (
        VerticalElasticService,
    )
    from dlrover.brain.python.jobmanagement.master_client_pool import (
        MasterClientPool,
    )


class JobCleanupCoordinator:
    """Clean job state in dependency order under an owner cleanup lease."""

    def __init__(
        self,
        job_owner_manager: JobOwnerManager,
        master_client_pool: MasterClientPool,
        vertical_elastic_service: VerticalElasticService,
    ):
        self._job_owner_manager = job_owner_manager
        self._master_client_pool = master_client_pool
        self._vertical_elastic_service = vertical_elastic_service

    def cleanup_job(
        self,
        job_name: str,
        job_id: str,
        expected_heartbeat_ts: str,
        expire_before: float,
    ) -> bool:
        """Coordinate idempotent, token-fenced cleanup for one job."""
        try:
            cleanup_lease = self._job_owner_manager.begin_cleanup(job_id)
        except JobOwnerStoreError as e:
            logger.warning("Cannot fence job %s for cleanup: %s", job_id, e)
            return False
        if cleanup_lease is None:
            logger.info("Job %s is already being cleaned up.", job_id)
            return False

        restore_owner = True
        try:
            if not self._master_client_pool.cleanup_job_state(
                job_id, expected_heartbeat_ts, expire_before
            ):
                return False

            restore_owner = False
            gpu_cleaned = self._vertical_elastic_service.cleanup_job_state(
                job_id
            )
            dedup_cleaned = self._job_owner_manager.cleanup_report_state(
                job_id
            )
            if not gpu_cleaned or not dedup_cleaned:
                logger.warning(
                    "Job cleanup was partially completed for %s/%s: "
                    "gpu_cleaned=%s, dedup_cleaned=%s. Remaining Redis "
                    "keys are bounded by TTL.",
                    job_name,
                    job_id,
                    gpu_cleaned,
                    dedup_cleaned,
                )
                return False
            return self._master_client_pool.finish_job_cleanup(
                job_id, expire_before
            )
        finally:
            try:
                released = self._job_owner_manager.finish_cleanup(
                    cleanup_lease,
                    restore_previous_owner=restore_owner,
                )
                if not released:
                    logger.warning(
                        "Cleanup marker ownership changed for job %s.", job_id
                    )
            except JobOwnerStoreError as e:
                logger.warning(
                    "Release cleanup marker for job %s failed: %s", job_id, e
                )
