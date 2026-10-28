import unittest
from unittest.mock import MagicMock

from dlrover.brain.python.jobmanagement.job_cleanup_coordinator import (
    JobCleanupCoordinator,
)


class TestJobCleanupCoordinator(unittest.TestCase):
    def setUp(self):
        self.owner = MagicMock()
        self.pool = MagicMock()
        self.vertical = MagicMock()
        self.coordinator = JobCleanupCoordinator(
            self.owner, self.pool, self.vertical
        )
        self.lease = MagicMock()
        self.owner.begin_cleanup.return_value = self.lease
        self.owner.finish_cleanup.return_value = True

    def test_cleanup_coordinates_all_state_in_order(self):
        calls = []
        self.owner.begin_cleanup.side_effect = (
            lambda job_id: calls.append("owner") or self.lease
        )
        self.pool.cleanup_job_state.side_effect = (
            lambda *args: calls.append("master") or True
        )
        self.vertical.cleanup_job_state.side_effect = (
            lambda job_id: calls.append("gpu") or True
        )
        self.owner.cleanup_report_state.side_effect = (
            lambda job_id: calls.append("dedup") or True
        )
        self.pool.finish_job_cleanup.side_effect = (
            lambda *args: calls.append("index") or True
        )
        self.owner.finish_cleanup.side_effect = (
            lambda *args, **kwargs: calls.append("finish") or True
        )

        cleaned = self.coordinator.cleanup_job(
            "job-a", "job-id-1", "100", 200
        )

        self.assertTrue(cleaned)
        self.assertEqual(
            calls,
            ["owner", "master", "gpu", "dedup", "index", "finish"],
        )
        self.owner.finish_cleanup.assert_called_once_with(
            self.lease, restore_previous_owner=False
        )

    def test_cleanup_restores_owner_when_master_heartbeat_changed(self):
        self.pool.cleanup_job_state.return_value = False

        cleaned = self.coordinator.cleanup_job(
            "job-a", "job-id-1", "100", 200
        )

        self.assertFalse(cleaned)
        self.vertical.cleanup_job_state.assert_not_called()
        self.owner.cleanup_report_state.assert_not_called()
        self.pool.finish_job_cleanup.assert_not_called()
        self.owner.finish_cleanup.assert_called_once_with(
            self.lease, restore_previous_owner=True
        )

    def test_partial_cleanup_keeps_index_for_retry(self):
        self.pool.cleanup_job_state.return_value = True
        self.vertical.cleanup_job_state.return_value = False
        self.owner.cleanup_report_state.return_value = True

        cleaned = self.coordinator.cleanup_job(
            "job-a", "job-id-1", "100", 200
        )

        self.assertFalse(cleaned)
        self.pool.finish_job_cleanup.assert_not_called()
        self.owner.finish_cleanup.assert_called_once_with(
            self.lease, restore_previous_owner=False
        )


if __name__ == "__main__":
    unittest.main()
