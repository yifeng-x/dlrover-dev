import tempfile
import unittest
from pathlib import Path

from dlrover.dashboard.elastic.store import EventStore
from dlrover.dashboard.elastic.reconcile import reconcile
from dlrover.dashboard.elastic.jct import statistics


class ClassificationTest(unittest.TestCase):
    def test_deleted_job_cancelled_and_terminal_not_resurrected(self):
        with tempfile.TemporaryDirectory() as directory:
            store = EventStore(Path(directory) / "jobs.db")
            item = dict(metadata=dict(uid="u", name="same", creationTimestamp="2026-01-01T00:00:00Z",
                                      annotations={"dlrover.io/elasticity-enabled": "false"}))
            reconcile(store, [item], "c", "n", "ElasticJob", now=1800000000)
            self.assertIsNone(store.jobs()[0]["elasticity_enabled"])
            reconcile(store, [], "c", "n", "ElasticJob", now=1800000001)
            self.assertEqual(store.jobs()[0]["status"], "cancelled")
            reconcile(store, [item], "c", "n", "ElasticJob", now=1800000002)
            self.assertEqual(store.jobs()[0]["status"], "cancelled")
            self.assertEqual(statistics(store.jobs(), 1800000003)["measured_jobs"], 0)

    def test_scope_and_unknown_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            store = EventStore(Path(directory) / "jobs.db")
            item = dict(metadata=dict(uid="u", name="no-elastic",
                                      creationTimestamp="2026-01-01T00:00:00Z"))
            reconcile(store, [item], "c", "n", "ElasticJob", now=1800000000)
            self.assertIsNone(store.jobs()[0]["elasticity_enabled"])
            reconcile(store, [], "other", "n", "ElasticJob", now=1800000001)
            self.assertEqual(store.jobs()[0]["status"], "running")

    def test_classification_from_real_configuration(self):
        from dlrover.python.common.job_classification import classify_job
        job = dict(kind="ElasticJob", metadata=dict(uid="real"),
                   spec=dict(replicaSpecs=dict(worker=dict(autoScale=False))))
        value = classify_job(job)
        self.assertEqual(value["platform_task_id"], "real")
        self.assertEqual(value["platform_type"], "kubernetes")
        self.assertIsNone(value["elasticity_enabled"])
        container = {"name": "main"}
        job["spec"]["replicaSpecs"]["dlrover-master"] = {
            "template": {"spec": {"containers": [container]}}}
        for flags, expected in [
            ("--elastic_dimension auto", True),
            ("--elastic_dimension=vertical --configmap_manual_scale_switch off", True),
            ("--configmap_manual_scale_switch on", True),
            ("--configmap_manual_scale_switch off", False),
            ("--resource_monitor_switch on", False),
            ("", False),
        ]:
            container["command"] = ["/bin/bash", "-c",
                "cd /app && python -m dlrover.python.master.main " + flags]
            self.assertEqual(classify_job(job)["elasticity_enabled"], expected)
        self.assertTrue(classify_job(job, master_enabled=True)["elasticity_enabled"])

    def test_metadata_backfill_identical_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            store = EventStore(Path(directory) / "jobs.db")
            snapshot = dict(job_uid="u", status="running", lifecycle_observed_at=1)
            store.register(snapshot)
            store.register(dict(snapshot, job_type="ElasticJob", elasticity_enabled=False))
            self.assertEqual(store.jobs()[0]["job_type"], "ElasticJob")
            self.assertEqual(store.jobs()[0]["elasticity_enabled"], 0)
