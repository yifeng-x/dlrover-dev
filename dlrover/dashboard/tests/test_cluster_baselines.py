import pytest

from dlrover.dashboard.elastic.aggregate import DashboardService
from dlrover.dashboard.elastic.baselines import preview
from dlrover.dashboard.elastic.store import EventStore


def test_manual_snapshot_pool_cutoff_and_persistence(tmp_path):
    path = tmp_path / "test.db"
    store = EventStore(path)
    service = DashboardService(store, clock=lambda: 100)
    for uid, pool in (("one", "A"), ("two", "B")):
        store.register(dict(job_uid=uid, cluster="c", resource_pool=pool,
                            submitted_at=10, completed_at=60, status="completed",
                            lifecycle_observed_at=61))
    form = dict(cluster="c", resource_pool="A", start=5, end=20,
                kind="disabled", name="test", confirmed=True)
    snap = preview(service, form)
    assert snap["metrics"]["total_jobs"] == 1
    assert snap["metrics"]["mean_seconds"] == 50
    assert store.baselines("c") == []
    store.save_baseline(snap)
    store.save_baseline(snap)
    assert EventStore(path).baselines("c") == [snap]
    assert store.baselines("c", "disabled", "A") == [snap]
    assert store.baselines("c", "enabled", "A") == []
    assert store.baselines("c", "disabled", "") == []
    for change in (dict(confirmed=False), dict(end=101), dict(resource_pool="C")):
        with pytest.raises(ValueError):
            preview(service, dict(form, **change))
