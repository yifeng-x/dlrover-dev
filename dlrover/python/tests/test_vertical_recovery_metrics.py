# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from kubernetes import client

from dlrover.python.common import comm
from dlrover.python.common.constants import ElasticJobLabel as Label
from dlrover.python.common.constants import VerticalElasticPhaseType as Phase
from dlrover.python.common.node import Node
from dlrover.python.master.monitor.vertical_metrics import VerticalExecutionMetrics
from dlrover.python.master.servicer import MasterServicer
from dlrover.python.master.watcher.k8s_watcher import _convert_pod_yaml_to_node
from dlrover.python.tests.test_vertical_execution_metrics import value


@pytest.fixture
def recovery():
    now = [100.0]
    metrics = VerticalExecutionMetrics(clock=lambda: now[0])
    metrics.start("add_gpus")
    now[0] = 110
    metrics.transition(Phase.SAVE_CHECKPOINT)
    metrics.track_replacement_workers({2, 3})
    now[0] = 120
    metrics.finish("completed")
    metrics.await_recovery({2, 3}, checkpoint_step=100, timeout=60)
    return metrics, now


def test_recovery_requires_ordered_progress_from_new_worker(recovery):
    metrics, now = recovery
    metrics.training_progress("worker", 0, 1000, 1000)  # old generation
    metrics.training_progress("ps", 2, 1000, 1000)  # wrong role
    metrics.training_progress("worker", 2, 100, 120)  # checkpoint only
    metrics.training_progress("worker", 2, 101, 119)  # out of order
    metrics.training_progress("worker", 3, 101, 121)  # another reporter
    assert value(metrics, "recovery_total") == 0
    now[0] = 135
    metrics.training_progress("worker", 2, 101, 121)
    metrics.training_progress("worker", 2, 102, 122)
    assert value(metrics, "recovery_total", result="recovered") == 1
    assert value(metrics, "recovery_duration_seconds_sum") == 15
    assert value(metrics, "resume_duration_seconds_sum") == 25
    assert value(metrics, "recovery_pending") == 0


def test_restored_step_does_not_confirm_recovery(recovery):
    metrics, _ = recovery
    metrics.training_progress("worker", 2, 150, 121)
    metrics.training_progress("worker", 2, 150, 122)
    assert value(metrics, "recovery_pending") == 1
    metrics.training_progress("worker", 2, 151, 123)
    assert value(metrics, "recovery_total", result="recovered") == 1


@pytest.mark.parametrize("reported,result", [(False, "unverified"), (True, "timeout")])
def test_recovery_timeout_runs_without_scraping(recovery, reported, result):
    metrics, now = recovery
    if reported:
        metrics.training_progress("worker", 2, 100, 121)
    now[0] = 181
    # Scrapes do not terminate recovery or increment outcome counters.
    assert value(metrics, "recovery_elapsed_seconds") == 61
    assert value(metrics, "recovery_total") == 0
    metrics.check_recovery()
    metrics.check_recovery()
    metrics.training_progress("worker", 2, 200, 200)
    assert value(metrics, "recovery_total", result=result) == 1
    assert value(metrics, "recovery_total") == 1
    assert value(metrics, "recovery_duration_seconds_count") == 0


def test_new_plan_and_shutdown_close_unconfirmed_recovery(recovery):
    metrics, now = recovery
    metrics.start("reduce_gpus")
    assert value(metrics, "recovery_total", result="unverified") == 1
    metrics.transition(Phase.SAVE_CHECKPOINT)
    metrics.finish("completed")
    metrics.await_recovery({4}, 100, 60)
    metrics.training_progress("worker", 2, 200, 200)
    metrics.training_progress("worker", 2, 201, 201)
    assert value(metrics, "recovery_pending") == 1
    metrics.stop_recovery()
    metrics.stop_recovery()
    assert value(metrics, "recovery_total", result="unverified") == 2


def test_global_step_rpc_passes_sender_identity(recovery):
    metrics, _ = recovery
    service = SimpleNamespace(
        _job_manager=SimpleNamespace(vertical_metrics=metrics),
        get_response=MagicMock(return_value=SimpleNamespace(success=False)),
        _collect_global_step=MagicMock(return_value=True),
    )
    for node_id, step, timestamp in [(0, 1000, 120), (2, 100, 121), (2, 101, 122)]:
        request = SimpleNamespace(
            node_type="worker", node_id=node_id,
            data=comm.serialize_message(comm.GlobalStep(step=step, timestamp=timestamp)),
        )
        response = MasterServicer.report(service, request, None)
        assert response.success
    assert value(metrics, "recovery_total", result="recovered") == 1


def test_kubernetes_timestamps_and_duplicate_watch_events(recovery):
    metrics, _ = recovery
    created = datetime(2026, 1, 1, tzinfo=timezone.utc)
    scheduled = created + timedelta(seconds=20)
    started = scheduled + timedelta(seconds=30)
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name="worker-2", creation_timestamp=created,
            labels={Label.REPLICA_TYPE_KEY: "worker", Label.REPLICA_INDEX_KEY: "2",
                    Label.RANK_INDEX_KEY: "0", Label.RELAUNCH_COUNT: "0"},
        ),
        spec=client.V1PodSpec(containers=[client.V1Container(
            name="main", resources=client.V1ResourceRequirements(
                requests={"cpu": "1", "memory": "1Gi"},
            ),
        )]),
        status=client.V1PodStatus(
            phase="Running", conditions=[client.V1PodCondition(
                type="PodScheduled", status="True", last_transition_time=scheduled,
            )], container_statuses=[client.V1ContainerStatus(
                name="main", image="test", image_id="test", ready=True,
                restart_count=0, state=client.V1ContainerState(
                    running=client.V1ContainerStateRunning(started_at=started),
                ),
            )],
        ),
    )
    node = _convert_pod_yaml_to_node(pod)
    assert node.scheduled_time == scheduled
    metrics.worker_event(node)
    metrics.worker_event(node)
    assert value(metrics, "worker_operation_duration_seconds_sum", operation="schedule_wait") == 20
    assert value(metrics, "worker_operation_duration_seconds_sum", operation="startup_wait") == 30
    assert value(metrics, "worker_operation_duration_seconds_count") == 2
    node.id = 0
    metrics.worker_event(node)
    assert value(metrics, "worker_operation_duration_seconds_count") == 2


def test_unknown_timestamps_do_not_export_zero_durations(recovery):
    metrics, _ = recovery
    node = Node("worker", 2)
    metrics.worker_event(node)
    assert value(metrics, "worker_operation_duration_seconds_count") == 0


def test_startup_time_uses_training_container_not_sidecar():
    from dlrover.python.master.watcher.k8s_watcher import _get_start_timestamp

    statuses = SimpleNamespace(container_statuses=[
        SimpleNamespace(name="sidecar", state=SimpleNamespace(
            running=SimpleNamespace(started_at=10),
        )),
        SimpleNamespace(name="main", state=SimpleNamespace(
            running=SimpleNamespace(started_at=30),
        )),
    ])
    assert _get_start_timestamp(statuses, "main") == 30
    assert _get_start_timestamp(statuses, "unknown") is None


def test_heartbeat_loop_checks_recovery_deadline(recovery):
    import threading
    from dlrover.python.master.node.dist_job_manager import DistributedJobManager

    metrics, now = recovery
    now[0] = 181
    manager = SimpleNamespace(
        vertical_metrics=metrics, _lock=threading.Lock(),
        _get_dead_node_event=lambda: [],
    )
    DistributedJobManager._monitor_node_heart_beat(manager)
    assert value(metrics, "recovery_total", result="unverified") == 1
