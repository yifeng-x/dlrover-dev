# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
import requests

from dlrover.python.master.resource.swanlab import (
    SwanLabTrainingMetrics,
    TrainingMetricsUnavailable,
)


ENV = {
    "DLROVER_SWANLAB_HTTP_URL": "https://metrics.example/metrics/scalar/by-task-name",
    "DLROVER_SWANLAB_TASK_ID": "test-task",
    "DLROVER_SWANLAB_HTTP_TOKEN": "test-token",
}


def response(data, status=200):
    result = MagicMock()
    result.__enter__.return_value = result
    result.status_code = status
    result.json.return_value = data
    return result


def rows(client, timestamps, generation="101"):
    """Build aligned HTTP scalar batches at two optimizer steps per second."""
    values = dict(target_work=10000, global_batch_size=64,
                  generation=int(generation), steady=1)
    return [
        dict(taskId=client.task_id, key=key, scalarMetricsList=[
            dict(epoch=index,
                 createTime=datetime.fromtimestamp(stamp, timezone.utc).isoformat(),
                 data=str({**values, "work": 160 + 2 * (stamp - timestamps[0]),
                           "observed_at": stamp}[name]))
            for index, stamp in enumerate(timestamps)
        ])
        for name, key in client.keys.items()
    ]


def test_fetch_contract_and_window():
    post = MagicMock()
    client = SwanLabTrainingMetrics(ENV, post)
    post.return_value = response(rows(client, [940, 970, 1000]))
    window = client.fetch("101", lambda: 1000, 3, 120, 60)
    assert window["start"] == dict(generation="101", observed_at=940, work=160)
    assert window["end"] == dict(generation="101", observed_at=1000, work=280)
    assert window["global_batch_size"] == 64
    post.assert_called_once_with(
        ENV["DLROVER_SWANLAB_HTTP_URL"],
        json={"taskId": client.task_id,
              "taskScalarList": [{"key": key} for key in client.keys.values()]},
        headers={"Accept": "application/json", "Authorization": "Bearer test-token"},
        timeout=3, allow_redirects=False,
    )


@pytest.mark.parametrize("fault", [
    "task", "missing_key", "duplicate_key", "unaligned", "conflict",
    "nan", "regression", "warmup", "batch_change", "generation", "stale",
])
def test_invalid_evidence_is_rejected(fault):
    client = SwanLabTrainingMetrics(ENV)
    data = rows(client, [940, 970, 1000])
    series = {name: next(row["scalarMetricsList"] for row in data
                         if row["key"] == key) for name, key in client.keys.items()}
    if fault == "task":
        data[0]["taskId"] = "another-task"
    elif fault == "missing_key":
        data.pop()
    elif fault == "duplicate_key":
        data.append(data[0])
    elif fault == "unaligned":
        series["work"].pop(1)
    elif fault == "conflict":
        series["work"].append({**series["work"][0], "data": "999"})
    elif fault == "nan":
        series["work"][0]["data"] = "NaN"
    elif fault == "regression":
        series["work"][-1]["data"] = "1"
    elif fault == "warmup":
        series["steady"][1]["data"] = "0"
    elif fault == "batch_change":
        series["global_batch_size"][-1]["data"] = "128"
    with pytest.raises(TrainingMetricsUnavailable):
        client.window(data, "202" if fault == "generation" else "101",
                      2000 if fault == "stale" else 1000, 120, 60)


@pytest.mark.parametrize("status", [302, 401, 500])
def test_http_errors_fail_closed(status):
    client = SwanLabTrainingMetrics(ENV, MagicMock(return_value=response([], status)))
    with pytest.raises(TrainingMetricsUnavailable):
        client.fetch("101", lambda: 1000, 3, 120, 60)


def test_timeout_fails_closed():
    client = SwanLabTrainingMetrics(ENV, MagicMock(side_effect=requests.Timeout))
    with pytest.raises(TrainingMetricsUnavailable):
        client.fetch("101", lambda: 1000, 3, 120, 60)


def test_after_boundary_excludes_previous_window():
    client = SwanLabTrainingMetrics(ENV)
    with pytest.raises(TrainingMetricsUnavailable):
        client.window(rows(client, [940, 970, 1000]), "101", 1000, 120, 60, after=970)
