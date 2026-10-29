# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import pytest

from dlrover.python.common.constants import NodeEnv
from dlrover.python.common.training_metrics import training_metric_payload
from dlrover.python.master.elastic_training.rdzv_manager import (
    ElasticTrainingRendezvousManager,
)


def test_generation_is_shared_and_changes_after_rendezvous(monkeypatch):
    tokens = iter([100, 200])
    monkeypatch.setattr(
        "dlrover.python.master.elastic_training.rdzv_manager.secrets.randbelow",
        lambda limit: next(tokens),
    )
    manager = ElasticTrainingRendezvousManager()
    manager.update_rdzv_params(2, 2, 60, 1)
    assert manager.get_training_generation() == ""
    manager.join_rendezvous(0, 0, 4)
    assert manager.get_comm_world_with_generation(0)[3] == ""
    manager.join_rendezvous(1, 1, 4)
    first = manager.get_comm_world_with_generation(0)
    assert first[3] == "101"
    assert manager.get_comm_world_with_generation(1) == first
    manager.join_rendezvous(0, 0, 8)
    assert manager.get_training_generation() == ""
    manager.join_rendezvous(1, 1, 8)
    second = manager.get_comm_world_with_generation(0)
    assert second[0] > first[0]
    assert second[3] == "201"
    assert manager.get_comm_world_with_generation(1) == second


@pytest.mark.parametrize("generation", [None, "", "0", "-1", "1.5", str(2**53)])
def test_payload_rejects_missing_or_invalid_generation(monkeypatch, generation):
    monkeypatch.delenv(NodeEnv.TRAIN_GENERATION, raising=False)
    if generation is not None:
        monkeypatch.setenv(NodeEnv.TRAIN_GENERATION, generation)
    with pytest.raises(ValueError):
        training_metric_payload(100, 1000, 64, observed_at=1000)


def test_training_payload_round_trips_through_swanlab(monkeypatch):
    from dlrover.python.master.resource.swanlab import SwanLabTrainingMetrics
    from dlrover.python.tests.test_swanlab_metrics import ENV, rows

    monkeypatch.setenv(NodeEnv.TRAIN_GENERATION, "101")
    monkeypatch.delenv("DLROVER_SWANLAB_KEYS", raising=False)
    client = SwanLabTrainingMetrics(ENV)
    data = rows(client, [940, 1000])
    for index, (stamp, step) in enumerate([(940, 100), (1000, 160)]):
        payload = training_metric_payload(step, 1000, 64, steady=True, observed_at=stamp)
        for series in data:
            series["scalarMetricsList"][index]["data"] = str(payload[series["key"]])
    window = client.window(data, "101", 1000, 120, 60)
    assert window["end"]["work"] - window["start"]["work"] == 60
    assert window["target_work"] == 1000
    assert training_metric_payload(160, 1000, 64)["train/steady"] == 0
