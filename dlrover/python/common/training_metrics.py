# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
"""Scalar payload for the training script's existing SwanLab log call."""

import json
import math
import os
import time

from dlrover.python.common.constants import NodeEnv


TRAINING_METRIC_KEYS = {
    "work": "train/global_step",
    "target_work": "train/total_steps",
    "global_batch_size": "train/global_batch_size",
    "generation": "train/generation",
    "observed_at": "train/observed_at",
    "steady": "train/steady",
}


def training_metric_payload(global_step, total_steps, global_batch_size,
                            steady=False, observed_at=None, keys=None):
    """Call on global rank zero after an optimizer step; log as ONE batch.

    Counts must describe completed optimizer steps and actual global batch size.
    The caller marks steady only after restore/replay/warmup has finished.
    No SwanLab dependency or automatic monkey-patching of the training script.
    """
    generation = int(os.environ.get(NodeEnv.TRAIN_GENERATION, "0"))
    if not 0 < generation < 2**53:
        raise ValueError("Missing or invalid DLRover training generation")
    for value in (global_step, total_steps, global_batch_size):
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < 2**53:
            raise ValueError("Training counters must be nonnegative safe integers")
    if total_steps < global_step or global_batch_size == 0 or total_steps == 0:
        raise ValueError("Invalid training target or global batch size")
    if not isinstance(steady, bool):
        raise ValueError("steady must be an explicit boolean")
    observed_at = time.time() if observed_at is None else observed_at
    if (isinstance(observed_at, bool) or not isinstance(observed_at, (int, float))
            or not math.isfinite(observed_at) or observed_at <= 0):
        raise ValueError("Invalid training observation time")
    names = dict(TRAINING_METRIC_KEYS)
    overrides = keys if keys is not None else json.loads(
        os.environ.get("DLROVER_SWANLAB_KEYS", "{}")
    )
    if not isinstance(overrides, dict) or set(overrides) - set(names):
        raise ValueError("Invalid training metric key mapping")
    names.update(overrides)
    if names.get("observed_at") is None:
        names.pop("observed_at")
    if (not all(isinstance(v, str) and v for v in names.values())
            or len(set(names.values())) != len(names)):
        raise ValueError("Invalid training metric key mapping")
    values = dict(work=global_step, target_work=total_steps,
                  global_batch_size=global_batch_size, generation=generation,
                  observed_at=observed_at, steady=int(steady))
    return {name: values[key] for key, name in names.items()}
