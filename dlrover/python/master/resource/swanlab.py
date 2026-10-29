# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
"""Adapt /metrics/scalar/by-task-name to a validated step observation window."""

import json
import math
import time
from datetime import datetime
from urllib.parse import urlparse

import requests

from dlrover.python.common.training_metrics import TRAINING_METRIC_KEYS
from dlrover.python.master.resource.benefit_log import BenefitLogger


class TrainingMetricsUnavailable(ValueError):
    pass


def scalar(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise TrainingMetricsUnavailable("invalid_scalar")
    try:
        result = float(value)
    except (ValueError, OverflowError):
        raise TrainingMetricsUnavailable("invalid_scalar")
    if not math.isfinite(result) or result < 0:
        raise TrainingMetricsUnavailable("invalid_scalar")
    return result


class SwanLabTrainingMetrics:
    def __init__(self, env, post=None):
        self.url = env.get("DLROVER_SWANLAB_HTTP_URL", "")
        self.task_id = env.get("DLROVER_SWANLAB_TASK_ID", "")
        self.token = env.get("DLROVER_SWANLAB_HTTP_TOKEN", "")
        self.post = post or requests.post
        self.keys = dict(TRAINING_METRIC_KEYS)
        # The deployed API's createTime is a training sampling timestamp.
        self.keys["observed_at"] = None
        self.utc_offset = env.get("DLROVER_SWANLAB_UTC_OFFSET", "+08:00")
        self._log = BenefitLogger("swanlab")
        self.valid = True
        try:
            overrides = json.loads(env.get("DLROVER_SWANLAB_KEYS", "{}"))
            if not isinstance(overrides, dict) or set(overrides) - set(self.keys):
                raise ValueError("invalid_key_mapping")
            self.keys.update(overrides)
            required = {k: v for k, v in self.keys.items() if k != "observed_at" or v is not None}
            if (not all(isinstance(v, str) and v for v in required.values())
                    or len(set(required.values())) != len(required)):
                raise ValueError("invalid_key_mapping")
            self.keys = required
            offset = datetime.fromisoformat("2000-01-01T00:00:00" + self.utc_offset)
            if offset.tzinfo is None:
                raise ValueError("missing_timezone")
            parsed = urlparse(self.url)
            if (parsed.scheme not in ("http", "https") or not parsed.hostname
                    or parsed.username or parsed.password or not self.task_id):
                raise ValueError("invalid_swanlab_configuration")
        except (ValueError, TypeError):
            self.valid = False

    def fetch(self, generation, clock, timeout, max_age, min_window, after=0,
              plan_id=None, request_id=None):
        started = time.monotonic()
        context = dict(task_id=self.task_id, generation=generation,
                       plan_id=plan_id, request_id=request_id)
        stage, status = "configuration", None
        self._log.emit("fetch_started", level="debug",
                       timeout_seconds=timeout, max_age_seconds=max_age,
                       min_window_seconds=min_window, after=after, **context)
        try:
            if not self.valid or not generation:
                raise TrainingMetricsUnavailable("swanlab_not_configured_or_no_generation")
            headers = {"Accept": "application/json"}
            if self.token:
                headers["Authorization"] = "Bearer " + self.token
            body = {"taskId": self.task_id, "taskScalarList": [
                {"key": key} for key in self.keys.values()
            ]}
            stage = "http"
            try:
                with self.post(self.url, json=body, headers=headers,
                               timeout=timeout, allow_redirects=False) as response:
                    status = response.status_code
                    if status != 200:
                        raise TrainingMetricsUnavailable("swanlab_http_error")
                    stage = "decode"
                    rows = response.json()
            except (requests.RequestException, ValueError) as error:
                raise TrainingMetricsUnavailable("swanlab_http_or_payload_error") from error
            stage = "window"
            window = self.window(rows, generation, clock(), max_age, min_window, after)
        except TrainingMetricsUnavailable as error:
            cause = error.__cause__
            self._log.emit(
                "fetch_failed", 
                level="warning",
                repeat_key=(plan_id, generation, stage, status, str(error)),
                stage=stage, http_status=status, reason=str(error),
                error_type=type(cause or error).__name__,
                duration_ms=round((time.monotonic() - started) * 1000, 3), **context,
            )
            raise
        start, end = window["start"], window["end"]
        self._log.emit(
            "window_selected", 
            repeat_key=(plan_id, generation, start["observed_at"], end["observed_at"]),
            http_status=status, 
            series_count=len(rows),
            sample_count=sum(len(row["scalarMetricsList"]) for row in rows),
            window_start=start["observed_at"], 
            window_end=end["observed_at"],
            work_start=start["work"], 
            work_end=end["work"],
            throughput=(end["work"] - start["work"]) / (end["observed_at"] - start["observed_at"]),
            target_work=window["target_work"], 
            global_batch_size=window["global_batch_size"],
            duration_ms=round((time.monotonic() - started) * 1000, 3), **context,
        )
        return window

    def window(self, rows, generation, now, max_age, min_window, after=0):
        """Require exact (epoch, createTime) alignment, never positional joins.

        The service must preserve a common epoch/createTime for each log batch.
        createTime is the sampling time unless an observed_at scalar is mapped.
        Conflicting duplicates or missing batches fail closed.
        """
        if not isinstance(rows, list):
            raise TrainingMetricsUnavailable("invalid_scalar_response")
        series = {}
        reverse = {key: name for name, key in self.keys.items()}
        for row in rows:
            if not isinstance(row, dict) or row.get("taskId") != self.task_id:
                raise TrainingMetricsUnavailable("task_identity_mismatch")
            key = row.get("key")
            if key not in reverse or key in series:
                raise TrainingMetricsUnavailable("unexpected_or_duplicate_key")
            points = row.get("scalarMetricsList")
            if not isinstance(points, list):
                raise TrainingMetricsUnavailable("invalid_scalar_series")
            values = {}
            for point in points:
                if not isinstance(point, dict):
                    raise TrainingMetricsUnavailable("invalid_scalar_point")
                epoch, created = point.get("epoch"), point.get("createTime")
                if (isinstance(epoch, bool) or not isinstance(epoch, (int, str))
                        or not isinstance(created, str) or not created):
                    raise TrainingMetricsUnavailable("missing_sample_identity")
                identity = (epoch, created)
                value = scalar(point.get("data"))
                if identity in values and values[identity] != value:
                    raise TrainingMetricsUnavailable("conflicting_scalar_sample")
                values[identity] = value
            series[key] = values
        if set(series) != set(reverse):
            raise TrainingMetricsUnavailable("missing_training_keys")
        batches = set()
        for batch, gen in series[self.keys["generation"]].items():
            if not gen.is_integer() or not 0 < gen < 2**53:
                raise TrainingMetricsUnavailable("invalid_generation")
            if str(int(gen)) == generation:
                batches.add(batch)
        # Incomplete batches are not silently bridged into a steady window.
        # Legacy/other-generation history need not contain the new scalar keys.
        if any(not batches.issubset(points) for points in series.values()):
            raise TrainingMetricsUnavailable("unaligned_scalar_samples")
        samples = []
        for batch in batches:
            sample = {name: series[key][batch] for name, key in self.keys.items()}
            if "observed_at" not in sample:
                try:
                    timestamp = datetime.fromisoformat(batch[1])
                    if timestamp.tzinfo is None:
                        timestamp = datetime.fromisoformat(batch[1] + self.utc_offset)
                    sample["observed_at"] = timestamp.timestamp()
                except (ValueError, OverflowError, OSError) as error:
                    raise TrainingMetricsUnavailable("invalid_sample_time") from error
            for name in ("work", "target_work", "global_batch_size"):
                value = sample[name]
                if not value.is_integer() or value >= 2**53:
                    raise TrainingMetricsUnavailable("invalid_step_counter")
            if (sample["global_batch_size"] <= 0 or sample["target_work"] <= 0
                    or sample["work"] > sample["target_work"]
                    or sample["steady"] not in (0, 1)
                    or not 0 < sample["observed_at"] <= now):
                raise TrainingMetricsUnavailable("invalid_training_sample")
            if sample["observed_at"] >= after:
                samples.append(sample)
        samples.sort(key=lambda point: point["observed_at"])
        if not samples or now - samples[-1]["observed_at"] > max_age:
            raise TrainingMetricsUnavailable("stale_or_missing_generation")
        stable = []
        for point in samples:
            if stable:
                previous = stable[-1]
                if point["observed_at"] == previous["observed_at"]:
                    if point == previous:
                        continue
                    raise TrainingMetricsUnavailable("conflicting_training_sample")
                if point["work"] < previous["work"]:
                    raise TrainingMetricsUnavailable("progress_regressed_within_generation")
                if any(point[key] != previous[key] for key in ("target_work", "global_batch_size")):
                    stable = []
            if point["steady"] != 1:
                stable = []
            else:
                stable.append(point)
        if len(stable) < 2:
            raise TrainingMetricsUnavailable("insufficient_steady_samples")
        end = stable[-1]
        starts = [p for p in stable[:-1]
                  if end["observed_at"] - p["observed_at"] >= min_window]
        if not starts or end["work"] <= starts[-1]["work"]:
            raise TrainingMetricsUnavailable("insufficient_progress_window")
        start = starts[-1]
        return dict(
            unit="steps", target_work=end["target_work"],
            steady=True, global_batch_size=end["global_batch_size"],
            start=dict(generation=generation, observed_at=start["observed_at"], work=start["work"]),
            end=dict(generation=generation, observed_at=end["observed_at"], work=end["work"]),
        )
