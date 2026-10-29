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
        self.task_id = env.get("LOW_CODE_PLATFORM_TASK_ID", "")
        self.mock = env.get("DLROVER_SWANLAB_MOCK", "false").strip().lower() == "true"
        self.mock_file = env.get("DLROVER_SWANLAB_MOCK_FILE", "")
        self.token = env.get("DLROVER_SWANLAB_HTTP_TOKEN", "")
        self.post = post or requests.post
        self.assume_fixed_batch = env.get("DLROVER_SWANLAB_ASSUME_FIXED_BATCH", "false").strip().lower() == "true"
        self.keys = {name: TRAINING_METRIC_KEYS[name]
                     for name in ("work", "target_work", "global_batch_size")}
        if self.assume_fixed_batch:
            self.keys = dict(work="global_step", target_work="max_steps", train_speed="train_speed")
        self.time_source = env.get("DLROVER_SWANLAB_TIME_SOURCE", "sample")
        self.utc_offset = env.get("DLROVER_SWANLAB_UTC_OFFSET", "+08:00")
        self._log = BenefitLogger("swanlab")
        self.valid = True
        try:
            self.warmup = float(env.get("DLROVER_SWANLAB_WARMUP_SECONDS", "60"))
            self.steady_windows = int(env.get("DLROVER_SWANLAB_STEADY_WINDOWS", "2"))
            self.max_variation = float(env.get("DLROVER_SWANLAB_MAX_THROUGHPUT_VARIATION", "0.1"))
            if (not math.isfinite(self.warmup) or self.warmup < 0
                    or self.steady_windows < 2
                    or not math.isfinite(self.max_variation) or not 0 <= self.max_variation <= 1):
                raise ValueError("invalid_steady_configuration")
            overrides = json.loads(env.get("DLROVER_SWANLAB_KEYS", "{}"))
            if not isinstance(overrides, dict) or set(overrides) - (set(TRAINING_METRIC_KEYS) | {"train_speed"}):
                raise ValueError("invalid_key_mapping")
            # Legacy generation/steady/observed_at mappings are not requested.
            self.keys.update({k: v for k, v in overrides.items() if k in self.keys})
            if (not all(isinstance(v, str) and v for v in self.keys.values())
                    or len(set(self.keys.values())) != len(self.keys)):
                raise ValueError("invalid_key_mapping")
            offset = datetime.fromisoformat("2000-01-01T00:00:00" + self.utc_offset)
            if offset.tzinfo is None:
                raise ValueError("missing_timezone")
            if not self.task_id:
                raise ValueError("missing_task_id")
            if self.mock:
                if not self.mock_file:
                    raise ValueError("missing_mock_file")
            else:
                parsed = urlparse(self.url)
                if (parsed.scheme not in ("http", "https") or not parsed.hostname
                        or parsed.username or parsed.password):
                    raise ValueError("invalid_swanlab_configuration")
        except (ValueError, TypeError):
            self.valid = False

    def fetch(self, generation, clock, timeout, max_age, min_window, after=0,
              plan_id=None, request_id=None, training_context=None):
        started = time.monotonic()
        context = dict(task_id=self.task_id, generation=generation,
                       plan_id=plan_id, request_id=request_id)
        context["data_source"] = "mock" if self.mock else "http"
        stage, status = "configuration", None
        self._log.emit("fetch_started", level="debug",
                       timeout_seconds=timeout, max_age_seconds=max_age,
                       min_window_seconds=min_window, after=after, **context)
        try:
            if not self.valid or not generation:
                raise TrainingMetricsUnavailable("swanlab_not_configured_or_no_generation")
            if self.mock:
                stage = "mock_file"
                try:
                    # Reload each poll to allow controlled generation/window changes.
                    with open(self.mock_file, encoding="utf-8") as stream:
                        rows = json.load(stream)
                except (OSError, ValueError) as error:
                    raise TrainingMetricsUnavailable("swanlab_mock_file_error") from error
            else:
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
            window = self.window(rows, generation, clock(), max_age, min_window, after, training_context)
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
            steady_source="inferred", time_source=self.time_source,
            steady_windows=self.steady_windows, warmup_seconds=self.warmup,
            window_start=start["observed_at"], 
            window_end=end["observed_at"],
            work_start=start["work"], 
            work_end=end["work"],
            throughput=(end["work"] - start["work"]) / (end["observed_at"] - start["observed_at"]),
            target_work=window["target_work"], 
            global_batch_size=window.get("global_batch_size"),
            batch_size_source=window.get("batch_size_source", "measured"),
            reported_train_speed=window.get("reported_train_speed"),
            speed_relative_difference=window.get("speed_relative_difference"),
            duration_ms=round((time.monotonic() - started) * 1000, 3), **context,
        )
        if self.mock:
            window["data_source"] = "mock"
        return window

    def window(self, rows, generation, now, max_age, min_window, after=0,
               training_context=None):
        """Infer steady progress from aligned, post-recovery sampling times.

        createTime must be confirmed as sampling time, not ingestion time.
        Generation is supplied by Master and never inferred from HTTP history.
        """
        if not self.valid:
            raise TrainingMetricsUnavailable("invalid_swanlab_configuration")
        if self.time_source != "sample":
            raise TrainingMetricsUnavailable("untrusted_sample_time")
        context = training_context or {}
        if not generation or context.get("generation") != generation:
            raise TrainingMetricsUnavailable("missing_or_changed_training_context")
        started = context.get("started_at")
        recovered = context.get("recovered_at", 0)
        if (any(isinstance(v, bool) or not isinstance(v, (int, float))
                or not math.isfinite(v) or v < 0 for v in (started, recovered))
                or started <= 0 or max(started, recovered) > now):
            raise TrainingMetricsUnavailable("invalid_generation_boundary")
        if context.get("recovery_ready") is not True:
            raise TrainingMetricsUnavailable("training_recovery_pending")
        boundary = max(started, recovered) + self.warmup
        lower = max(boundary, after)
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
                try:
                    timestamp = datetime.fromisoformat(created)
                    if timestamp.tzinfo is None:
                        timestamp = datetime.fromisoformat(created + self.utc_offset)
                    observed = timestamp.timestamp()
                except (ValueError, OverflowError, OSError) as error:
                    raise TrainingMetricsUnavailable("invalid_sample_time") from error
                if not 0 < observed <= now:
                    raise TrainingMetricsUnavailable("invalid_sample_time")
                if observed < lower or observed <= started:
                    continue
                identity = (epoch, observed)
                value = scalar(point.get("data"))
                if identity in values and values[identity] != value:
                    raise TrainingMetricsUnavailable("conflicting_scalar_sample")
                values[identity] = value
            series[key] = values
        if set(series) != set(reverse):
            raise TrainingMetricsUnavailable("missing_training_keys")
        batches = set().union(*(set(points) for points in series.values()))
        if any(set(points) != batches for points in series.values()):
            raise TrainingMetricsUnavailable("unaligned_scalar_samples")
        samples = []
        for batch in batches:
            sample = {name: series[key][batch] for name, key in self.keys.items()}
            sample["observed_at"] = batch[1]
            if any(not sample[name].is_integer() or sample[name] >= 2**53
                   for name in ("work", "target_work", "global_batch_size") if name in sample):
                raise TrainingMetricsUnavailable("invalid_step_counter")
            if (sample.get("global_batch_size", 1) <= 0 or sample.get("train_speed", 1) <= 0
                    or sample["target_work"] <= 0
                    or sample["work"] > sample["target_work"]):
                raise TrainingMetricsUnavailable("invalid_training_sample")
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
                if (point["observed_at"] - previous["observed_at"] > max_age
                        or any(point[k] != previous[k] for k in ("target_work", "global_batch_size") if k in point)):
                    stable = []
            stable.append(point)
        if len(stable) < self.steady_windows + 1:
            raise TrainingMetricsUnavailable("insufficient_steady_samples")
        # Nonoverlapping subwindows cover at least min_window seconds in total.
        ends = [stable[-1]]
        interval = min_window / self.steady_windows
        for point in reversed(stable[:-1]):
            if ends[-1]["observed_at"] - point["observed_at"] >= interval:
                ends.append(point)
                if len(ends) == self.steady_windows + 1:
                    break
        if len(ends) != self.steady_windows + 1:
            raise TrainingMetricsUnavailable("insufficient_progress_window")
        rates = [(end["work"] - start["work"]) / (end["observed_at"] - start["observed_at"])
                 for end, start in zip(ends, ends[1:])]
        if min(rates) <= 0:
            raise TrainingMetricsUnavailable("no_progress")
        if (max(rates) - min(rates)) / min(rates) > self.max_variation:
            raise TrainingMetricsUnavailable("unstable_throughput")
        start, end = ends[-1], ends[0]
        result = dict(
            unit="steps", target_work=end["target_work"], steady=True,
            steady_source="inferred", time_source="sample", steady_windows=self.steady_windows,
            start=dict(generation=generation, observed_at=start["observed_at"], work=start["work"]),
            end=dict(generation=generation, observed_at=end["observed_at"], work=end["work"]),
        )
        if self.assume_fixed_batch:
            computed = (end["work"] - start["work"]) / (end["observed_at"] - start["observed_at"])
            result.update(batch_size_source="assumed_fixed", global_batch_size_fixed=True,
                          reported_train_speed=end["train_speed"],
                          speed_relative_difference=abs(end["train_speed"] - computed) / computed)
        else:
            result["global_batch_size"] = end["global_batch_size"]
        return result
