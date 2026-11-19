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

"""Submission-to-completion JCT, including queueing, from platform snapshots."""

import math

from .store import finite


def durations(job, now):
    start, end = job.get("submitted_at"), job.get("completed_at")
    actual = elapsed = None
    state = "unavailable"
    if finite(start) and 0 <= start <= now:
        if (
            job["status"] == "completed"
            and finite(end)
            and start <= end <= now
        ):
            actual = elapsed = end - start
            state = "completed"
        elif job["status"] == "running" and end is None:
            elapsed = now - start
            state = "running"
    return dict(
        actual_seconds=actual,
        elapsed_seconds=elapsed,
        measurement_status=state,
    )


def estimate_baseline(job, records):
    """Use a durable prediction window preceding the first resource change."""
    result = dict(baseline_source="unavailable", baseline_seconds=None,
                  baseline_frozen_at=None, baseline_basis=None,
                  baseline_description="缺少调整前的步数与速度观测")
    for event in records:
        if event.get("event_type") in ("checkpoint_ready", "control_completed") or (
            event.get("event_type") == "phase_changed"
            and event.get("phase") in ("save_checkpoint", "replacing_workers", "resumed")
        ):
            break
        if event.get("event_type") != "evaluated":
            continue
        benefit = event.get("benefit") or {}
        if not isinstance(benefit, dict):
            continue
        evidence = benefit.get("evidence") or {}
        training = evidence.get("training") if isinstance(evidence, dict) else None
        if not isinstance(training, dict) or training.get("unit") != "steps":
            continue
        start, end = training.get("start"), training.get("end")
        if not isinstance(start, dict) or not isinstance(end, dict):
            continue
        steps = training.get("target_work")
        t0, t1 = start.get("observed_at"), end.get("observed_at")
        w0, w1 = start.get("work"), end.get("work")
        if not (all(finite(v) for v in (steps, t0, t1, w0, w1))
                and steps == int(steps) and 0 <= w0 < w1 <= steps
                and 0 < t0 < t1 <= event["occurred_at"]
                and start.get("generation") == end.get("generation")
                and training.get("steady") is True and training.get("comparable") is True):
            continue
        speed = (w1 - w0) / (t1 - t0)
        if not finite(speed) or speed <= 0 or not finite(steps / speed):
            continue
        submitted, started = job.get("submitted_at"), evidence.get("workers_started_at")
        queue = (started - submitted if finite(submitted) and finite(started)
                 and 0 <= submitted <= started <= t0 else None)
        result.update(
            baseline_source="estimate", baseline_frozen_at=event["occurred_at"],
            baseline_description="首次调整前的稳定训练窗口",
            baseline_seconds=steps / speed + queue if queue is not None else None,
            baseline_basis=dict(total_steps=int(steps), average_steps_per_second=speed,
                                queue_seconds=queue, data_source=training.get("data_source")
                                or "unknown"),
        )
        break
    return result


def statistics(jobs, now):
    values = sorted(
        value
        for job in jobs
        if (value := durations(job, now)["actual_seconds"]) is not None
    )

    def percentile(fraction):
        # Nearest-rank percentile: a result is always an observed JCT.
        return (
            values[max(0, math.ceil(len(values) * fraction) - 1)]
            if values
            else None
        )

    return dict(
        generated_at=now,
        total_jobs=len(jobs),
        measured_jobs=len(values),
        excluded_jobs=len(jobs) - len(values),
        running_jobs=sum(job["status"] == "running" for job in jobs),
        cancelled_jobs=sum(job["status"] == "cancelled" for job in jobs),
        failed_jobs=sum(job["status"] == "failed" for job in jobs),
        completed_jobs=sum(job["status"] == "completed" for job in jobs),
        unknown_jobs=sum(job["status"] == "unknown" for job in jobs),
        mean_seconds=math.fsum(value / len(values) for value in values)
        if values
        else None,
        min_seconds=values[0] if values else None,
        max_seconds=values[-1] if values else None,
        p50_seconds=percentile(0.5),
        p95_seconds=percentile(0.95),
        percentile_method="nearest_rank",
        time_basis="submission_to_completion",
    )
