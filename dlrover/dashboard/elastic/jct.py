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
