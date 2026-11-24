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

"""Optional instant-query fallback; durable event evaluations take precedence."""

import json
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from dlrover.python.common.log import default_logger as logger

from .aggregate import choice, num, positive_time

PREFIX = "dlrover_master_vertical_benefit_"


class PrometheusBenefits:
    def __init__(self, url, token=None, timeout=3):
        parsed = urlparse(url)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.netloc
            or parsed.username
        ):
            raise ValueError("Invalid Prometheus URL")
        self.url, self.token, self.timeout = url.rstrip("/"), token, timeout

    def latest(self, job):
        labels = {
            "cluster": job["cluster"],
            "namespace": job["namespace"],
            "job_uid": job["uid"],
        }
        selector = ",".join(k + "=" + json.dumps(v) for k, v in labels.items())
        query = '{__name__=~"' + PREFIX + '.*",' + selector + "}"
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        request = Request(
            self.url + "/api/v1/query?" + urlencode({"query": query}),
            headers=headers,
        )
        with urlopen(request, timeout=self.timeout) as response:
            body = response.read(2 * 1024 * 1024 + 1)
        if len(body) > 2 * 1024 * 1024:
            raise ValueError("Prometheus response too large")
        return self.parse(json.loads(body), labels)

    @staticmethod
    def parse(body, expected):
        data = body.get("data", {})
        if (
            body.get("status") != "success"
            or data.get("resultType") != "vector"
        ):
            raise ValueError("Invalid Prometheus response")
        groups = {}
        for item in data.get("result", []):
            labels = item.get("metric", {})
            if any(labels.get(k) != v for k, v in expected.items()):
                continue
            stage = labels.get("stage")
            if stage not in ("prediction", "exploration"):
                continue
            key = tuple(
                labels.get(k)
                for k in (
                    "stage",
                    "objective",
                    "work_unit",
                    "data_source",
                    "strategy",
                )
            )
            value = item.get("value", [])
            if len(value) != 2:
                raise ValueError("Invalid Prometheus value")
            name = labels.get("__name__", "")
            if not name.startswith(PREFIX):
                continue
            group = groups.setdefault(key, {})
            field = name[len(PREFIX) :]
            # Multiple scrape targets must be deduplicated upstream, not summed.
            if field in group:
                raise ValueError("Ambiguous Prometheus series")
            group[field] = num(float(value[1]))
            group["sample_at"] = max(
                group.get("sample_at", 0), float(value[0])
            )
        if not groups:
            return None
        key, values = max(
            groups.items(),
            key=lambda pair: (
                positive_time(pair[1].get("observed_timestamp_seconds"))
                or pair[1]["sample_at"],
                pair[0][0],
            ),
        )
        stage, objective, unit, source, _ = key
        result = dict(
            stage=stage,
            objective=choice(objective, ("time", "gpu_hours", "cluster_jct")),
            work_unit=choice(unit, ("steps", "samples", "tokens")),
            data_source=choice(source, ("mock", "http")),
            observed_at=positive_time(
                values.get("observed_timestamp_seconds")
            ),
            max_age_seconds=None,
            min_gain_ratio=None,
            windows=None,
            state=None,
            reported=None,
        )
        fields = (
            "time_gain_seconds",
            "gpu_seconds_saved",
            "gain_ratio",
            "baseline_throughput",
            "candidate_throughput",
        )
        result.update({field: values.get(field) for field in fields})
        result["valid"] = values.get("valid") == 1 and all(
            result[k] is not None for k in fields
        )
        return result

    def gpu_history(self, job, start, end):
        """Bound chart resolution while retaining freshness checks per point."""
        import math

        if start > end:
            return []
        labels = {k: job[k] for k in ("cluster", "namespace")}
        labels["job_uid"] = job["uid"]
        selector = ",".join(k + "=" + json.dumps(v) for k, v in labels.items())
        names = (
            "running_worker_allocated_gpus|worker_gpu_observation_valid|"
            "worker_gpu_observed_timestamp_seconds"
        )
        start = int(start)
        step = max(10, math.ceil((end - start) / 1999))
        params = dict(
            query='{__name__=~"dlrover_master_('
            + names
            + ')",'
            + selector
            + "}",
            start=start,
            end=end,
            step=step,
        )
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        request = Request(
            self.url + "/api/v1/query_range?" + urlencode(params),
            headers=headers,
        )
        limit = 8 * 1024 * 1024
        with urlopen(request, timeout=self.timeout) as response:
            body = response.read(limit + 1)
        if len(body) > limit:
            raise ValueError("Prometheus response too large")
        return self.parse_gpu(json.loads(body), labels, start, end, step)

    @staticmethod
    def parse_gpu(body, expected, start, end, step):
        data = body.get("data", {})
        if (
            body.get("status") != "success"
            or data.get("resultType") != "matrix"
        ):
            raise ValueError("Invalid Prometheus range response")
        groups = {}
        for item in data.get("result", []):
            labels = item.get("metric", {})
            if any(labels.get(k) != v for k, v in expected.items()):
                continue
            target = tuple(
                sorted((k, v) for k, v in labels.items() if k != "__name__")
            )
            name = labels.get("__name__", "")
            group = groups.setdefault(target, {})
            if name in group:
                raise ValueError("Ambiguous Prometheus GPU series")
            group[name] = {
                float(t): num(float(v)) for t, v in item.get("values", [])
            }
        points = []
        stale_candidates = conflicts = missing_points = 0
        prefix = "dlrover_master_"
        # query_range evaluates on this grid. Missing evaluations remain null.
        for i in range(min(2000, int((end - start) / step) + 1)):
            at = start + i * step
            candidates = []
            for group in groups.values():

                def value(name):
                    return group.get(prefix + name, {}).get(at)

                observed = value("worker_gpu_observed_timestamp_seconds")
                gpus = value("running_worker_allocated_gpus")
                valid = value("worker_gpu_observation_valid") == 1
                if valid and observed is not None and at - observed > 30:
                    stale_candidates += 1
                if (
                    valid
                    and observed is not None
                    and 0 <= at - observed <= 30
                    and gpus is not None
                    and 0 <= gpus <= 2147483647
                    and gpus == int(gpus)
                ):
                    candidates.append((observed, int(gpus)))
            gpus = None
            if candidates:
                newest = max(t for t, _ in candidates)
                counts = {v for t, v in candidates if t == newest}
                if len(counts) == 1:
                    gpus = counts.pop()
                else:
                    conflicts += 1
            if gpus is None:
                missing_points += 1
            points.append(dict(at=at, gpus=gpus, throughput=None))
        logger.info(
            "GPU history job_uid=%r window=[%s,%s] step=%s targets=%d points=%d "
            "missing_points=%d stale_candidates=%d conflicting_points=%d",
            expected.get("job_uid"), start, end, step, len(groups), len(points),
            missing_points, stale_candidates, conflicts,
        )
        return points
