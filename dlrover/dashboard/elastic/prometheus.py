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
