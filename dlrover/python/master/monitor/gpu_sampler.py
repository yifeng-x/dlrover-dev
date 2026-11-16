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

"""Read-only periodic observations of running Worker allocations."""

import threading
import time

from dlrover.python.common.constants import (
    ElasticJobLabel,
    NodeStatus,
    NodeType,
)
from dlrover.python.common.log import default_logger as logger
from dlrover.python.common.metric.prometheus import MetricSample
from dlrover.python.scheduler.kubernetes import get_main_container


def running_gpu_sample(pods, job_uid):
    if pods is None:
        raise ValueError("Pod list unavailable")
    workers = gpus = 0
    seen = set()
    resources = {}
    for pod in pods:
        metadata = pod.metadata
        if (metadata.labels or {}).get(
            ElasticJobLabel.REPLICA_TYPE_KEY
        ) != NodeType.WORKER:
            continue
        owners = metadata.owner_references or []
        if not owners:
            raise ValueError("Worker ownership unavailable")
        if not any(str(owner.uid) == job_uid for owner in owners):
            continue
        if (
            metadata.deletion_timestamp
            or pod.status.phase != NodeStatus.RUNNING
        ):
            continue
        if not metadata.uid:
            raise ValueError("Worker Pod UID unavailable")
        if metadata.uid in seen:
            continue
        seen.add(metadata.uid)
        container = get_main_container(pod)
        if container is None:
            raise ValueError("Training container unavailable")
        status = next(
            (
                s
                for s in pod.status.container_statuses or []
                if s.name == container.name
            ),
            None,
        )
        if status is None or status.state is None:
            raise ValueError("Training container state unavailable")
        if status.state.running is None:
            continue
        limits = container.resources.limits if container.resources else {}
        requests = container.resources.requests if container.resources else {}
        values = {**(limits or {}), **(requests or {})}
        for key, value in values.items():
            if not key.startswith(
                (
                    "nvidia.com/",
                    "huawei.com/",
                    "metax-tech.com/",
                    "kunlunxin.com/",
                )
            ):
                continue
            if not (
                key
                in (
                    "nvidia.com/gpu",
                    "metax-tech.com/gpu",
                    "kunlunxin.com/xpu",
                )
                or key.startswith(("nvidia.com/mig-", "huawei.com/Ascend"))
            ):
                raise ValueError("Unsupported accelerator resource")
            count = int(value)
            if count < 0 or str(count) != str(value):
                raise ValueError("Invalid accelerator allocation")
            resources[key] = resources.get(key, 0) + count
            gpus += count
        workers += 1
    return dict(
        basis="running_worker_allocations",
        complete=True,
        workers=workers,
        gpus=gpus,
        resources=resources,
    )


class RunningGPUReporter:
    def __init__(self, job, list_pods, interval=10):
        self.job_uid = str(job["job_uid"])
        self._lock = threading.Lock()
        self._snapshot = (0, {})
        self.list_pods = list_pods
        self.interval = interval
        self._stop = threading.Event()
        self._thread = None
        self._failed = False

    def collect(self):
        started = time.time()
        try:
            result = self.list_pods()
            sample = running_gpu_sample(
                result.items if result is not None else None,
                self.job_uid,
            )
            # A slow listing is not a fresh, simultaneous observation.
            if time.time() - started > self.interval:
                raise ValueError("Pod observation expired")
            self._failed = False
        except Exception:
            if not self._failed:
                logger.warning("Running Worker GPU observation unavailable")
            self._failed = True
            sample = dict(
                basis="running_worker_allocations",
                complete=False,
                workers=None,
                gpus=None,
                resources={},
            )
        if not self._stop.is_set():
            with self._lock:
                self._snapshot = (started, sample)

    def metrics(self, labels):
        # Scrapes only read memory; never perform Kubernetes or HTTP I/O.
        with self._lock:
            observed, sample = self._snapshot
        valid = (
            not self._stop.is_set()
            and sample.get("complete", False)
            and 0 <= time.time() - observed <= 30
        )
        values = {
            "worker_gpu_observation_valid": int(valid),
            "worker_gpu_observed_timestamp_seconds": observed,
        }
        if valid:
            values["running_worker_allocated_gpus"] = sample["gpus"]
            values["running_worker_count"] = sample["workers"]
        for name, value in values.items():
            yield MetricSample("dlrover_master_" + name, value, labels)

    def start(self):
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, name="worker-gpu-sampler", daemon=True
        )
        self._thread.start()

    def _run(self):
        while not self._stop.is_set():
            try:
                self.collect()
            except Exception:
                logger.warning("Running Worker GPU reporting failed")
            self._stop.wait(self.interval)

    def stop(self):
        # Never wait for Kubernetes or HTTP I/O in the Master shutdown path.
        self._stop.set()
