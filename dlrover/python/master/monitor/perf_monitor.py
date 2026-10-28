# Copyright 2022 The DLRover Authors. All rights reserved.
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

import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple

from dlrover.python.common.constants import EventReportConstants
from dlrover.python.common.event.reporter import get_event_reporter
from dlrover.python.common.global_context import Context
from dlrover.python.common.log import default_logger as logger

_dlrover_context = Context.singleton_instance()


class GlobalStepRecord(object):
    """Record a global step with the time and the number of workers.
    Attributes:
        global_step: The global iteration step of a training job.
        timestamp: the timestampe to collect the global step.
        worker_num: the number of worker at the timestamp.
    """

    def __init__(self, global_step, timestamp, worker_num):
        self.global_step = global_step
        self.timestamp = timestamp
        self.worker_num = worker_num


class EvaluationTime(object):
    def __init__(self):
        self.start_eval = 0
        self.eval_time = 0


@dataclass(frozen=True)
class PerfMetricsSnapshot:
    completed_global_step: int
    last_progress_timestamp: float
    running_speed: float
    running_worker_count: int
    target_worker_count: int


class PerfMonitor(object):
    """Monitor the training speed by the number of batch per second"""

    def __init__(self):
        self._global_step_records: List[GlobalStepRecord] = []
        self._workers: Set[Tuple[str, int]] = set()
        self._max_record_count = _dlrover_context.train_speed_record_num
        self._global_step = 0
        self._target_worker_num = 0
        self._last_progress_timestamp = 0.0
        self._lock = threading.RLock()
        self._init_time = time.time()
        self._start_training_time = None
        self._sample_count = 0
        self._worker_eval_times: Dict[int, EvaluationTime] = {}
        self._event_reporter = get_event_reporter()

    def set_target_worker_num(self, worker_num):
        """Set the target number of workers"""
        with self._lock:
            self._target_worker_num = worker_num
        logger.info(
            "The target number of worker : %s", self._target_worker_num
        )

    def reduce_target_worker_num(self, workers: List[Tuple[str, int]]):
        """Reduce the target number of workers"""
        with self._lock:
            num = 0
            for worker in workers:
                if worker in self._workers:
                    num += 1
            if self._target_worker_num > num:
                self._target_worker_num -= num

    def set_start_timestamp(self):
        """Set the start timestamp of training"""
        with self._lock:
            if self._global_step == 0 and not self._global_step_records:
                self._global_step_records.append(
                    GlobalStepRecord(0, int(time.time()), len(self._workers))
                )

    def collect_global_step(self, global_step, timestamp):
        """The sampling interval should be bigger than 6s. It means
        that we calculate the speed with interval 1min.
        """
        observed_at = time.time()
        with self._lock:
            if not self._start_training_time:
                self._start_training_time = observed_at
                logger.info(
                    "The initial training time is %s",
                    self._start_training_time - self._init_time,
                )
                if self._event_reporter:
                    self._event_reporter.report(
                        EventReportConstants.TYPE_INFO,
                        "job",
                        EventReportConstants.ACTION_TRAINING_START,
                        "",
                        {},
                    )

            self._global_step = global_step
            self._last_progress_timestamp = observed_at

            if (
                not self._global_step_records
                or global_step > self._global_step_records[-1].global_step
            ):
                self._sample_count += 1
                self._global_step_records.append(
                    GlobalStepRecord(
                        global_step, timestamp, len(self._workers)
                    )
                )
                if len(self._global_step_records) > self._max_record_count:
                    self._global_step_records.pop(0)
                logger.info(
                    "Global step = %s, Worker number = %s, speed = %s steps/s",
                    self._global_step,
                    self._global_step_records[-1].worker_num,
                    round(self._running_speed_unlocked(), 2),
                )
                if self._event_reporter:
                    self._event_reporter.report(
                        EventReportConstants.TYPE_INFO,
                        "job",
                        EventReportConstants.ACTION_GLOBAL_STEP,
                        f"global_step={self._global_step}",
                        {},
                    )

    def get_sample_count(self):
        with self._lock:
            return self._sample_count

    def _running_speed_unlocked(self):
        if len(self._global_step_records) < 2:
            return 0
        last_record = self._global_step_records[-1]
        first_record = self._global_step_records[-2]
        time_diff = last_record.timestamp - first_record.timestamp
        if time_diff <= 0:
            return 0
        speed = (last_record.global_step - first_record.global_step) / (
            time_diff
        )
        return speed

    @property
    def running_speed(self):
        with self._lock:
            return self._running_speed_unlocked()

    def add_running_worker(self, type, worker_id):
        with self._lock:
            self._workers.add((type, worker_id))

    def remove_running_worker(self, type, worker_id):
        with self._lock:
            if (type, worker_id) in self._workers:
                self._workers.remove((type, worker_id))
                logger.info(
                    f"Speed monitor removes a worker {type}-{worker_id} and "
                    f"the remaining workers are {self._workers}."
                )
            elif self._workers:
                logger.info(
                    f"The worker {type}-{worker_id} is not in speed monitor and "
                    f"the remaining workers are {self._workers}."
                )

    @property
    def init_training_time(self):
        with self._lock:
            if self._start_training_time:
                return round(self._start_training_time - self._init_time)
            return 0

    @property
    def completed_global_step(self):
        with self._lock:
            return self._global_step

    @property
    def last_global_step_timestamp(self):
        with self._lock:
            return self._last_progress_timestamp

    @property
    def running_workers(self):
        with self._lock:
            return set(self._workers)

    @property
    def target_worker_num(self):
        with self._lock:
            return self._target_worker_num

    def get_metrics_snapshot(self) -> PerfMetricsSnapshot:
        """Return one internally consistent snapshot for Prometheus."""
        with self._lock:
            return PerfMetricsSnapshot(
                completed_global_step=self._global_step,
                last_progress_timestamp=self._last_progress_timestamp,
                running_speed=self._running_speed_unlocked(),
                running_worker_count=len(self._workers),
                target_worker_count=self._target_worker_num,
            )

    def reset_running_perf_monitor(self):
        """Reset the perf monitor by clearing the record of global records
        and running workers."""
        with self._lock:
            self._global_step_records = []
            self._workers = set()

    def set_worker_start_eval_time(self, worker_id):
        self._worker_eval_times.setdefault(worker_id, EvaluationTime())
        self._worker_eval_times[worker_id].start_eval = time.time()

    def update_worker_eval_time(self, worker_id):
        if worker_id in self._worker_eval_times:
            eval_time = self._worker_eval_times[worker_id]
            if eval_time.start_eval > 0:
                eval_time.eval_time += time.time() - eval_time.start_eval
                eval_time.start_eval = 0

    def get_worker_eval_time(self, worker_id):
        if worker_id in self._worker_eval_times:
            eval = self._worker_eval_times[worker_id]
            if eval.start_eval > 0:
                eval.eval_time += time.time() - eval.start_eval
                eval.start_eval = 0
            return eval.eval_time
        return 0

    def all_worker_joined(self):
        with self._lock:
            return len(self._workers) == self._target_worker_num

    def worker_adjustment_finished(self):
        """Check the number of workers is equal to the target
        in the enough time, such 5min"""
        with self._lock:
            if not self._global_step_records:
                return False
            worker_num = self._global_step_records[-1].worker_num
            last_time = self._global_step_records[-1].timestamp
            if worker_num != self._target_worker_num:
                return False
            for record in reversed(self._global_step_records):
                if (
                    record.worker_num == worker_num
                    and last_time - record.timestamp
                    >= _dlrover_context.seconds_for_stable_worker_count
                ):
                    return True
            return False
