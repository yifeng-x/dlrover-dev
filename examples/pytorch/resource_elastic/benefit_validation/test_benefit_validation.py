from copy import deepcopy
from math import isclose
from dlrover.python.master.resource.benefit import evaluate

now = 1000
request = {
    "schema_version": 1, "request_id": "validation-1",
    "job": {"job_uid": "validation-job"}, "generation": "123",
    "current": {"workers": 1, "gpus_per_worker": 4, "gpu_type": "nvidia.com/gpu"},
    "target": {"workers": 1, "gpus_per_worker": 8, "gpu_type": "nvidia.com/gpu"},
}
snapshot = {
    **deepcopy(request), "observed_at": now,
    "training": {
        "unit": "steps", "comparable": True, "steady": True,
        "global_batch_size": 64, "target_work": 3760,
        "start": {"generation": "123", "observed_at": 940, "work": 100},
        "end": {"generation": "123", "observed_at": 1000, "work": 160},
    },
    "candidate": {"throughput_lower": 2, "evidence": "profile", "evidence_id": "fixture-1"},
    "transition": {"lost_seconds_upper": 120, "replay_work_upper": 0, "gpu_seconds_upper": 960},
    "scheduling": {"target_feasible": True, "constraints_satisfied": True},
    "limits": {"max_delay_seconds": 1800, "deadline_at": None},
}
time_result = evaluate(snapshot, request, now, objective="time")
gpu_result = evaluate(snapshot, request, now, objective="gpu_hours")
assert time_result.status == "positive"
assert time_result.estimates["time_gain_seconds"] == 1680
assert gpu_result.status == "negative"
assert isclose(gpu_result.estimates["gpu_hours_saved"], -4 / 15)
print(time_result.to_dict())
print(gpu_result.to_dict())
