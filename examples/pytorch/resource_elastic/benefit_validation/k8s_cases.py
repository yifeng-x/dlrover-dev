"""Scenario inputs from vertical-elastic-benefit-k8s-mock-test-cn.md."""

import copy


def cases(total_steps=3760):
    if total_steps not in (3760, 1044):
        raise ValueError("The documented expectations support total_steps=3760 or 1044")
    result = {}

    def add(key, name, status="negative", reason="insufficient_gain", **fixture):
        result[key] = dict(id=key, fixture=dict(name=name, **fixture),
                           mode="enforce", objective="time", initial_gpus=1,
                           target_gpus=2, status=status, reason=reason,
                           decision="allow" if status == "positive" else "reject",
                           env={}, total_steps=total_steps)

    add("G01", "time-positive", "positive", None, q1=2)
    add("G02", "time-negative", q1=0.8)
    add("G03", "below-threshold", q1=1.18 if total_steps == 1044 else 1.05)
    add("G04", "gpu-cost-negative", q1=2)
    add("G05", "gpu-saving", "positive", None, q1=0.75)
    add("G06", "shrink-time-negative", q1=0.75)
    add("G07", "queue-positive", "positive", None, q1=0.75,
        impacts_complete=True, other_jobs=[dict(job_id="waiting-fixture", weight=1,
        baseline_remaining_seconds=6000, candidate_remaining_seconds_upper=2520)])
    add("G08", "queue-unknown", "unknown", "invalid_or_missing_evidence", q1=2)
    add("G09", "candidate-unavailable", "unknown", "candidate_unavailable", candidate="unavailable")
    add("G10", "candidate-missing", "unknown", "invalid_or_missing_evidence", candidate="missing")
    add("G11", "infeasible", reason="target_infeasible", feasible=False)
    add("G12", "constraints", reason="scheduler_constraints", constraints=False)
    add("G13", "delay-budget", reason="delay_budget", q1=0.8,
        max_delay=200 if total_steps == 1044 else 500)
    add("G14", "deadline", reason="deadline", q1=2,
        deadline_in=500 if total_steps == 1044 else 1800)
    add("G15", "stale-evidence", "unknown", "invalid_or_missing_evidence", snapshot_age=1200)
    add("G16", "wrong-identity", "unknown", "invalid_or_missing_evidence", identity_mismatch=True)
    add("G17", "incomparable", "unknown", "invalid_or_missing_evidence", comparable=False)
    add("G18", "http-error", "unknown", "http_error", http_status=503)
    add("G19", "http-timeout", "unknown", "http_or_payload_error", sleep_seconds=15)
    add("G20", "shadow-negative", q1=0.8)
    add("G21", "shadow-unknown", "unknown", "candidate_unavailable", candidate="unavailable")
    add("G22", "off-http-error", None, None, http_status=503)
    add("G23", "replay", "positive", None, q1=2, replay=100)
    for key in ("G04", "G05"):
        result[key]["objective"] = "gpu_hours"
    for key in ("G05", "G06", "G07"):
        result[key].update(initial_gpus=2, target_gpus=1)
    for key in ("G07", "G08"):
        result[key]["objective"] = "cluster_jct"
    for key in ("G20", "G21", "G22"):
        result[key].update(mode="off" if key == "G22" else "shadow", decision="allow")
    reasons = ["swanlab_mock_file_error", "task_identity_mismatch",
               "swanlab_not_configured_or_no_generation", "stale_or_missing_generation",
               "insufficient_progress_window", "stale_or_missing_generation",
               "unstable_throughput", "missing_training_keys", "untrusted_sample_time",
               "candidate_unavailable"]
    for index, reason in enumerate(reasons, 1):
        key = f"S{index:02}"
        add(key, "time-positive", "unknown", reason, q1=2)
    result["S04"]["env"]["DLROVER_BENEFIT_MAX_AGE_SECONDS"] = "60"
    result["S09"]["env"]["DLROVER_SWANLAB_TIME_SOURCE"] = "ingestion"
    result["S10"]["fixture"] = dict(name="candidate-unavailable", candidate="unavailable")
    for index in range(1, 11):
        key = f"E{index:02}"
        result[key] = copy.deepcopy(result["G09"])
        result[key].update(id=key, total_steps=107260, decision="explore",
                           terminal={1: "ACCEPTED", 2: "RETAINED", 3: "RETAINED",
                                     4: "INCONCLUSIVE", 8: "ACCEPTED"}.get(index))
        result[key]["env"]["DLROVER_BENEFIT_EXPLORE"] = "true"
    for key in ("E05", "E06", "E07"):
        result[key]["decision"] = "reject"
    result["E06"]["objective"] = "cluster_jct"
    result["E07"].update(reason="invalid_or_missing_evidence",
                         fixture=dict(name="candidate-missing", candidate="missing"))
    result["E04"]["env"]["DLROVER_BENEFIT_TRIAL_SECONDS"] = "300"
    return result
