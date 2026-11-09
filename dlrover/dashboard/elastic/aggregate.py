# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
"""Translate durable events into the React contract, without inventing samples."""

import time
from collections import defaultdict

from .store import finite
from .jct import durations, statistics


def obj(value):
    return value if isinstance(value, dict) else {}


def num(value):
    return value if finite(value) else None


def positive_time(value):
    return value if finite(value) and value > 0 else None


def choice(value, allowed, default="unknown"):
    return value if value in allowed else default


def public_job(job):
    return {key: job[key] for key in ("uid", "name", "cluster", "namespace", "status")}


def evaluation(event, exploration=False):
    benefit = obj(event.get("benefit"))
    trial = obj(event.get("exploration")) if exploration else {}
    values = obj((trial if exploration else benefit).get("estimates"))
    evidence = obj(benefit.get("evidence"))
    training = obj(evidence.get("training"))
    objective = choice(benefit.get("objective", trial.get("objective")),
                       ("time", "gpu_hours", "cluster_jct"))
    windows = num(trial.get("windows"))
    windows = int(windows) if windows is not None and windows >= 0 and windows == int(windows) else None
    state = str(trial.get("status", "unknown")).lower() if exploration else None
    gpu = num(values.get("gpu_seconds_saved")) if exploration else num(values.get("gpu_hours_saved"))
    if not exploration and gpu is not None:
        gpu = num(gpu * 3600)
    ratio = num(values.get("gain_ratio"))
    if ratio is None and not exploration and objective in ("time", "gpu_hours"):
        gain = num(values.get("time_gain_seconds" if objective == "time" else "gpu_hours_saved"))
        baseline = num(values.get("baseline_seconds" if objective == "time" else "baseline_gpu_hours"))
        if gain is not None and baseline is not None and baseline > 0:
            ratio = num(gain / baseline)
    result = dict(
        stage="exploration" if exploration else "prediction", objective=objective,
        work_unit=choice(values.get("unit", training.get("unit")), ("steps", "samples", "tokens")),
        data_source=choice(values.get("data_source", training.get("data_source")), ("mock", "http")),
        observed_at=positive_time(trial.get("observed_at") if exploration else obj(training.get("end")).get("observed_at")),
        max_age_seconds=(benefit["max_age_seconds"] if finite(benefit.get("max_age_seconds"))
                         and benefit["max_age_seconds"] >= 0 else None),
        time_gain_seconds=num(values.get("time_gain_seconds")),
        gpu_seconds_saved=gpu, gain_ratio=ratio,
        min_gain_ratio=num(benefit.get("min_gain_ratio")),
        baseline_throughput=num(values.get("baseline_throughput" if exploration else "current_throughput")),
        candidate_throughput=num(values.get("candidate_throughput")),
        windows=windows, state=state,
        reported=trial.get("reported") if isinstance(trial.get("reported"), bool) else None,
    )
    valid = (windows is not None and windows >= 2 and state in (
        "accepted", "retained", "inconclusive", "failed", "rolled_back")) if exploration else (
        benefit.get("mode") != "off" and benefit.get("evaluation_status") in ("positive", "negative"))
    result["valid"] = bool(valid and all(result[k] is not None for k in (
        "baseline_throughput", "candidate_throughput", "gain_ratio", "time_gain_seconds", "gpu_seconds_saved")))
    if benefit.get("mode") == "off" and not exploration:
        for key in ("time_gain_seconds", "gpu_seconds_saved", "gain_ratio"):
            result[key] = None
    return result


PHASES = {
    "preparing": "准备调整", "resumed": "重启续接计划",
    "save_checkpoint": "开始保存 checkpoint", "replacing_workers": "开始替换 Worker",
    "tune_param": "开始调整训练参数", "rendezvous_ready": "等待训练进程就绪",
    "failed": "执行失败", "skipped": "跳过调整", "completed": "控制执行完成",
}
STAGES = {
    "checkpoint_ready": "checkpoint 完成", "control_completed": "控制执行完成",
    "training_recovered": "确认训练恢复", "exploration_decided": "探索评估完成",
}


def group_events(events):
    request_plans = defaultdict(set)
    for event in events:
        if event.get("request_id") and event.get("plan_id"):
            request_plans[event["request_id"]].add(event["plan_id"])
    grouped = defaultdict(list)
    for event in events:
        plan = event.get("plan_id")
        request = event.get("request_id")
        if not plan and len(request_plans[request]) == 1:
            plan = next(iter(request_plans[request]))
        key = "plan:" + plan if plan else "request:" + (request or event["event_id"])
        grouped[key].append(event)
    return grouped


def aggregate_plan(key, records):
    context, stages, latest, latest_at = {}, [], None, None
    attempted = False
    for event in records:
        context.update(event)
        event_type = event["event_type"]
        if event_type == "evaluated":
            latest, latest_at = evaluation(event), event["occurred_at"]
        if event_type in ("exploration_decided", "exploration_updated"):
            observed = evaluation(event, True)
            # A delayed delivery acknowledgement is not a new evaluation.
            compared = lambda value: {k: v for k, v in (value or {}).items() if k != "reported"}
            if compared(observed) != compared(latest):
                latest_at = event["occurred_at"]
            latest = observed
        phase = event.get("phase")
        if (event_type == "phase_changed" and phase in (
            "save_checkpoint", "replacing_workers", "tune_param", "rendezvous_ready", "resumed"
        )) or event_type in ("checkpoint_ready", "control_completed", "training_recovered"):
            attempted = True
        label = PHASES.get(phase) if event_type == "phase_changed" else STAGES.get(event_type)
        if event_type == "evaluated":
            label = "收益门禁拒绝" if obj(event.get("benefit")).get("decision") == "reject" else "调整前收益评估"
        if event_type == "finished" and event.get("execution_status") == "failed":
            label = "执行失败"
        if label:
            stage = {"name": label, "at": event["occurred_at"]}
            if not stages or stage != stages[-1]:
                stages.append(stage)
    resources = obj(context.get("resources"))
    before, target = obj(resources.get("before")), obj(resources.get("target"))
    actual = obj(resources.get("final")) if any(e["event_type"] == "finished" for e in records) else obj(resources.get("actual_after"))
    action = context.get("action")
    kind = {"add_gpus": "expand", "reduce_gpus": "shrink"}.get(action, "other")
    if finite(before.get("total_gpus")) and finite(target.get("total_gpus")):
        delta = target["total_gpus"] - before["total_gpus"]
        kind = "expand" if delta > 0 else "shrink" if delta < 0 else "other"
    execution = choice(context.get("execution_status"), ("pending", "running", "completed", "failed", "skipped"))
    if execution == "pending" and attempted:
        execution = "running"
    benefit = obj(context.get("benefit"))
    state = str(obj(context.get("exploration")).get("status", "")).lower()
    conclusion = choice(state, ("accepted", "retained", "inconclusive", "failed"))
    if not state and benefit.get("decision") == "reject":
        conclusion = "rejected"
    if execution == "failed" and conclusion == "unknown":
        conclusion = "failed"
    if execution == "unknown" and context.get("request_result") in ("rejected", "reject", "skipped"):
        execution, conclusion = "skipped", "rejected"
    item = dict(
        id=key, plan_id=context.get("plan_id"),
        occurred_at=min(e["occurred_at"] for e in records), type=kind,
        trigger=str(context.get("trigger_source") or "unknown"),
        before_gpus=num(before.get("total_gpus")), target_gpus=num(target.get("total_gpus")),
        actual_gpus=num(actual.get("total_gpus")), execution_status=execution,
        recovery_status=str(context.get("recovery_status") or "unknown"),
        conclusion=conclusion, reason=str(context.get("reason_code") or benefit.get("reason_code") or "unknown"),
        interruption_seconds=None, interruption_start=None, interruption_end=None,
        evaluation=latest, stages=stages, before_finish_at=None, after_finish_at=None,
    )
    # 'failed' alone is insufficient evidence that checkpoint/mutation started.
    return item, attempted, latest_at


class DashboardService:
    def __init__(self, store, prometheus=None, clock=time.time):
        self.store, self.prometheus, self.clock = store, prometheus, clock

    def jobs(self):
        return {"items": [public_job(job) for job in self.store.jobs()]}

    def jct_statistics(self, cluster=None, namespace=None):
        jobs = [job for job in self.store.jobs()
                if (cluster is None or job["cluster"] == cluster)
                and (namespace is None or job["namespace"] == namespace)]
        return dict(statistics(jobs, self.clock()),
                    scope=dict(cluster=cluster, namespace=namespace))

    def dashboard(self, uid):
        job, raw = self.store.read(uid)
        now = self.clock()
        jct = durations(job, now)
        plans = [aggregate_plan(key, records) for key, records in group_events(raw).items()]
        events = sorted((p[0] for p in plans), key=lambda e: (e["occurred_at"], e["id"]), reverse=True)
        evaluated = [p for p in plans if p[2] is not None]
        latest = max(evaluated, key=lambda p: p[2])[0]["evaluation"] if evaluated else None
        missing = ["JCT 不调整基线尚未冻结", "尚未接入连续实际 GPU 与训练吞吐时序",
                   "真实训练中断区间尚未采集", "事件记录为 best-effort，不能保证全生命周期完整"]
        if job["status"] == "unknown":
            missing.append("作业生命周期尚未接入；弹性计划完成不代表训练完成")
        if job["status"] in ("completed", "running") and jct["measurement_status"] == "unavailable":
            missing.append("JCT 时间缺失或无效；需同步平台提交及完成时间")
        if self.prometheus and latest is None:
            try:
                latest = self.prometheus.latest(job)
            except Exception:
                missing.append("Prometheus 收益查询暂不可用")
        if latest is None:
            missing.append("暂无收益评估")
        counts = dict(attempted=None, succeeded=None, failed=None, complete=False)
        # Observed counts are lower bounds; the UI explicitly labels them as such.
        if raw:
            counts.update(
                attempted=sum(p[1] for p in plans),
                succeeded=sum(p[1] and p[0]["execution_status"] == "completed" for p in plans),
                failed=sum(p[1] and p[0]["execution_status"] == "failed" for p in plans),
            )
        return dict(
            schema_version=1, generated_at=now, job=public_job(job),
            jct=dict(**jct, submitted_at=job["submitted_at"], completed_at=job["completed_at"],
                     baseline_seconds=None, baseline_frozen_at=None, baseline_source="unavailable",
                     baseline_description="尚未接入不可变 JCT 基线账本"),
            latest_evaluation=latest, summary=counts,
            series=dict(work_unit=latest["work_unit"] if latest else "unknown", points=[]),
            events=events, missing_data=missing,
        )
