"""Freeze a server-computed snapshot after the operator reviews it."""
import hashlib
import json

from .jct import statistics
from .store import finite, text


def preview(service, form):
    if not isinstance(form, dict):
        raise ValueError("无效的基线设置")
    cluster = text(form.get("cluster"), "cluster")
    pool = form.get("resource_pool") or ""
    if pool:
        text(pool, "resource_pool")
    name = text(form.get("name"), "name")
    kind = form.get("kind")
    if kind not in ("disabled", "enabled", "history"):
        raise ValueError("请选择基线类型")
    strategy = text(form.get("strategy"), "策略版本") if kind == "enabled" else None
    if kind != "history" and form.get("confirmed") is not True:
        raise ValueError("请确认周期内资源池的弹性场景")
    now = service.clock()
    start, end = form.get("start"), form.get("end")
    if not (finite(start) and finite(end) and 0 <= start < end <= now):
        raise ValueError("请选择已结束的有效基线周期")
    scoped = [j for j in service.store.jobs() if j["cluster"] == cluster
              and (not pool or j.get("resource_pool") == pool)]
    jobs = [j for j in scoped if finite(j.get("submitted_at")) and start <= j["submitted_at"] < end]
    cutoff = form.get("preview_cutoff", now)
    if not finite(cutoff) or not end <= cutoff <= now:
        raise ValueError("统计截止时间无效，请重新预览")
    stats = statistics(jobs, cutoff)
    if not jobs:
        raise ValueError("该周期没有提交任务")
    if kind == "disabled":
        for job in scoped:
            if (finite(job.get("submitted_at")) and job["submitted_at"] >= end
                    or finite(job.get("completed_at")) and job["completed_at"] < start):
                continue
            _, events = service.store.read(job["uid"])
            if any(start <= e["occurred_at"] < end and e["event_type"] == "control_completed"
                   and e.get("execution_status") == "completed" for e in events):
                raise ValueError("该周期有调整完成记录，请选择全池关闭弹性的周期")
    snapshot = dict(cluster=cluster, resource_pool=pool, name=name, kind=kind,
                    strategy=strategy, source="manual", start=start, end=end,
                    metrics={k:stats[k] for k in ("total_jobs","measured_jobs","mean_seconds","p50_seconds","p95_seconds")},
                    version=1)
    snapshot["metrics"].update(gpu_utilization=None, gpu_coverage=None,
                               queue_p50_seconds=None, effective_events=None)
    snapshot["cutoff"] = cutoff
    digest = hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest()
    return dict(snapshot, id=digest)
