"""Task classification derived from explicit platform configuration."""


def classify_job(job, resource_monitor=None, manual_scale=None):
    metadata = job.get("metadata") or {}
    kind = job.get("kind") or "ElasticJob"
    annotations = metadata.get("annotations") or {}
    enabled = None
    explicit = annotations.get("dlrover.io/elasticity-enabled")
    if explicit in ("true", "false"):
        enabled = explicit == "true"
    elif kind != "ElasticJob":
        enabled = False
    elif resource_monitor == "on" or manual_scale == "on":
        enabled = True
    else:
        worker = ((job.get("spec") or {}).get("replicaSpecs") or {}).get("worker") or {}
        auto = worker.get("autoScale")
        if isinstance(auto, bool):
            enabled = auto
    return dict(job_type=kind, platform_type="kubernetes",
                platform_task_id=metadata.get("uid"), elasticity_enabled=enabled)
