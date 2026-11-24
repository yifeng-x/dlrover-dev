"""Task classification from the Master configuration supplied at submission."""

import shlex

from dlrover.python.common.log import default_logger as logger

MASTER_JOB_TYPE = "ElasticJob"


def master_elasticity_enabled(dimension, manual_scale):
    return dimension in ("auto", "horizontal", "vertical") or manual_scale == "on"


def _template_enabled(job):
    replicas = (job.get("spec") or {}).get("replicaSpecs") or {}
    master = replicas.get("dlrover-master") or {}
    containers = ((master.get("template") or {}).get("spec") or {}).get("containers") or []
    container = next((c for c in containers if c.get("name") == "main"), None)
    if container is None and len(containers) == 1:
        container = containers[0]
    if not container:
        return None
    tokens = list(container.get("command") or []) + list(container.get("args") or [])
    if not tokens:
        return None
    if len(tokens) >= 3 and tokens[0].split("/")[-1] in ("sh", "bash") and "c" in tokens[1]:
        try:
            tokens = shlex.split(tokens[2], comments=True)
        except ValueError:
            return None
        if "dlrover.python.master.main" not in tokens:
            return None
        tokens = tokens[tokens.index("dlrover.python.master.main") + 1:]
    options = {}
    for index, token in enumerate(tokens):
        if token in ("&&", ";", "||", "|"):
            break
        name, sep, value = token.partition("=")
        if name in ("--elastic_dimension", "--configmap_manual_scale_switch"):
            if not sep:
                value = tokens[index + 1] if index + 1 < len(tokens) else ""
            options[name] = value.lower()
    dimension = options.get("--elastic_dimension")
    manual = options.get("--configmap_manual_scale_switch")
    if master_elasticity_enabled(dimension, manual):
        return True
    if dimension is not None or manual not in (None, "off"):
        return None
    return False


def classify_job(job, *, master_enabled=None):
    metadata = job.get("metadata") or {}
    kind = job.get("kind") or MASTER_JOB_TYPE
    enabled = False
    if kind == MASTER_JOB_TYPE:
        enabled = master_enabled if master_enabled is not None else _template_enabled(job)
    source = "job_kind" if kind != MASTER_JOB_TYPE else (
        "master_args" if master_enabled is not None else "master_template"
    )
    log = logger.warning if enabled is None else logger.debug
    log("Job classification job_uid=%s job_type=%s source=%s elasticity_enabled=%s",
        metadata.get("uid"), kind, source, enabled)
    return dict(job_type=kind, platform_type="kubernetes",
                platform_task_id=metadata.get("uid"), elasticity_enabled=enabled)
