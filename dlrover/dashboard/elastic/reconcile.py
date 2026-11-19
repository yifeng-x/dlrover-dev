"""Reconcile a complete Kubernetes resource list with the dashboard ledger.

Run periodically using a read-only Kubernetes identity. API failures never
change job states. Cluster and namespace scope must match the stored identity.
"""
import argparse
import json
import subprocess
import time
from datetime import datetime

from .store import create_store
from dlrover.python.common.job_classification import classify_job


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() if value else None


def reconcile(store, items, cluster, namespace, job_type, now=None):
    now = time.time() if now is None else now
    seen = set()
    for item in items:
        meta = item["metadata"]
        uid = meta["uid"]
        seen.add(uid)
        status = item.get("status", {})
        conditions = status.get("conditions", [])
        completed = next((c for c in conditions if c.get("type") in ("Complete", "Succeeded")
                          and str(c.get("status")).lower() == "true"), None)
        failed = any(c.get("type") == "Failed" and str(c.get("status")).lower() == "true"
                     for c in conditions)
        phase = str(status.get("phase", "")).lower()
        state = ("completed" if completed or phase in ("succeeded", "completed") else
                 "failed" if failed or phase == "failed" else
                 "cancelled" if meta.get("deletionTimestamp") else "running")
        annotations = meta.get("annotations", {})
        flag = annotations.get("dlrover.io/elasticity-enabled")
        # Explicit configuration only: no inference from job name or event count.
        enabled = flag == "true" if flag in ("true", "false") else None
        if job_type != "ElasticJob":
            enabled = False
        classification = classify_job(dict(item, kind=job_type))
        enabled = classification["elasticity_enabled"]
        store.register(dict(job_uid=uid, job_name=meta["name"], cluster=cluster,
                            namespace=namespace, job_type=job_type, platform_type="kubernetes",
                            platform_task_id=uid, elasticity_enabled=enabled, status=state,
                            submitted_at=timestamp(meta.get("creationTimestamp")),
                            completed_at=timestamp(status.get("completionTime") or
                                (completed or {}).get("lastTransitionTime")) if state == "completed" else None,
                            lifecycle_observed_at=now))
    for job in store.jobs():
        if (job["cluster"] == cluster and job["namespace"] == namespace
                and job.get("job_type") == job_type and job["status"] == "running"
                and job["uid"] not in seen):
            store.register(dict(job_uid=job["uid"], job_name=job["name"],
                                cluster=cluster, namespace=namespace, status="cancelled",
                                submitted_at=job["submitted_at"], completed_at=None,
                                lifecycle_observed_at=now))


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--context", required=True)
    args = parser.parse_args()
    # Fetch both lists successfully before any writes.
    batches = []
    for resource, kind in (("elasticjobs", "ElasticJob"), ("jobs.batch", "Job")):
        result = subprocess.run(["kubectl", "--context", args.context, "-n", args.namespace,
                                 "get", resource, "-o", "json"], check=True,
                                capture_output=True, text=True)
        data = json.loads(result.stdout)
        if not isinstance(data.get("items"), list) or data.get("metadata", {}).get("continue"):
            raise ValueError("A complete Kubernetes list is required")
        batches.append((kind, data["items"]))
    store = create_store()
    for kind, items in batches:
        reconcile(store, items, args.cluster, args.namespace, kind)


if __name__ == "__main__":
    main()
