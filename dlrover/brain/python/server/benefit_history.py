# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
"""Optional Brain evidence endpoint backed by the training database."""

import hashlib
import json
import math
import os
import secrets
import time

import requests
from fastapi import APIRouter, Header, HTTPException, Response
from sqlalchemy import JSON, Column, DateTime, Float, Index, String, func, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from dlrover.brain.python.common.db import Base, get_db_session
from dlrover.python.common.log import default_logger as logger

router = APIRouter()


class ExplorationHistory(Base):
    __tablename__ = "elastic_exploration_history"

    __table_args__ = (
        Index("history_lookup", "cluster", "reuse_key", "observed_at"),
    )

    cluster = Column(String(128), primary_key=True)
    job_uid = Column(String(128), primary_key=True)
    exploration_id = Column(String(128), primary_key=True)
    status = Column(String(32), nullable=False)
    current_resource = Column(JSON, nullable=False)
    target_resource = Column(JSON, nullable=False)
    report = Column(JSON, nullable=False)
    baseline_throughput = Column(Float)
    candidate_throughput = Column(Float)
    observed_at = Column(Float)
    reuse_key = Column(String(64))
    created_at = Column(DateTime, nullable=False, server_default=func.now())


def reuse_key(request, context, training):
    """Match the same recipe and scaling transition within one namespace."""
    namespace = request["job"].get("namespace")
    if not namespace or not isinstance(context, dict):
        return None
    if not all(context.get(k) for k in ("template_id", "config_revision", "phase")):
        return None
    fields = [namespace, request["current"], request["target"], context,
              training.get("unit"), training.get("global_batch_size"),
              training.get("global_batch_size_fixed")]
    return hashlib.sha256(json.dumps(fields, sort_keys=True).encode()).hexdigest()


def record(request):
    trial = request["exploration"]
    status = trial["status"]
    if trial.get("policy") != "observe_only" or status not in (
        "ACCEPTED", "RETAINED", "INCONCLUSIVE", "FAILED"
    ):
        raise ValueError("Expected a terminal observation")
    job = request["job"]
    result = trial.get("result") or {}
    training = trial.get("baseline_training") or {}
    q1 = result.get("candidate_throughput")
    baseline = trial.get("baseline_throughput", result.get("baseline_throughput"))
    observed = trial.get("candidate_observed_at")
    key = None
    if (status in ("ACCEPTED", "RETAINED")
            and type(q1) in (int, float) and math.isfinite(q1) and q1 > 0
            and type(baseline) in (int, float) and math.isfinite(baseline) and baseline > 0
            and trial.get("baseline_context") == trial.get("history_context")
            and observed and result.get("windows", 0) >= 2
            and result.get("data_source") == "http" and training.get("data_source") != "mock"
            and training.get("comparable") is True and result.get("unit") in ("steps", "tokens", "samples")
            and training.get("unit") == result["unit"]
            and result.get("candidate_resource") == request["target"]):
        key = reuse_key(request, trial.get("history_context"), training)
    row = ExplorationHistory(
        cluster=job["cluster"], job_uid=job["job_uid"], exploration_id=trial["id"],
        status=status, current_resource=request["current"], target_resource=request["target"],
        report=trial, baseline_throughput=baseline,
        candidate_throughput=q1, observed_at=observed, reuse_key=key,
    )
    with get_db_session() as db:
        db.add(row)
        try:
            db.commit()
        except IntegrityError:
            db.rollback()
            existing = db.get(ExplorationHistory, (job["cluster"], job["job_uid"], trial["id"]))
            if existing is None:
                raise
            if (existing.report != trial or existing.target_resource != request["target"]
                    or existing.current_resource != request["current"]):
                logger.warning("Conflicting exploration report job=%s id=%s", job["job_uid"], trial["id"])
                raise HTTPException(409, "Conflicting exploration result")
    logger.info("Exploration history recorded job=%s id=%s status=%s reusable=%s",
                job["job_uid"], trial["id"], status, bool(key))
    return dict(request_id=request["request_id"], exploration_id=trial["id"], recorded=True)


def fill_candidate(request, snapshot):
    if request.get("exploration") or snapshot.get("candidate") != {"status": "unavailable"}:
        return
    training = request.get("training") or snapshot.get("training", {})
    if snapshot.get("training", {}).get("comparable") is not True or training.get("data_source") == "mock":
        return
    job = request["job"]
    key = reuse_key(request, snapshot.get("history_context"), training)
    if key is None:
        return
    start, end = training.get("start", {}), training.get("end", {})
    try:
        work = end["work"] - start["work"]
        seconds = end["observed_at"] - start["observed_at"]
    except (KeyError, TypeError):
        return
    if not (work > 0 and seconds > 0):
        return
    q0 = work / seconds
    now = time.time()
    max_age = float(os.getenv("DLROVER_BENEFIT_HISTORY_MAX_AGE_SECONDS", "86400"))
    # Use the lowest observed multiplier; the selected trial explains the estimate.
    ratio = ExplorationHistory.candidate_throughput / ExplorationHistory.baseline_throughput
    query = select(ExplorationHistory).where(
        ExplorationHistory.cluster == job["cluster"],
        ExplorationHistory.reuse_key == key,
        ExplorationHistory.observed_at.between(now - max_age, now),
        ExplorationHistory.baseline_throughput > 0,
    ).order_by(ratio, ExplorationHistory.observed_at.desc(),
               ExplorationHistory.job_uid, ExplorationHistory.exploration_id).limit(1)

    with get_db_session() as db:
        row = db.execute(query).scalar_one_or_none()
        if row is None:
            logger.debug("No matching exploration history job=%s key=%s", job["job_uid"], key)
            return
        multiplier = row.candidate_throughput / row.baseline_throughput
        q1 = q0 * multiplier
        if not math.isfinite(q1) or q1 <= 0:
            return
        snapshot["candidate"] = dict(
            throughput_lower=q1, evidence="history", evidence_id=row.exploration_id,
            source_job_uid=row.job_uid, source_cluster=row.cluster,
        )
        logger.info("Reused exploration ratio job=%s source_job=%s trial=%s q0=%s ratio=%s q1=%s",
                    job["job_uid"], row.job_uid, row.exploration_id, q0, multiplier, q1)


@router.post("/benefit/snapshot")
def snapshot(request: dict, authorization: str = Header(default="")):
    token = os.getenv("DLROVER_BENEFIT_HTTP_TOKEN")
    if token and not secrets.compare_digest(authorization, "Bearer " + token):
        raise HTTPException(401, "Invalid benefit token")
    try:
        if request.get("exploration", {}).get("report_result") is True:
            return record(request)
        upstream = requests.post(
            os.environ["DLROVER_BENEFIT_UPSTREAM_URL"], json=request,
            headers={"Authorization": authorization} if authorization else {},
            timeout=float(os.getenv("DLROVER_BENEFIT_TIMEOUT_SECONDS", "3")), allow_redirects=False,
        )
        if upstream.status_code != 200:
            return Response(status_code=upstream.status_code)
        data = upstream.json()
        fill_candidate(request, data)
        return data
    except SQLAlchemyError as exc:
        logger.error("Exploration history DB operation failed job=%s request=%s error=%s",
                     request.get("job"), request.get("request_id"), type(exc).__name__)
        raise HTTPException(503, "Exploration history database unavailable") from exc
    except requests.RequestException as exc:
        logger.warning("Benefit upstream failed request=%s error=%s", request.get("request_id"), type(exc).__name__)
        raise HTTPException(502, "Benefit upstream unavailable") from exc
    except (KeyError, ValueError, TypeError) as exc:
        raise HTTPException(422, "Invalid benefit evidence payload") from exc
