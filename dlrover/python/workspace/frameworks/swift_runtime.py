# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

"""Read-only Swift worker capability checks and rollout protocol probes."""

import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import urllib.request

from .swift import options, scalar
from dlrover.python.workspace.role_plan import DEVICE_ENVS


def validate_worker(roles):
    if not roles:
        return {"versions": {}, "models": {}}
    versions = {}
    for package in ("ms-swift", "torch", "vllm"):
        versions[package] = importlib.metadata.version(package)
    # Record versions for diagnostics and cross-worker consistency, but use
    # the installed argument schemas to check compatibility below.
    from dataclasses import fields as dataclass_fields
    from swift.arguments import RLHFArguments, RolloutArguments

    # Inspect the actual inherited dataclass fields, without constructing an
    # argument instance (whose post-init connects services/initializes devices).
    schemas = {
        "trainer": {f.name for f in dataclass_fields(RLHFArguments)},
        "rollout": {f.name for f in dataclass_fields(RolloutArguments)},
    }
    fingerprints = {}
    for role in roles:
        env = role["environment"]
        key = next(k for k in DEVICE_ENVS if k in env)
        if key.startswith("ASCEND"):
            versions["vllm-ascend"] = importlib.metadata.version("vllm-ascend")
        allocated = os.getenv(key)
        if allocated:
            if not set(map(str, role["devices"])) <= set(allocated.split(",")):
                raise ValueError(
                    "role devices exceed container visibility: " + key
                )
        else:
            # Read-only backend enumeration, with no model or CUDA context allocation.
            import torch

            if key.startswith("ASCEND"):
                import torch_npu  # noqa: F401

                versions["vllm-ascend"] = importlib.metadata.version(
                    "vllm-ascend"
                )
                count = torch.npu.device_count()
            else:
                count = torch.cuda.device_count()
            if any(d >= count for d in role["devices"]):
                raise ValueError(
                    "role device index exceeds container device count"
                )
        cwd = role["cwd"] or os.getcwd()
        if not Path(cwd).is_dir():
            raise ValueError("worker cwd does not exist: " + cwd)
        argv = role["command"]
        if role["launch_kind"] == "distributed":
            argv = ["swift", "rlhf", *argv[2:]]
        opts = options(argv)
        unknown = set(opts) - schemas[role["name"]]
        if unknown:
            raise ValueError(
                "unknown Swift options (abbreviations are rejected): "
                + ",".join(sorted(unknown))
            )
        model = scalar(opts, "model")
        model_path = (
            Path(cwd, model)
            if model and not os.path.isabs(model)
            else Path(model or "")
        )
        if not model_path.is_dir():
            raise ValueError(
                "adapted tasks require a pre-staged model directory: "
                + str(model_path)
            )
        config = model_path / "config.json"
        metadata = config.read_bytes()
        for file in sorted(model_path.glob("*.index.json")):
            metadata += file.read_bytes()
        fingerprints[role["name"]] = hashlib.sha256(metadata).hexdigest()
        for key in ("dataset", "external_plugins", "adapters"):
            for value in opts.get(key, []):
                if (
                    value.startswith(("/", "./", "../"))
                    and not Path(cwd, value).exists()
                ):
                    raise ValueError("worker path does not exist: " + value)
    return {"versions": versions, "models": fingerprints}


def probe(endpoint):
    host = endpoint["host"]
    authority = "[%s]" % host if ":" in host else host
    base = endpoint.get("url", "http://%s:%s" % (authority, endpoint["port"]))
    # Disable ambient HTTP proxies for Pod-to-Pod model service traffic.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(base + "/health/", timeout=2) as response:
        health = json.load(response)
    if health.get("status") != "ok":
        raise RuntimeError("rollout health check did not report ok")
    with opener.open(base + "/get_world_size/", timeout=2) as response:
        world = json.load(response)
    if int(world.get("world_size", 0)) < 1:
        raise RuntimeError("rollout has no initialized engine world")
