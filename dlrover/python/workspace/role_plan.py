# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import json
import math
import re
import shlex
import uuid
from dataclasses import asdict, dataclass, field

from dlrover.python.workspace.frameworks import get_adapter
from dlrover.python.workspace.frameworks.script import parse_input
from dlrover.python.workspace.network import (
    WorkspaceLaunchCommandBuilder,
    WorkspaceNetworkConfig,
)

ADAPT_OPTIONS = (
    "framework",
    "deployment_mode",
    "rollout_management",
    "trainer_ranks",
    "rollout_ranks",
    "trainer_devices",
    "rollout_devices",
    "rollout_ready_timeout",
)
DEVICE_ENVS = ("ASCEND_RT_VISIBLE_DEVICES", "CUDA_VISIBLE_DEVICES")
LAUNCH_ENV = (
    "NPROC_PER_NODE",
    "NNODES",
    "NODE_RANK",
    "NODE_NUM",
    "MASTER_ADDR",
    "MASTER_PORT",
    "RANK",
    "LOCAL_RANK",
    "WORLD_SIZE",
    "LOCAL_WORLD_SIZE",
)


def enabled(args):
    return any(getattr(args, name, None) is not None for name in ADAPT_OPTIONS)


def integer_list(value, label):
    if value is None:
        return None
    parts = value.split(",")
    if not parts or any(not re.fullmatch(r"\d+", p.strip()) for p in parts):
        raise ValueError(
            label + " requires comma-separated non-negative integers"
        )
    result = [int(p) for p in parts]
    if len(set(result)) != len(result):
        raise ValueError(label + " contains duplicates")
    return result


@dataclass
class RoleSpec:
    name: str
    rank: int
    pod_uid: str
    command: list
    environment: dict
    cwd: str
    devices: list
    nproc: int
    node_rank: int
    launch_kind: str
    source: str
    shell: str = ""
    endpoint: dict = field(default_factory=dict)


@dataclass
class RunPlan:
    run_id: str
    framework: str
    adapter_version: int
    deployment: str
    management: str
    network_mode: str
    pods: list
    roles: list
    ready_timeout: float
    sources: dict
    pending_checks: list

    def to_dict(self):
        return asdict(self)

    def describe(self):
        # Commands may contain credentials in arguments, URLs or environments.
        data = self.to_dict()
        for role in data["roles"]:
            role.pop("shell", None)
            role["environment"] = {
                k: (
                    "<redacted>"
                    if re.search(
                        r"token|secret|password|key|credential", k, re.I
                    )
                    else v
                )
                for k, v in role["environment"].items()
            }
            argv, hidden = [], False
            for arg in role["command"]:
                if arg.startswith("--"):
                    hidden = bool(
                        re.search(
                            r"token|secret|password|api.key|credential",
                            arg,
                            re.I,
                        )
                    )
                    argv.append(
                        arg.split("=", 1)[0] + "=<redacted>"
                        if hidden and "=" in arg
                        else arg
                    )
                else:
                    argv.append(
                        "<redacted>"
                        if hidden
                        or re.search(r"://[^/]*@|[?&](token|key)=", arg, re.I)
                        else arg
                    )
            role["command"] = argv
        return json.dumps(data, indent=2, ensure_ascii=False)


def build_plan(args, pods, explicit_run_args=False):
    if args.cmd or args.foreground:
        raise ValueError(
            "framework adaptation requires --framework-cmd and does not support --foreground"
        )
    if not pods:
        raise ValueError("no worker Pods selected")
    pods = sorted(pods, key=lambda p: p.rank)
    if any(p.run_mode != "workspace" or not p.uid for p in pods):
        raise ValueError(
            "adaptation requires workspace Pods with Kubernetes UIDs"
        )
    if any(p.exit_policy == "release" for p in pods):
        raise ValueError(
            "adapted release is not enabled until the task cleanup barrier is hardware-validated"
        )
    selected = {p.rank for p in pods}
    if len(selected) != len(pods):
        raise ValueError("duplicate worker ranks in Pod snapshot")
    if args.network_mode == "none" and len(pods) != 1:
        raise ValueError("adapted network-mode none requires exactly one Pod")
    commands = parse_input(args.framework_cmd)
    adapter = get_adapter(args.framework or "auto", commands)
    templates, mode, management = adapter.parse(
        commands, args.deployment_mode or "auto", args.rollout_management
    )
    trainer = integer_list(args.trainer_ranks, "trainer-ranks")
    rollout = integer_list(args.rollout_ranks, "rollout-ranks")
    if management == "managed":
        if (trainer is None) != (rollout is None):
            raise ValueError("specify both trainer-ranks and rollout-ranks")
        trainer, rollout = set(trainer or selected), set(rollout or selected)
        if trainer | rollout != selected or not trainer or not rollout:
            raise ValueError(
                "trainer/rollout ranks must cover exactly the selected Pods"
            )
    else:
        if rollout is not None or args.rollout_devices is not None:
            raise ValueError("rollout resources require managed deployment")
        if trainer is not None and set(trainer) != selected:
            raise ValueError("trainer-ranks must equal selected Pods")
        trainer, rollout = selected, set()
    timeout = (
        args.rollout_ready_timeout
        if args.rollout_ready_timeout is not None
        else 600
    )
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("rollout-ready-timeout must be finite and positive")
    layouts, sources = {}, {}
    for name, template in templates.items():
        device_keys = [k for k in DEVICE_ENVS if k in template.environment]
        if len(device_keys) != 1:
            raise ValueError(
                "%s: specify exactly one backend visibility variable in the script"
                % name
            )
        key = device_keys[0]
        override = getattr(args, name + "_devices")
        devices = integer_list(
            override if override is not None else template.environment[key],
            name + "-devices",
        )
        template.environment[key] = ",".join(map(str, devices))
        layouts[name] = (key, devices)
        sources[name + "_devices"] = (
            "CLI" if override is not None else "script"
        )
    if rollout & trainer:
        tk, td = layouts["trainer"]
        rk, rd = layouts["rollout"]
        if tk != rk or set(td) & set(rd):
            raise ValueError(
                "roles on the same Pod require the same backend and disjoint devices"
            )
    run_args = shlex.split(args.dlrover_run_args) if explicit_run_args else []
    # Accept launcher options as data; never evaluate user shell expansions.
    normalized = []
    i = 0
    values = {}
    while i < len(run_args):
        token = run_args[i]
        if not token.startswith("--"):
            raise ValueError(
                "dlrover-run-args must contain launcher options only"
            )
        key, sep, value = token[2:].partition("=")
        key = key.replace("-", "_")
        if (
            not sep
            and i + 1 < len(run_args)
            and not run_args[i + 1].startswith("--")
        ):
            i += 1
            value = run_args[i]
        if key in values:
            raise ValueError("duplicate launcher argument: " + key)
        values[key] = value
        if key in ("nnodes", "nproc_per_node", "max_restarts"):
            pass
        elif key in (
            "node_rank",
            "rdzv_id",
            "rdzv_endpoint",
            "rdzv_backend",
            "standalone",
            "module",
            "no_python",
            "run_path",
            "auto_config",
            "auto_tunning",
            "node_unit",
        ):
            raise ValueError("adapter owns launcher option: " + key)
        else:
            normalized.append("--" + key + ("=" + value if value else ""))
        i += 1
    if values.get("nnodes", str(len(trainer))) not in (
        str(len(trainer)),
        "$NODE_NUM",
        "${NODE_NUM}",
    ):
        raise ValueError("explicit nnodes conflicts with trainer layout")
    if values.get("max_restarts", "0") != "0":
        raise ValueError("adapted tasks require max_restarts=0")
    template = templates["trainer"]
    if "NNODES" in template.environment and template.environment[
        "NNODES"
    ] != str(len(trainer)):
        raise ValueError("script NNODES conflicts with trainer layout")
    devices = layouts["trainer"][1]
    if "nproc_per_node" in values:
        nproc, source = int(values["nproc_per_node"]), "CLI"
    elif args.trainer_devices is not None:
        nproc, source = len(devices), "trainer-devices"
    else:
        nproc, source = (
            int(template.environment.get("NPROC_PER_NODE", len(devices))),
            "script/devices",
        )
    if not 0 < nproc <= len(devices):
        raise ValueError(
            "trainer nproc must be positive and no larger than visible devices"
        )
    sources["nproc"] = {
        "source": source,
        "value": nproc,
        "script_default": template.environment.get("NPROC_PER_NODE"),
        "local_rank_devices": devices[:nproc],
    }
    from dlrover.python.common.constants import Accelerators

    accelerator = (
        Accelerators.ASCEND_NPU
        if layouts["trainer"][0].startswith("ASCEND")
        else Accelerators.NVIDIA_GPU
    )
    if "accelerator" in values and values["accelerator"] != accelerator:
        raise ValueError(
            "launcher accelerator conflicts with role device backend"
        )
    if "accelerator" not in values:
        normalized.append("--accelerator=" + accelerator)
    adapter.validate_resources(
        templates, mode, nproc, layouts.get("rollout", (None, []))[1]
    )
    normalized.extend(
        [
            "--nnodes=" + str(len(trainer)),
            "--nproc_per_node=" + str(nproc),
            "--max_restarts=0",
        ]
    )
    rank_map = {r: i for i, r in enumerate(sorted(trainer))}
    endpoints = []
    if rollout:
        for pod in pods:
            if pod.rank in rollout:
                if not pod.pod_ip or pod.pod_ip in (
                    "127.0.0.1",
                    "0.0.0.0",
                    "::1",
                    "::",
                ):
                    raise ValueError(
                        "managed rollout requires reachable Pod IPs"
                    )
                endpoints.append(
                    adapter.service_endpoint(templates["rollout"], pod.pod_ip)
                )
        templates["trainer"] = adapter.bind_endpoints(template, endpoints)
    roles = []
    for name, ranks in (("rollout", rollout), ("trainer", trainer)):
        for pod in pods:
            if pod.rank not in ranks:
                continue
            template = templates[name]
            env = {
                k: v
                for k, v in template.environment.items()
                if k not in LAUNCH_ENV
            }
            if any(
                k.startswith("DLROVER_WORKSPACE_")
                or k.startswith("TORCHELASTIC_")
                for k in env
            ):
                raise ValueError(
                    "script cannot override workspace execution identity"
                )
            distributed = name == "trainer" and args.network_mode != "none"
            if name == "trainer" and not distributed:
                env["NPROC_PER_NODE"] = str(nproc)
            command = (
                adapter.worker_entrypoint(template.argv)
                if distributed
                else template.argv
            )
            shell = ""
            if distributed:
                shell = WorkspaceLaunchCommandBuilder().build(
                    None,
                    shlex.join(command),
                    shlex.join(normalized),
                    WorkspaceNetworkConfig(len(trainer), rank_map),
                )
            endpoint = (
                endpoints[sorted(rollout).index(pod.rank)]
                if name == "rollout"
                else {}
            )
            roles.append(
                RoleSpec(
                    name,
                    pod.rank,
                    pod.uid,
                    command,
                    env,
                    template.cwd,
                    layouts[name][1],
                    nproc if name == "trainer" else 1,
                    rank_map.get(pod.rank, -1) if name == "trainer" else -1,
                    (
                        "distributed"
                        if distributed
                        else ("service" if name == "rollout" else "local")
                    ),
                    "%s:%s" % (template.source, template.line),
                    shell,
                    endpoint,
                )
            )
    return RunPlan(
        uuid.uuid4().hex,
        adapter.name,
        adapter.version,
        mode,
        management,
        args.network_mode,
        [asdict(p) for p in pods],
        roles,
        timeout,
        sources,
        adapter.pending_checks(management),
    )
