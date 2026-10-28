# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import shlex
from dataclasses import dataclass
from typing import Iterable, Optional

NETWORK_MODE_NONE = "none"
NETWORK_MODE_ALL = "all"
NETWORK_MODE_RANKS = "ranks"


@dataclass
class WorkspaceNetworkConfig:
    node_num: int
    rank_map: Optional[dict] = None


@dataclass
class WorkspaceNetworkPlan:
    ranks: Optional[Iterable[int]]
    config: Optional[WorkspaceNetworkConfig]


class WorkspaceNetworkPlanner:
    def __init__(self, launcher):
        self._launcher = launcher

    def plan(
        self,
        network_mode: str,
        ranks: Optional[Iterable[int]] = None,
        pods=None,
    ):
        if network_mode == NETWORK_MODE_NONE:
            return WorkspaceNetworkPlan(ranks=ranks, config=None)

        network_ranks = self._resolve_network_ranks(network_mode, ranks)
        if pods is None:
            pods = self._launcher.list_worker_pods(ranks=network_ranks)
        if not pods:
            raise RuntimeError("no worker pods found")
        if network_ranks is not None:
            self._validate_selected_ranks(network_ranks, pods)

        rank_map = None
        if network_mode == NETWORK_MODE_RANKS:
            rank_map = {pod.rank: index for index, pod in enumerate(pods)}
        return WorkspaceNetworkPlan(
            ranks=network_ranks,
            config=WorkspaceNetworkConfig(
                node_num=len(pods),
                rank_map=rank_map,
            ),
        )

    def _resolve_network_ranks(
        self, network_mode: str, ranks: Optional[Iterable[int]]
    ):
        if network_mode == NETWORK_MODE_ALL:
            if ranks is not None:
                raise ValueError("--ranks cannot be used with --network-mode all.")
            return None

        if network_mode == NETWORK_MODE_RANKS:
            if not ranks:
                raise ValueError("--ranks is required with --network-mode ranks.")
            return ranks

        raise ValueError(f"unsupported workspace network mode: {network_mode}")

    def _validate_selected_ranks(self, requested_ranks, pods):
        requested = set(requested_ranks)
        found = {pod.rank for pod in pods}
        missing = sorted(requested - found)
        if missing:
            raise RuntimeError(
                "worker pods not found for ranks: "
                + ",".join(str(rank) for rank in missing)
            )


class WorkspaceLaunchCommandBuilder:
    def build(
        self,
        cmd: Optional[str],
        framework_cmd: Optional[str],
        dlrover_run_args: Optional[str],
        network_config: Optional[WorkspaceNetworkConfig] = None,
    ):
        if cmd:
            command = cmd
        else:
            command = self._build_framework_command(
                framework_cmd,
                dlrover_run_args,
                network_config,
            )

        if network_config:
            return f"{self._build_rank_env_prefix(network_config)} {command}"
        return command

    def _build_framework_command(
        self,
        framework_cmd: Optional[str],
        dlrover_run_args: Optional[str],
        network_config: Optional[WorkspaceNetworkConfig],
    ):
        framework_cmd = (framework_cmd or "").strip()
        if not framework_cmd:
            raise ValueError("--framework-cmd cannot be empty.")

        try:
            framework_parts = shlex.split(framework_cmd)
        except ValueError as e:
            raise ValueError(f"Invalid --framework-cmd: {e}") from e

        if network_config is None:
            return framework_cmd
        if framework_parts and framework_parts[0] == "dlrover-run":
            return framework_cmd

        dlrover_run_args = (dlrover_run_args or "").strip()
        if not dlrover_run_args:
            raise ValueError("--dlrover-run-args cannot be empty.")
        return f"dlrover-run {dlrover_run_args} {framework_cmd}"

    def _build_rank_env_prefix(self, network_config: WorkspaceNetworkConfig):
        parts = [
            f"export NODE_NUM={int(network_config.node_num)};",
        ]
        rank_map = network_config.rank_map or {}
        if rank_map:
            cases = []
            for original_rank, workspace_rank in sorted(rank_map.items()):
                cases.append(
                    f"{int(original_rank)}) "
                    f"export NODE_RANK={int(workspace_rank)} ;;"
                )
            cases.append(
                "*) echo \"NODE_RANK ${NODE_RANK:-} is not selected for "
                "workspace networking\"; exit 1 ;;"
            )
            parts.append(
                f"case \"${{NODE_RANK:-}}\" in {' '.join(cases)} esac;"
            )
        return " ".join(parts)
