# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

from dlrover.python.common.constants import NodeType

_WORKSPACE_RUN_MODES = {"workspace"}
_RUN_MODE_KEY = "dlrover.io/run-mode"
_EXIT_POLICY_KEY = "dlrover.io/workspace-exit-policy"
EXIT_POLICY_RETAIN = "retain"
EXIT_POLICY_RELEASE = "release"


def _get_workspace_run_mode(node) -> str:
    if not getattr(node, "config_resource", None):
        return ""
    labels = node.config_resource.kwargs.get("pod_labels", {}) or {}
    annotations = node.config_resource.kwargs.get("pod_annotations", {}) or {}
    run_mode = labels.get(_RUN_MODE_KEY) or annotations.get(_RUN_MODE_KEY, "")
    return str(run_mode).lower()


def is_workspace_node(node) -> bool:
    return _get_workspace_run_mode(node) in _WORKSPACE_RUN_MODES


def get_workspace_exit_policy(node) -> str:
    if not getattr(node, "config_resource", None):
        return EXIT_POLICY_RETAIN
    labels = node.config_resource.kwargs.get("pod_labels", {}) or {}
    annotations = node.config_resource.kwargs.get("pod_annotations", {}) or {}
    value = labels.get(_EXIT_POLICY_KEY) or annotations.get(
        _EXIT_POLICY_KEY, EXIT_POLICY_RETAIN
    )
    return str(value).lower()


def is_releasing_workspace_node(node) -> bool:
    return is_workspace_node(node) and (
        get_workspace_exit_policy(node) == EXIT_POLICY_RELEASE
    )


def sync_workspace_node_metadata(target_node, source_node) -> bool:
    """Sync the workspace marker from an observed Pod node.

    The configured resource on ``target_node`` is deliberately retained.  It
    is used by resource optimization and relaunch, while the source resource
    describes the live Pod and carries its Kubernetes metadata.
    """
    run_mode = _get_workspace_run_mode(source_node)
    if run_mode not in _WORKSPACE_RUN_MODES:
        return False
    if not getattr(target_node, "config_resource", None):
        return False
    labels = target_node.config_resource.kwargs.setdefault("pod_labels", {})
    labels[_RUN_MODE_KEY] = run_mode
    exit_policy = get_workspace_exit_policy(source_node)
    labels[_EXIT_POLICY_KEY] = exit_policy
    return True


class WorkspaceTrainingState:
    def __init__(self, run_mode: str = "", exit_policy: str = "retain"):
        self._workspace_job = str(run_mode).lower() in _WORKSPACE_RUN_MODES
        self._persistent_job = (
            self._workspace_job
            and str(exit_policy).lower() != EXIT_POLICY_RELEASE
        )
        self._releasing_job = (
            self._workspace_job
            and str(exit_policy).lower() == EXIT_POLICY_RELEASE
        )
        self._active = False
        self._run_id = ""
        self._nodes = set()

    @property
    def active(self):
        return self._active

    @property
    def run_id(self):
        return self._run_id

    @property
    def active_nodes(self):
        return sorted(self._nodes)

    @property
    def releasing(self):
        return self._releasing_job

    def start(self, node_type: str, node_id: int, run_id: str = ""):
        if node_type != NodeType.WORKER:
            return False
        if run_id and self._run_id and run_id != self._run_id:
            self._nodes.clear()
        self._active = True
        self._run_id = run_id
        self._nodes.add(node_id)
        return True

    def finish(self, node_id: int, run_id: str = ""):
        if run_id and self._run_id and run_id != self._run_id:
            return False
        self._nodes.discard(node_id)
        if not self._nodes:
            self._active = False
            self._run_id = ""
        return True

    def should_skip_no_heartbeat_relaunch(self, node) -> bool:
        if self._releasing_job:
            return False
        return (
            node.type == NodeType.WORKER
            and (self._persistent_job or is_workspace_node(node))
            and not is_releasing_workspace_node(node)
            and node.id not in self._nodes
        )

    def should_skip_pending_early_stop(self, nodes) -> bool:
        """Return whether all live workers belong to an idle workspace."""
        if self._active or self._releasing_job:
            return False
        live_nodes = [node for node in nodes if not node.is_released]
        if not live_nodes:
            return False
        if self._persistent_job:
            return all(node.type == NodeType.WORKER for node in live_nodes)
        return all(
            is_workspace_node(node) and not is_releasing_workspace_node(node)
            for node in live_nodes
        )
