# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import unittest

from dlrover.python.common.constants import NodeType
from dlrover.python.master.node.workspace_state import (
    WorkspaceTrainingState,
    is_releasing_workspace_node,
    is_workspace_node,
    sync_workspace_node_metadata,
)


class _Resource:
    def __init__(self, cpu=1):
        self.cpu = cpu
        self.kwargs = {}


class _Node:
    def __init__(self, node_type, node_id, config_resource=None):
        self.type = node_type
        self.id = node_id
        self.config_resource = config_resource
        self.is_released = False


def _node(
    node_type=NodeType.WORKER,
    node_id=0,
    run_mode=None,
    exit_policy=None,
):
    resource = _Resource()
    if run_mode:
        resource.kwargs["pod_annotations"] = {
            "dlrover.io/run-mode": run_mode,
        }
    if exit_policy:
        resource.kwargs.setdefault("pod_annotations", {})[
            "dlrover.io/workspace-exit-policy"
        ] = exit_policy
    return _Node(node_type, node_id, config_resource=resource)


class WorkspaceStateTest(unittest.TestCase):
    def test_is_workspace_node_checks_labels_and_annotations(self):
        annotation_node = _node(run_mode="workspace")
        label_node = _node()
        label_node.config_resource.kwargs["pod_labels"] = {
            "dlrover.io/run-mode": "workspace",
        }
        normal_node = _node()

        self.assertTrue(is_workspace_node(annotation_node))
        self.assertTrue(is_workspace_node(label_node))
        self.assertFalse(is_workspace_node(normal_node))

        release_node = _node(exit_policy="release", run_mode="workspace")
        self.assertTrue(is_releasing_workspace_node(release_node))

    def test_sync_workspace_metadata_keeps_configured_resource(self):
        target = _node()
        target.config_resource = _Resource(cpu=8)
        source = _node(run_mode="workspace")
        source.config_resource.cpu = 16

        self.assertTrue(sync_workspace_node_metadata(target, source))
        self.assertTrue(is_workspace_node(target))
        self.assertEqual(target.config_resource.cpu, 8)

        normal_source = _node()
        self.assertFalse(sync_workspace_node_metadata(target, normal_source))
        self.assertTrue(is_workspace_node(target))

    def test_training_state_tracks_active_nodes_by_run_id(self):
        state = WorkspaceTrainingState()

        self.assertTrue(state.start(NodeType.WORKER, 0, "run-0"))
        self.assertTrue(state.start(NodeType.WORKER, 1, "run-0"))
        self.assertTrue(state.active)
        self.assertEqual(state.run_id, "run-0")
        self.assertEqual(state.active_nodes, [0, 1])

        self.assertFalse(state.finish(0, "old-run"))
        self.assertEqual(state.active_nodes, [0, 1])

        self.assertTrue(state.finish(0, "run-0"))
        self.assertTrue(state.active)
        self.assertEqual(state.active_nodes, [1])

        self.assertTrue(state.finish(1, "run-0"))
        self.assertFalse(state.active)
        self.assertEqual(state.run_id, "")

    def test_training_state_skips_only_idle_workspace_workers(self):
        state = WorkspaceTrainingState()
        workspace_worker = _node(NodeType.WORKER, 0, "workspace")
        active_worker = _node(NodeType.WORKER, 1, "workspace")
        normal_worker = _node(NodeType.WORKER, 2)
        workspace_ps = _node(NodeType.PS, 0, "workspace")

        state.start(NodeType.WORKER, active_worker.id, "run-0")

        self.assertTrue(
            state.should_skip_no_heartbeat_relaunch(workspace_worker)
        )
        self.assertFalse(
            state.should_skip_no_heartbeat_relaunch(active_worker)
        )
        self.assertFalse(
            state.should_skip_no_heartbeat_relaunch(normal_worker)
        )
        self.assertFalse(state.should_skip_no_heartbeat_relaunch(workspace_ps))

    def test_pending_early_stop_is_skipped_only_for_idle_workspace(self):
        state = WorkspaceTrainingState()
        workspace_nodes = [
            _node(NodeType.WORKER, 0, "workspace"),
            _node(NodeType.WORKER, 1, "workspace"),
        ]

        self.assertTrue(state.should_skip_pending_early_stop(workspace_nodes))

        state.start(NodeType.WORKER, 0, "run-0")
        self.assertFalse(state.should_skip_pending_early_stop(workspace_nodes))

        state.finish(0, "run-0")
        mixed_nodes = workspace_nodes + [_node(NodeType.WORKER, 2)]
        self.assertFalse(state.should_skip_pending_early_stop(mixed_nodes))

        job_state = WorkspaceTrainingState("workspace")
        unlabeled_nodes = [
            _node(NodeType.WORKER, 0),
            _node(NodeType.WORKER, 1),
        ]
        self.assertTrue(
            job_state.should_skip_pending_early_stop(unlabeled_nodes)
        )
        self.assertTrue(
            job_state.should_skip_no_heartbeat_relaunch(unlabeled_nodes[0])
        )

        release_state = WorkspaceTrainingState("workspace", "release")
        self.assertTrue(release_state.releasing)
        release_nodes = [
            _node(exit_policy="release", run_mode="workspace")
        ]
        self.assertFalse(
            release_state.should_skip_pending_early_stop(release_nodes)
        )
        self.assertFalse(
            release_state.should_skip_pending_early_stop(
                [_node(run_mode="workspace")]
            )
        )
        self.assertFalse(
            release_state.should_skip_no_heartbeat_relaunch(
                _node(run_mode="workspace")
            )
        )
        self.assertFalse(
            release_state.should_skip_no_heartbeat_relaunch(release_nodes[0])
        )


if __name__ == "__main__":
    unittest.main()
