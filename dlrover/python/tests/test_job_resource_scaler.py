import unittest
from unittest.mock import MagicMock

from dlrover.python.common.constants import NodeStatus, NodeType
from dlrover.python.common.node import Node, NodeGroupResource, NodeResource
from dlrover.python.master.node.job_resource_scaler import (
    AllreduceTrainingResourceScaler,
)
from dlrover.python.master.node.job_context import get_job_context
from dlrover.python.master.node.worker import WorkerManager
from dlrover.python.master.resource.job import JobResource


class AllreduceTrainingResourceScalerTest(unittest.TestCase):
    def setUp(self):
        self.job_context = get_job_context()
        self.job_context.clear_job_nodes()

    def tearDown(self):
        self.job_context.clear_job_nodes()

    def test_vertical_elastic_keeps_worker_count_stable(self):
        job_resource = JobResource()
        job_resource.node_group_resources[NodeType.WORKER] = NodeGroupResource(
            10, NodeResource(4, 8192, "nvidia.com/gpu", 1)
        )
        worker_manager = WorkerManager(
            job_resource,
            3,
            lambda node_type, rank: f"{node_type}-{rank}.default.svc:2222",
            lambda node_type, node_id: f"{node_type}-{node_id}",
        )
        scaler = AllreduceTrainingResourceScaler(
            job_resource,
            MagicMock(),
            MagicMock(),
            worker_manager,
            MagicMock(),
        )
        for index in range(3):
            worker = Node(
                NodeType.WORKER,
                index,
                NodeResource(4, 8192, "nvidia.com/gpu", 1),
                name=f"worker-{index}",
                status=NodeStatus.RUNNING,
                rank_index=index,
                service_addr=f"worker-{index}.default.svc:2222",
            )
            self.job_context.update_job_node(worker)

        target = NodeGroupResource(
            10, NodeResource(4, 8192, "nvidia.com/gpu", 2)
        )
        job_resource.node_group_resources[NodeType.WORKER] = target
        plan = scaler.get_worker_plan_with_vertical_elastic(target)

        self.assertEqual(len(plan.remove_nodes), 3)
        self.assertEqual(len(plan.launch_nodes), 3)
        self.assertEqual(
            [node.rank_index for node in plan.launch_nodes],
            [0, 1, 2],
        )
        self.assertEqual(
            [node.name for node in plan.launch_nodes],
            ["worker-3", "worker-4", "worker-5"],
        )
        self.assertTrue(
            all(node.config_resource.gpu_num == 2 for node in plan.launch_nodes)
        )
        self.assertTrue(all(node.is_released for node in plan.remove_nodes))


if __name__ == "__main__":
    unittest.main()
