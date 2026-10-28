# Copyright 2022 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import threading
import time
from abc import ABC, abstractmethod
from typing import Dict, Optional, Sequence, Tuple, Union

import requests
from dlrover.brain.python.common.job import JobMeta

from dlrover.python.common import comm, env_utils
from dlrover.python.common.comm import BaseRequest, BaseResponse
from dlrover.python.common.constants import (
    CommunicationType,
    JobConstant,
    NodeEnv,
    CommunicationReqMeta,
)
from dlrover.python.common.log import default_logger as logger
from dlrover.python.common.singleton import Singleton
from dlrover.python.util.function_util import retry


class MasterClient(Singleton, ABC):
    """MasterClient provides some APIs connect with the master
    service via gRPC/http/ray call.

    Args:
        master_addr: the master address
        node_id (int), the unique and ordered node ID assigned
        by dlrover command-line.
        node_type: the job type of node contains "worker", "ps"
            "evaluator" and "chief".
        timeout (int): the timeout second of requests.
    """

    _instance_lock = threading.Lock()

    def __init__(self, master_addr, job_name, job_id, node_id, node_type, timeout=5):
        logger.info(
            f"Build master client with master_addr: {master_addr}, "
            f"job_name: {job_name}, job_id: {job_id}, node_id: {node_id}, node_type: {node_type}."
        )
        self._timeout = timeout
        self._master_addr = master_addr
        self._job_name = job_name
        self._job_id = job_id
        self._node_id = node_id
        self._node_type = node_type
        self._node_ip = os.getenv("NODE_IP", "")

    @retry()
    @abstractmethod
    def _report(self, message: comm.Message):
        """Abstraction of report function."""
        pass

    @retry()
    @abstractmethod
    def _get(self, message: comm.Message):
        """Abstraction of get function."""
        pass

    def trigger_elastic(self, elastic_dimension = "", scale_action = "", job_meta = JobMeta):
        message = comm.TriggerElastic(
            elastic_dimension=elastic_dimension, 
            scale_action=scale_action,
            job_name=job_meta.name,
            job_uuid=job_meta.uuid,
            namespace=job_meta.namespace,
            cluster=job_meta.cluster,
        )
        self._report(message)

    # @classmethod
    # def singleton_instance(cls, *args, **kwargs):
    #     if not cls._instance:
    #         with cls._instance_lock:
    #             if not cls._instance:
    #                 cls._instance = build_master_client(*args, **kwargs)
    #     return cls._instance


try:
    from dlrover.proto import elastic_training_pb2, elastic_training_pb2_grpc

    class GrpcMasterClient(MasterClient):
        """
        Examples::
        channel = elasticai_api.util.grpc_utils.build_channel(
            "localhost:50001"
        )
        mc = MasterClient(channel, work_id=0)
        # get task unit from master service
        mc.get_task(...)
        """

        def __init__(self, master_addr, job_name, job_id, node_id, node_type, timeout=5):
            super(GrpcMasterClient, self).__init__(
                master_addr, job_name, job_id, node_id, node_type, timeout
            )
            self._open_grpc_channel()

        def __del__(self):
            self._close_grpc_channel()

        def _close_grpc_channel(self):
            if self._channel:
                self._channel.close()

        def _open_grpc_channel(self):
            self._channel = comm.build_grpc_channel(self._master_addr)
            self._stub = elastic_training_pb2_grpc.MasterStub(self._channel)

        def _gen_request_meta(self) -> Sequence[Tuple[str, Union[str, bytes]]]:
            return [
                (
                    CommunicationReqMeta.COMM_META_JOB_UID,
                    env_utils.get_job_uid(),
                )
            ]

        @retry()
        def _report(self, message: comm.Message):
            request = elastic_training_pb2.Message()
            request.node_id = self._node_id
            request.node_type = self._node_type
            request.data = message.serialize()
            return self._stub.report(
                request,
                timeout=self._timeout,
                metadata=self._gen_request_meta(),
            )

        @retry()
        def _get(self, message: comm.Message):
            request = elastic_training_pb2.Message()
            request.node_id = self._node_id
            request.node_type = self._node_type
            request.data = message.serialize()
            response = self._stub.get(
                request,
                timeout=self._timeout,
                metadata=self._gen_request_meta(),
            )
            res_message = comm.deserialize_message(response.data)
            return res_message

except ImportError:
    logger.warning("Protobuf is not installed.")


class HttpMasterClient(MasterClient):
    def __init__(self, master_addr, job_name, job_id, node_id, node_type, timeout=5):
        super(HttpMasterClient, self).__init__(
            master_addr, job_name, job_id, node_id, node_type, timeout
        )

    def _get_http_request_url(self, path: str) -> str:
        return "http://" + self._master_addr + path

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            CommunicationReqMeta.COMM_META_JOB_UID: env_utils.get_job_uid(),
        }

    @retry()
    def _report(self, message: comm.Message):
        with requests.post(
            self._get_http_request_url("/report"),
            json=self._gen_request(message).to_json(),
            headers=self._get_headers(),
        ) as response:
            if response.status_code != 200:
                error_msg = f"Failed to report master with http request: {type(message)}."
                raise RuntimeError(error_msg)
            response_data: BaseResponse = comm.deserialize_message(
                response.content
            )
            return response_data

    @retry()
    def _get(self, message: comm.Message):
        with requests.post(
            self._get_http_request_url("/get"),
            json=self._gen_request(message).to_json(),
            headers=self._get_headers(),
        ) as response:
            if response.status_code != 200:
                error_msg = f"Failed to get from master with http request: {type(message)}."
                raise RuntimeError(error_msg)
            response_data: BaseResponse = comm.deserialize_message(
                response.content
            )
            return comm.deserialize_message(response_data.data)

    def _gen_request(self, message: comm.Message):
        request = BaseRequest()
        request.node_id = self._node_id
        request.node_type = self._node_type
        request.data = message.serialize()

        return request


try:
    import ray
    from ray.util.state import get_actor

    class RayMasterClient(MasterClient):
        def __init__(self, master_addr, job_name, job_id, node_id, node_type, timeout=5):
            super(RayMasterClient, self).__init__(
                master_addr, job_name, job_id, node_id, node_type, timeout
            )
            self._master_actor_handle = None

        def _get_master_actor_handle(self):
            if not self._master_actor_handle:
                self._master_actor_handle = ray.get_actor(
                    get_actor(self._master_addr).name
                )
            return self._master_actor_handle

        @retry()
        def _report(self, message: comm.Message):
            response = ray.get(
                self._get_master_actor_handle().agent_report.remote(
                    self._gen_request(message).to_json()
                ),
                timeout=self._timeout,
            )

            return response

        @retry()
        def _get(self, message: comm.Message):
            response = ray.get(
                self._get_master_actor_handle().agent_get.remote(
                    self._gen_request(message).to_json()
                ),
                timeout=self._timeout,
            )

            return comm.deserialize_message(response.data)

        def _gen_request(self, message: comm.Message):
            request = BaseRequest()
            request.node_id = self._node_id
            request.node_type = self._node_type
            request.data = message.serialize()

            return request

        def get_elastic_run_config(self) -> Dict[str, str]:
            # no need to get config from master
            return {}

except (ImportError, TypeError):
    logger.warning("Ray is not installed or deps not satisfied.")


def build_master_client(
    master_addr=None, job_name = "", job_id = "", timeout=JobConstant.MASTER_CLIENT_DEFAULT_TIMEOUT
):
    """
    Build a master client.

    Args:
        master_addr (Union[str, ActorHandle]): the address of the job master,
            the format is "{IP}:{PORT}" if str type
        timeout (int): the timeout second of grpc requests.
    """
    if master_addr is None:
        master_addr = os.getenv(NodeEnv.DLROVER_MASTER_ADDR, "")
    node_id = env_utils.get_node_id()
    node_type = "Brain"

    try:
        _timeout = int(os.getenv(NodeEnv.MASTER_CLIENT_TIMEOUT, ""))
        logger.info(f"set master_client timeout to env {_timeout}")
    except Exception:
        _timeout = timeout
        logger.info(f"set master_client timeout to {_timeout}")

    master_client = None
    master_service_type = os.getenv(
        NodeEnv.DLROVER_MASTER_SERVICE_TYPE,
        CommunicationType.COMM_SERVICE_GRPC,
    )
    logger.info(f"Use [{master_service_type}] type for master client.")

    if master_addr:
        try:
            if master_service_type == CommunicationType.COMM_SERVICE_GRPC:
                master_client = GrpcMasterClient(
                    master_addr, job_name, job_id, node_id, node_type, timeout
                )
            elif master_service_type == CommunicationType.COMM_SERVICE_HTTP:
                master_client = HttpMasterClient(
                    master_addr, job_name, job_id, node_id, node_type, timeout
                )
            else:
                master_client = RayMasterClient(
                    master_addr, job_name, job_id, node_id, node_type, timeout
                )
        except Exception:
            logger.warning("The master is not available.")

    return master_client
