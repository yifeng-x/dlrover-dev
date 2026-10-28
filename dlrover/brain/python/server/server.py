# Copyright 2026 The DLRover Authors. All rights reserved.
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
import sys
import time
import threading
import uvicorn
import asyncio
import signal
import json
import os
import requests

from fastapi import (
    FastAPI, 
    status,
    Header,
    HTTPException,
    Response as FastAPIResponse,
)
from dlrover.brain.python.common.http_schemas import (
    JobMetricsRequest,
    JobMetricsResponse,
    OptimizeRequest,
    OptimizeResponse,
    JobConfigResponse,
    JobConfigRequest,
    RegisterMasterClientRequest,
    RegisterMasterClientResponse,
    MasterHeartBeatRequest,
    MasterHeartBeatResponse,
)

from dlrover.brain.python.jobmanagement.job_config_manager import (
    JobConfigManager,
)
from dlrover.brain.python.jobmanagement.job_elastic_manager import (
    JobElasticManager,
)
from dlrover.brain.python.jobmanagement.master_client_pool import (
    MasterClientPool,
)
from dlrover.brain.python.jobmanagement.elastic.vertical_elastic import (
    VerticalElasticService,
)
from dlrover.brain.python.jobmanagement.job_cleanup_coordinator import (
    JobCleanupCoordinator,
)
from dlrover.brain.python.jobmanagement.job_owner import JobOwnerManager
from dlrover.brain.python.jobmanagement.job_owner_routing_service import (
    JobOwnerRoutingError,
    JobOwnerRoutingService,
)
from dlrover.brain.python.jobmanagement.master_control_service import (
    MasterControlService,
)
from dlrover.brain.python.optimization.optimizer_manager import (
    OptimizerManager,
)
from dlrover.brain.python.common.args import get_parsed_args
from dlrover.brain.python.common.http_schemas import Response
from dlrover.python.common.log import default_logger as logger
from dlrover.python.common.comm import TriggerElastic
from dlrover.python.common.constants import PlatformType
from dlrover.python.common.global_context import Context
from dlrover.brain.python.platform.k8s.factory import new_configmap_scale_watcher
from dlrover.python.common.constants import WorkerResourceScaleAction, NodeEnv
from dlrover.brain.python.common.distributed_lock import RedisDistributedLock
from dlrover.brain.python.common.distributed_lock import _redis_client
from dlrover.brain.python.server.prometheus import BrainMetricsServer

JSON_KEY = "config.json"
# Lock TTL: 30 seconds (enough for scale operation)
DISTRIBUTED_LOCK_TTL = 30
_DEFAULT_HANDLED_CONFIGMAP_STORE_KEY = "dlrover:brain:handled_configmap_version"

class BrainServer:
    """
    The training jobs access the service of DLRover brain via BrainServer.
    The BrainServer provides two APIs via HTTP protocol:
        - optimize: optimize the resource configuration of a training job.
        - get_config: retrieve the configuration of a job.
    """

    def __init__(self):
        self._server = FastAPI(title="DLRover Brain Server")
        self._optimizer_manager = OptimizerManager()
        self._config_manager = JobConfigManager()
        self._master_client_pool = MasterClientPool()
        self._job_owner_manager = JobOwnerManager()
        self._job_owner_routing_service = JobOwnerRoutingService(
            self._job_owner_manager
        )
        self._metrics_server = BrainMetricsServer()
        self._vertical_elastic_service = VerticalElasticService(
            metrics_persister=self._metrics_server.vertical_metrics
        )
        self._elastic_manager = JobElasticManager(
            master_client_pool=self._master_client_pool,
            job_owner_manager=self._job_owner_manager,
            vertical_elastic_service=self._vertical_elastic_service,
        )
        self._master_control_service = MasterControlService(
            self._master_client_pool, self._job_owner_manager
        )
        self._job_cleanup_coordinator = JobCleanupCoordinator(
            self._job_owner_manager,
            self._master_client_pool,
            self._vertical_elastic_service,
        )
        self._master_client_pool.set_job_cleanup_callback(
            self._job_cleanup_coordinator.cleanup_job
        )
        
        self._is_running = True
        self._start_time = time.time()
        self._metrics_server.set_snapshot_provider(
            self._elastic_manager.metrics_snapshot
        )

        self.register_routes()

        self._configmap_manual_scaler = new_configmap_scale_watcher(
            PlatformType.PY_KUBERNETES,
            self.handle_configmap_update_with_lock,
        )
        self._shared_store_ttl = int(
            os.getenv("DLROVER_BRAIN_MASTER_ADDR_TTL_SECONDS", "3600")
        )
        self._handled_configmap_key = os.getenv(
            "DLROVER_BRAIN_HANDLED_CONFIGMAP_STORE_KEY",
            _DEFAULT_HANDLED_CONFIGMAP_STORE_KEY
        )

        self._redis = _redis_client()

    def register_routes(self):
        # We map the URL to the class method 'self.optimize'
        self._server.post(
            "/optimize",
            response_model=OptimizeResponse,
            status_code=status.HTTP_201_CREATED,
        )(self.optimize)
        self._server.post(
            "/internal/optimize",
            response_model=OptimizeResponse,
            status_code=status.HTTP_200_OK,
            include_in_schema=False,
        )(self.optimize_internal)

        self._server.post(
            "/job_config",
            response_model=JobConfigResponse,
            status_code=status.HTTP_200_OK,
        )(self.job_config)

        self._server.post(
            "/report_runtime_node_stats",
            response_model=JobMetricsResponse,
            status_code=status.HTTP_200_OK,
        )(self.report_runtime_node_stats)
        self._server.post(
            "/internal/report_runtime_node_stats",
            response_model=JobMetricsResponse,
            status_code=status.HTTP_200_OK,
            include_in_schema=False,
        )(self.report_runtime_node_stats_internal)

        self._server.post(
            "/register_master_client",
            response_model=RegisterMasterClientResponse,
            status_code=status.HTTP_200_OK,
        )(self.register_master_client)
        self._server.post(
            "/internal/register_master_client",
            response_model=RegisterMasterClientResponse,
            status_code=status.HTTP_200_OK,
            include_in_schema=False,
        )(self.register_master_client_internal)

        self._server.post(
            "/report_heart_beat",
            response_model=MasterHeartBeatResponse,
            status_code=status.HTTP_200_OK,
        )(self.report_heart_beat)
        self._server.post(
            "/internal/report_heart_beat",
            response_model=MasterHeartBeatResponse,
            status_code=status.HTTP_200_OK,
            include_in_schema=False,
        )(self.report_heart_beat_internal)

        self._server.get("/healthz", include_in_schema=False)(self.healthz)
        self._server.get("/readyz", include_in_schema=False)(self.readyz)

    def healthz(self):
        return {"status": "ok"}

    def readyz(self):
        if not self._elastic_manager.is_ready():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="brain runtime processor or Redis is not ready",
            )
        return {"status": "ready"}

    def optimize(self, request: OptimizeRequest):
        return self._route_optimize(request)

    def optimize_internal(
        self,
        request: OptimizeRequest,
        x_dlrover_owner_token: str = Header(default=""),
    ):
        return self._route_optimize(
            request,
            forwarded=True,
            owner_token=x_dlrover_owner_token,
        )

    def _route_optimize(
        self,
        request: OptimizeRequest,
        forwarded: bool = False,
        owner_token: str = "",
    ) -> OptimizeResponse:
        try:
            route = (
                self._job_owner_routing_service.resolve_existing_owner(
                    request.job.uuid,
                    forwarded=forwarded,
                    owner_token=owner_token,
                )
            )
            if route.local:
                plan = self._optimizer_manager.optimize(
                    request.job, request.config
                )
                self._job_owner_routing_service.validate_local_owner(
                    request.job.uuid, route.owner.token
                )
                return OptimizeResponse(job_opt_plan=plan)

            body = self._job_owner_routing_service.forward_json(
                route.owner,
                "/internal/optimize",
                request.model_dump(),
                "optimize",
            )
            logger.info(
                "Forwarded optimize request for job %s to Brain %s.",
                request.job.uuid,
                route.owner.instance_id,
            )
            return OptimizeResponse(**body)
        except JobOwnerRoutingError as e:
            logger.warning(
                "Route optimize request for job %s failed: %s",
                request.job.uuid,
                e.reason,
            )
            code = (
                status.HTTP_503_SERVICE_UNAVAILABLE
                if e.retryable
                else status.HTTP_400_BAD_REQUEST
            )
            raise HTTPException(status_code=code, detail=e.reason) from e

    def _handled_configmap_store_key(self, job_uid: str) -> str:
        """生成 job_name -> job_uid 映射的 Redis key"""
        return f"{self._handled_configmap_key}:{job_uid}"

    def handle_configmap_update_with_lock(self, callback_data: dict):
        """
        Wrapper function with distributed lock for multi-instance Brain deployment.
        Acquires a fine-grained lock per task before processing the ConfigMap update.
        If lock acquisition fails, the instance skips processing (lets other instance handle it).

        Lock key format: brain:scale:{task_uid}
        - Different tasks can be processed in parallel
        - Same task is serialized across multiple Brain instances

        Args:
            callback_data: dict containing:
                - data: ConfigMap data (dict)
                - resource_version: resource_version (str)
        """
        # Extract data and metadata
        data = callback_data.get("data")
        resource_version = callback_data.get("resource_version")

        if not data:
            logger.warning(f"Invalid callback_data format, skip processing.")
            return

        try:
            json_str_content = data[JSON_KEY]
            manual_config = json.loads(json_str_content)

            job_name = manual_config.get("job_names", NodeEnv.JOB_NAME)
            # Get task_uid by job_name
            task_uid = self._master_client_pool.get_job_id_by_name(job_name)
            if task_uid is None:
                logger.warning(f"Cannot get task_uid for job_name '{job_name}', skip processing.")
                return

            # Create distributed lock with fine-grained key
            lock_key = f"dlrover:brain:scale:{task_uid}"
            distributed_lock = RedisDistributedLock(lock_key, ttl_seconds=DISTRIBUTED_LOCK_TTL)

            # Check if Redis is available
            if not distributed_lock.available:
                logger.warning(f"Redis is not available, processing without lock for job '{job_name}'.")
                return

            # Try to acquire lock
            if not distributed_lock.acquire():
                logger.info(f"Failed to acquire lock for task '{task_uid}' (job: '{job_name}'), "
                            "another Brain instance is processing. Skip.")
                return

            last_handled_version = self._redis.get(self._handled_configmap_store_key(task_uid))
            if last_handled_version and last_handled_version == resource_version:
                distributed_lock.release()
                logger.info(f"job {task_uid} ,the configMap version {resource_version}, has been handled, skip.")
                return

            try:
                self.handle_configmap_update_callback(data, resource_version, task_uid)
            finally:
                # Always release lock after processing
                distributed_lock.release()
                logger.info(f"Released lock for task '{task_uid}' (job: '{job_name}').")

        except Exception as e:
            logger.error(f"Error in handle_configmap_update_with_lock: {e}")

    def handle_configmap_update_callback(self, data: dict, resource_version: str = None, task_uid: str = None):
        """
        Process the ConfigMap update and trigger manual scaling.

        Args:
            data: ConfigMap data (dict)
            resource_version: resource_version
        """
        if not data:
            logger.warning(f"Manual scale configMap has no data, skip manual resource scaling.")
            return

        if JSON_KEY not in data:
            logger.warning(f"The key {JSON_KEY} was not found in the manual scale ConfigMap, skip manual resource scaling.")
            return
        json_str_content = data[JSON_KEY]

        # parse JSON str to Python dict
        manual_config = json.loads(json_str_content)
        if not isinstance(manual_config, dict):
            logger.warning(f"ConfigMap data is not a dictionary, type: {type(manual_config)}. Skip manual resource scaling")
            return

        scale_action = manual_config.get("scale_action", WorkerResourceScaleAction.NOT_ADJUST)
        if scale_action is None or scale_action == WorkerResourceScaleAction.NOT_ADJUST:
            logger.warning(f"scale action is not adjusted.")
            return

        job_name = manual_config.get("job_names", NodeEnv.JOB_NAME)
        if job_name is None:
            logger.warning(f"job name is null.")
            return

        client = self._master_client_pool.get_master_client_by_job_name(job_name)
        if client is None:
            logger.info(f"close grpc channel.")
            return
        logger.info(f"{job_name} Manual scale ConfigMap changed, will start resource scaling.")
        trigger_data = TriggerElastic(
            elastic_dimension="",
            scale_action=WorkerResourceScaleAction.MANUAL,
            job_name=job_name,
            namespace="",
            cluster=""
        )
        client._report(trigger_data)
        client._close_grpc_channel()
        # record this resource_version ——>  job_uid:version
        if resource_version:
            key = self._handled_configmap_store_key(task_uid)
            self._redis.set(key, resource_version, ex=self._shared_store_ttl)

    def job_config(self, request: JobConfigRequest):
        config = self._config_manager.get_job_config(request.job)
        if config is None:
            vals = {}
        else:
            vals = config.get_config_values()
        return JobConfigResponse(
            job_configs=vals,
        )

    def report_runtime_node_stats(self, request: JobMetricsRequest):
        result = self._elastic_manager.dispatch_job_runtime_info(request.job_metrics)
        if not result.success and result.retryable:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=result.reason,
            )
        return JobMetricsResponse(
            response=Response(
                success=result.success, 
                reason=result.reason,
            ),
        )

    def report_runtime_node_stats_internal(
        self,
        request: JobMetricsRequest,
        x_dlrover_owner_token: str = Header(default=""),
    ):
        result = self._elastic_manager.dispatch_job_runtime_info(
            request.job_metrics,
            forwarded=True,
            owner_token=x_dlrover_owner_token,
        )
        if not result.success and result.retryable:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=result.reason,
            )
        return JobMetricsResponse(
            response=Response(
                success=result.success,
                reason=result.reason,
            )
        )

    def register_master_client(self, request: RegisterMasterClientRequest):
        result = self._master_control_service.register_master(
            request.job, request.master_addr
        )
        if not result.success and result.retryable:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=result.reason,
            )
        return RegisterMasterClientResponse(
            response=Response(
                success=result.success,
                reason=result.reason,
            ),
        )

    def register_master_client_internal(
        self,
        request: RegisterMasterClientRequest,
        x_dlrover_owner_token: str = Header(default=""),
    ):
        result = self._master_control_service.register_master(
            request.job,
            request.master_addr,
            forwarded=True,
            owner_token=x_dlrover_owner_token,
        )
        if not result.success and result.retryable:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=result.reason,
            )
        return RegisterMasterClientResponse(
            response=Response(success=result.success, reason=result.reason)
        )

    def report_heart_beat(self, request: MasterHeartBeatRequest):
        result = self._master_control_service.heartbeat_master(
            request.job, request.master_addr
        )
        if not result.success and result.retryable:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=result.reason,
            )
        return MasterHeartBeatResponse(
            response=Response(
                success=result.success,
                reason=result.reason,
            ),
        )
    
    def report_heart_beat_internal(
        self,
        request: MasterHeartBeatRequest,
        x_dlrover_owner_token: str = Header(default=""),
    ):
        result = self._master_control_service.heartbeat_master(
            request.job,
            request.master_addr,
            forwarded=True,
            owner_token=x_dlrover_owner_token,
        )
        if not result.success and result.retryable:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=result.reason,
            )
        return MasterHeartBeatResponse(
            response=Response(success=result.success, reason=result.reason)
        )

    def run(self, args):
        host = "0.0.0.0"
        port = args.port

        # 0. start metrics server
        self._metrics_server.start(args.metrics_port)

        # 1.Start the processor first, while readiness remains false because
        # this instance has not joined the Redis allocator membership yet.
        self._elastic_manager.start()

        # 2. start server
        server_thread = threading.Thread(
            target=self.start_server,
            args=(host, port),
            daemon=True
        )
        server_thread.start()
        time.sleep(1)
        if not server_thread.is_alive():
            self._elastic_manager.stop()
            raise RuntimeError("Brain HTTP server failed before startup")

        namespace = args.namespace

        if args.configmap_monitor == "on":
            self._configmap_manual_scaler.start(namespace)

        # 3.Join allocator membership only after the internal HTTP endpoint is
        # listening, so another Brain can safely forward a new registration.
        self._job_owner_manager.start()
        self._master_client_pool.start_auto_clean_thread(timeout=60)
        logger.info(f"Brain server started at {host}:{port}")

        # main thread keep running, gracefully exit
        self.wait_for_exit()

    def start_server(self, host="0.0.0.0", port=8000):
        try:
            uvicorn.run(
                self._server, 
                host=host, 
                port=port,
                log_level="warning", # only report warning or error level log
            )
        except Exception as e:
            logger.error(f"Failed to start brain server: {e}")
        
    def wait_for_exit(self):
        """
        gracefully exit
        """
        signal.signal(signal.SIGINT, self._handle_singal) # register signal handler, Ctrl+C
        signal.signal(signal.SIGTERM, self._handle_singal) # register signal handler, kill/ Container stop

        while self._is_running:
            time.sleep(1)
    
    def _handle_singal(self, signum, frame):
        """
        signal handler, gracefully exit
        """
        logger.info(f"Got signal {signum}, exiting...")
        self._is_running = False

        # Leave allocator membership first so no new job is assigned while
        # the remaining workers and connection cache shut down.
        if self._job_owner_manager:
            self._job_owner_manager.stop()
        if self._elastic_manager:
            self._elastic_manager.stop()
        if self._master_control_service:
            self._master_control_service.close()
        if self._job_owner_routing_service:
            self._job_owner_routing_service.close()
        if self._master_client_pool:
            self._master_client_pool.stop()
        
        sys.exit(0)

if __name__ == "__main__":
    args = get_parsed_args()
    brain_server = BrainServer()
    brain_server.run(args)
