import threading
import json
from time import sleep
from typing import Callable, Dict
from dlrover.brain.python.common.log import default_logger as logger
from kubernetes import client, config, watch
from dlrover.python.scheduler.kubernetes import k8sClient
from dlrover.python.common.global_context import (
    Context,
)


JSON_KEY = "config.json"
MANUAL_AVAILABLE = "MANUAL_AVAILABLE"
_dlrover_context = Context.singleton_instance()


class K8sConfigMapScaleWatcher:
    """ConfigMapScaleWatcher monitors the manual brain configmap on the cluster.
    It generates a ResourcePlan by a brain configmap and notidy the
    JobManager to adjust job resource by the brain ResourcePlan.
    """

    def __init__(self, namespace, configmap_name, on_update_callback: Callable[[dict], None]):
        self._namespace = namespace
        self._configmap_name = configmap_name
        self.on_update_callback = on_update_callback
        self._k8s_client = k8sClient.singleton_instance(namespace)
        # key:job_name , value:resource_version
        self._job_version: Dict[str, str] = {}

    def watch(self):

        while True:
            try:
                w = watch.Watch()
                v1 = self._k8s_client.client
                stream_kwargs = {
                    "namespace": self._namespace,
                    "timeout_seconds": 0  # 0 means "listen as long as possible"
                }

                for event in w.stream(v1.list_namespaced_config_map, **stream_kwargs):
                    obj = event['object']
                    if obj.data is None:
                        continue
                    if obj.data.get(JSON_KEY) is None:
                        continue
                    json_str_content = obj.data[JSON_KEY]
                    manual_config = json.loads(json_str_content)
                    # 判断当前configMap是否为手动扩缩容的configMap
                    manual = manual_config.get("manual_available", MANUAL_AVAILABLE)
                    if manual != "True":
                        logger.info(f"manual available is not true: {manual}")
                        continue
                    # 判断当前监控的configMap版本，是否已经进行过扩缩容操作
                    job_name = manual_config.get("job_name", "")
                    resource_version = obj.metadata.resource_version
                    if self._job_version.get(job_name, "") == obj.metadata.resource_version:
                        continue
                    self._job_version[job_name] = resource_version

                    event_type = event['type']

                    if event_type == "MODIFIED" or event_type == "ADDED":
                        if obj.data:
                            callback_data = {
                                "data": obj.data,
                                "resource_version": obj.metadata.resource_version
                            }
                            self.on_update_callback(callback_data)
            except Exception as e:
                logger.warning(f"Watch connection broken ({e}). Retrying in 5 seconds...")
                sleep(5)

    def start(self, namespace):
        self._namespace = namespace
        thread = threading.Thread(
            target=self.watch(),
            name="Configmap-scale-watcher",
            daemon=True
        )
        thread.start()
        if thread.is_alive():
            logger.info("ConfigMap scale watcher initialized successfully.")