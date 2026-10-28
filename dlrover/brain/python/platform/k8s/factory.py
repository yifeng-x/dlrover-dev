import os
from typing import Callable

from dlrover.python.common.constants import PlatformType
from dlrover.python.common.log import default_logger as logger


def new_configmap_scale_watcher(platform, on_update_callback: Callable[[dict], None]):
    logger.info("New %s configmap scale Watcher", platform)

    default_namespace = "dlrover" # 默认为dlrover,实际watch在start的时候会获取brain服务启动时指定的命名空间

    if platform in (PlatformType.KUBERNETES, PlatformType.PY_KUBERNETES):
        from dlrover.brain.python.platform.k8s.watcher import (
            K8sConfigMapScaleWatcher,
        )
        configmap_name = os.getenv(
            "DLROVER_MANUAL_SCALE_CONFIGMAP_NAME", "brain-manual-scale-config"
        ).lower()
        return K8sConfigMapScaleWatcher(default_namespace, configmap_name, on_update_callback)
    elif platform in (PlatformType.RAY):
        from dlrover.python.master.watcher.ray_watcher import (
            RayScalePlanWatcher,
        )

        return RayScalePlanWatcher(default_namespace)
    else:
        raise ValueError("Not support engine %s", platform)

