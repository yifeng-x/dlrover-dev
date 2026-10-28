# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import os
import signal
import time

from dlrover.python.common.constants import (
    Accelerators,
    NodeEnv,
)
from dlrover.python.common.log import default_logger as logger
from dlrover.python.elastic_agent.monitor.resource import ResourceMonitor


def _accelerator_from_env():
    accelerator = os.getenv("DLROVER_ACCELERATOR", "")
    if accelerator:
        return accelerator
    return Accelerators.NVIDIA_GPU


def main():
    if os.getenv("DLROVER_RUNTIME_MONITOR", "false").lower() != "true":
        logger.info("DLRover runtime monitor is disabled.")
        return 0

    if not os.getenv(NodeEnv.DLROVER_MASTER_ADDR, ""):
        logger.warning("DLRover master addr is empty, skip runtime monitor.")
        return 0

    stopped = False

    def _stop(*_):
        nonlocal stopped
        stopped = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    monitor = ResourceMonitor.singleton_instance(_accelerator_from_env())
    logger.info("Start DLRover runtime resource monitor.")
    while not stopped:
        try:
            monitor.report_resource()
        except Exception as e:
            logger.warning(f"Report runtime resource failed: {e}")
        time.sleep(int(os.getenv("DLROVER_RUNTIME_MONITOR_INTERVAL", "20")))
    logger.info("Stop DLRover runtime resource monitor.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
