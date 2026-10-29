# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
"""Bounded, best-effort diagnostics for benefit control flow."""

import json
import time

from dlrover.python.common.log import default_logger as logger


class BenefitLogger:
    def __init__(self, component, clock=time.monotonic):
        self.component = component
        self.clock = clock
        self._last = {}

    def emit(self, event, level="info", repeat_key=None, **fields):
        """Callers supply selected fields, never bodies, credentials or errors.

        Repeated polling outcomes emit at most once per minute. Each fixed
        event name retains only its latest signature, not a history of plans.
        """
        try:
            suppressed = 0
            if repeat_key is not None:
                now = self.clock()
                previous = self._last.get(event)
                if previous and previous[0] == repeat_key:
                    suppressed = previous[2]
                    if now - previous[1] < 60:
                        self._last[event] = (repeat_key, previous[1], suppressed + 1)
                        return
                self._last[event] = (repeat_key, now, 0)
            write = {"debug": logger.debug, "info": logger.info,
                     "warning": logger.warning}[level]
            write("BenefitFlow %s", json.dumps(
                dict(component=self.component, event=event,
                     suppressed=suppressed, **fields),
                ensure_ascii=True, allow_nan=False, sort_keys=True,
            ))
        except Exception:
            # Logging must never change an admission decision or mutation.
            pass
