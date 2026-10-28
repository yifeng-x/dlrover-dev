# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import contextlib
import fcntl
import json
import os
import time
from dataclasses import asdict, dataclass
from typing import Iterator, Optional

from dlrover.brain.python.common.log import default_logger as logger


STATE_IDLE = "IDLE"
STATE_LAUNCHING = "LAUNCHING"
STATE_RUNNING = "RUNNING"
STATE_STOPPING = "STOPPING"
STATE_STOPPED = "STOPPED"
STATE_FAILED = "FAILED"
STATE_UNKNOWN = "UNKNOWN"

STATE_FILE_NAME = "train.json"
LOCK_FILE_NAME = "train.lock"


@dataclass
class RunState:
    version: int = 1
    state: str = STATE_IDLE
    launch_id: str = ""
    command: str = ""
    pid: Optional[int] = None
    pgid: Optional[int] = None
    log_file: str = ""
    started_at: float = 0.0
    updated_at: float = 0.0
    exit_code: Optional[int] = None
    last_error: str = ""

    @classmethod
    def from_dict(cls, data):
        if not isinstance(data, dict):
            return cls(updated_at=time.time())
        allowed = {field.name for field in cls.__dataclass_fields__.values()}
        kwargs = {key: value for key, value in data.items() if key in allowed}
        return cls(**kwargs)

    def touch(self):
        self.updated_at = time.time()
        return self


class WorkspaceStateStore:
    def __init__(self, state_dir: str):
        self.state_dir = state_dir
        self.state_file = os.path.join(state_dir, STATE_FILE_NAME)
        self.lock_file = os.path.join(state_dir, LOCK_FILE_NAME)

    def ensure_dir(self):
        os.makedirs(self.state_dir, exist_ok=True)

    @contextlib.contextmanager
    def locked(self) -> Iterator[None]:
        self.ensure_dir()
        with open(self.lock_file, "a+") as lock_fp:
            fcntl.flock(lock_fp.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_fp.fileno(), fcntl.LOCK_UN)

    def load(self) -> RunState:
        try:
            with open(self.state_file, "r") as fp:
                return RunState.from_dict(json.load(fp))
        except FileNotFoundError:
            logger.warning("Workspace state file %s not found, initializing with IDLE state.", self.state_file)
            return RunState(updated_at=time.time())
        except Exception as e:
            logger.warning(
                "Failed to load workspace state file %s.", self.state_file
            )
            return RunState(
                state=STATE_UNKNOWN,
                updated_at=time.time(),
                last_error="failed to load state: %s" % e,
            )

    def save(self, state: RunState):
        self.ensure_dir()
        state.touch()
        tmp_file = "%s.%s.tmp" % (self.state_file, os.getpid())
        with open(tmp_file, "w") as fp:
            json.dump(asdict(state), fp, sort_keys=True)
            fp.write("\n")
            fp.flush()
            os.fsync(fp.fileno())
        try:
            os.replace(tmp_file, self.state_file)
        except Exception:
            logger.error(
                "Failed to replace workspace state file tmp=%s target=%s.",
                tmp_file,
                self.state_file,
            )
            raise
