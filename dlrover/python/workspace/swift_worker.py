# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

"""Direct Swift worker entry and native-launch initialization callback plugin."""

import os
import runpy
from functools import wraps
from pathlib import Path


def install_callback():
    from transformers import Trainer, TrainerCallback

    if getattr(Trainer, "_dlrover_workspace_callback", False):
        return

    # Swift single-device mode can rewrite LOCAL_RANK to zero; the global
    # rank and local world size still identify the original local worker.
    local_rank = os.getenv("LOCAL_RANK", "0")
    if "RANK" in os.environ and "LOCAL_WORLD_SIZE" in os.environ:
        local_rank = str(
            int(os.environ["RANK"]) % int(os.environ["LOCAL_WORLD_SIZE"])
        )

    class FirstStep(TrainerCallback):
        def on_step_end(self, args, state, control, **kwargs):
            if state.global_step > 0:
                directory = os.environ["DLROVER_WORKSPACE_INIT_DIR"]
                Path(directory).mkdir(parents=True, exist_ok=True)
                Path(directory, "rank-" + local_rank).touch()
            return control

    original = Trainer.__init__

    # Swift inspects this signature to select processing_class vs tokenizer.
    @wraps(original)
    def init(self, *args, **kwargs):
        original(self, *args, **kwargs)
        self.add_callback(FirstStep())

    Trainer.__init__ = init
    Trainer._dlrover_workspace_callback = True


if os.getenv("DLROVER_WORKSPACE_INIT_DIR"):
    install_callback()

if __name__ == "__main__":
    for key in ("NPROC_PER_NODE", "NNODES", "NODE_RANK"):
        os.environ.pop(key, None)
    # Bypass swift.cli.main, whose environment-based torchrun is a launcher.
    runpy.run_module("swift.cli.rlhf", run_name="__main__")
