# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

import inspect
import os
from pathlib import Path
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest import mock
from dataclasses import dataclass

from dlrover.python.workspace.frameworks.swift import SwiftAdapter
from dlrover.python.workspace.frameworks.swift_runtime import validate_worker
from dlrover.python.workspace import swift_worker


class SwiftRuntimeTests(unittest.TestCase):
    def test_callback_preserves_trainer_parameter_detection(self):
        def modern_init(self, model=None, *, processing_class=None):
            self.processor = processing_class
            self.callbacks = []

        def legacy_init(self, model=None, *, tokenizer=None):
            self.processor = tokenizer
            self.callbacks = []

        for original in (modern_init, legacy_init):
            with self.subTest(original=original.__name__):
                trainer = type(
                    "Trainer",
                    (),
                    {
                        "__init__": original,
                        "add_callback": lambda self, cb: self.callbacks.append(cb),
                    },
                )
                module = ModuleType("transformers")
                module.Trainer = trainer
                module.TrainerCallback = object
                with mock.patch.dict("sys.modules", {"transformers": module}):
                    swift_worker.install_callback()
                    swift_worker.install_callback()
                    parameters = inspect.signature(trainer.__init__).parameters
                    key = (
                        "processing_class"
                        if "processing_class" in parameters
                        else "tokenizer"
                    )
                    processor = object()
                    instance = trainer(**{key: processor})
                self.assertEqual(
                    inspect.signature(trainer.__init__), inspect.signature(original)
                )
                self.assertIs(instance.processor, processor)
                self.assertEqual(len(instance.callbacks), 1)

    def test_exact_role_schema_without_argument_post_init(self):
        @dataclass
        class Trainer:
            model: str = ""
            max_steps: int = 1

            def __post_init__(self):
                raise AssertionError(
                    "preflight must not instantiate arguments"
                )

        @dataclass
        class Rollout:
            model: str = ""
            port: int = 8000

        module = ModuleType("swift.arguments")
        module.RLHFArguments = Trainer
        module.RolloutArguments = Rollout
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "config.json").write_text("{}")
            role = {
                "name": "trainer",
                "launch_kind": "local",
                "command": [
                    "swift",
                    "rlhf",
                    "--model",
                    directory,
                    "--max_steps",
                    "1",
                ],
                "environment": {"CUDA_VISIBLE_DEVICES": "0"},
                "devices": [0],
                "cwd": "",
            }
            with mock.patch.dict(
                "sys.modules", {"swift.arguments": module}
            ), mock.patch.dict(
                os.environ, {"CUDA_VISIBLE_DEVICES": "0"}
            ):
                for version in (
                    "3.12.0",
                    "4.0.0",
                    "4.1.0",
                    "4.2.0.dev0",
                    "4.4.2",
                ):
                    with self.subTest(version=version), mock.patch(
                        "importlib.metadata.version", return_value=version
                    ):
                        result = validate_worker([role])
                        self.assertIn("trainer", result["models"])
                        self.assertEqual(result["versions"]["ms-swift"], version)
                        invalid_role = dict(
                            role, command=role["command"] + ["--port", "8000"]
                        )
                        with self.assertRaisesRegex(ValueError, "port"):
                            validate_worker([invalid_role])

    def test_external_ports_must_match_swift_client_protocol(self):
        with self.assertRaises(ValueError):
            SwiftAdapter.endpoints(
                {
                    "vllm_server_host": ["10.0.0.1", "10.0.0.2"],
                    "vllm_server_port": ["8000"],
                }
            )

    def test_first_step_uses_original_worker_rank(self):
        class Trainer:
            def __init__(self):
                self.callbacks = []

            def add_callback(self, callback):
                self.callbacks.append(callback)

        module = ModuleType("transformers")
        module.Trainer = Trainer
        module.TrainerCallback = object
        with tempfile.TemporaryDirectory() as directory:
            with mock.patch.dict(
                "sys.modules", {"transformers": module}
            ), mock.patch.dict(
                os.environ,
                {
                    "RANK": "3",
                    "LOCAL_WORLD_SIZE": "2",
                    "LOCAL_RANK": "0",
                    "DLROVER_WORKSPACE_INIT_DIR": directory,
                },
            ):
                swift_worker.install_callback()
                instance = Trainer()
                (callback,) = instance.callbacks
                callback.on_step_end(
                    None, SimpleNamespace(global_step=0), None
                )
                self.assertFalse(Path(directory, "rank-1").exists())
                callback.on_step_end(
                    None, SimpleNamespace(global_step=1), None
                )
                self.assertTrue(Path(directory, "rank-1").exists())
                self.assertFalse(Path(directory, "rank-0").exists())


if __name__ == "__main__":
    unittest.main()
