# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");

"""Swift GRPO adapter. Swift/vLLM protocol details stay outside orchestration."""

from copy import deepcopy
from urllib.parse import urlsplit


def options(argv):
    result = {}
    key = None
    for token in argv[2:]:
        if token.startswith("--"):
            key, sep, value = token[2:].partition("=")
            key = key.replace("-", "_")
            if key in result:
                raise ValueError("duplicate Swift option: --" + key)
            result[key] = [value] if sep else []
        elif key is None:
            raise ValueError("unexpected Swift positional argument: " + token)
        else:
            result[key].append(token)
    return result


def scalar(opts, key, default=None):
    values = opts.get(key)
    if values is None:
        return default
    if len(values) != 1:
        raise ValueError("--%s requires exactly one value" % key)
    return values[0]


def replace_options(argv, updates):
    result, skip = argv[:2], False
    for token in argv[2:]:
        if token.startswith("--"):
            skip = token[2:].split("=", 1)[0].replace("-", "_") in updates
        if not skip:
            result.append(token)
    for key, values in updates.items():
        if values is not None:
            result.extend(["--" + key, *values])
    return result


class SwiftAdapter:
    name = "swift"
    version = 1
    # Runtime verifies the installed release and exact option fields as well.
    capabilities = {
        "version_policy": "capability-based",
        "backends": ["cuda", "ascend"],
        "deployments": ["colocate", "disaggregated"],
        "launch_kinds": ["distributed", "local", "service"],
        "initialization": "first completed optimizer step on every trainer worker",
    }

    @staticmethod
    def detect(commands):
        return all(
            c.argv[:2] in (["swift", "rlhf"], ["swift", "rollout"])
            for c in commands
        )

    def parse(self, commands, deployment, management):
        roles = {}
        for command in commands:
            if command.argv[:2] not in (
                ["swift", "rlhf"],
                ["swift", "rollout"],
            ):
                raise ValueError(
                    "%s:%s: unsupported Swift entrypoint"
                    % (command.source, command.line)
                )
            role = "trainer" if command.argv[1] == "rlhf" else "rollout"
            if role in roles:
                raise ValueError(
                    "expected exactly one %s command template" % role
                )
            roles[role] = deepcopy(command)
        if "trainer" not in roles:
            raise ValueError("swift rlhf trainer command is required")
        opts = options(roles["trainer"].argv)
        if (
            scalar(opts, "rlhf_type") != "grpo"
            or scalar(opts, "use_vllm", "false").lower() != "true"
        ):
            raise ValueError(
                "Swift adapter requires --rlhf_type grpo --use_vllm true"
            )
        if any(k in opts for k in ("vllm_server_api_key", "api_key")):
            raise ValueError(
                "authenticated rollout protocol is not supported by this adapter version"
            )
        if "max_step" in opts:
            raise ValueError(
                "use the complete Swift option --max_steps, not --max_step"
            )
        if "config" in opts:
            raise ValueError(
                "expand --config into explicit Swift arguments before adaptation"
            )
        mode = {"colocate": "colocate", "server": "disaggregated"}.get(
            scalar(opts, "vllm_mode")
        )
        if mode is None:
            raise ValueError("specify Swift --vllm_mode colocate or server")
        if deployment not in ("auto", mode):
            raise ValueError(
                "deployment-mode conflicts with Swift --vllm_mode"
            )
        if mode == "colocate":
            if management is not None or "rollout" in roles:
                raise ValueError(
                    "colocate does not accept a rollout command or management"
                )
            management = "internal"
        else:
            management = management or "external"
            if management == "external" and "rollout" in roles:
                raise ValueError(
                    "script contains rollout; explicitly use --rollout-management managed"
                )
            if management == "managed" and "rollout" not in roles:
                raise ValueError("managed requires a swift rollout command")
            if management == "external":
                self.endpoints(opts)
        if "rollout" in roles:
            rollout = options(roles["rollout"].argv)
            if "api_key" in rollout or "config" in rollout:
                raise ValueError(
                    "rollout --api_key/--config is not supported by this adapter version"
                )
            for key in ("model_type", "model_revision"):
                if scalar(opts, key) != scalar(rollout, key):
                    raise ValueError(
                        "trainer and rollout --%s must match" % key
                    )
            for role_opts in (opts, rollout):
                if not scalar(role_opts, "model"):
                    raise ValueError(
                        "explicit --model is required for both roles"
                    )
            # Different mounts require verified model content, not a guessed alias.
            if scalar(opts, "model") != scalar(rollout, "model"):
                raise ValueError(
                    "trainer/rollout model differs; use the same model identity and mount path"
                )
            if scalar(opts, "async_generate", "false").lower() == "true":
                raise ValueError(
                    "asynchronous rollout is outside the initial adapter capability"
                )
            port = int(scalar(rollout, "port", "8000"))
            if not 0 < port < 65536:
                raise ValueError("invalid rollout service port")
            roles["rollout"].argv = replace_options(
                roles["rollout"].argv, {"host": ["0.0.0.0"]}
            )
        return roles, mode, management

    def bind_endpoints(self, command, endpoints):
        command = deepcopy(command)
        command.argv = replace_options(
            command.argv,
            {
                "vllm_server_host": [e["host"] for e in endpoints],
                "vllm_server_port": [str(e["port"]) for e in endpoints],
                "vllm_server_base_url": None,
            },
        )
        return command

    @staticmethod
    def endpoints(opts):
        urls = opts.get("vllm_server_base_url")
        if urls:
            result = []
            for value in urls:
                url = urlsplit(value)
                if (
                    url.scheme not in ("http", "https")
                    or not url.hostname
                    or url.username
                    or url.path not in ("", "/")
                ):
                    raise ValueError("invalid external rollout base URL")
                result.append(
                    {
                        "url": value.rstrip("/"),
                        "host": url.hostname,
                        "port": url.port
                        or (443 if url.scheme == "https" else 80),
                    }
                )
            return result
        hosts = opts.get("vllm_server_host", [])
        ports = opts.get("vllm_server_port", ["8000"])
        if not hosts or len(ports) != len(hosts):
            raise ValueError(
                "external rollout requires hosts and matching ports"
            )
        result = []
        for host, port in zip(hosts, ports):
            if host in ("0.0.0.0", "::") or not 0 < int(port) < 65536:
                raise ValueError("invalid external rollout endpoint")
            authority = "[%s]" % host if ":" in host else host
            result.append(
                {
                    "host": host,
                    "port": int(port),
                    "url": "http://%s:%s" % (authority, port),
                }
            )
        return result

    @staticmethod
    def validate_resources(templates, mode, nproc, rollout_devices):
        if mode == "colocate":
            tp = int(
                scalar(
                    options(templates["trainer"].argv),
                    "vllm_tensor_parallel_size",
                    "1",
                )
            )
            if tp < 1 or nproc % tp:
                raise ValueError(
                    "colocate rollout TP must divide trainer processes per Pod"
                )
        if "rollout" in templates:
            opts = options(templates["rollout"].argv)
            tp = int(scalar(opts, "vllm_tensor_parallel_size", "1"))
            dp = int(scalar(opts, "vllm_data_parallel_size", "1"))
            if tp < 1 or dp < 1 or tp * dp > len(rollout_devices):
                raise ValueError(
                    "rollout TP * DP exceeds assigned devices or is not positive"
                )

    @staticmethod
    def service_endpoint(command, host):
        return {
            "host": host,
            "port": int(scalar(options(command.argv), "port", "8000")),
        }

    @staticmethod
    def external_endpoints(role):
        argv = role["command"]
        if role["launch_kind"] == "distributed":
            argv = ["swift", "rlhf", *argv[2:]]
        return SwiftAdapter.endpoints(options(argv))

    @staticmethod
    def prepare_local_command(argv):
        from dlrover.python.workspace import swift_worker

        return replace_options(
            argv,
            {
                "external_plugins": options(argv).get("external_plugins", [])
                + [swift_worker.__file__]
            },
        )

    @staticmethod
    def validate_worker(roles):
        from .swift_runtime import validate_worker

        return validate_worker(roles)

    @staticmethod
    def readiness(endpoint):
        from .swift_runtime import probe

        return probe(endpoint)

    @staticmethod
    def pending_checks(management):
        return [
            "installed Swift argument schemas and exact CLI options",
            "model metadata and worker paths",
            "container device allocation",
            "rollout HTTP/model readiness",
            "Ascend HCCL interfaces and dynamic weight-sync ports",
            "first training step on every worker",
            (
                "external model identity is not verified"
                if management == "external"
                else "LoRA/full weight synchronization"
            ),
        ]

    @staticmethod
    def worker_entrypoint(argv):
        return ["-m", "dlrover.python.workspace.swift_worker", *argv[2:]]
