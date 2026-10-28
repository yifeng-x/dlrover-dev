# DLRover Workspace 命令参数说明

本文档整理当前 Workspace 模式下的配置项、CLI 参数、内部控制命令、
环境变量和默认文件路径。用户主要使用 ElasticJob annotation 和
`dlrover-workspace`；`agentctl` 是由 `dlrover-workspace` 通过
Kubernetes `pods/exec` 注入到 worker Pod 内执行的内部命令。

CLI 调用链和实现流程图见
[DLRover Workspace CLI 实现逻辑](./dlrover-workspace-cli-implementation-cn.md)。

## ElasticJob 配置参数

Workspace 通过 ElasticJob 的 `spec.runMode` 或 annotation 开启：

```yaml
metadata:
  annotations:
    dlrover.io/run-mode: workspace
    dlrover.io/workspace-exit-policy: retain
    dlrover.io/workspace-debug-mode: "false"
    dlrover.io/jupyter-enabled: "true"
    dlrover.io/jupyter-port: "8888"
    dlrover.io/jupyter-service-type: "NodePort"
    dlrover.io/jupyter-node-port: "30088"
    dlrover.io/jupyter-token: "dlrover"
    dlrover.io/jupyter-args: ""
    dlrover.io/idle-command: "sleep infinity"
    dlrover.io/init-command: "pip install -r /nas/requirements.txt"
    dlrover.io/runtime-monitor-enabled: "true"
```

| 参数 | 默认值 | 作用 |
| --- | --- | --- |
| `spec.runMode` | 空 | 优先级高于 annotation。仅设置为 `workspace` 时开启 Workspace；旧值 `runtime` 不再支持。 |
| `dlrover.io/run-mode` | 空 | `workspace` 表示启用 Workspace 架构；Jupyter 和训练结束策略由独立参数控制。 |
| `dlrover.io/workspace-exit-policy` | `retain` | `retain` 在训练结束后保留 Pod；`release` 删除仍在运行的 worker。已经进入 Failed phase 的 Pod 保留用于诊断，随 ElasticJob 最终清理。 |
| `dlrover.io/workspace-debug-mode` | `false` | release 调试兼容开关。开启后允许 `<setup> && dlrover-workspace launch ...`；setup 在所有 worker 执行，launch 仍只在 rank 0 执行。生产环境不建议开启。 |
| `dlrover.io/jupyter-enabled` | `true` | 是否在 rank 0 worker 启动 Jupyter；`release` 首版要求为 `false`。 |
| `dlrover.io/jupyter-port` | `8888` | Jupyter Lab 监听端口，也是 `<job-name>-jupyter` Service 的 service port/target port。 |
| `dlrover.io/jupyter-service-type` | `NodePort` | Jupyter Service 暴露方式，支持 `NodePort`、`ClusterIP`、`LoadBalancer`。`NodePort` 端口由 Kubernetes 自动分配。 |
| `dlrover.io/jupyter-node-port` | 空 | `jupyter-service-type=NodePort` 时可指定固定 nodePort；为空时由 Kubernetes 自动分配。指定值必须位于集群 `service-node-port-range` 内。 |
| `dlrover.io/jupyter-token` | `dlrover` | Jupyter 访问 token。生产环境建议改为由 Secret 注入。 |
| `dlrover.io/jupyter-args` | 空 | 追加到 `jupyter lab` 命令后的额外参数。 |
| `dlrover.io/idle-command` | `sleep infinity` | retain 模式下非 Jupyter worker 的主进程命令；release 模式忽略该参数并使用退出监督进程。 |
| `dlrover.io/init-command` | 空 | retain 下在所有 worker 执行；release 下必须包含完整的全量 `dlrover-workspace launch`。默认仅 rank 0 执行；debug mode 下 setup 部分在所有 worker 执行。 |
| `dlrover.io/runtime-monitor-enabled` | Workspace 下为 `true` | 是否启动 `python -m dlrover.python.workspace.runtime_monitor` 上报 CPU、内存和 GPU 使用情况。 |

DLRover 生成的 Jupyter 命令形态：

```bash
jupyter lab \
  --ip=0.0.0.0 \
  --port=<jupyter-port> \
  --no-browser \
  --allow-root \
  --ServerApp.token="${JUPYTER_TOKEN:-<jupyter-token>}" \
  <jupyter-args>
```

retain 模式配置了 `init-command` 和 runtime monitor 时，worker 主命令整体顺序为：

```bash
python -m dlrover.python.workspace.runtime_monitor &
monitor_pid=$!
trap 'kill ${monitor_pid} >/dev/null 2>&1 || true' TERM INT EXIT
set -e
<init-command>
<jupyter lab 或 idle-command>
```

release 模式不使用 idle-command。rank 0 执行完整的 workspace launch，所有 worker
随后以前台 supervisor 等待本机训练结果；训练失败时 supervisor 非零退出，使 Pod
进入 Failed phase 并释放资源：

```bash
python -m dlrover.python.workspace.runtime_monitor &
monitor_pid=$!
trap 'kill ${monitor_pid} >/dev/null 2>&1 || true' TERM INT EXIT
set -e
<仅 rank 0 执行 init-command 中的 workspace launch>
python -m dlrover.python.workspace.release_supervisor wait
```

## dlrover-workspace 通用参数

`dlrover-workspace` 支持 `status`、`launch`、`logs`、`stop` 四个子命令。

所有子命令都支持以下通用参数：

| 参数 | 默认值 | 作用 |
| --- | --- | --- |
| `--job-name` | 环境变量 `ELASTIC_JOB_NAME` | ElasticJob 名称。用于通过 label 查找 worker Pods；未设置环境变量时必须显式传入。 |
| `--namespace` | 环境变量 `NAMESPACE`，否则 `default` | ElasticJob 所在 namespace。 |
| `--container` | `main` | 在 worker Pod 中执行 `pods/exec` 的目标容器名。 |
| `--ranks` | 空 | 逗号分隔的 rank 列表，例如 `0,1`。为空表示作用于所有 worker。rank 是 worker 的系统标识，建议先通过 `status --cli-log on` 查看 rank 与 Pod/实例的映射。 |
| `--cli-log` | `off` | 控制 CLI 执行日志和 workspace 调试上下文，仅支持 `on`/`off`。默认 `off`，只展示命令业务输出；设为 `on` 时输出 CLI 异常定位日志，并在各命令结果中展示 `[workspace]` 的 rank、pod、namespace、phase、IP、node 等调试信息。也可通过环境变量 `DLROVER_WORKSPACE_CLI_LOG=on/off` 设置默认值。 |

worker Pod 查找依赖以下 label：

```text
elasticjob.dlrover/name=<job-name>
elasticjob.dlrover/replica-type=worker
elasticjob.dlrover/rank-index=<rank>
```

## status

查看 workspace 环境和训练进程状态：

```bash
dlrover-workspace status
dlrover-workspace status --job-name workspace-test --namespace dlrover
dlrover-workspace status --ranks 0,1
```

参数：

| 参数 | 默认值 | 作用 |
| --- | --- | --- |
| `--log-dir` | `/tmp/dlrover-workspace` | 仅控制训练日志目录；status/stop 保留此参数以兼容旧命令，但不使用它定位状态。未指定时使用默认目录，指定时使用传入目录。 |
| `--state-dir` | `/tmp/dlrover-workspace-state` | Pod 内容器本地状态、锁、pid、pgid 和 launch_id 目录。自定义后，各管理命令必须指定相同值。 |

行为：

- 先按 job label 查询 worker Pods，并按 rank 排序。
- 对 `Running` Pod 执行内部 `agentctl status`。
- 对非 `Running` Pod 不执行 `pods/exec`，展示不可用状态；`--cli-log on` 时额外展示 rank、Pod 名称、namespace、phase、IP、node。
- 输出包含 `launch_id`、训练状态、进程组、日志文件、`dlrover-run` 进程和 trainer worker 进程。
- 默认 `--cli-log off` 不展示 `[workspace]` 调试上下文；排查 Pod/IP/node 等问题时使用 `--cli-log on`。

输出示例：

```text
===== rank 0 workspace-test-edljob-worker-0 [STATUS SUCCESS] =====
  launch_id: 6f1...
  state: RUNNING
  process_group: 809
  log_file: /tmp/dlrover-workspace/train.rank_0.log
  dlrover-run:
    process:
      pid: 809
      ppid: 1
      pgid: 809
      stat: Ss
      cmd: /bin/bash -lc ...
  training:
    worker[0]:
      process:
        pid: 1078
        ppid: 809
        pgid: 809
        stat: Sl
        cmd: python train.py ...
```

开启 CLI 调试日志后会额外输出 workspace 上下文：

```bash
dlrover-workspace status --cli-log on
```

```text
===== rank 0 workspace-test-edljob-worker-0 [STATUS SUCCESS] =====
[workspace]
  rank: 0
  pod: workspace-test-edljob-worker-0
  namespace: dlrover
  phase: Running
  ip: 10.0.0.1
  node: node-a
  launch_id: 6f1...
  ...
```

## launch

在 worker Pod 中启动训练命令：

```bash
dlrover-workspace launch --framework-cmd \
  "python train.py"

dlrover-workspace launch --network-mode all --framework-cmd \
  "python train.py"

dlrover-workspace launch --network-mode ranks --ranks 0,1 --framework-cmd \
  "python train.py"
```

参数：

| 参数 | 默认值 | 作用 |
| --- | --- | --- |
| `--cmd` | 与 `--framework-cmd` 二选一 | 要在目标 worker Pod 中执行的完整训练命令。默认会在所有目标 worker 上执行同一条命令。 |
| `--framework-cmd` | 与 `--cmd` 二选一 | 用户原生训练框架命令，例如 `swift sft --model /nas/model`。默认不自动包装 `dlrover-run`。 |
| `--network-mode`, `--auto-network` | `none` | 控制是否使用 DLRover 自动组网。`none` 不自动组网；`all` 使用全部 worker Pod 组网；`ranks` 仅使用 `--ranks` 指定的 worker Pod 组网。 |
| `--dlrover-run-args` | `--nnodes=$NODE_NUM --nproc_per_node=1` | `--network-mode` 不为 `none` 且使用 `--framework-cmd` 时自动拼接的 DLRover 启动参数。 |
| `--log-dir` | `/tmp/dlrover-workspace` | 仅控制训练日志目录；status/stop 保留此参数以兼容旧命令，但不使用它定位状态。未指定时使用默认目录，指定时使用传入目录。 |
| `--state-dir` | `/tmp/dlrover-workspace-state` | Pod 内容器本地状态、锁、pid、pgid 和 launch_id 目录。自定义后，各管理命令必须指定相同值。 |
| `--foreground` | `false` | 前台模式。直接通过 `pods/exec` 执行命令，不写 workspace 状态文件，也不创建后台进程组。主要用于调试。 |
| `--max-workers` | `32` | CLI 并发 exec 的最大 worker 数。实际并发数为 `min(max_workers, pod_count)`。 |

组网行为：

- 默认 `--network-mode none`。CLI 不自动包装 `dlrover-run`，也不修改用户命令；如果用户在 `--cmd` 中显式调用 `dlrover-run`，仍保持 DLRover 原有组网逻辑。
- `--network-mode all` 会忽略局部选择并要求不传 `--ranks`，对当前 job 的全部 worker Pod 执行 launch，并为本次命令设置 `NODE_NUM=<worker 数>`。
- `--network-mode ranks` 必须同时传 `--ranks`，只在指定 worker Pod 上执行 launch，设置 `NODE_NUM=<选中 worker 数>`，并把选中 Pod 的原始 `NODE_RANK` 临时映射为从 0 开始的连续 rank，避免原始 rank 不连续影响 rendezvous。
- `--ranks` 中不能包含重复 rank；指定 rank 必须全部存在，并且目标 Pod 必须全部处于 `Running`，否则 launch 会失败。
- 如果使用 `--cmd`，CLI 不会改写用户命令，只注入上述环境变量；用户可以在命令里自行调用 `dlrover-run`、`torchrun` 或其它启动器。
- 如果使用 `--framework-cmd` 且 `--network-mode` 不为 `none`，CLI 会包装为 `dlrover-run <dlrover-run-args> <framework-cmd>`；如果 `--framework-cmd` 已经以 `dlrover-run` 开头，则不会重复包装。

后台模式，也就是默认模式，会执行以下动作：

- 校验所有目标 worker Pod 都处于 `Running`。
- 为本次 launch 生成统一的 `DLROVER_WORKSPACE_LAUNCH_ID`。
- 将用户命令 base64 编码后传给 Pod 内 `agentctl launch`，避免复杂 shell 引号破坏命令。
- 在每个 Pod 内创建独立 session/process group。
- 写入 `train.json`、`train.pid`、`train.pgid`、`train.launch_id`。
- 将训练输出写入 rank 日志；默认同时 tee 到容器 stdout。
- 启动后等待最多 5 秒扫描 trainer worker 进程，并在 launch 输出中展示进程信息。
- 默认 `--cli-log off` 不展示 `[workspace]` 的 Pod 调试上下文；需要查看 pod、phase、IP、node 或 CLI 异常定位日志时使用 `--cli-log on`。

如果已有训练仍存活，`launch` 会返回冲突信息：

```text
training already running pid=<pid> pgid=<pgid> launch_id=<launch_id>
```

`--foreground` 的注意事项：

- 命令结束前 `dlrover-workspace launch` 不会返回。
- 不写 `train.json`、`train.pid`、`train.pgid`、`train.launch_id`。
- 不使用 `agentctl` 后台管理逻辑。
- 后续 `dlrover-workspace stop/status/logs` 无法基于 workspace 元数据管理该前台命令。

## logs

查看训练日志：

```bash
dlrover-workspace logs
dlrover-workspace logs --tail 200
dlrover-workspace logs --follow
dlrover-workspace logs -f --ranks 0
```

参数：

| 参数 | 默认值 | 作用 |
| --- | --- | --- |
| `--log-dir` | `/tmp/dlrover-workspace` | 仅控制训练日志目录；status/stop 保留此参数以兼容旧命令，但不使用它定位状态。未指定时使用默认目录，指定时使用传入目录。 |
| `--state-dir` | `/tmp/dlrover-workspace-state` | Pod 内容器本地状态、锁、pid、pgid 和 launch_id 目录。自定义后，各管理命令必须指定相同值。 |
| `--tail` | `100` | 输出每个 rank 最近 N 行日志。 |
| `--max-workers` | `32` | 并发 exec 的最大 worker 数。 |
| `--follow`, `-f` | `false` | 持续跟随日志，内部使用 `tail -F`。 |

默认日志文件：

```text
/tmp/dlrover-workspace/train.rank_${NODE_RANK:-unknown}.log
```

输出会按 rank 加前缀：

```text
===== rank 0 workspace-test-edljob-worker-0 [LOGS SUCCESS] =====
...

[rank 1 workspace-test-edljob-worker-1] loss=...
```

## stop

retain 模式下停止训练进程，但保留 worker Pod、Jupyter/idle 主进程和已申请资源；
release 模式下停止结果进入终态聚合并释放 worker：

```bash
dlrover-workspace stop
dlrover-workspace stop --grace-period 300 --kill-timeout 30
dlrover-workspace stop --ranks 0,1
```

参数：

| 参数 | 默认值 | 作用 |
| --- | --- | --- |
| `--log-dir` | `/tmp/dlrover-workspace` | 仅控制训练日志目录；status/stop 保留此参数以兼容旧命令，但不使用它定位状态。未指定时使用默认目录，指定时使用传入目录。 |
| `--state-dir` | `/tmp/dlrover-workspace-state` | Pod 内容器本地状态、锁、pid、pgid 和 launch_id 目录。自定义后，各管理命令必须指定相同值。 |
| `--max-workers` | `32` | 并发 exec 的最大 worker 数。 |
| `--grace-period` | `300` | 向 trainer worker 发送 `SIGUSR1` 后等待优雅退出的秒数。 |
| `--kill-timeout` | `30` | 发送 `SIGTERM` 后等待退出的秒数；超时后进入 `SIGKILL`。 |

停止流程分 7 步，并通过 logger 输出进度信息；仅在 `--cli-log on` 时显示：

```text
[1/7] loaded stop metadata pid=... pgid=... launch_id=...
[2/7] resolved process group pgid=...
[3/7] scanning training worker processes
[4/7] sent SIGUSR1 graceful stop signal to worker pids=...
[5/7] waiting for graceful exit
[6/7] gracefully stopped ... 或 TERM/KILL 清理进程组
[7/7] cleaned workspace stop metadata
```

停止策略：

- 优先按 `launch_id` 找到 trainer worker 进程，并发送 `SIGUSR1`。
- 在 `grace-period` 内等待训练框架保存 checkpoint 并退出。
- 超时后对训练进程组发送 `SIGTERM`。
- 再超时后发送 `SIGKILL`。
- 最后按 `DLROVER_WORKSPACE_LAUNCH_ID` 扫描残留进程并清理。
- 删除兼容状态文件 `train.pid`、`train.pgid`、`train.launch_id`，并更新 `train.json`。

## Pod 内部 agentctl 参数

`agentctl` 是内部命令，由 `dlrover-workspace` 自动执行。一般不需要用户手动调用。

入口：

```bash
python -m dlrover.python.workspace.agentctl --log-dir /tmp/dlrover-workspace <subcommand>
```

通用参数：

| 参数 | 默认值 | 作用 |
| --- | --- | --- |
| `--log-dir` | `/tmp/dlrover-workspace` | 仅控制训练日志目录；status/stop 保留此参数以兼容旧命令，但不使用它定位状态。未指定时使用默认目录，指定时使用传入目录。 |
| `--state-dir` | `/tmp/dlrover-workspace-state` | Pod 内容器本地状态、锁、pid、pgid 和 launch_id 目录。自定义后，各管理命令必须指定相同值。 |
| `--cli-log` | `off` | 内部 CLI 日志开关，仅支持 `on`/`off`。由外层 `dlrover-workspace --cli-log` 自动透传，用于控制 Pod 内 `agentctl`、`process_manager` 的执行日志。 |

子命令参数：

| 子命令 | 参数 | 默认值 | 作用 |
| --- | --- | --- | --- |
| `launch` | `--cmd-b64` | 必填 | base64 编码后的用户训练命令。 |
| `launch` | `--launch-id` | 必填 | 本次 launch 的唯一 id，由外层 CLI 生成并传入所有目标 Pod。 |
| `launch` | `--wait-for-workers-seconds` | `5` | 启动后等待 trainer worker 进程出现的秒数，用于 launch 输出进程信息。 |
| `status` | `--wait-for-workers-seconds` | `0` | status 时等待 trainer worker 进程出现的秒数。 |
| `stop` | `--grace-period` | `300` | 同 `dlrover-workspace stop --grace-period`。 |
| `stop` | `--kill-timeout` | `30` | 同 `dlrover-workspace stop --kill-timeout`。 |
| `logs` | `--tail` | `100` | 输出最近 N 行日志。 |
| `logs` | `--follow`, `-f` | `false` | 持续跟随日志。 |

`dlrover-workspace` 会通过如下方式选择 Pod 内 Python：

```bash
python_bin=${DLROVER_WORKSPACE_AGENTCTL_PYTHON:-python}
```

如果镜像中 `python` 不可用，可以在容器环境中设置：

```bash
export DLROVER_WORKSPACE_AGENTCTL_PYTHON=python3
```

## 关键环境变量

DLRover master 注入的 Workspace 环境变量：

| 环境变量 | 作用 |
| --- | --- |
| `ELASTIC_JOB_NAME` | 当前 ElasticJob 名称，`dlrover-workspace` 默认用它查找 worker Pods。 |
| `NAMESPACE` | 当前 Pod namespace，`dlrover-workspace` 默认用它访问 Kubernetes API。 |
| `NODE_RANK` | 当前 worker rank，用于生成 rank 日志文件名。 |
| `NODE_NUM` | worker 节点数，用户命令可用于 `dlrover-run --nnodes=$NODE_NUM`。 |
| `DLROVER_MASTER_ADDR` | DLRover master 地址，`dlrover-run` 和 runtime monitor 使用。 |
| `DLROVER_WORKSPACE_MODE` | 标记当前容器为 Workspace 环境。 |
| `DLROVER_RUN_MODE` | 运行模式，Workspace 下固定为 `workspace`。 |
| `DLROVER_WORKSPACE_EXIT_POLICY` | Workspace 退出策略，值为 `retain` 或 `release`。 |
| `DLROVER_WORKSPACE_DEBUG_MODE` | 是否启用 release 调试兼容模式，默认 `false`。 |
| `DLROVER_WORKSPACE_DEBUG_READY_FILE` | 调试 setup 完成标记路径，供 rank 0 全量就绪检查使用。 |
| `DLROVER_WORKSPACE_ROLE` | 当前 worker 角色，通常为 `jupyter` 或 `worker`。 |
| `DLROVER_JUPYTER_SERVICE` | Jupyter Service 名称。 |
| `DLROVER_RUNTIME_MONITOR` | 是否启用 runtime monitor。 |

Workspace 训练管理环境变量：

| 环境变量 | 默认值 | 作用 |
| --- | --- | --- |
| `DLROVER_WORKSPACE_LAUNCH_ID` | 每次 launch 自动生成 | 标识本轮训练。`dlrover-run` 会将它拼入 `TORCHELASTIC_RUN_ID`，stop/status 也用它扫描进程。 |
| `DLROVER_WORKSPACE_USER_COMMAND` | launch 时写入 | Pod 内 runner 使用的原始训练命令。 |
| `DLROVER_WORKSPACE_MIRROR_STDOUT` | `true` | 是否将训练输出同时 tee 到容器 stdout。设置为 `false` 时只写 rank 日志。 |
| `DLROVER_WORKSPACE_CONTAINER_STDOUT` | `/proc/1/fd/1` | tee 到容器 stdout 的目标文件。 |
| `DLROVER_WORKSPACE_AGENTCTL_PYTHON` | `python` | Pod 内执行 `agentctl` 使用的 Python 命令。 |

runtime monitor 环境变量：

| 环境变量 | 默认值 | 作用 |
| --- | --- | --- |
| `DLROVER_RUNTIME_MONITOR_INTERVAL` | `20` | runtime monitor 上报资源使用情况的间隔，单位秒。 |
| `DLROVER_ACCELERATOR` | `nvidia.com/gpu` | runtime monitor 使用的加速器类型。 |

## 默认文件路径

`--state-dir` 默认为 `/tmp/dlrover-workspace-state`。目录下的文件如下：

| 文件 | 作用 |
| --- | --- |
| `train.json` | 当前训练状态，包含 state、launch_id、command、pid、pgid、log_file、exit_code、last_error 等。 |
| `train.lock` | Pod 内状态文件锁，防止 launch/stop/status 并发写状态。 |
| `train.pid` | 兼容文件，记录 `dlrover-run` 后台 launcher pid。 |
| `train.pgid` | 兼容文件，记录训练进程组 id。 |
| `train.launch_id` | 兼容文件，记录本轮 launch id。 |

`--log-dir` 默认为 `/tmp/dlrover-workspace`，仅保存训练日志：

```text
train.rank_<rank>_<YYYYMMDDTHHMMSSffffffZ>.log
```

时间戳为各 Pod 创建日志文件时的 UTC 微秒时间；rank 是 Pod 的 `NODE_RANK`。每次后台 launch 创建新文件，同次 launch 内的训练重启继续写该文件。重名时使用 `_1`、`_2` 等序号后缀，历史日志不会被覆盖。`train.json` 记录实际日志文件的绝对路径。

`logs` 未指定 `--log-dir` 时使用默认目录，指定时使用传入目录，并选择该目录中当前 rank 时间戳最新的日志文件。不使用状态中记录的路径覆盖日志目录选择。`--follow` 固定跟随选中的文件，不自动切换到下一次训练。兼容读取旧版 `train.rank_<rank>.log`。

不同作业应使用独立日志目录。不同 Pod 的状态目录必须彼此隔离，不应指向共享存储中的同一目录。更换日志目录不会改变同一状态目录中的训练身份或绕过重复启动检查。

旧版本的状态位于原日志目录。管理旧训练时传入 `--state-dir <旧log-dir>`；停止旧训练后，新 launch 可使用新默认布局。不自动扫描或搬迁旧状态。

`--foreground` 继续直接在 Pod 中执行命令，不经后台进程管理器，因此不创建上述状态文件或训练日志文件。

## 推荐用法

启动 Workspace：

```bash
kubectl apply -f examples/pytorch/workspace/jupyter_swift_elastic_job.yaml
kubectl get svc -n dlrover workspace-test-jupyter
```

在 Jupyter Terminal 中查看环境：

```bash
dlrover-workspace status
```

启动训练：

```bash
dlrover-workspace launch --cmd \
  "dlrover-run --nnodes=\$NODE_NUM --nproc_per_node=8 train.py"
```

跟随日志：

```bash
dlrover-workspace logs --follow
```

优雅停止训练并保留环境：

```bash
dlrover-workspace stop --grace-period 300 --kill-timeout 30
```

调试单个 rank：

```bash
dlrover-workspace status --ranks 0
dlrover-workspace logs --follow --ranks 0
dlrover-workspace stop --ranks 0
```

临时前台调试命令：

```bash
dlrover-workspace launch --foreground --ranks 0 --cmd "env | sort"
```

前台调试不会写 workspace 状态文件，不适合作为正式训练启动方式。
