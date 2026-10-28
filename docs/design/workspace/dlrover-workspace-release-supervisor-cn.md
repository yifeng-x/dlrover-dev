# DLRover Workspace Release Supervisor 设计

终态上报和 Master/PodScaler 收尾已按
[release 终态收尾优化](dlrover-workspace-release-finalization-cn.md) 更新。
本文件的 run/wait 职责继续适用；下文旧版同步等待和逐 Pod 超时描述由新设计替代。

## 背景

DLRover Workspace 的 `dlrover-workspace launch` 使用 agentctl 将训练进程放到后台，
提交成功后立即返回。retain 模式随后运行 Jupyter 或 `sleep infinity`，训练生命周期
与容器生命周期彼此独立，这是可重复调试场景需要的行为。

release 模式的目标不同：一次训练结束后释放 worker 资源，并在训练最终失败时保留
Kubernetes Failed Pod 供诊断。如果 release 继续使用 `sleep infinity`，即使
`dlrover-run` 已经非零退出，容器主进程仍然存活，Pod phase 仍是 Running。Master
只能删除这个 Running Pod，无法同时做到释放计算资源和保留失败现场。

Release Supervisor 用于在后台训练进程和容器主进程之间传递最终结果，使训练退出码
能够成为容器退出码。

## 目标

- release 模式不再使用 `idle-command` 或隐式 `sleep infinity`。
- 保持 `dlrover-workspace launch` 的异步提交语义，不要求 CLI 等待训练结束。
- 将后台训练进程的真实退出码可靠写入现有 Workspace state。
- 让 worker 容器按训练结果退出：成功为 0，失败为非 0。
- 训练失败时使 Pod 进入 Failed phase，释放 CPU、内存和 GPU 调度资源，同时保留
  Pod 对象、日志和退出原因。
- retain 模式的 Jupyter、idle 和多轮 launch 行为完全不变。
- 不新增 Master RPC、CRD 字段或另一套训练终态协议。

## 非目标

- Supervisor 不判断整个分布式任务是否成功。
- Supervisor 不创建、删除或重建 Pod。
- Supervisor 不替代 ElasticAgent 的重试和 `max_restarts`。
- Supervisor 不负责收集训练日志，日志仍由 WorkspaceProcessManager 管理。
- release 模式不支持在同一组 worker Pod 上执行多轮训练。

## 总体设计

Release Supervisor 提供两个内部角色：

```text
run   后台训练结果包装器
wait  worker 容器前台生命周期监督器
```

两者通过每个 worker 本地已有的 Workspace state 文件 `train.json` 通信：

```text
dlrover-workspace launch
        |
        v
WorkspaceProcessManager
        |
        +-- 后台启动 release_supervisor run
        |             |
        |             +-- 执行真实 dlrover-run/framework command
        |             +-- 写入 STOPPED/FAILED 和 exit_code
        |
容器前台 release_supervisor wait
        |
        +-- 轮询 train.json
        +-- 使用训练退出码结束
                      |
             +--------+--------+
             |                 |
          exit 0            exit != 0
             |                 |
       Pod 可正常结束       Pod phase=Failed
                              |
                     资源释放，Pod 保留
```

`run` 和 `wait` 必须分离：WorkspaceProcessManager 需要继续保持 launch 异步返回；
容器又必须有一个前台进程持续等待训练结果。让同一个进程同时承担两种角色会重新把
CLI 生命周期和训练生命周期耦合起来。

## 容器命令

retain 模式维持原命令：

```bash
<init-command>
<jupyter 或 idle-command>
```

release 模式下，PodScaler 为所有 worker 设置前台监督命令：

```bash
python_bin=${DLROVER_WORKSPACE_AGENTCTL_PYTHON:-python}
"$python_bin" -m dlrover.python.workspace.release_supervisor wait
```

如果配置了自动启动，rank 0 的完整命令语义为：

```bash
set -e
dlrover-workspace launch \
  --network-mode all \
  --framework-cmd "<framework command>"
python -m dlrover.python.workspace.release_supervisor wait
```

其它 rank 不执行 init-command，直接进入 `wait`，等待 rank 0 通过 pods/exec 在本机
启动训练。如果 rank 0 的 launch 预检或远程执行失败，`set -e` 使 rank 0 容器直接
失败；Master 保留该 Failed Pod 并释放其它等待中的 worker。

runtime monitor 可以继续包装上述命令。其 EXIT trap 只负责停止 monitor，不改变
Supervisor 的退出语义。

## 状态协议

Supervisor 复用 `WorkspaceStateStore` 和 `RunState`，不引入新的状态文件。

默认目录及文件：

```text
/tmp/dlrover-workspace-state/train.json
/tmp/dlrover-workspace-state/train.lock
```

涉及的字段：

| 字段 | 用途 |
| --- | --- |
| `state` | 本轮训练状态，Supervisor 关注 RUNNING、STOPPED 和 FAILED。 |
| `launch_id` | 标识训练轮次，防止旧进程覆盖新一轮状态。 |
| `command` | WorkspaceProcessManager 记录的实际训练命令。 |
| `pid` / `pgid` | 后台包装器的进程和进程组标识。 |
| `exit_code` | 真实训练退出码，也是前台 Supervisor 的退出依据。 |
| `last_error` | 非零退出或启动异常的简要诊断信息。 |

状态变化：

```text
IDLE -> RUNNING -> STOPPED(exit_code=0)
                -> FAILED(exit_code!=0)
```

写入时继续使用 `train.lock` 和原子 `os.replace`，避免 `run`、agentctl status/stop
并发访问时读取半写入 JSON。

## 函数设计

实现文件：`dlrover/python/workspace/release_supervisor.py`。

### `_normalize_exit_code(return_code)`

将 `subprocess` 返回值转换为合法进程退出码：

- 非负值原样保留，最大限制为 255。
- 信号终止返回负值，例如 `-9`，转换为 `128 + 9 = 137`。
- 该结果同时用于 state 中的 `exit_code` 和 Supervisor 最终退出。

### `_wait_for_launch_state(store, launch_id, timeout=10)`

等待相同 `launch_id` 的 RUNNING 状态写入，解决短任务竞态：

```text
错误时序：
后台进程快速失败并写 FAILED -> launch 主流程随后写 RUNNING -> FAILED 被覆盖

修正时序：
launch 主流程写 RUNNING -> 后台进程执行训练 -> 写最终状态
```

默认最多等待 10 秒。超时后仍继续执行训练，避免状态基础设施异常永久阻塞训练；最终
写入前还会再次校验 `launch_id`。

### `run_training(state_dir)`

作为 WorkspaceProcessManager 启动的后台包装器，职责如下：

1. 读取 `DLROVER_WORKSPACE_USER_COMMAND` 和
   `DLROVER_WORKSPACE_LAUNCH_ID`。
2. 等待本轮 RUNNING 状态完成落盘。
3. 使用 `/bin/bash -lc` 执行真实训练命令。
4. 获取并规范化训练退出码。
5. 加锁读取 state，并校验 `launch_id`。
6. 成功写 `STOPPED + exit_code=0`；失败写
   `FAILED + exit_code!=0 + last_error`。
7. 返回同一退出码。

如果 state 已属于另一个 `launch_id`，包装器忽略旧结果，避免过期进程污染当前训练。

### `wait_for_training(state_dir, poll_interval=0.2)`

作为 worker 容器的前台监督器，每 0.2 秒轮询 state：

| state | 行为 |
| --- | --- |
| state 文件不存在 | 继续等待 rank 0 发起 launch。 |
| IDLE/RUNNING/STOPPING | 继续等待。 |
| FAILED | 返回记录的非零退出码；缺少退出码时返回 1。 |
| STOPPED 且存在 `exit_code` | 返回该退出码。 |

`wait` 本身不设置超时。正常的自动启动超时由 rank 0 launch 处理；如果某个等待 worker
无法启动训练，rank 0 会失败，Master 随后删除其它仍在等待的 worker，避免单个
Supervisor 自行超时造成分布式任务状态分裂。

### `build_parser()`

定义内部命令行：

```bash
python -m dlrover.python.workspace.release_supervisor run \
  --state-dir <state-dir>

python -m dlrover.python.workspace.release_supervisor wait \
  --state-dir <state-dir>
```

`run` 由 WorkspaceProcessManager 使用，`wait` 由 PodScaler 生成，不作为最终用户需要
直接调用的 Workspace 命令。

### `main(argv=None)`

解析角色并调用 `run_training` 或 `wait_for_training`。模块入口通过
`raise SystemExit(main())` 将返回值转换为 Linux 进程和容器退出码。

## 与 WorkspaceProcessManager 的集成

`WorkspaceProcessManager._build_runner_command()` 根据
`DLROVER_WORKSPACE_EXIT_POLICY` 分流：

- retain：直接执行原始训练命令。
- release：后台执行 `release_supervisor run`，真实训练命令通过
  `DLROVER_WORKSPACE_USER_COMMAND` 环境变量传入。

日志镜像仍包裹最终的 runner command，因此训练输出继续写入 Workspace 日志和
容器 stdout。用户传入的 `--framework-cmd` 不需要增加任何 Supervisor 参数。

## 与 Master 和 PodScaler 的集成

ElasticAgent 仍使用现有 `SUCCEEDED_EXITED`、`FAILED_EXITED` 向 Master 报告训练终态。
Supervisor 不替代这条控制面通道，只负责数据面上的容器退出。

失败事件可能早于 kubelet 更新 Pod phase：

```text
ElasticAgent 上报 FAILED_EXITED
        |
Master 生成 release plan
        |
训练包装器写 FAILED
        |
前台 Supervisor 非零退出
        |
kubelet 将 Pod 更新为 Failed
```

因此 PodScaler 对报告 `FAILED_EXITED` 的 release worker 最多等待 30 秒：

- Pod 进入 Failed：跳过删除，保留诊断现场。
- 查询 Kubernetes API 失败：继续删除，防止资源泄漏。
- 超时仍是 Running/Pending：继续删除，防止 Supervisor 异常导致 GPU 泄漏。

等待时间可以通过 Master 环境变量
`DLROVER_WORKSPACE_FAILED_POD_WAIT_SECONDS` 调整。

成功 worker 不需要等待 Pod phase。Master 完成全局终态聚合后可以直接删除其 Pod；
如果未被提前删除，Supervisor 会以 0 退出。

## 退出码语义

| 场景 | state | Supervisor 退出码 | 预期 Pod 结果 |
| --- | --- | ---: | --- |
| 训练成功 | STOPPED | 0 | 正常退出或被 Master 删除 |
| 训练命令返回非零 | FAILED | 原始非零值 | Failed，保留 |
| 训练被 SIGKILL | FAILED | 137 | Failed，保留 |
| 执行训练命令发生异常 | FAILED | 1 | Failed，保留 |
| state 中 FAILED 但无退出码 | FAILED | 1 | Failed，保留 |
| init-command/launch 失败 | 可能尚无训练 state | shell 非零退出 | Failed，保留 rank 0 |

`max_restarts` 只控制 `dlrover-run`/TorchElastic 内部重试。只有重试耗尽、最外层训练
命令最终非零退出后，`run_training` 才会写 FAILED。

## 并发与幂等性

- 每个 worker 只读写自己的本地 state，不共享文件系统状态。
- state 写入受文件锁保护并采用原子替换。
- `launch_id` 防止旧包装器覆盖新状态。
- Master 的 `is_released` 保持单调，迟到的 Kubernetes 事件不能把已释放节点恢复为
  可重建状态。
- release 模式强制关闭 worker 自动扩缩容，并在节点状态流入口拒绝 relaunch，避免
  Supervisor 退出后产生新的 worker。
- Pod 的 `restartPolicy` 为 Never，保证非零退出转换为 Failed，而不是由 kubelet
  重启容器。

## 可观测性

训练失败后可以使用：

```bash
kubectl get pod <worker-pod>
kubectl logs <worker-pod>
kubectl describe pod <worker-pod>
```

预期现象：

- `STATUS` 为 `Failed` 或显示主容器非零退出。
- 容器 termination state 中包含真实退出码。
- Workspace 日志保留训练 traceback。
- Pod 不再占用 GPU、CPU 和内存调度资源。

如果 Master 日志出现“did not enter Failed pod phase”并删除 Pod，说明 Supervisor 未在
等待窗口内完成容器退出，应结合容器命令、state 文件和镜像中的 DLRover 版本排查。

## 改动范围

| 文件 | 改动 |
| --- | --- |
| `dlrover/python/workspace/release_supervisor.py` | 新增 run/wait 两种内部角色及退出码传递。 |
| `dlrover/python/workspace/process_manager.py` | release 下通过 run 包装器执行训练。 |
| `dlrover/python/master/scaler/pod_scaler.py` | release worker 使用 wait 前台命令；失败时等待 Pod phase 并决定保留或删除。 |
| `dlrover/python/master/node/dist_job_manager.py` | 保持 release 状态单调，禁止 relaunch 和自动扩缩容。 |
| `dlrover/python/master/main.py` | release 模式关闭全局 worker 自动扩缩容。 |
| `dlrover/python/tests/test_workspace_release_supervisor.py` | 覆盖成功、失败和 wait 退出语义。 |
| `dlrover/python/tests/test_workspace_process_manager.py` | 覆盖包装命令和真实失败码落盘。 |
| `dlrover/python/tests/test_pod_scaler.py` | 覆盖前台命令、Failed Pod 保留和等待超时删除。 |
| `dlrover/python/tests/test_job_manager.py` | 覆盖失败组释放、迟到事件、禁止重建和自动扩缩容。 |

## 测试与验收

### 单元测试

- `run` 执行成功命令，state 为 STOPPED、退出码为 0。
- `run` 执行失败命令，state 为 FAILED、退出码保持原值。
- `wait` 观察 FAILED 后返回相同非零退出码。
- 极短命令的终态不会被稍后的 RUNNING 写入覆盖。
- release worker 的容器命令包含 `release_supervisor wait`，不包含
  `sleep infinity`。
- FAILED_EXITED 早于 Pod phase 更新时，PodScaler 等待并保留 Failed Pod。
- 等待超时后删除仍然 Running 的 Pod，确保资源不泄漏。
- release worker 不触发 autoscale 或 relaunch。
- retain Workspace 的命令和生命周期回归测试保持通过。

### 集成验收

1. 训练成功：Master 最终状态为成功，不创建新 worker，全部 worker 资源释放。
2. 训练失败且 `max_restarts` 耗尽：失败 worker Pod 为 Failed 并保留，其余 worker
   删除，Master 最终状态为失败。
3. `kubectl logs` 能读取失败 traceback，`kubectl describe` 能看到真实退出码。
4. Failed Pod 不再占用 GPU 等可调度资源。
5. release YAML 即使误开 worker autoScale，也不会在终态后创建 `worker-1`。
6. retain 模式训练结束后仍保留容器，不受 Supervisor 逻辑影响。

## 部署约束

- Master 和 worker 镜像必须同时包含本设计版本的 DLRover。
- worker 镜像必须能通过所选 Python 解释器导入
  `dlrover.python.workspace.release_supervisor`。
- 更新代码后需要重新创建 ElasticJob/Pod；已有 Pod 的主命令不会动态变更。
- 建议 release YAML 显式设置 `autoScale: false`、`restartCount: 0` 和
  `restartPolicy: Never`，即使 Master 已提供相应保护，也便于配置审计。
