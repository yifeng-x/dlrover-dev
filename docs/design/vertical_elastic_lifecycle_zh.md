# 垂直弹性退出与 Worker 替换说明

本文说明本次垂直弹性生命周期修改，包括 UCP 转换、旧训练进程退出、旧 Pod 清理及新 Worker 启动。实现沿用现有阶段和 RPC，不新增独立状态系统，也不引入自动回滚机制。

## 一、问题背景

原实现存在两个直接关联的问题：

1. 垂直弹性主动停止训练进程后，监控结果可能为 `WorkerState.FAILED`。原代码将其改为 `SUCCEEDED`，但没有补齐各个 global rank 的返回值，导致 PyTorch 在记录事件时抛出 `ValueError: Unknown worker: 0`，最终使 agent 异常退出。
2. 禁止 relaunch 后，旧 Pod 仍可能处于 `Failed` 或 `Succeeded`。原替换逻辑只从存活节点中生成删除计划，导致已经退出的旧 Worker 被遗漏。

此外，原 master 等待的是历史最新 UCP 任务，未明确绑定本轮选择的 checkpoint；旧任务成功可能提前放行新一轮替换。

## 二、需要区分的对象和状态

| 对象或状态 | 含义 |
| --- | --- |
| 训练进程 | 执行训练代码的子进程，可以在保存 checkpoint 后退出 |
| Agent | 监督训练进程、维护 checkpoint 保存线程并与 master 通信的进程 |
| Pod | 容纳 agent 和训练进程，由 master 发起删除 |
| `WorkerState` | PyTorch agent 对本地训练进程组的监控状态 |
| `NodeStatus` | Master 根据 Pod 事件维护的节点状态 |
| `VERTICAL_ELASTIC_EXITED` | 旧 agent 完成计划性停止后上报的事件 |

调用 `_graceful_exit_workers()` 只是发送信号，不代表所有训练进程已经退出。调用 `_stop_workers()` 停止训练进程组，也不等同于删除 Pod。

计划性退出返回 `SUCCEEDED` 表示旧 agent 完成了退出职责，不表示整个训练任务成功，也不表示新训练已经恢复。

## 三、修改后的执行流程

### 1. 固定本轮旧节点集合

Master 在进入 `SAVE_CHECKPOINT` 前记录旧 Worker ID 集合。后续退出识别和删除计划均以该集合为依据，不再根据退出后的存活状态重新筛选。

### 2. 通知训练进程优雅退出并提交 checkpoint

Agent 向最低 local rank 对应的训练进程发送 `SIGUSR1`，并触发现有 checkpoint 保存逻辑。

Agent 保持存活，使 checkpoint 保存线程能够继续工作。训练程序如何响应信号、如何协调其他 rank，由训练侧 callback 实现。

Rank 0 使用成功保存的 checkpoint 提交 UCP 任务，并向 master 上报：

- 当前 `plan_id`；
- checkpoint 目录；
- checkpoint step。

UCP 模式下，没有可用 checkpoint、没有有效 UCP 任务或缺少 DeepSpeed checkpoint saver，均按失败处理，不自动降级为冷启动。

显式使用非 UCP 模式时，保留既有冷启动行为。

### 3. 等待本轮 UCP 成功

Master 根据本轮上报的目录和 step 查找 UCP 任务，保存对应的 task ID，并等待该任务成功。

历史其他任务的成功不会直接放行当前计划。任务成功后，master 将其输出选为恢复 checkpoint。

Agent 此时轮询当前计划阶段，等待替换授权。UCP 服务仍通过 master 上报任务状态，不直接向旧 agent 推送通知。

### 4. 确认旧训练进程退出

Master 进入 `REPLACING_WORKERS` 后，agent 开始有限时的退出确认：

- 训练进程已经退出：不再调用 `_stop_workers()` 强杀；
- 宽限期结束仍未退出：调用 `_stop_workers()` 兜底；
- 完成停止后：上报 `VERTICAL_ELASTIC_EXITED`。

Agent 返回的成功结果包含所有本地 Worker 的 global rank，满足 PyTorch 事件记录要求，避免再次触发 `Unknown worker`。

计划性退出统一在普通训练成功或失败处理之前判断，避免不同退出码进入不同的任务结束流程。

### 5. 清理旧 Pod 并创建新 Worker

Master 等待旧 agent 的退出确认或 Pod 终态。超过等待期限仍未响应时，继续删除本轮旧 Pod，避免永久阻塞替换。

删除集合包含本轮选中的 Failed、Succeeded 和 critical Worker；已退出不再是漏删原因。这里对 critical Worker 的处理仅用于明确的垂直替换，不改变普通缩容策略。

新 Worker 的训练 rank 从 0 连续分配，节点 ID 继续递增。替换数量以本轮旧节点集合大小为准。

### 6. 新 Worker 配置确认

新 agent 根据目标卡数设置 `nproc_per_node`。即使原配置已经正确，也上报配置就绪，避免 master 无谓等待。

UCP 成功不会绕过 rendezvous 的显式阻塞条件。

## 四、计划性退出与失败处理

本轮旧节点的终态事件不会触发普通故障 relaunch、失败统计或任务成功回调；新 Worker 继续使用普通故障处理。

| 场景 | 处理方式 |
| --- | --- |
| 旧训练进程自然退出 | 回收退出结果，完成计划性退出 |
| 旧训练进程超过宽限期仍未结束 | 强制停止残留训练进程 |
| Agent 等待替换授权超时 | 上报失败并抛出异常 |
| 本轮 checkpoint 缺失或 UCP 最终失败 | 计划失败，由 master 结束任务 |
| UCP 成功，但旧 agent 未按时确认退出 | Master 在 drain 超时后清理旧 Pod |
| 旧 Pod 已变为 Failed/Succeeded | 仍按本轮节点集合删除 |
| 新 Worker 发生真实故障 | 保留普通故障恢复逻辑 |

本次采用明确失败策略：进入替换流程后出现 checkpoint/UCP 失败，不静默从头训练。尚未实现按旧资源配置自动回滚。

## 五、超时配置

| 环境变量 | 配置位置 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `DLROVER_VERTICAL_EXIT_TIMEOUT` | 训练 Worker/agent | 1800 秒 | 完成 checkpoint 提交流程后，等待 master 授权替换的最长时间 |
| `DLROVER_VERTICAL_EXIT_GRACE_SECONDS` | 训练 Worker/agent | 30 秒 | 获得替换授权后，等待训练进程自然退出的宽限时间 |
| `DLROVER_VERTICAL_DRAIN_TIMEOUT` | Master | 60 秒 | 删除旧 Pod 前，等待旧 agent 退出确认的最长时间 |

配置时应满足：

- Master 的 drain 时间大于 agent 的退出宽限时间，并预留轮询和 RPC 延迟。
- Agent 等待替换的时间与 master 现有 checkpoint/UCP 等待上限协调设置。
- UCP 输入及输出存储应独立于旧 Pod 生命周期，旧 Pod 删除后仍可访问。

## 六、修改文件

以下路径均相对于仓库根目录。

| 文件 | 主要修改 |
| --- | --- |
| `dlrover/python/elastic_agent/torch/training.py` | 退出结果补齐、等待替换授权、自然退出确认及强制停止兜底、配置就绪上报 |
| `dlrover/python/master/node/job_resource_scaler.py` | 旧节点快照、本轮 UCP 绑定、恢复点选择、退出确认等待 |
| `dlrover/python/master/node/worker.py` | 按固定旧节点集合生成替换计划 |
| `dlrover/python/master/node/dist_job_manager.py` | 计划性替换节点识别、禁止误重建、替换失败终止 |
| `dlrover/python/master/node/job_manager.py` | 增加默认的计划性替换判断接口 |
| `dlrover/python/master/node/event_callback.py` | 避免旧节点终态触发普通任务成功或失败处理 |
| `dlrover/python/master/elastic_training/ucp/task_manager.py` | 按 checkpoint/task 查询任务、选择本轮恢复点 |
| `dlrover/python/master/elastic_training/rdzv_manager.py` | UCP 成功后仍检查显式阻塞 |
| `dlrover/python/master/servicer.py` | 移除 checkpoint 上报时提前清空 rendezvous 等待节点的逻辑 |
| `dlrover/python/tests/test_elastic_training_agent.py` | 更新退出回归测试 |
| `dlrover/python/tests/test_vertical_elastic_lifecycle.py` | 新增生命周期专项回归测试 |
| `docs/design/vertical_elastic_lifecycle.md` | 英文说明文档 |
| `docs/design/vertical_elastic_lifecycle_zh.md` | 独立中文说明文档 |

## 七、测试结果

本次在本地环境执行的结果：

- 专项测试与资源替换测试：16 项通过，另有 3 个子测试通过。
- 扩展回归：175 项通过、15 项失败、4 项跳过，另有 3 个子测试通过。
- 使用修改前代码副本执行对照：最终保留的 15 个失败项在修改前也存在，未发现新增失败项。

专项覆盖包括：

- 真实 PyTorch `agent.run()` 的 Worker 事件记录路径；
- 计划性 `FAILED`、`UNHEALTHY`、`SUCCEEDED` 的统一处理；
- 自然退出不强杀、退出超时兜底；
- checkpoint 缺失、UCP 失败及等待超时；
- 历史 UCP 成功不能放行新计划；
- Failed/Succeeded/critical 旧节点清理；
- 计划内旧节点不误触发 relaunch 或任务结束；
- 已满足目标进程数时仍上报 ready。

可在具备项目依赖的环境运行专项测试：

```bash
python -m pytest -q \
  dlrover/python/tests/test_vertical_elastic_lifecycle.py \
  dlrover/python/tests/test_job_resource_scaler.py
```

## 八、部署要求与验证边界

Master 和训练 Worker 应同步升级。本轮 checkpoint 上报及等待语义已调整，不应将旧 agent 与新 master 混用后直接视为兼容。

本次尚未完成真实 Ascend/Kubernetes 联调。部署验证应覆盖连续两轮扩缩卡、UCP 长时间转换、旧 Pod 异常退出以及新 Worker 恢复训练，并确认实际恢复 step 与本轮选择的 checkpoint 一致。

现有 `COMPLETED` 阶段仍表示替换配置已就绪，不保证新训练已经完成首个恢复后的 step。本次没有新增训练进度确认、master 重启后计划恢复或自动回滚机制。
