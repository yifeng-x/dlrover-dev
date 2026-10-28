# DLRover Workspace 空闲 Worker 修复改动范围

## 1. 文档目的

本文梳理 workspace 空闲 worker 被误判为 `NoHeartBeat`、替代 worker 长期
`Pending` 并导致 master 退出问题的代码改动范围、行为边界和验证范围。

问题原因、故障链路和总体解决方案参见
[DLRover Workspace 空闲 Worker 误重拉与 Pending 退出问题分析](./dlrover-workspace-idle-worker-relaunch-pending-cn.md)。

## 2. 改动目标

本次修改只调整 workspace/runtime 作业在空闲状态下的节点判断：

1. master 能可靠识别 workspace/runtime 作业和 worker。
2. 空闲 workspace worker 不因缺少训练心跳被重新拉起。
3. 空闲 workspace 不因 worker Pending 或训练节点不足而结束 master。
4. workspace 开始训练后，恢复原有 NoHeartBeat、PendingTimeout 和节点不足判断。
5. 非 workspace 作业的判断顺序、超时条件和运行流程保持不变。

## 3. 文件改动范围

### 3.1 作业参数

#### `dlrover/python/scheduler/job.py`

在 `JobArgs` 中新增：

```python
self.run_mode = ""
```

默认值为空，所有非 Kubernetes 或未配置 run-mode 的作业保持非 workspace 行为。

#### `dlrover/python/scheduler/kubernetes.py`

`K8sJobArgs.initilize()` 从以下位置解析 run-mode：

1. `spec.runMode`；
2. `metadata.annotations["dlrover.io/run-mode"]`。

`spec.runMode` 优先，解析结果统一转换为小写。只有 `workspace` 会被 workspace
状态管理识别；旧值 `runtime` 不再支持，其原有无 Jupyter、保留 Pod 语义使用
`workspace + jupyter-enabled=false + workspace-exit-policy=retain` 表达。

### 3.2 Workspace 状态管理

#### `dlrover/python/master/node/workspace_state.py`

新增或调整以下职责：

- 统一读取节点 label/annotation 中的 `dlrover.io/run-mode`。
- 判断节点是否属于 `workspace` 或 `runtime`。
- 将 Pod watcher 事件中的 workspace 标识同步到 master 当前节点。
- 保存作业级 workspace 标识，作为 Pod 尚未创建或事件尚未到达时的权威兜底。
- 判断空闲 workspace worker 是否跳过 NoHeartBeat 重拉。
- 判断空闲 workspace 是否跳过 worker Pending early-stop。

同步节点元数据时只写入 run-mode，不整体替换 `config_resource`。以下配置继续使用
master 原有值：

- CPU；
- 内存；
- GPU/NPU 类型和数量；
- priority；
- 资源优化相关扩展字段。

### 3.3 Master 节点管理

#### `dlrover/python/master/node/dist_job_manager.py`

改动分为三处。

第一处是在初始化 `WorkspaceTrainingState` 时传入 `job_args.run_mode`：

```text
ElasticJob run-mode
  -> K8sJobArgs.run_mode
  -> DistributedJobManager
  -> WorkspaceTrainingState
```

第二处是在处理 Pod `ADDED/MODIFIED` 事件时，将事件节点的 workspace 标识同步到
`cur_node`。这修复了 watcher 已读取 metadata、但 master 当前节点没有保存 metadata
的问题。

第三处是在 `should_early_stop()` 中增加空闲 workspace 判断：

- PS Pending 判断仍然优先执行；
- 空闲 workspace 跳过 worker Pending hang 判断；
- 空闲 workspace 跳过 all-reduce worker 数量不足判断；
- workspace 有活跃训练时执行原有判断。

### 3.4 测试

#### `dlrover/python/tests/test_workspace_state.py`

新增以下覆盖：

- workspace label 和 annotation 识别；
- Pod 元数据同步；
- 同步时不覆盖配置 CPU；
- 作业级 run-mode 识别；
- 空闲 workspace 跳过 Pending early-stop；
- 活跃 workspace 和混合节点不跳过。

#### `dlrover/python/tests/test_job_manager.py`

新增以下覆盖：

- 模拟 Pod watcher 事件进入 `DistributedJobManager`；
- 验证事件中的 workspace label 能进入 `cur_node`；
- 验证空闲 worker 心跳超时不生成死亡事件；
- 验证空闲 workspace 不调用 worker Pending 和节点不足判断；
- 验证 workspace 开始训练后仍返回 `PendingTimeout`。

#### `dlrover/python/tests/test_job_params.py`

新增以下覆盖：

- 普通 ElasticJob 的 `run_mode` 默认为空；
- workspace annotation 能解析为小写的 `workspace`。

## 4. 运行流程变化

### 4.1 Workspace 空闲态

```text
解析 ElasticJob run-mode
  -> master 将作业标记为 workspace/runtime
  -> Pod 事件补充节点级 run-mode
  -> worker 没有训练心跳
  -> 跳过 NoHeartBeat 重拉
  -> 跳过 worker Pending/节点不足 early-stop
  -> Pod 和 master 保持运行
```

### 4.2 Workspace 训练态

```text
worker 上报 workspace rendezvous restart
  -> 节点加入 active training nodes
  -> NoHeartBeat 对 active worker 生效
  -> worker Pending 和节点不足判断生效
  -> 训练结束后上报 finished
  -> 节点回到 workspace 空闲态
```

### 4.3 非 Workspace 作业

```text
run_mode 为空或不是 workspace/runtime
  -> workspace 空闲判断返回 False
  -> PS Pending 判断保持不变
  -> worker Pending 判断保持不变
  -> all-reduce 节点不足判断保持不变
  -> NoHeartBeat 和重拉流程保持不变
```

## 5. 明确不在改动范围内的内容

本次没有修改：

- worker 心跳上报周期；
- 默认 600 秒 NoHeartBeat 窗口；
- Pending 超时时间和 pending fail strategy；
- `restartCount` 和最大重拉次数；
- scaler 创建、删除 Pod 的通用实现；
- Kubernetes 调度器、配额、亲和性和设备分配逻辑；
- PS Pending 和 OOM 恢复节点 Pending 判断；
- 非 workspace 节点状态机；
- workspace CLI launch、stop、logs 和网络组网参数；
- rendezvous 协议和训练容错策略。

## 6. 兼容性边界

| 场景 | 修复后行为 | 是否改变原流程 |
| --- | --- | --- |
| 普通 worker 心跳超时 | 继续重拉 | 否 |
| 普通 worker Pending 超时 | master 继续失败退出 | 否 |
| 普通 all-reduce 节点不足 | 继续 early-stop | 否 |
| 空闲 workspace worker 心跳超时 | 跳过重拉 | 是，符合 workspace 语义 |
| 空闲 workspace worker Pending | 不结束 master | 是，符合 workspace 语义 |
| 训练中 workspace worker 心跳超时 | 继续重拉 | 否 |
| 训练中 workspace worker Pending 超时 | master 继续失败退出 | 否 |
| workspace PS Pending | 保持原判断 | 否 |
| 未知 run-mode | 按普通作业处理 | 否 |

## 7. 风险控制

### 7.1 防止误伤非 Workspace 作业

特殊逻辑只在满足以下任一条件时启用：

- 作业级 run-mode 明确为 `workspace` 或 `runtime`；
- 所有未释放 worker 的节点级 metadata 明确标记为 workspace/runtime。

普通作业默认 `run_mode=""`，不会进入该分支。

### 7.2 防止 Workspace 训练故障被忽略

worker 上报 rendezvous restart 后会进入 active training nodes。只要 workspace
训练处于 active 状态，Pending 和节点不足 early-stop 不会被跳过；active worker
的 NoHeartBeat 也不会被跳过。

### 7.3 防止资源配置被污染

Pod watcher 事件只同步 run-mode，不覆盖配置资源，避免 Pod 实际资源信息影响资源
优化、OOM 调整或后续重拉计划。

## 8. 验证与发布建议

当前已完成：

- workspace 状态单元测试；
- 修改文件 AST 语法解析；
- `git diff --check`。

发布前建议在包含完整依赖的 CI 环境执行：

```bash
python -m pytest dlrover/python/tests/test_workspace_state.py
python -m pytest dlrover/python/tests/test_job_params.py
python -m pytest dlrover/python/tests/test_job_manager.py
```

集群验证至少覆盖：

1. workspace 创建后空闲超过两个 600 秒窗口，Pod 数量不增加且 master 不退出；
2. workspace 启动训练后模拟 active worker 心跳丢失，确认仍执行容错重拉；
3. workspace 训练中制造替代 worker Pending，确认仍按 PendingTimeout 处理；
4. 提交普通 all-reduce 作业，确认 NoHeartBeat 和 PendingTimeout 行为不变；
5. 检查日志中出现空闲跳过信息，且没有对应的 relaunch scale plan。

预期空闲日志：

```text
Skip no-heartbeat relaunch for idle workspace node ...
Skip worker pending early-stop for idle workspace nodes.
```
