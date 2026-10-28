# DLRover Workspace 空闲 Worker 误重拉与 Pending 退出问题分析

具体文件、调用链和兼容性改动范围参见
[DLRover Workspace 空闲 Worker 修复改动范围](./dlrover-workspace-idle-worker-fix-change-scope-cn.md)。

## 1. 问题现象

workspace 作业创建一组常驻 worker Pod 后，在没有训练任务运行的情况下，约
600 秒后 master 将 worker 判定为 `NoHeartBeat`，并创建相同数量的新 worker。
新 worker 可能因原 Pod 仍占用 CPU、内存或加速卡而长期处于 `Pending`。Pending
再次超过超时时间后，master 以 `PendingTimeout` 结束作业。

典型日志顺序如下：

```text
worker has not sent a heartbeat for over 600 seconds
status change: Running to Deleted, Exit reason is NoHeartBeat
Relaunch node worker-1 to 2
Scale the job by plan {"launch_nodes": [...]}
worker-2 exceeded pending timeout
Job exit with the reason PendingTimeout
```

## 2. 故障链路

```text
workspace worker 进入 Jupyter/idle 状态
  -> ElasticAgent 不再发送训练心跳
  -> master 未能识别该节点属于 workspace
  -> 按普通训练 worker 生成 NoHeartBeat/DELETED 事件
  -> 按 restartCount 创建替代 worker
  -> 原常驻 Pod 可能仍占用资源
  -> 替代 worker 无法调度并持续 Pending
  -> Pending 超时触发 master early-stop
```

master 自身的 heartbeat 成功只说明 master 服务存活，与 worker 的训练心跳无关。

## 3. 根因

### 3.1 workspace 元数据没有进入 master 中的当前节点对象

Pod watcher 会从 Kubernetes Pod 中读取 labels 和 annotations，并保存到事件节点的
`config_resource.kwargs`：

```python
resource.kwargs["pod_labels"] = labels
resource.kwargs["pod_annotations"] = annotations
```

但是 `DistributedJobManager` 将 Pod 事件合并到已有 `cur_node` 时，只更新 Pod
名称、创建时间、启动时间、宿主机和重拉次数等字段，没有同步 workspace 标识。

初始 `cur_node.config_resource` 来自 ElasticJob 的资源请求，只包含 CPU、内存和
加速卡。因此 `is_workspace_node(cur_node)` 返回 `False`，已有的“空闲 workspace
节点跳过 NoHeartBeat”逻辑没有生效。

原有测试通过手工向 `cur_node` 写入 workspace annotation 验证状态机，没有覆盖
Pod watcher 事件到 `cur_node` 的真实数据链路。

### 3.2 Pending early-stop 没有区分 workspace 空闲态和训练态

普通训练任务中，worker 长期 Pending 且 Running 数量低于最小训练节点数，说明
训练无法继续，master 以 `PendingTimeout` 退出是合理行为。

workspace 空闲态不同：Pod 生命周期与训练生命周期分离，当前没有训练需要推进。
此时因 worker Pending 结束 master 会破坏 workspace 的常驻和复用语义。

## 4. 修复原则

1. 只有明确带 `dlrover.io/run-mode=workspace` 或 `runtime` 的 worker 使用
   workspace 特殊逻辑。
2. 非 workspace 节点的心跳、Pending、节点不足和退出判断保持不变。
3. workspace 有活跃训练时，继续使用原有 NoHeartBeat、Pending 和节点不足容错。
4. workspace 空闲时，不因训练心跳缺失重拉 worker，也不因 worker Pending 或
   Running 数量不足结束 master。
5. 不用观测到的 Pod 资源覆盖配置资源，只同步 workspace 标识，避免影响资源优化。

## 5. 实现方案

### 5.1 使用作业级 run-mode，并同步节点标识

`K8sJobArgs` 从 `spec.runMode` 或 ElasticJob annotation
`dlrover.io/run-mode` 解析作业级 run-mode。`WorkspaceTrainingState` 使用该字段作为
workspace 作业的权威标识，避免尚未创建 Pod、Pod 事件丢失或事件到达顺序导致
节点被短暂识别为普通 worker。

处理 Pod `ADDED/MODIFIED` 事件时，从事件节点读取 run-mode，并写入 master 当前
节点的 `config_resource.kwargs["pod_labels"]`，作为节点级判断和兼容旧调用入口的
补充。仅支持 `workspace` 和 `runtime`，非 workspace 事件不改变当前节点。

配置资源中的 CPU、内存、加速卡和优先级继续保留，不用 Pod watcher 的实时资源
对象整体替换。

### 5.2 workspace 空闲态跳过训练型 Pending 早停

当以下条件同时成立时，跳过 worker Pending 和 worker 数量不足的 early-stop：

- 当前没有活跃 workspace 训练；
- 至少存在一个未释放 worker；
- 所有未释放 worker 都明确属于 workspace/runtime。

PS Pending、OOM 恢复节点 Pending 等原有判断不变。只要任一 workspace worker
通过 rendezvous restart 进入训练态，原 Pending 和节点不足判断立即恢复。

### 5.3 NoHeartBeat 行为

| 节点状态 | NoHeartBeat 行为 |
| --- | --- |
| 非 workspace worker | 保持原逻辑，超时后容错重拉 |
| workspace 空闲 worker | 跳过，不创建替代 Pod |
| workspace 训练中 worker | 保持原逻辑，超时后容错重拉 |

### 5.4 Pending 行为

| 作业状态 | Worker Pending 行为 |
| --- | --- |
| 非 workspace | 保持原 early-stop 逻辑 |
| workspace 空闲 | 不因训练型 Pending/节点不足结束 master |
| workspace 训练中 | 保持原 PendingTimeout 逻辑 |

## 6. 测试范围

- Pod 事件中的 workspace label 能同步到 `cur_node`。
- 同步元数据不会覆盖配置 CPU 等资源。
- 空闲 workspace worker 心跳超时不生成死亡事件。
- workspace 空闲时跳过 worker Pending 和节点不足 early-stop。
- workspace 进入训练态后恢复原 PendingTimeout 行为。
- 混合或非 workspace 节点不进入 workspace 特殊分支。

## 7. 运维验证

修复后，空闲超过心跳窗口应出现以下日志，而不应再出现 relaunch plan：

```text
Skip no-heartbeat relaunch for idle workspace node ...
Skip worker pending early-stop for idle workspace nodes.
```

如果训练已启动，应同时看到：

```text
Workspace training started by node ...
Workspace training finished on node ...
```

若新 Pod 仍然 Pending，应使用 `kubectl describe pod` 和 namespace events 确认
调度失败原因；资源不足、设备不足、配额和亲和性问题仍应按正常训练故障处理。
