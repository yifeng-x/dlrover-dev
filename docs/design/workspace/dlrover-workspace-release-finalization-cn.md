# Workspace release 终态收尾优化

## 问题与范围

本设计替代原 exit-policy / release-supervisor 文档中“终态 RPC 内同步执行
ScalePlan、提前标记节点 Failed 和已释放、逐 Pod 等待 30 秒”的实现。
仅调整 Workspace release 收尾，沿用现有终态 RPC、Pod watcher 和 Master 主循环。
不新增 CRD、公开 Job 状态或后台收尾线程。

原实现存在循环依赖：Master 在 FAILED_EXITED RPC 内等待 Pod Failed，而 worker
必须等该 RPC 返回，才能退出 dlrover-run、触发 Supervisor 结束容器。同时，Master
提前设置逻辑 Failed / is_released，可能在 RPC 完成前关闭服务。worker 随后的通用
长重试拖延退出，finally 中抛出的通信异常又覆盖了原始训练异常。

## 目标时序

```text
训练最终失败，保留原始异常
  -> worker 上报 FAILED_EXITED
  -> Master 记录 reported_status，立即返回 ACK
  -> worker 完成本地收尾，传播原始训练异常
  -> dlrover-run 非零退出
  -> Supervisor 写最终 state，以非零码结束容器
  -> Kubernetes Pod Failed
  -> Master 确认并保留 Failed Pod，清理其余 worker
  -> 全部资源处理完成，Master 优雅关闭 RPC 后失败退出
```

Master 接受上报不代表容器已经退出。成功路径同样等待自然退出及 Pod 清理确认。

## 状态边界

| 信息 | 更新来源 | 含义 |
| --- | --- | --- |
| reported_status | worker 终态 RPC | 训练上报结果 |
| status | Pod watcher / 成功的 Kubernetes 查询 | 实际 Pod 生命周期 |
| relaunchable | release 策略 | 是否允许重建；release worker 禁止重建 |
| is_released | 收尾确认 | Failed Pod 已保留，或待清理 Pod 已确认不存在 |

不因一个 worker 失败而批量改写其它 worker 的物理状态。全局失败结果单独保留，
失败一旦发生不能回退为成功。worker 集合在节点初始化时固定，删除或释放节点不会
缩小本轮预期集合。已观察到的成功结果保留，用于处理上报丢失后 Pod 成功并被删除。

## worker 上报与清理

release 最终事件直接走单次 transport 调用，不嵌套普通 RPC 的十次重试：

- 最多尝试 3 次，每次请求超时 2 秒；间隔 0.5 秒。
- ACK 丢失时可以重试，Master 幂等记录事件。
- 全部失败返回空结果并记录警告，不向外抛出通信异常。
- gRPC 和 HTTP 支持该路径，其它普通 RPC 保留原有重试流程。
- release 的最终事件记录、rendezvous shutdown、executor 和 monitor 清理分别隔离
  异常，保证后续清理仍执行，原始训练异常仍能传播。

Supervisor 仍等待真实外层命令退出后再写最终状态。不提前发布可导致容器退出的
FAILED 状态，以免截断 traceback 和清理工作。容器退出码是外层 dlrover-run 的
退出码，不保证与 trainer 子进程退出码相同；原始 traceback 和子进程错误信息仍需保留。

## Master 收尾

终态 RPC 不执行 Kubernetes I/O，也不调用阻塞的 scale。release 专用的 Pod 事件
处理在锁内合并现有节点，避免旧事件覆盖已经确认的上报或释放标记。心跳超时属于
逻辑失败，不能作为 Pod 已退出的证据。
启动回调继续执行，保留 rendezvous alive-node 注册；仅终态回调由 release 收尾替代，
避免终态回调直接提前停止 Master。

Master 主循环每秒推进一次 `reconcile_workspace_release()`：

1. 尚未失败且尚未全部完成时，继续训练监控。
2. 任一最终失败、实际 Pod 失败或主动停止时开始失败收尾；全部上报成功或通过 Pod
   终态确认完成时开始成功收尾。
3. 记录整组唯一的单调时钟起点，关闭节点重建。
4. 在 JobManager 锁外执行一轮有界的 Kubernetes 查询和清理，RPC 可以继续返回。
5. 只有实际确认 Failed Pod 保留或 Pod 不存在后，才标记资源处理完成。
6. 返回 None 表示继续运行/收尾，True 表示成功，False 表示失败。

release 不再走普通 Master 中可能提前退出的 all_workers_exited、任务完成等分支。
显式停止和 early-stop 也先进入收尾。收尾期间不再将正常减少的活跃 worker 数误判为
训练资源不足。最终关闭 RPC 使用 5 秒宽限期，并等待在途请求结束。

## Pod 清理与超时

每轮批量 LIST 本任务 Pod。绕过会将 API 重试失败转换为 None 的便捷接口，使用带
请求超时的原始 Kubernetes API，明确区分查询失败与 Pod 不存在。

| 查询结果 | 处理 |
| --- | --- |
| Failed | 保留，标记资源处理完成 |
| Succeeded | 请求删除；下一次成功查询确认不存在后才完成 |
| Running / Pending | 宽限期内等待；超时后请求删除 |
| Pod 不存在 | 无在途创建时确认完成 |
| API 查询失败 | 保持待处理，不删除、不宣称释放完成 |
| 已带 deletionTimestamp | 等待实际删除完成 |

删除附带 UID 和 resourceVersion 前置条件，防止 LIST 后 Pod 转为 Failed 或同名 Pod
被替换时仍被删除；冲突后下一轮重新查询。创建队列出队和 Future 注册与清理检查
同步，存在在途创建或重试队列时不能把暂时不存在的 Pod 当作已清理。

整组正常退出宽限期默认 60 秒，通过 Master 环境变量
`DLROVER_WORKSPACE_RELEASE_GRACE_SECONDS` 调整；非法值回退为 60 秒。
旧的逐 Pod 等待参数 `DLROVER_WORKSPACE_FAILED_POD_WAIT_SECONDS` 不再使用。
宽限期到达后执行清理兜底，再允许 60 秒确认删除完成。重复 RPC 不重置截止时间。
每轮清理不逐 Pod sleep，限制一轮发起删除的时间窗口，API 调用自身也配置超时。

超过总期限仍有未处理 Pod，Master 失败退出并输出未完成 Pod 名称，不把它们标记
为已释放。API 故障、finalizer 卡住等情况下资源可能仍在，需运维继续回收；本次不
新增持久化清理控制器。保留的 Failed Pod 仍受 ElasticJob 删除级联和集群 GC 管理。

## 改动范围

| 文件 | 内容 |
| --- | --- |
| elastic_agent/master_client.py | release 最终上报使用独立短重试，原始异常不被 RPC 错误替代 |
| elastic_agent/torch/training.py | release 本地最终清理异常隔离 |
| master/node/dist_job_manager.py | 分离训练结果与物理状态；周期推进收尾；不在 RPC 内释放 |
| master/scaler/pod_scaler.py | 批量查询、保留 Failed、条件删除和删除完成确认 |
| master/dist_master.py | release 收尾控制正常退出，优雅停止 RPC |
| tests/test_workspace_release_lifecycle.py | 上报、Master 收尾和 Kubernetes 竞态测试 |
| 现有 JobManager / PodScaler / ElasticAgent 测试 | 更新收尾预期并验证模式隔离 |

## 验收

- 失败上报不会提前改变实际 Pod 状态或触发 Kubernetes 清理。
- Master 不可用时，终态上报经过有限次尝试后结束；原始训练异常继续传播。
- ACK 丢失、重复上报不重复计数，不重置共享截止时间。
- worker 最终上报丢失时，可以通过实际 Pod Failed/Succeeded 确认结果。
- Failed Pod 被保留；删除返回成功但 Pod 仍存在时 Master 继续等待。
- API 不可用不触发盲目删除，清理超时明确输出未完成资源。
- LIST 后 Pod 状态变化导致条件删除冲突时，下一轮重新判断。
- 同步或多 worker 退出时，等待不按节点数量串行叠加。
- retain 和普通训练不进入 release 专用收尾路径。

部署需同时更新 Master 与 worker 的 DLRover 版本，并重新创建任务。验证需包含
真实 Kubernetes 集群中的脚本失败、短成功任务、多 worker 失败与删除延迟场景。
