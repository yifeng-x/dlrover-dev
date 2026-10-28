# 垂直弹性结构化事件

## 范围与投递方式

Master 使用 `VerticalElasticEvent {JSON}` 结构化日志记录一次垂直弹性调整。
复用现有日志采集链路，不增加消息队列、数据库或控制状态机。
事件上报及序列化失败仅记录 warning，不影响资源调整、收益门禁及恢复确认。
默认 sink 只写本地日志，不在控制路径调用外部 HTTP。

该实现是尽力投递，不保证跨崩溃无遗漏，也不提供持久化重试。
事件不是探索 journal 的替代物；探索恢复和防止重复执行仍以现有 journal 为准。

## 统一字段

| 字段 | 含义 |
| --- | --- |
| `schema_version` | 当前为 1 |
| `event_id` | 每条事件的 UUID；采集链路重投同一条日志时保留原值，用于去重 |
| `event_type` | 事件类型 |
| `occurred_at` | 事件产生时间，Unix 秒 |
| `source` | 当前为 dlrover-master |
| `job` | job_uid、job_name、namespace、cluster；测试或缺少作业上下文时可能不完整 |
| `swanlab_task_id` | 独立 scalar 客户端配置的任务 ID，未配置为 null |
| `request_id` | 关联请求与计划；无请求上下文或恢复原计划时允许为空 |
| `plan_id` | 一次资源调整的标识，日期/序号后统一增加 UUID，普通模式同样生效 |
| `root_plan_id` | 原探索计划 ID，原调整与回退共用 |
| `parent_plan_id` | 回退计划指向原计划；普通计划为空 |
| `action` | add_gpus、reduce_gpus、manual 等控制动作；无法确定时为 unknown |
| `trigger_source` | rpc、configmap、exploration_rollback、master_recovery、unknown |
| `started_at` | 当前事件上下文建立时间；Master 恢复场景不冒充原计划开始时间 |

RPC 入口不推测调用者一定是 Brain，故记录 rpc；手工 ConfigMap 路径明确记录 configmap。
通过阶段时间计算 `previous_phase_duration_seconds`，终态给出 `elapsed_seconds`。
这些是事件上下文的时间，不替代已有 monotonic 时钟统计的 Prometheus 时长。

## 事件类型及产生位置

| 类型 | 产生时机 | 语义 |
| --- | --- | --- |
| requested | 请求被接受或拒绝 | 尚未关联计划，plan_id 为 null；忙碌拒绝不得串到活动计划 |
| phase_changed | 建立计划或推进阶段 | 保留阶段；恢复观测使用 resumed，停止探索使用 suspended |
| evaluated | 收益门禁判断完成，包括 off 模式 | 区分收益结论与实际执行决策 |
| control_completed | checkpoint/替换/参数调整控制流程完成 | 恢复状态仍可能 pending，不代表训练恢复 |
| training_recovered | 现有进度确认逻辑判定恢复 | 指定替换 worker 的两次有序进度报告越过 checkpoint 基线 |
| exploration_decided | 接受、保留、请求回退、回退确认或探索失败 | 包含观测后估算及整体/回退收益 |
| finished | 跳过、失败、恢复确认/超时或探索决策终态 | 同一进程中的同一计划上下文只产生一次 |

普通计划控制完成后等待恢复确认，恢复超时或无法确认时为 unverified。
探索计划等待探索决策终态；其恢复超时不会提前宣称探索结束。
原计划请求回退时以 rollback_requested 结束，final 暂为空。随后独立回退计划通过
root_plan_id/parent_plan_id 关联，回退终态记录最终已知资源。
新计划开始或旧恢复回调晚到时，旧恢复结果使用保存的原计划上下文。

请求结果日志可能晚于工作线程内的首个阶段日志到达，消费者应按 request_id 关联，不能依赖文件到达顺序。
重启恢复会产生新的事件 ID，不保证相同业务状态跨重启只上报一次。

## 资源四态

`resources` 包含 before、target、actual_after、final，每个已知快照结构为：

```json
{
  "workers": 2,
  "gpus_per_worker": 4,
  "total_gpus": 8,
  "gpu_resource_type": "nvidia.com/gpu",
  "cpu_per_worker": 8,
  "memory_mb_per_worker": 32768
}
```

- before：收益判断使用的当前资源快照。
- target：计划期望的目标资源。
- actual_after：首次从指定替换 worker 状态确认到的资源配置，不用 target 补值。
- final：结束时再次读取的资源配置；未知或尚待回退则为 null。

观测要求指定 worker 全部 Running、未释放且资源配置一致。
缺节点、未运行或混合资源以 resource_observation.status 表达，资源值保留 null。
matches_target 表示观测配置是否等于目标。basis=dlrover_worker_state 表示读取
DLRover 已观测到的 worker 配置，并不是 GPU 设备的独立硬件验收。
若后续节点状态丢失，已知 actual_after 保留，final 仍可为空。
Master 恢复探索时未持久化的替换节点 ID 不会被猜测，因此资源观测可能仍为空。

## 收益与执行结果

`benefit` 保存判断时的固定快照：

- mode、objective、rule_version、min_gain_ratio。
- evaluation_status：positive、negative、unknown；off 模式为 not_evaluated。
- decision：allow、reject、explore。
- reason_code、estimates、evidence_request_id。
- evidence：白名单内的训练窗口、工作量、切换预算、候选性能、时限及调度影响信息。
- estimate_kind=prediction。

shadow 模式可能同时为 negative 和 allow；未知候选吞吐可能同时为 unknown 和 explore。
off 模式不沿用上一次评估结果。非有限测量值转换成 null，避免 NaN 导致整条拒绝事件无法输出。
不记录 HTTP token、完整 SwanLab 历史序列或证据服务任意附加字段。

`execution_status` 与 `recovery_status` 独立记录。恢复事件提供 node_id、step、timestamp
及 recovery_basis=replacement_worker_progress。generation_before/after 是 Master 可提供的代次；
原生进度上报没有逐点 generation，因此不把这种恢复依据描述为严格的逐点代次证明。

`exploration` 保存观测后的判断：status、reason_code、estimates、spent_gpu_seconds、
candidate_generation、gain_unit，以及 estimate_kind=observed_throughput_projection。
time 目标下 gain_unit=seconds，gpu_hours 目标下此处为 gpu_seconds。
该结果包含实测吞吐和已发生开销，但剩余运行时间仍是预测，不命名为最终实际收益。
ACCEPTED 为 positive；其他未接受的结论不一概当成负收益，失败或无法判断时为 unknown。
探索窗口验证成功时可记录 recovery_status=verified_by_exploration，与原生恢复报告区分。

## 使用边界

- checkpoint_ref 是训练端提供的 checkpoint 引用；不要在该引用内放入访问凭证。
- 未获得资源计划就跳过时，资源字段允许为 null，不伪造收益或资源结果。
- 无明确来源时记录 unknown，不通过动作方向猜测人工或自动来源。
- 不把 event_id、plan_id、generation 加入 Prometheus 标签；事件用于逐次追踪，指标用于聚合统计。
- 如要求严格的跨重启审计，需要后续增加可靠投递和存储能力；本次不新增这些组件。

## 代码位置

- [事件结构与日志上报](../../dlrover/python/master/monitor/vertical_events.py)
- [执行与恢复事件挂接](../../dlrover/python/master/monitor/vertical_metrics.py)
- [资源、收益与探索事件挂接](../../dlrover/python/master/node/job_resource_scaler.py)
- [事件测试](../../dlrover/python/tests/test_vertical_events.py)
