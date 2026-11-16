# 弹性收益与事件 UI 核心指标

本文按当前收益与事件页面分类，说明指标的代码字段名、数据来源、实时性及人和 Agent 的使用方式。

## 1. 口径与数据链路

- 前端统一读取聚合服务 API，不直接查询 MySQL、Prometheus 或 SwanLab。
- MySQL 是任务生命周期、弹性事件和 GPU 时序的主要来源。
- 只有没有持久化收益评估且配置了 Prometheus 时，最新收益才回退查询 Prometheus。数据库中的负收益或无效评估不会被其他结果替换。
- 单个事件的 `events[].evaluation` 来自数据库历史记录，不用当前 Prometheus 值覆盖历史。
- 实时性统一标记为“是/否”：**是**表示当前运行状态或执行进度会随采样、事件上报或页面刷新持续更新，不保证毫秒级延迟；**否**表示历史记录、固定配置、评估快照、事后统计或当前尚未接通的数据。
- “实时性=否”不表示该字段永远不更新。例如最新收益会随新评估替换，但仍是评估时刻的快照，不代表当前时刻重新计算的收益。
- 缺失或不可用数值使用 `null`，不能当成零。`valid=true` 仅表示数值完整有效，不表示收益为正。

| 数据表 | 存储内容 |
| --- | --- |
| `elastic_jobs` | 任务身份、生命周期状态及提交/完成时间 |
| `elastic_events` | 弹性请求、执行阶段、资源快照、收益评估及探索结果 |
| Prometheus GPU Gauge | 运行 Worker 分配 GPU 数的时序 |

## 2. 任务运行状态：现在运行得怎样

| 页面指标 | UI/API 实际字段 | 数据来源 | 实时性 | 人与 Agent 的用途 |
| --- | --- | --- | --- | --- |
| 运行 GPU 数 | `series.points[].gpus` | Kubernetes → Master → Prometheus；原始指标 `dlrover_master_running_worker_allocated_gpus`，结合有效性及采样时间计算 | 是 | 人：资源阶梯曲线；Agent：资源规模和变化分析 |
| 训练吞吐 | `series.points[].throughput` | 计划读取 SwanLab 历史接口，当前聚合尚未接通 | 否 | 人：吞吐曲线；Agent：性能变化分析 |
| 采样时间 | `series.points[].at` | 原始观测时间 | 是 | 人：时间轴；Agent：时间对齐、新鲜度判断 |
| 吞吐单位 | `series.work_unit` | 训练数据口径，steps / samples / tokens / unknown | 否 | 人、Agent：避免混用不同吞吐单位 |
| 任务已耗时 | `jct.elapsed_seconds` | MySQL `elastic_jobs` 提交时间＋查询时间 | 是 | 人、Agent：任务耗时判断 |

运行 GPU 数约每 10 秒采样到内存，由 Prometheus 拉取保存；任务已耗时只在运行中随刷新增长，成功完成后固定。训练吞吐历史尚未接通，当前实时性标记为“否”。

运行 GPU 数的口径为 **运行 Worker 分配的加速器数量**，不是忙碌 GPU 数，也不等于参与训练的 rank 数。当前采集检查任务归属、Pod 状态及训练容器状态，按 Pod UID 去重后求和；包含空闲但运行中的 Worker。MIG 数量表示实例数，不折算整卡。

确认无运行 Worker 时记录 0；采集失败或状态不完整时记录 null。超过 30 秒未采样保留断点。每次查询最多 2000 个等间距点；长任务降低显示分辨率，历史范围受 Prometheus 保留时间约束。采样周期内短暂的资源切换可能无法捕获。

## 3. 最新收益评估：这次调整是否划算

以下字段前缀均为 `latest_evaluation.`。同结构也用于单个事件的 `events[].evaluation`。

| 页面指标 | 字段名 | 数据来源 | 实时性 | 人与 Agent 的用途 |
| --- | --- | --- | --- | --- |
| 预计节省时间 | `time_gain_seconds` | MySQL `elastic_events`；缺评估时回退 Prometheus | 否 | 人：时间收益；Agent：调整收益参考 |
| 预计节省 GPU 卡时 | `gpu_seconds_saved` | 同上 | 否 | 人：页面除以 3600 显示 GPU·小时；Agent：资源成本比较 |
| 收益率 | `gain_ratio` | 同上 | 否 | 人、Agent：衡量收益比例，负值表示负收益 |
| 收益目标 | `objective` | 事件配置或 Prometheus 标签 | 否 | 区分 time / gpu_hours / cluster_jct，明确收益率分母 |
| 原配置吞吐 | `baseline_throughput` | 事件评估或 Prometheus；原始训练证据来自 SwanLab 等接口 | 否 | 人、Agent：性能对比基准，不是当前实时吞吐 |
| 候选配置吞吐 | `candidate_throughput` | 候选性能证据或探索观测，经事件/Prometheus 输出 | 否 | 人、Agent：解释收益来源，必须区分预测与观测 |
| 最低收益门槛 | `min_gain_ratio` | MySQL 事件中的评估配置 | 否 | 人：解释决策；Agent：校验门槛；Prometheus 回退时为空 |
| 评估阶段 | `stage` | 事件类型或 Prometheus 标签 | 否 | 区分 prediction / exploration |

### 3.1 UI 实际读取的 Prometheus 收益指标

以下均为 Gauge。收益在 Master 中计算，Prometheus 负责采集和保存；聚合服务不通过 PromQL 重新计算收益。

| Prometheus 实际指标名 | 性质 | 对应 UI 字段 | 含义 |
| --- | --- | --- | --- |
| `dlrover_master_vertical_benefit_baseline_throughput` | 计算指标 | `baseline_throughput` | 原配置观测窗口吞吐 |
| `dlrover_master_vertical_benefit_candidate_throughput` | 预测/计算指标 | `candidate_throughput` | 候选配置吞吐下界或探索窗口吞吐 |
| `dlrover_master_vertical_benefit_time_gain_seconds` | 计算指标 | `time_gain_seconds` | 考虑调整成本后的预计时间收益 |
| `dlrover_master_vertical_benefit_gpu_seconds_saved` | 计算指标 | `gpu_seconds_saved` | 预计资源收益，GPU·秒，可为负值 |
| `dlrover_master_vertical_benefit_gain_ratio` | 计算指标 | `gain_ratio` | 所选目标下的收益比例 |
| `dlrover_master_vertical_benefit_valid` | 状态指标 | `valid` | 关键收益数值是否完整有效 |
| `dlrover_master_vertical_benefit_observed_timestamp_seconds` | 观测元数据 | `observed_at` | 训练证据观测时间，Unix 秒 |

查询按 `cluster`、`namespace`、`job_uid` 匹配，并按 `stage`、`objective`、`work_unit`、`data_source`、`strategy` 分组。不同目标、单位或抓取副本不能直接相加。

### 3.2 收益计算使用的核心原始输入

这些字段是收益计算的输入，来源主要为 SwanLab、Master 资源配置和收益证据接口；并非由上述 Prometheus Gauge 反推。候选性能与切换成本可能是上游估算，并不全部是实测值。

| 原始字段 | 来源 | 含义及用途 |
| --- | --- | --- |
| `training.start.work` / `training.end.work` | SwanLab 训练进度 | 计算窗口内完成工作量 |
| `training.start.observed_at` / `training.end.observed_at` | 训练采样时间 | 计算窗口时长及证据年龄 |
| `training.target_work` | SwanLab 训练目标 | 计算剩余工作量 |
| `training.global_batch_size` | SwanLab 或明确配置 | 校验训练口径可比性 |
| `candidate.throughput_lower` | 候选性能证据接口 | 候选吞吐下界，用于保守预测 |
| `current/target.workers`、`gpus_per_worker` | Master 当前资源与调整计划 | 计算原配置和目标配置的 GPU 数 |
| `transition.lost_seconds_upper` | 切换成本证据接口 | 调整损失时间上界 |
| `transition.replay_work_upper` | 切换成本证据接口 | 可能重放的工作量上界 |
| `transition.gpu_seconds_upper` | 切换成本证据接口 | 调整阶段 GPU·秒成本上界 |
| `execution.trial_gpu_seconds` | 探索执行证据接口 | 已消耗探索 GPU·秒，用于探索净收益计算 |

默认 SwanLab 字段映射包括 `train/global_step`、`train/total_steps`、`train/global_batch_size`，也支持配置映射及兼容模式。收益评估选取的有效窗口，与看板所需的完整历史曲线是两条不同链路。

普通预测的核心公式：

```text
q0 = 工作量增量 / 采样时间差
R = 目标工作量 − 已完成工作量
T0 = R / q0
T1 = 切换损失时间上界 + (R + 重放工作量上界) / 候选吞吐下界
预计节省时间 = T0 − T1

H0 = 原 GPU 数 × T0 / 3600
H1 = (切换 GPU·秒成本上界 + 目标 GPU 数 × 候选训练运行时间) / 3600
预计节省 GPU 卡时 = H0 − H1
```

时间目标收益率为 `(T0 − T1) / T0`，卡时目标收益率为 `(H0 − H1) / H0`。集群 JCT 目标还需要其他受影响任务的加权时间影响。探索评估则使用探索前冻结的剩余时间基线，扣除已耗时间/资源及预计后续消耗；仍包含对剩余训练的外推。

## 4. 任务 JCT 复盘：整个任务最终改善多少

| 页面指标 | 指标名 | 数据来源 | 实时性 | 人与 Agent 的用途 |
| --- | --- | --- | --- | --- |
| 实际 JCT | `jct.actual_seconds` | MySQL `elastic_jobs`：completed_at − submitted_at | 否 | 人、Agent：实际任务耗时，包含排队时间 |
| 不调整基线 JCT | `jct.baseline_seconds` | 冻结基线账本，当前尚未接入 | 否 | 人、Agent：任务级比较基准 |
| JCT 缩短时间 | `jctValues(data).gain` | 前端：基线 JCT − 实际 JCT | 否 | 人：复盘；Agent 使用需统一后端输出 |
| JCT 改善率 | `jctValues(data).ratio` | 前端：缩短时间 / 基线 JCT | 否 | 同上 |
| 基线来源、冻结时间 | `jct.baseline_source` / `baseline_frozen_at` | 基线账本，当前尚未接入 | 否 | 人：来源说明；Agent：可比性和追溯 |

`jct.baseline_seconds` 是完整任务基线；收益预测内部的 `benefit.estimates.baseline_seconds` 是预计剩余时间。两者不可混用。单次评估的时间收益也不能直接相加为任务实际 JCT 改善。

## 5. 弹性事件与执行轨迹：发生了什么

以下字段前缀均为 `events[].`，来源为 MySQL `elastic_events`。

| 页面指标 | 字段名 | 实时性 | 人与 Agent 的用途 |
| --- | --- | --- | --- |
| 事件时间、类型、来源 | `occurred_at` / `type` / `trigger` | 否 | 人：事件列表；Agent：事件关联 |
| 调整前、目标、实际 GPU 数 | `before_gpus` / `target_gpus` / `actual_gpus` | 否 | 计划与执行对比；目标值不能当成实际值 |
| 执行状态 | `execution_status` | 是 | 执行进度及结果 |
| 训练恢复状态 | `recovery_status` | 是 | 判断训练是否恢复，区别于控制执行完成 |
| 评估结论 | `conclusion` | 否 | 接受、保留、拒绝、失败或无法确定 |
| 阶段及时间 | `stages[].name` / `stages[].at` | 是 | 人：时间线；Agent：卡点分析 |
| 训练中断时长 | `interruption_seconds` | 否 | 真实训练中断成本，不能以计划执行耗时代替 |
| 计划标识 | `plan_id` | 否 | 人：详情追溯；Agent：精确关联 |

## 6. 执行结果统计：整体执行情况如何

| 页面指标 | 指标名 | 数据来源 | 实时性 | 人与 Agent 的用途 |
| --- | --- | --- | --- | --- |
| 已记录调整次数 | `summary.attempted` | MySQL 事件按计划聚合 | 否 | 统计有实际执行证据的尝试，区别于请求次数 |
| 执行完成次数 | `summary.succeeded` | 同上 | 否 | 执行完成情况，不表示收益一定为正 |
| 执行失败次数 | `summary.failed` | 同上 | 否 | 故障情况 |
| 统计完整性 | `summary.complete` | 聚合服务标记 | 否 | 人：说明统计范围；Agent：避免推断全量成功率 |

以下 Prometheus 指标已经存在，但当前 UI 不直接用它们替代数据库事件统计：

| Prometheus 实际指标名 | 类型 | 含义 |
| --- | --- | --- |
| `dlrover_master_vertical_requests_total` | Counter | 调整请求计数 |
| `dlrover_master_vertical_plans_started_total` | Counter | 创建计划计数 |
| `dlrover_master_vertical_plans_finished_total` | Counter | 计划终态计数 |
| `dlrover_master_vertical_plan_duration_seconds` | Histogram | 计划执行耗时，不等于训练中断时长 |
| `dlrover_master_vertical_exploration_windows` | Gauge | 探索有效窗口数，当前回退适配器未读取 |
| `dlrover_master_vertical_exploration_result_reported` | Gauge | 探索结果回传确认状态，当前回退适配器未读取 |

## 7. 数据质量与决策证据：数据是否可信

| 指标 | 指标名 | 数据来源 | 实时性 | 人与 Agent 的用途 |
| --- | --- | --- | --- | --- |
| 评估观测时间 | `latest_evaluation.observed_at` | MySQL / Prometheus | 否 | 人：时间标签；Agent：时效判断 |
| 评估有效性 | `latest_evaluation.valid` | MySQL / Prometheus | 否 | 判断数值完整性；有效不等于正收益 |
| 最大允许年龄 | `latest_evaluation.max_age_seconds` | MySQL 评估配置 | 否 | 人：过期提示；Agent：拒绝陈旧证据 |
| 有效窗口数 | `latest_evaluation.windows` | MySQL 探索事件 | 是 | 证据充分性 |
| 回传确认状态 | `latest_evaluation.reported` | MySQL 探索事件 | 是 | 判断结果是否已被接收 |
| 数据来源 | `latest_evaluation.data_source` | MySQL / Prometheus 标签 | 否 | 识别 mock / http / unknown；http 本身不证明数据真实 |
| GPU 采样完整性与口径 | `dlrover_master_worker_gpu_observation_valid` / `dlrover_master_worker_gpu_observed_timestamp_seconds` | Prometheus 原始 Gauge | 是 | 人：缺口说明；Agent：区分未知与零、明确资源口径 |
| 数据缺失说明 | `missing_data` | 聚合服务 | 否 | 明确不可用数据，禁止自行补造 |

有效窗口数在探索期间更新，回传状态随确认事件更新，探索结束后保留最终值。评估有效性、观测时间和缺失说明属于质量元数据，实时性标记为“否”。

有效性与采样时间是 Prometheus 原始指标，当前不作为 `series.points[]` 的独立字段返回。Agent 若要核验每次采样的原始证据，需要受控的原始数据查询能力，不能只读取 UI 曲线。

## 8. 人与 Agent 的展示边界

- **人看主视图**：运行曲线、最新收益、JCT 复盘、事件轨迹和执行统计；数据质量通过来源标签、过期提示、缺口提示及详情展示。
- **Agent 看结构化数据**：数值、单位、时间戳、状态、完整性、来源、评估阶段必须一起读取，不能仅凭收益率推断执行动作。
- Agent 做深入诊断还需要原始 `event_id/request_id/plan_id`、`event_type/phase/reason_code`、训练代次、窗口起止值、基线规则及调度约束。这些并非全部包含在当前 UI 响应中。
- 当前 UI API 适合只读分析和总结；自动扩缩容仍应通过现有控制链路重新校验状态、证据时效与执行约束。

## 9. 当前缺口

| 内容 | 当前状态 |
| --- | --- |
| SwanLab 历史吞吐曲线 | 收益评估已有窗口适配；聚合服务历史趋势尚未接通 |
| JCT 冻结基线及真实改善率 | 实际 JCT 已实现，完整基线账本尚未实现 |
| 真实训练中断区间 | 尚未采集，相关字段返回 null |
| 全生命周期实际 GPU 卡时 | GPU 时序由 Prometheus 保存，尚未实现完整积分和缺口处理 |
| 事件全生命周期完整性 | 当前为 best-effort 异步投递，需日志补录保障覆盖 |

## 10. 实现索引

- [聚合逻辑](../../dlrover/dashboard/elastic/aggregate.py)
- [Prometheus 回退读取](../../dlrover/dashboard/elastic/prometheus.py)
- [Master 收益指标](../../dlrover/python/master/monitor/vertical_metrics.py)
- [收益预测计算](../../dlrover/python/master/resource/benefit.py)
- [探索评估计算](../../dlrover/python/master/resource/exploration.py)
- [SwanLab 窗口适配](../../dlrover/python/master/resource/swanlab.py)
- [GPU 采样](../../dlrover/python/master/monitor/gpu_sampler.py)
- [JCT 计算](../../dlrover/dashboard/elastic/jct.py)
- [前端字段与 JCT 展示计算](../../web/elastic-dashboard/src/model.ts)
- [完整 API 契约](elastic-dashboard-api-cn.md)

### GPU 时序采集与计算

Master 每 10 秒只读采集运行 Worker 的 GPU 分配数，在内存中更新 Gauge，由 Prometheus 拉取；采集线程和指标抓取均不依赖聚合服务。聚合服务通过 `/api/v1/query_range` 查询历史曲线，不再周期写入 MySQL。

| Prometheus 原始指标 | 含义 |
| --- | --- |
| `dlrover_master_running_worker_allocated_gpus` | 运行 Worker 训练容器分配的加速器数量 |
| `dlrover_master_running_worker_count` | 运行 Worker 数 |
| `dlrover_master_worker_gpu_observation_valid` | 采样有效为 1，否则为 0 |
| `dlrover_master_worker_gpu_observed_timestamp_seconds` | 最近一次采集开始时间，Unix 秒 |

标签沿用 `cluster`、`namespace`、`job_uid`、`strategy`。必须配置聚合服务 `DLROVER_PROMETHEUS_URL`，并让 Prometheus 抓取 Master 的 `/metrics`（端口由 Master 的 `metrics_port` 配置决定），建议抓取间隔 10 秒。部署聚合服务不会自动创建 Prometheus 抓取配置。

`series.points[].gpus` 由 GPU 数、有效性及采样时间联合计算：采样失败、缺失或超过 30 秒返回 null；确认没有运行 Worker 才返回 0。多个抓取目标取最新有效观测，同一观测时间值冲突返回 null，不累加副本。查询覆盖任务提交至完成/当前时间，最多 2000 个等间距点，长任务会降低显示分辨率；缺少提交时间时使用已记录事件起点或最近一天，并在 missing_data 提示。历史覆盖范围取决于 Prometheus 保留时间，长期回溯需配置长期存储。该曲线用于展示，不作为精确计费依据。

MySQL 仅保存弹性事件、事件中的资源快照和任务生命周期/JCT。GPU 周期采样仅由 Prometheus 保存；events 接口拒绝 gpu_sample 上报并返回 422。Prometheus 查询失败仅令 GPU 曲线不可用并返回缺失说明，不阻塞其他弹性组件。

口径是实际运行容器的资源分配，包含空闲 GPU；MIG 按实例计数，不折算整卡。SwanLab 吞吐历史仍待接入。
