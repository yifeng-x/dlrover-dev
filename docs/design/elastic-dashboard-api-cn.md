# 弹性收益 React 看板接口与数据缺口

本文对应 `web/elastic-dashboard`，整理于 2026-11-06。**`/api/v1/elastic/*` 已由独立服务 `dlrover.dashboard.elastic` 实现，不是每个 Master 内嵌的 REST API。** 已实现 MySQL 事件持久化、日志导入、收益聚合和查询，以及无状态 K8s Deployment；部署及采集接入见[服务说明](../../dlrover/dashboard/elastic/README.md)。JCT 基线账本、连续趋势与真实中断采集仍待补齐。

## 1. 架构与现状

```text
Master /metrics → Prometheus ──────────┐
VerticalElasticEvent / BenefitFlow → 事件存储 ─┤
探索 journal / 证据服务 ───────────────┤→ 看板聚合服务 → 同源 /api → React
平台作业生命周期 / 冻结基线账本 ────────┘
```

页面默认演示模式，顶部始终显示“示例数据”；切换真实 API 后不回退到 mock。也可使用 `?mode=api` 直接进入真实数据模式。聚合服务提供数据关联、历史查询、非有限值转换，内部接口不使用服务令牌；用户鉴权与租户隔离由平台网关或隔离实例负责；前端不直连证据服务、不暴露 HTTP token、不发起资源调整。

代码依据：

- [benefit.py](../../dlrover/python/master/resource/benefit.py)：普通预测公式及 `BenefitResult.estimates`。
- [exploration.py](../../dlrover/python/master/resource/exploration.py)：`TrialStore`、探索结果及 journal。
- [vertical_metrics.py](../../dlrover/python/master/monitor/vertical_metrics.py)：收益与执行 Prometheus 指标。
- [vertical_events.py](../../dlrover/python/master/monitor/vertical_events.py)：真实结构化事件字段。
- [prometheus.py](../../dlrover/python/master/monitor/prometheus.py)：Master 采集，目前 `dlrover_training_speed_steps_per_second` 的 yield 被注释。
- [现有指标清单](vertical-elastic-benefit-current-metrics-cn.md)、[收益指标](vertical-elastic-benefit-prometheus-cn.md)、[关键日志](vertical-elastic-benefit-logging-cn.md)。

## 2. 通用协议

- 同源 JSON，UTF-8，Unix **秒**（不是毫秒）；时间在页面按 Asia/Shanghai 显示。
- 响应 `schema_version=1`。完整可执行校验见 [src/model.ts](../../web/elastic-dashboard/src/model.ts)。表中字段均必须存在，可空字段用 `null`，数组无数据用 `[]`。
- `NaN`、`Infinity`、缺失序列须转换为 `null`。未知不等于 0；0 保留真实零含义，负收益必须保留。
- job UID 是 API 路由键。后端需保证 UID 在其服务范围唯一；多集群若存在 UID 冲突须扩展组合路由。不能用 SwanLab task_id 当作 job_uid。
- HTTP 401/403：认证或权限失败；404：作业不可见或不存在；422：参数错误；503：上游不可用。建议错误体 `{ "code": "UPSTREAM_UNAVAILABLE", "message": "...", "request_id": "..." }`。当前前端显示 HTTP 状态，不将服务异常降级成示例。
- 部分数据缺失但作业可用返回 200，相关字段 null 并填 `missing_data`。协议不合法、响应非 JSON 或返回 UID 错误时前端拒绝展示。
- 本地 Vite `/api` 代理到 `127.0.0.1:8080`；生产需反向代理及鉴权。Cookie 采用同源凭据，生产网关须逐作业鉴权并过滤作业列表；当前聚合服务为内部接口，作业列表包含该数据库全部作业。

## 3. 作业列表

`GET /api/v1/elastic/jobs`

```json
{
  "items": [{
    "uid": "job-uid-001", "name": "train-001", "cluster": "cluster-a",
    "namespace": "elastic-training", "status": "completed"
  }]
}
```

`status`：`running | completed | failed | unknown`；completed 只代表成功完成训练，不能把被取消、失败或 Master stopped 直接映射成 completed。缺少平台生命周期时返回 unknown，不推测运行/完成。当前服务一次返回数据库全部作业，无分页；用户可见范围由网关或实例隔离保证。

## 4. 作业看板

`GET /api/v1/elastic/jobs/{job_uid}/dashboard`

返回全生命周期聚合快照。当前前端的“最近 1 小时 / 10 分钟”对 events、series 作本地筛选，基准是完成时间，否则为 `generated_at`；不会改变顶部任务级 JCT、最新收益或生命周期统计。后端应对时序降采样并保留资源变更、缺口、恢复边界，不能截断事件后仍声称统计完整。大量事件的 cursor 分页是后续扩展，当前客户端未实现。

顶层：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| schema_version | 1 | 协议版本 |
| generated_at | number | 聚合快照生成时间 |
| job | Job | 与列表字段相同，UID 必须匹配路由 |
| jct | Jct | 任务级事后比较 |
| latest_evaluation | Evaluation / null | 最近一次选定阶段评估；没有则 null |
| summary | Summary | 生命周期调整尝试统计 |
| series | Series | 同一工作量单位的真实时序 |
| events | Event[] | 按计划聚合的弹性事件，非原始逐条日志 |
| missing_data | string[] | 用户可读的缺失原因，禁止敏感信息 |

### 4.1 Jct

| 字段 | 类型 | 含义及规则 |
| --- | --- | --- |
| submitted_at | number / null | 平台提交时间；若采用 CR creationTimestamp 必须统一说明排队起点 |
| completed_at | number / null | 成功完成训练的时间；运行中 null |
| actual_seconds | number / null | 服务端计算的成功任务 JCT；失败、未完成或时间无效返回 null |
| elapsed_seconds | number / null | 运行中为 generated_at - submitted_at；成功后固定为实际 JCT；其余 null |
| measurement_status | completed / running / unavailable | JCT 数据可用状态，不替代 job.status |
| baseline_seconds | number / null | 不调整的完整 JCT，正数，**不是**剩余时间 T0 |
| baseline_frozen_at | number / null | 冻结基线的时间；没有冻结记录不展示比较收益 |
| baseline_source | estimate / control_run / unavailable | 估算、对照任务或不可用 |
| baseline_description | string | 原配置吞吐估算 / 同配置对照作业等来源描述 |

计算规则（服务端统一计算，前端兼容旧接口）：

```text
actual_jct = completed_at - submitted_at（仅 completed 作业且时间顺序合法）
jct_gain = baseline_seconds - actual_jct
jct_gain_ratio = jct_gain / baseline_seconds
```

建议基线账本额外持久化 `baseline_id / rule_version / source_event_id / remaining_work / q0 / target_work / comparable_config_hash / control_job_uid`；这些是后端审计扩展，不属于当前页面必填字段。估算基线应按冻结时刻同口径构造：`baseline_jct = frozen_at - submitted_at + baseline_remaining_seconds`。若 T0 来自较早窗口，必须统一时间锚点，不能把时间差重复计入。多次调整选择明确、固定的任务基线，不能把每个事件的收益求和称为任务 JCT 改善。

实际 JCT 和运行中耗时已实现，单位均为秒，起点为平台提交时间，包含排队时间；运行中耗时随页面刷新更新，成功完成后不再增长。时间必须非负且不晚于 generated_at；缺失、倒置或未来时间不生成有效统计。新字段为 schema_version=1 的增量扩展，前端兼容旧响应。

当前实现没有完整 JCT 基线账本和事后收益结算服务。结构化事件可能保留了预测剩余时间，但这不足以自动证明全生命周期反事实。估算改善不宣称为已验证的因果收益。

### 4.2 Evaluation

| 字段 | 类型 | 来源 / 规则 |
| --- | --- | --- |
| stage | prediction / exploration | 调整前预测 / 实测吞吐外推 |
| objective | time / gpu_hours / cluster_jct / unknown | 决定收益率名称与分母 |
| work_unit | steps / samples / tokens / unknown | 当前 SwanLab 主要是 steps |
| data_source | mock / http / unknown | http 不自动代表非合成数据 |
| valid | boolean | 完整有限数值结果，不代表正收益或满足约束 |
| observed_at | number / null | 训练窗口末端，不是 scrape 时间；原指标 0 转 null |
| max_age_seconds | number / null | 有效年龄配置；旧日志或 Prometheus 回退缺少配置时 null，前端显示阈值未知 |
| time_gain_seconds | number / null | 当前任务预计节省秒数，可负 |
| gpu_seconds_saved | number / null | 当前任务预计节省 GPU 秒，可负 |
| gain_ratio | number / null | 比例 0.05=5%，按 objective 命名 |
| min_gain_ratio | number / null | 收益阈值，读配置/事件，不硬编码 |
| baseline_throughput / candidate_throughput | number / null | work_unit/s；候选探索采用独立完整窗口最小吞吐 |
| windows | integer / null | 探索独立窗口数，普通预测可 null |
| state | string / null | 探索状态，如 accepted/retained/inconclusive |
| reported | boolean / null | 结果服务是否确认入库；非预测成功标志 |

前端 valid=false 时屏蔽三个收益数值，但允许已知吞吐显示；valid=true 的负值照常展示。历史终态结果保持原值，不随着当前时钟重新计算。快照时间减 observed_at 超过 max_age 时显示“历史观测”，不把已完成任务历史收益清零。

所有收益 Gauge 前缀为 `dlrover_master_vertical_`：

| 页面字段 | 指标后缀 |
| --- | --- |
| baseline_throughput | benefit_baseline_throughput |
| candidate_throughput | benefit_candidate_throughput |
| gain_ratio | benefit_gain_ratio |
| time_gain_seconds | benefit_time_gain_seconds |
| gpu_seconds_saved | benefit_gpu_seconds_saved |
| valid | benefit_valid |
| observed_at | benefit_observed_timestamp_seconds |
| windows | exploration_windows |
| state | exploration_state{state} 中值为 1 的状态 |
| reported | exploration_result_reported |

前七项按相同 `cluster/namespace/job_uid/strategy/stage/objective/work_unit/data_source` 标签匹配。不能把不同阶段、单位或来源序列求和。探索状态/窗口/回传指标不具有全部收益标签，后端应按作业和对应探索关联，不能仅按时间猜测归属。阈值和 max_age 没有对应上述 Gauge，需要配置/事件来源。

普通预测 `estimates.gpu_hours_saved` 转 GPU 秒时乘 3600；探索 `result.gpu_seconds_saved` 已经是秒。页面再除 3600 显示 GPU 小时，避免重复换算。cluster_jct 的 gain_ratio 是集群加权收益，time_gain_seconds 和 gpu_seconds_saved 仍只属于当前任务，不能把它们显示成集群总值。

### 4.3 Summary

```json
{ "attempted": 2, "succeeded": 1, "failed": 1, "complete": true }
```

前三项为非负整数或 null。当前服务 complete 固定为 false，数值是已记录事件的下界，前端标注“已记录调整尝试”；没有事件记录时返回 null。attempted 是实际进入执行的计划数，排除 rejected/skipped 及仅提交未执行的请求；succeeded 表示执行完成，不暗示恢复或收益达标；进行中的计划可计入 attempted。complete 表示全生命周期记录是否完整，不能仅凭 Prometheus 抓取成功置 true。数据不够时 null，而不是用空列表推出 0。

### 4.4 Series

```json
{
  "work_unit": "steps",
  "points": [
    { "at": 1793946600, "gpus": 2, "throughput": 2.0 },
    { "at": 1793946720, "gpus": 2, "throughput": null }
  ]
}
```

`at` Unix 秒；gpus/throughput 非负数或 null。后端保证时间点唯一、按时间升序、单位一致；前端亦排序。缺失区间需返回 null 点，不可省略后连线。只有确认无有效进展时吞吐才填 0。GPU 曲线是**实际总卡数**阶梯线，不能用 worker 数或目标每 worker 卡数替代。无采样返回 `points: []`。最新收益 Gauge 不是逐点训练吞吐时序，不能填充整条曲线。

### 4.5 Event

| 字段 | 类型 | 规则 |
| --- | --- | --- |
| id | string | 聚合事件稳定唯一 ID，供列表和图表联动 |
| plan_id | string / null | 原计划关联键，无计划请求可 null |
| occurred_at | number | 计划发起时间 |
| type | expand / shrink / other | 依据前后同口径总卡数或原 action |
| trigger | string | 可读触发原因，未知明确说明 |
| before_gpus / target_gpus / actual_gpus | number / null | 原资源、计划目标、最终观测实际资源分别存储 |
| execution_status | pending / running / completed / failed / skipped / unknown | 控制执行状态 |
| recovery_status | string | 原恢复状态，如 recovered/verified_by_exploration/pending/unknown |
| conclusion | accepted / retained / inconclusive / failed / rejected / unknown | 独立收益结论；retained 表示保留配置 |
| reason | string | 原因码或可读说明 |
| interruption_seconds | number / null | 经确认无有效训练进展的时长，不是整个计划时长 |
| interruption_start / interruption_end | number / null | 中断区间，缺证据填 null；end≥start |
| evaluation | Evaluation / null | 本事件评估快照；不能取另一事件最新 Gauge 填入历史行 |
| stages | Array<{name:string, at:number}> | 按发生顺序的执行节点 |
| before_finish_at / after_finish_at | number / null | 相应时间锚点下的预计完成时刻，缺少锚点填 null |

映射方式：`VerticalElasticEvent` 每条有独立 `event_id`，先去重，再按 `(cluster, namespace, job_uid, plan_id)` 聚合；只有 request_id 的请求须单独关联，不得与空 plan_id 请求合并。记录 `root_plan_id`、`parent_plan_id` 便于审计。重启续接不能重复计数。

- `resources.before/target/final.total_gpus` 映射资源字段；final 缺失不能拷贝 target。`actual_after` 是控制完成时的快照，不必等于最终值。
- `execution_status` 与 `recovery_status` 原样分开。`finished.result=accepted` 属于探索终态，不能拿来替换执行状态。
- `benefit.estimates` 为预测结果；`exploration.estimates` 是探索整体结果。`estimate_kind` 区分 prediction 与 observed_throughput_projection。
- `BenefitFlow` 的 checkpoint_ready、replacement_submitted 与 `VerticalElasticEvent` 的 phase_changed、control_completed、training_recovered 可组成 stages；“提交替换”不是“替换完成”。
- phase_changed 的 previous_phase_duration_seconds、finished 的 elapsed_seconds 不能直接当作 interruption_seconds。
- 结构化日志是 best-effort，可能缺失、被节流或跨重启不完整。当前 journal 保存最近探索，不是完整多事件历史数据库。

## 5. 最小可用响应示例（数据暂缺）

该响应符合前端 schema，可用于真实聚合接口渐进接入；页面不会伪造收益。

```json
{
  "schema_version": 1,
  "generated_at": 1793947200,
  "job": {"uid":"job-uid-001","name":"train-001","cluster":"cluster-a","namespace":"elastic-training","status":"running"},
  "jct": {"submitted_at":null,"completed_at":null,"baseline_seconds":null,"baseline_frozen_at":null,"baseline_source":"unavailable","baseline_description":"尚未接入冻结基线"},
  "latest_evaluation": null,
  "summary": {"attempted":null,"succeeded":null,"failed":null,"complete":false},
  "series": {"work_unit":"steps","points":[]},
  "events": [],
  "missing_data": ["JCT 生命周期尚未接入", "事件存储尚未接入"]
}
```

完整演示响应由 `src/demo.ts` 的 `demoDashboard()` 提供并经过相同 schema 校验，包含成功、失败、拒绝、负收益和数据缺失。

## 6. 当前缺失数据与补齐优先级

| 优先级 | 页面能力 | 当前已有 | 仍缺什么 / 建议实现 |
| --- | --- | --- | --- |
| P0 | API 接入 | 两个 GET 接口、内部采集端 POST、MySQL 聚合已实现 | 生产网关用户/租户授权、部署与上游采集配置 |
| P0 | 作业列表及提交/完成时间 | ElasticJob metadata、内嵌 commonv1.JobStatus 与 Phase | 平台生命周期同步、成功完成语义、排队起点统一及历史保留；不是现成 dashboard 字段 |
| P0 | JCT 基线/事后收益 | 预测 `baseline_seconds` 是剩余时间；探索保存 q0/T0 | 不可变基线账本、全生命周期转换、规则版本、可比性、完成后结算 |
| P0 | 持久化事件及准确次数 | MySQL 事务落盘、跨 Pod 写入去重、排序、计划聚合、重启保留已实现 | 可靠采集投递、覆盖率和历史分页；当前仅报告已记录下界 |
| P0 | 实际 GPU 曲线 | 事件 resource_observation/final；计划 gpus_per_worker Gauge | 持续实际分配 GPU 总数时序，定义分配/就绪资源口径；计划 Gauge 会清理且不是实际卡数 |
| P0 | 连续训练吞吐 | SwanLab global_step、采样时间，固定 batch 模式 train_speed；窗口日志 | 保存/查询连续历史、代次过滤、缺口标识；Master 通用 speed Gauge 目前被注释，不能假定已导出 |
| P1 | 真正中断区间与耗时 | phase/recovery/resume Histogram 和恢复事件 | 最后有效进展、停训起点、恢复有效进展证据；resume_duration 明确不是 downtime |
| P1 | 事件前后预计完成时间 | estimates 剩余秒数、窗口时间、探索 elapsed/keep | 统一时间锚点并持久化每事件 ETA，避免把抓取时间当评估时间 |
| P1 | 阈值与时效 | guard 配置、benefit.min_gain_ratio、部分日志 | 聚合服务输出 max_age/min_gain，缺失时阈值 null |
| P1 | 预测与真实结果追踪 | 收益 Gauge 与探索 journal | 普通预测重启会丢，作业结束 Master 消失后仍需保留最终快照和来源 |
| P2 | 全生命周期实际 GPU 成本 | exploration_gpu_seconds 仅最近合法探索消耗 | 全作业资源积分、重叠资源计费和资源账本；当前不展示“实际累计节省卡时” |
| P2 | 可信度/质量与分页 | 数据有效性校验 | 覆盖率、上报延迟、可比性评分、质量退化校验、大数据 cursor 查询 |

## 7. 后端查询与实现注意

收益查询示例，必须用请求作业的完整身份标签；此处省略实际值：

```promql
dlrover_master_vertical_benefit_time_gain_seconds{job_uid="...",stage="exploration"}
dlrover_master_vertical_benefit_valid{job_uid="...",stage="exploration"}
dlrover_master_vertical_benefit_observed_timestamp_seconds{job_uid="...",stage="exploration"}
```

这些 Gauge 是最新评估快照，禁止 `rate/increase` 或跨时间累加。`explorations_total` 是探索终态次数，不等于全部资源调整次数；`plans_finished_total` 是进程内控制结果统计，也不是持久审计记录。Counter 重启、首次出现、漏采均可能影响统计。

已有 `recovery_duration_seconds` 表示控制完成到训练恢复；`resume_duration_seconds` 表示进入 checkpoint 到恢复，不能重命名为停训耗时。Histogram 分位数可用于运营监控，不能还原单个事件的精确耗时。

前端读取只读接口，不需要 CORS 通配或上游服务 token。日志源必须过滤凭据/路径等不应公开的信息；event/plan/request ID 保留在事件存储和 JSON 内，不增加到 Prometheus 高基数标签。

## 8. 当前服务补充规则

- `VerticalElasticEvent` 新增 checkpoint_ready，以及 exploration_updated（观测/回传确认，不新增调整次数）。收益事件补充 max_age_seconds，探索补充 observed_at/windows/reported。
- 持久事件评估优先，只有没有评估事件时才使用 Prometheus 回退；不将最新 Gauge 填入历史行。
- 旧事件缺少 max_age_seconds 返回 null；schema_version 仍为 1，客户端已兼容 unknown 生命周期及 null 时效配置，旧客户端需同步升级。
- 单作业超过 10000 条原始事件返回 503；写请求单条限制 1 MiB。
- 写入接口、平台生命周期快照及启动命令见[服务说明](../../dlrover/dashboard/elastic/README.md)。

## 9. 验收建议

1. 同作业关联成功，不串 task_id；服务返回错误 UID 时前端报错。
2. mock 标识始终可见；API 失败不出现示例值。
3. valid=false、缺失、NaN → null 显示 —；有效负收益原样显示。
4. time/gpu_hours/cluster_jct 对应不同收益率标题，steps/s 不误标为 tokens/s。
5. 运行中 JCT 待完成；基线未冻结无法算改善；完成后基线120分钟/实际96分钟得到24分钟/20%。
6. rejected/skipped 不计实际调整尝试；执行完成、恢复、收益结论独立。
7. 点击事件联动详情；筛选、关键词、时间范围组合有效；空事件及空时序有明确说明。
8. 中断区间来自真实证据，GPU 曲线来自实际资源，不能用示例或目标值补缺。

## 10. MySQL 与 Kubernetes

生产后端默认 MySQL，清单见 [deploy/elastic-dashboard](../../deploy/elastic-dashboard/README.md)。提供两副本 Deployment、ClusterIP Service 和 ConfigMap；数据库表提前通过 schema.sql 创建，不部署建表 Job，不配置接口鉴权 Secret。`/healthz` 不依赖 DB；`/readyz` 检查 DB 连接及表结构。MySQL 异常统一返回 503，不泄露连接信息。Master 可使用 `DLROVER_ELASTIC_EVENT_URL` 发送现有事件；HTTP 模式优先于旧 SQLite 路径。Master 通过有界队列和后台守护线程异步发送；网络故障不阻塞弹性控制，队列满时跳过 HTTP 投递，原始日志仍保留供补导。

### 任务 JCT 汇总

`GET /api/v1/elastic/jct-statistics?cluster=c&namespace=ns`

cluster、namespace 为可选的精确过滤条件；省略则统计库中全部已登记任务，不局限于发生过弹性事件的任务。空值、重复或未知参数返回 422。当前按全生命周期统计，不按页面事件时间窗口筛选。

| 字段 | 说明 |
| --- | --- |
| generated_at / scope | 本次统计时间（Unix 秒）及集群、命名空间范围 |
| total_jobs / completed_jobs / running_jobs / failed_jobs / unknown_jobs | 范围内作业数及各状态数量 |
| measured_jobs / excluded_jobs | 成功且提交/完成时间有效的样本数；其他排除数量 |
| mean_seconds / min_seconds / max_seconds | 有效成功任务 JCT 的均值、最小、最大值 |
| p50_seconds / p95_seconds | nearest-rank 分位数，排序后取 ceil(N×p) 对应样本 |
| percentile_method / time_basis | 固定 nearest_rank / submission_to_completion |

没有有效样本时所有时长统计返回 null，不返回 0。失败任务不计入成功 JCT；均值不代表可比任务的因果收益。每个 job_uid 只统计一次；重放、过期生命周期快照不增加样本。统计仅依赖生命周期表，不扫描弹性事件，无需新增表或修改 MySQL schema。

数据仍通过 `POST /api/v1/elastic/job-snapshots` 从平台接入：运行中需提交 submitted_at，成功后需同时提供 completed_at、status=completed 以及最新 lifecycle_observed_at。这里没有自动增加 Kubernetes watcher，也不使用 Master 启动时间或扩缩容完成时间替代平台生命周期。接口失败不得作为平台任务状态推进的前置条件，采集方需异步补报。
