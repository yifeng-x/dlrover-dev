# 垂直弹性收益门禁与 HTTP 数据契约

## 实现范围

收益门禁位于 Master 获取真实资源计划之后、进入 SAVE_CHECKPOINT 之前。
支持时间、GPU 卡时、集群加权 JCT 三种目标。返回 positive、negative、unknown。
没有候选性能观测时返回 unknown，不从 GPU 利用率或卡数推导候选吞吐；可以单独开启下述受控探索。

默认关闭，保持现有行为。shadow 只观察，不拦截原有扩缩容；enforce 默认仅放行 positive；显式开启探索后，满足独立准入条件的 candidate_unavailable 可进入一次探索。
强制模式对手动和自动触发的实际资源计划均生效，不绕过缺失数据检查。
本实现包含预测门禁、可选的单次受控探索和 SwanLab scalar HTTP 客户端。
提供训练脚本的 scalar 上报辅助函数，不自动修改用户训练循环，不包含集群调度模拟器。
已有执行和恢复指标继续记录实际过程；本次不实现跨重启的收益结算账本。

## 配置

在 Master 容器中配置以下环境变量，不是 Brain 容器：

| 环境变量 | 默认值 | 含义 |
| --- | --- | --- |
| DLROVER_BENEFIT_MODE | off | off / shadow / enforce；未知值禁止放行 |
| DLROVER_BENEFIT_HTTP_URL | 空 | 归一化数据适配服务 URL；不是假定存在的 SwanLab 原生路径 |
| DLROVER_BENEFIT_HTTP_TOKEN | 空 | 可选 Bearer token，不输出到日志 |
| DLROVER_BENEFIT_TIMEOUT_SECONDS | 3 | HTTP 请求连接和读取超时参数，无自动重试 |
| DLROVER_BENEFIT_MAX_AGE_SECONDS | 60 | 快照及训练窗口末端允许的最大数据年龄 |
| DLROVER_BENEFIT_MIN_WINDOW_SECONDS | 30 | 当前吞吐最小测量窗口 |
| DLROVER_BENEFIT_OBJECTIVE | time | time / gpu_hours / cluster_jct |
| DLROVER_BENEFIT_MIN_GAIN_RATIO | 0.05 | 对应目标的最小相对改善幅度，范围 [0,1] |

建议先配置 shadow 并验收接口数据，再启用 enforce。默认 5% 是可配置门槛，不是收益保证。
HTTP 不跟随重定向；服务端应快速返回。超时、非 200、非法 JSON 或数据缺失均为 unknown。
请求在资源调整线程中执行，不在指标 scrape 或训练主线程执行。

## 请求

Master POST JSON，传递真实当前与候选资源档位、作业身份和计划标识。
`request_id` 每次请求唯一，响应必须回传，以防串任务或使用旧缓存。
`job` 由实际 JobArgs 注入，不使用示例名称兜底。

```json
{
  "schema_version": 1,
  "request_id": "request-uuid",
  "job": {"job_uid": "job-uid", "namespace": "training", "cluster": "cluster-a"},
  "plan_id": "vertical_elastic_plan_...",
  "generation": "10000001",
  "requested_at": 1790000000,
  "objective": "time",
  "current": {"workers": 1, "gpus_per_worker": 4, "gpu_type": "nvidia.com/gpu"},
  "target": {"workers": 1, "gpus_per_worker": 8, "gpu_type": "nvidia.com/gpu"}
}
```

`gpu_type` 是现有资源对象中的字段，可能只是 Kubernetes 资源名，不一定是硬件型号。
适配服务需另外核实真实设备型号、并行方式、batch 及拓扑可比性，不能仅凭字符串一致通过。
只支持 worker 数不变、资源类型一致的垂直调整。

## 响应

以下时间是假设值。实际接口必须使用源观测时间，不能每次查询就将旧样本时间改为当前时间。

```json
{
  "schema_version": 1,
  "request_id": "request-uuid",
  "job": {"job_uid": "job-uid", "namespace": "training", "cluster": "cluster-a"},
  "current": {"workers": 1, "gpus_per_worker": 4, "gpu_type": "nvidia.com/gpu"},
  "target": {"workers": 1, "gpus_per_worker": 8, "gpu_type": "nvidia.com/gpu"},
  "observed_at": 1790000000,
  "training": {
    "unit": "steps",
    "comparable": true,
    "steady": true,
    "start": {"generation": "10000001", "observed_at": 1789999940, "work": 100},
    "end": {"generation": "10000001", "observed_at": 1790000000, "work": 160},
    "target_work": 3760
  },
  "candidate": {
    "throughput_lower": 2,
    "evidence": "trial",
    "evidence_id": "trial-001"
  },
  "transition": {
    "lost_seconds_upper": 120,
    "replay_work_upper": 0,
    "gpu_seconds_upper": 960
  },
  "scheduling": {
    "target_feasible": true,
    "constraints_satisfied": true
  },
  "limits": {
    "max_delay_seconds": 300,
    "deadline_at": null
  }
}
```

字段契约：

- work 为当前有效累计进度，unit 选择 steps / samples / tokens，所有吞吐与重放使用同一单位。
- start/end 必须是同恢复代次、同稳态训练阶段的有序点，不能跨回退或预热拼接。
- steps 口径要求有效 optimizer step 且前后实际 global batch 一致；否则用 samples/tokens。
- comparable 表示适配层已核实模型、数据、设备、并行配置和训练语义可比；不能用该字段代替实际校验。
- steady 表示适配层依据原始窗口确认稳定。Master 使用两端进度计算当前吞吐，适配层负责窗口内波动和数据完整度判断。
- target_work 必须由真实训练目标产生，不能把预计训练时长当成目标进度。
- candidate 必须来自 trial / profile / history，有可追溯 evidence_id，throughput_lower 是针对本候选配置的保守吞吐。候选数据时效和匹配由适配层核实。
- lost_seconds_upper 表示预计无有效进展时间，包含恢复预热影响的保守情景；不得将仍有进展的 checkpoint 时间直接重复计入。
- replay_work_upper 是预计丢失后需重放的工作量，明确为零时也必须提供。
- gpu_seconds_upper 是切换部分的总 GPU 卡秒，包括新旧 worker 重叠；不能遗漏或与随后稳态运行部分重复计价。
- target_feasible 是调度器对本候选档位的检查，不是集群总空闲卡数比较。
- constraints_satisfied 表示同一队列快照下已检查其他任务时限、优先级及资源约束。它不是资源预留；实际执行仍可能失败。
- max_delay_seconds 为当前任务允许的额外完成时间；deadline_at 为绝对 Unix 秒或 null（明确无截止时间）。

未配置独立 SwanLab 地址时，适配服务提供完整 training 字段；配置后由下述客户端获取真实进度。
候选性能、切换预算、队列和业务约束来自试运行记录、DLRover、任务平台或调度器，不能假定 SwanLab 自带。
服务未实现或字段缺失时 enforce 不调整，shadow 仅记录 unknown。

## SwanLab 实际 scalar 接口接入

Master 在收益评估及探索观测时，先调用训练数据接口，再请求收益证据服务。
训练请求使用实际格式，不向 SwanLab 发送原收益快照请求体：

```http
POST /metrics/scalar/by-task-name
Content-Type: application/json
```

```json
{
  "taskId": "实际 SwanLab taskId",
  "taskScalarList": [
    {"key": "train/global_step"},
    {"key": "train/total_steps"},
    {"key": "train/global_batch_size"}
  ]
}
```

响应按 `[{taskId, key, scalarMetricsList: [{index, data, createTime, epoch}]}]`
解析。`data` 支持数值字符串；请求与响应 taskId 必须一致。
`index` 不作为 global step，`epoch` 不作为恢复代次。
`train/token_acc` 是准确率，不作为训练产出；`train/remaining_time` 不代替固定总训练目标。
本客户端首期只生成 steps 口径，不能通过把 work key 改成 token_acc 或 token 数改变单位。

### 固定 global batch size 的平台接入

当前平台只提供 `global_step`、`max_steps`、`train_speed`（单位 step/s），没有 global_batch_size。
按本任务约定，扩缩容前后 optimizer step 工作量保持不变，使用：

```yaml
DLROVER_SWANLAB_ASSUME_FIXED_BATCH: "true"
DLROVER_SWANLAB_KEYS: '{"work":"global_step","target_work":"max_steps","train_speed":"train_speed"}'
```

此模式只请求上述三个指标，不要求 global_batch_size，也不伪造数值。归一化训练数据携带
`global_batch_size_fixed=true`、`batch_size_source=assumed_fixed`，日志和事件保留该假设。
默认不开启此假设，旧的在线 batch-size 校验模式继续可用；验证示例已按当前平台配置开启。

收益计算与稳态判断仍使用 `Δglobal_step / ΔcreateTime`，避免瞬时或平滑后的 train_speed
掩盖停滞、恢复或重放。返回的最新 train_speed 作为 `reported_train_speed` 记录，
`speed_relative_difference` 为它与窗口吞吐的相对差异，供核对，差异本身不直接决定放行。
train_speed 必须是有限正数，三类指标仍需按采样批次精确对齐。
剩余工作量为 `max_steps - global_step`；elapsed time 和 remaining time 不属于当前必需查询指标。

探索基线与观测必须采用一致的 batch 假设，不可在同一探索中切换该开关。
固定 step 工作量不自动证明模型、数据集或并行语义一致，证据服务仍需提供 comparable。

### Master 配置

本地联调可通过默认关闭的 `DLROVER_SWANLAB_MOCK` 开关读取 scalar JSON 文件；
配置与生成数据步骤见 [SwanLab mock 模式验证](vertical-elastic-benefit-mock-cn.md)。

SwanLab 的 `taskId` 仅从提交任务的顶层 `metadata.labels.low_code_platform_task_id` 读取：

```yaml
metadata:
  name: workspace-test
  namespace: elastic-training
  labels:
    low_code_platform_task_id: "xxxxxxx"
```

Master 将解析结果传入当前任务的收益客户端环境配置 `LOW_CODE_PLATFORM_TASK_ID`，
不修改进程全局环境。label 是唯一来源；缺失、为空或不是字符串时，SwanLab 客户端不可用，
即使环境变量中存在任务 ID 也不会使用。
该 ID 在扩缩容、worker 替换及 training generation 变化期间保持不变。
不读取 annotations 或任何环境变量中的任务 ID。
未提供 ID 时，已启用的 SwanLab 客户端不可用，不会以 job name 或 job UID 猜测 taskId。
请求体的 `taskId`、响应身份校验和事件中的 `swanlab_task_id` 使用同一个值。
该值在客户端初始化时读取；`BenefitFlow.training_metrics_configured` 记录 task ID、来源和配置有效性。

| 环境变量 | 默认值 | 含义 |
| --- | --- | --- |
| DLROVER_SWANLAB_HTTP_URL | 空 | 完整 scalar 接口 URL；非空开启独立训练数据获取 |
| DLROVER_SWANLAB_HTTP_TOKEN | 空 | 可选 Bearer token，与收益证据服务 token 分离 |
| DLROVER_SWANLAB_UTC_OFFSET | +08:00 | createTime 无时区时使用的 UTC 偏移 |
| DLROVER_SWANLAB_KEYS | {} | 可映射 work、target_work、global_batch_size、train_speed；按模式请求三项，旧 generation/steady/observed_at 映射忽略 |
| DLROVER_SWANLAB_ASSUME_FIXED_BATCH | false | 显式 true 时假设全任务 step 工作量固定，改为获取 global_step、max_steps、train_speed |
| DLROVER_SWANLAB_TIME_SOURCE | sample | 已确认 createTime 为采样时间；显式 unknown/ingestion 返回 unknown |
| DLROVER_SWANLAB_WARMUP_SECONDS | 60 | 当前代次开始、恢复确认两者较晚时间之后的预热秒数 |
| DLROVER_SWANLAB_STEADY_WINDOWS | 2 | 判断稳定所需的非重叠子窗口数，至少 2 |
| DLROVER_SWANLAB_MAX_THROUGHPUT_VARIATION | 0.1 | 子窗口吞吐的 (最大值−最小值)/最小值 上限 |

例如：

```yaml
env:
  - name: DLROVER_BENEFIT_MODE
    value: "shadow"
  - name: DLROVER_BENEFIT_HTTP_URL
    value: "http://benefit-service/benefit/snapshot"
  - name: DLROVER_SWANLAB_HTTP_URL
    value: "http://metrics-service/metrics/scalar/by-task-name"
  - name: DLROVER_SWANLAB_KEYS
    value: '{"work":"train/global_step","target_work":"train/total_steps","global_batch_size":"train/global_batch_size"}'
```

内部字段及默认 key 如下：

| 内部字段 | 默认 key / 来源 | 语义 |
| --- | --- | --- |
| work | train/global_step | 已完成的 optimizer step，checkpoint 恢复后继承其进度 |
| target_work | train/total_steps | 固定训练目标 |
| global_batch_size | train/global_batch_size | 每个 optimizer step 实际处理的全局样本数 |
| generation | DLRover 内部上下文 | 当前训练 rendezvous 的代次；不向 SwanLab 请求 |
| steady | DLRover 推断 | 恢复、预热、进度和连续窗口吞吐检查通过后为 true |
| observed_at | createTime | 必须是采样时间，不能使用接口请求时间或入库时间 |

### 代次边界与推断稳态

Master 在训练 rendezvous 完成时同时记录 generation 和开始时间，并原子读取二者。
从 SwanLab 返回的数据仅选择当前代次开始时间之后、且晚于预热下界的数据。
不直接给全部历史数据附加当前 generation。generation 仍经 Agent 传递到训练进程，
但 SwanLab 不再要求上报 `train/generation`、`train/steady` 或 `train/observed_at`。
现有 `training_metric_payload()` 可继续使用，其中这三个额外字段不参与 scalar 查询。

已确认真实 SwanLab 接口的 `createTime` 是采样时间，因此 `DLROVER_SWANLAB_TIME_SOURCE` 默认值为 `sample`。
显式设置 unknown/ingestion 时仍返回 `untrusted_sample_time`，包括 mock 模式。Master 与采样端需要时钟同步，
否则不能可靠地用代次边界归属采样点。若服务返回入库时间，
需先改进时间语义；当前实现不会以请求时间估算吞吐，也不会在 enforce 下放行未知数据。

稳态准入流程：

1. 获取当前 generation 和开始时间；未完成训练 rendezvous 或边界缺失则 unknown。
2. 垂直调整进入 checkpoint 阶段后阻止观测；替换 worker 的连续进度确认恢复后记录恢复时间。
   恢复超时或未验证不会被当作恢复完成。首次训练则以 rendezvous 边界和后续有效进度判断。
3. 以 `max(代次开始时间, 恢复确认时间) + 预热秒数` 为采样下界，探索还需满足 observation_after 和上一窗口终点约束。
4. 同批三个指标的 `(epoch, createTime)` 必须精确对齐。要求至少 N+1 个独立采样点，
   总步数和 global batch size 一致、进度不回退、每个子窗口均有正进度。
5. 从最新点向前选取 N 个不重叠子窗口，每段至少 `min_window/N` 秒，
   因而总体跨度至少 min_window 秒。默认 N=2，吞吐相对波动不超过 10%。
   批次配置变化或采样间隔超过 max_age 时重新积累稳态窗口，重复点不会增加证据。
6. 通过后生成内部 `steady=true`、`steady_source=inferred`、`time_source=sample`，
   并附加 Master generation。请求期间代次或恢复上下文改变，本次评估作废。

数据新鲜度依然由采样时间计算。HTTP 超时、最小总窗口和数据时效沿用 DLROVER_BENEFIT 配置。
未配置 scalar URL 且未开启 mock 时保留原完整快照模式；启用后失败不回退使用旧 training 数据。

这是推断稳态，不是训练框架明确上报的稳态证明；模型、数据集和并行语义的可比性仍由证据服务核实。
Master 重启后，在新训练 rendezvous 建立之前没有可靠代次边界，收益评估返回 unknown。

### 与原收益 HTTP 契约组合

`DLROVER_BENEFIT_HTTP_URL` 仍必需，不能直接填 scalar 接口地址。
正常请求新增 `generation` 和已归一化的 `training`；证据服务据此匹配候选配置。
响应仍提供原有身份、资源、时间、candidate、transition、scheduling、limits 等字段。
开启 scalar 客户端时，响应的 training 只需提供 `{"comparable": true}`，前提是服务已核实
前后模型、数据和训练语义可比；其他训练字段由本次 scalar 观测覆盖。
缺少 comparable 或值不为 true 时不放行收益门禁。

资源替换前的预留校验和终态释放请求不访问训练指标接口，避免暂停训练时无法释放资源。
探索观测仍需要证据服务提供执行确认、checkpoint、实际卡秒与预留状态，不能从 scalar 数据虚构。

## 集群 JCT 扩展

cluster_jct 要求 scheduling 额外包含：

```json
{
  "impacts_complete": true,
  "job_weight": 1,
  "other_jobs": [
    {
      "job_id": "waiting-job",
      "weight": 1,
      "baseline_remaining_seconds": 6000,
      "candidate_remaining_seconds_upper": 2520
    }
  ]
}
```

other_jobs 由调度适配层针对同一受影响任务集合，在“不调整”和“本候选计划”两种情景下计算。
两个时间都从本次预测时刻起算，包含排队。禁止重复 job_id、包含当前任务或丢弃受负面影响的任务。
impacts_complete 是适配层对集合完整性的声明；未知运行时间不能填零。
空数组仅表示已核实没有其他受影响任务，不能表示未查询队列。

## 计算与拒绝规则

```text
q0 = (end.work - start.work) / (end.time - start.time)
R = target_work - end.work
T0 = R / q0
T1 = lost_seconds_upper + (R + replay_work_upper) / throughput_lower
H0 = 当前总卡数 × T0 / 3600
H1 = [gpu_seconds_upper + 目标总卡数 × (T1 - lost_seconds_upper)] / 3600
```

时间目标比较 (T0-T1)/T0；卡时目标比较 (H0-H1)/H0。
集群目标比较所有受影响任务加权剩余完成时间减少量与其基线加权剩余完成时间之比。
由于每个任务提交时间不变，完成时刻减少量等于 JCT 减少量；这里的比率归一化分母为剩余时间，不是历史已发生的完整 JCT。
所有目标都检查当前任务最大延后与 deadline。超出限制为 negative。
数据过期、未来时间戳、NaN/Infinity、负数、跨恢复代次、资源身份不匹配或缺少证据均为 unknown。
模型输出为情景估计，不是已实现收益或统计置信区间。

## 可观测与回归

每次评估输出 plan ID、mode、result、reason 和估计值的结构化日志内容；不记录 HTTP token 或原始响应。
Prometheus提供收益评估/探索终态计数及最新吞吐、收益、有效性、探索状态Gauge，复用现有Master标签，详见[收益Prometheus指标](vertical-elastic-benefit-prometheus-cn.md)。
最新请求及结果保存在 guard.last_request / guard.last_result（进程内，非持久化账本）。
强制拒绝在通知 checkpoint 之前结束计划，记录 skipped/benefit_unknown 或 skipped/benefit_negative。
未开启探索时，被门禁拒绝的计划不会替换 worker。受控探索的准入与观测见下文。

针对性测试覆盖时间正收益但卡时负收益、缩容队列收益、重放、deadline、非法/缺失数据、HTTP 超时、身份校验、三种运行模式及 checkpoint 前拦截。


## 可选：无候选性能时的只观测探索

策略固定为 `observe_only`：执行一次候选配置切换，采集至少两个独立稳定窗口，记录实测吞吐与预计收益。收益不足也保留候选配置，不生成反向计划，不再次保存 checkpoint 回退。普通收益门禁的事前拒绝逻辑不变；探索默认关闭，每个 job_uid 最多一次。

### 配置与准入

仅 `enforce`、`time/gpu_hours`、候选明确 `{"status":"unavailable"}` 时允许申请探索。HTTP 错误、基线无效或缺字段不属于探索许可。Master 配置：

| 环境变量 | 默认值 | 含义 |
| --- | --- | --- |
| DLROVER_BENEFIT_EXPLORE | false | 必须显式设 true |
| DLROVER_BENEFIT_STATE_DIR | 空 | 共享持久 POSIX 目录，支持 flock、原子 rename、fsync |
| DLROVER_BENEFIT_TRIAL_SECONDS | 600 | 从准入起总时间上限，包含 checkpoint、切换、恢复和观测 |
| DLROVER_BENEFIT_TRIAL_POLL_SECONDS | 10 | 观测轮询间隔 |
| DLROVER_BENEFIT_TRIAL_GPU_HOURS | 0 | 必须显式设置的总卡时预算 |
| DLROVER_BENEFIT_TRIAL_COOLDOWN_SECONDS | 1800 | 完成后冷却；不意味着允许第二次探索 |

准入响应在完整收益快照上增加：

```json
{
  "candidate": {"status": "unavailable"},
  "exploration": {
    "policy": "observe_only",
    "allowed": true,
    "checkpoint_compatible": true,
    "quiescent_checkpoint": true,
    "gpu_tiers": [1, 2]
  }
}
```

`policy` 必须显式返回，以防旧服务按自动回退策略准入。GPU 仅允许相邻档位，其他资源形状不变。无需 `rollback_supported`、`rollback_seconds_upper`、`rollback_gpu_seconds_upper` 或 `rollback_reservation`。服务不能在收到探索请求时暗中建立回退预留。

设基线剩余时间为 T0，有效探索时长 D=min(trial_seconds,T0)。D 必须大于 2×min_window，max_delay_seconds≥D；若设置 deadline，则需 deadline≥now+T0+D。准入 GPU 秒检查为 max(原单worker卡数,候选单worker卡数)×worker数×D≤gpu_hours×3600。以上只是准入检查，不能保证在预算内获得两轮有效窗口；真实切换过慢仍会超时。

### 替换前授权

保存真实 checkpoint 后持久化 SWITCHING，发送 `exploration.authorize_mutation=true`，上下文包含 policy、id、status、checkpoint_path；请求 current/target 为原配置/候选配置。服务必须回显 schema_version=1、request_id、job、current、target、exploration_id，提供新鲜 observed_at、scheduling.target_feasible=true、constraints_satisfied=true，以及 exploration.policy=observe_only、allowed=true。

控制请求不依赖新 SwanLab 窗口，训练此时可能已暂停。授权失败不提交后续 worker 替换，记录 exploration_authorization_denied；仍需核对已开始的 checkpoint 协议。

### 观测、结果与约束

切换控制完成后进入 OBSERVING。此时请求 current 和 target **都为候选配置**，原资源位于 exploration.original，不再表达反向调度意图。上下文另含 applied_plan_id、checkpoint_path、observation_after、last_window_end、started_at、id、policy。

响应保留完整快照，增加 exploration_id 和真实 execution：

```json
{
  "exploration_id": "本次探索ID",
  "execution": {
    "applied_plan_id": "本次探索ID",
    "restored_checkpoint": "实际恢复的checkpoint路径",
    "all_workers_ready": true,
    "generation": "实际新代次",
    "trial_gpu_seconds": 1200
  }
}
```

trial_gpu_seconds 是从探索开始的真实累计资源占用，不能由利用率推算。execution 与 training generation 必须一致且不同于原代次；训练单位、目标、batch 语义须与基线一致，step 超过真实 checkpoint，窗口不早于恢复/观测下界和上一窗口终点。至少两个非重叠完整窗口，取吞吐最小值 q1。重复文件不能增加窗口数。

设已耗时间 E、当前剩余工作 R、累计 GPU 秒 S、原总GPU数 G0、候选总GPU数 G1：

```text
keep_seconds = max(0,R)/q1
时间收益 = T0 - E - keep_seconds
卡时目标收益 = G0*T0 - S - G1*keep_seconds  （单位GPU秒）
```

使用基线和当前 limits 中更严格的延迟/deadline。结果处理：

| 条件 | 状态与行为 |
| --- | --- |
| 两窗口有效、约束满足且整体正收益达到最小比例 | ACCEPTED / positive_observed_gain，继续候选配置 |
| 两窗口有效、约束满足但收益不足 | RETAINED / insufficient_observed_gain，继续候选配置，evaluation_status=negative |
| 有效测量但延迟/deadline违反 | INCONCLUSIVE / constraint_violation，保存实测负向结果，阻止后续自动调整 |
| 观测时间/卡秒耗尽、完成训练前不足两窗口 | INCONCLUSIVE，阻止后续自动调整；不声明获得有效正收益 |
| checkpoint/执行失败 | FAILED，保留保护 |

所有分支均不自动回退。INCONCLUSIVE/FAILED 不保证训练健康，也不停止现有进程；需要核对真实执行状态。RETAINED 表示完成观测，不表示获得正收益。正收益仍是基于实测吞吐的剩余时间预测，不是完整作业实际收益结算。

### 终态回传

ACCEPTED、RETAINED、INCONCLUSIVE、FAILED 均调用 `report_result`，发送 exploration.report_result=true、policy、id、status、reason、objective、result、windows、candidate_observed_at、candidate_generation。result 在有效测量时包含 candidate_throughput、baseline_throughput、overall_gain、gain_unit、gain_ratio、time_gain_seconds、gpu_seconds_saved、keep_seconds、elapsed_seconds、remaining_work、trial_gpu_seconds、evaluation_status、constraints_satisfied、unit、data_source、windows、candidate_resource；证据不足时 result 可为 null，不得作为已验证候选收益入库。

服务按探索 ID 幂等记录，返回匹配 request_id、exploration_id 和 recorded=true，Master 才将 reported=true 持久化。回传不获取 SwanLab 数据，不因训练已结束而失效；失败保留本地结果，在下次进入/重启尝试回传，不提供后台无限重试。结果可作为后续 candidate.evidence=trial 的数据来源；本实现没有新增候选数据库或自动生成历史候选模型。

### 重启兼容

新策略仅 OBSERVING 可恢复观测，不重置到期时间。PREPARING/SWITCHING 中断、FAILED/INCONCLUSIVE 保持保护，不能自动重放资源修改。关闭探索开关也不清除 journal。

无 policy 或旧策略的未完成 journal（包括 OBSERVING、ROLLING_BACK、VERIFYING_ROLLBACK）要求人工核对，不自动继续旧回退。已完成的旧 ACCEPTED/RETAINED/ROLLED_BACK 仍可识别；未回传者仅发送兼容 release_reservation 通知来结清旧预留，绝不触发反向计划。升级时保留 journal 和现场，核对 Pod、checkpoint、旧服务账本，不通过删除 journal 绕过保护。
