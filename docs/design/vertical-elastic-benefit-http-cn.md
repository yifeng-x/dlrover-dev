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
    {"key": "train/global_batch_size"},
    {"key": "train/generation"},
    {"key": "train/steady"}
  ]
}
```

响应按 `[{taskId, key, scalarMetricsList: [{index, data, createTime, epoch}]}]`
解析。`data` 支持数值字符串；请求与响应 taskId 必须一致。
`index` 不作为 global step，`epoch` 不作为恢复代次。
`train/token_acc` 是准确率，不作为训练产出；`train/remaining_time` 不代替固定总训练目标。
本客户端首期只生成 steps 口径，不能通过把 work key 改成 token_acc 或 token 数改变单位。

### Master 配置

| 环境变量 | 默认值 | 含义 |
| --- | --- | --- |
| DLROVER_SWANLAB_HTTP_URL | 空 | 完整 scalar 接口 URL；非空开启独立训练数据获取 |
| DLROVER_SWANLAB_TASK_ID | 空 | 必填，训练任务的实际 taskId，不默认使用 job_uid |
| DLROVER_SWANLAB_HTTP_TOKEN | 空 | 可选 Bearer token，与收益证据服务 token 分离 |
| DLROVER_SWANLAB_UTC_OFFSET | +08:00 | createTime 无时区时使用的 UTC 偏移 |
| DLROVER_SWANLAB_KEYS | {} | JSON 对象，将内部字段映射到实际训练 key |

例如：

```yaml
env:
  - name: DLROVER_BENEFIT_MODE
    value: "shadow"
  - name: DLROVER_BENEFIT_HTTP_URL
    value: "http://benefit-service/benefit/snapshot"
  - name: DLROVER_SWANLAB_HTTP_URL
    value: "http://metrics-service/metrics/scalar/by-task-name"
  - name: DLROVER_SWANLAB_TASK_ID
    value: "实际任务 ID"
  - name: DLROVER_SWANLAB_KEYS
    value: '{"work":"train/global_step","target_work":"train/total_steps","global_batch_size":"train/global_batch_size","generation":"train/generation","steady":"train/steady"}'
```

内部字段及默认 key 如下：

| 内部字段 | 默认 key / 来源 | 语义 |
| --- | --- | --- |
| work | train/global_step | 已完成的 optimizer step，checkpoint 恢复后继承其进度 |
| target_work | train/total_steps | 固定训练目标 |
| global_batch_size | train/global_batch_size | 每个 optimizer step 实际处理的全局样本数 |
| generation | train/generation | DLRover 为当前训练代次生成的整数标识 |
| steady | train/steady | 0 为恢复、重放或预热中；1 为可用于稳态测量 |
| observed_at | 默认使用 createTime | 可映射到如 train/observed_at，值为训练采样 Unix 秒 |

如果 createTime 后续改成服务入库时间，应设置
`"observed_at":"train/observed_at"` 并由训练端同步上报。
HTTP 超时、窗口长度和时效阈值沿用 DLROVER_BENEFIT 配置；两次 HTTP 各自受超时约束。
未配置 scalar URL 时保留原完整快照模式；配置后任何 scalar 获取失败都不回退使用远端旧 training 数据。

### generation 生成与训练端上报

训练 rendezvous 每次完成时，由 DLRover Master 生成新的随机整数代次，范围为
`1 .. 2^53-1`，可由 float64 scalar 链路精确表示。RPC 中以字符串传递。
同一轮全部 rank 共用；首次训练、扩缩容、回退、故障后重新 rendezvous 都更新代次。
Agent 在启动训练进程前设置 `DLROVER_TRAIN_GENERATION`，训练子进程继承。
网络检查 rendezvous 不覆盖训练代次。此链路适用于 DLRover Master rendezvous launcher；
绕过该 launcher 的自定义训练入口需要接入相同代次机制。

仓库不包含用户的 SwanLab 训练上报实现。训练脚本应在 global rank 0、完成 optimizer step 后，
将辅助函数返回的指标与原有指标放进同一次日志上报：

```python
from dlrover.python.common.training_metrics import training_metric_payload

# 与已有 SwanLab log 调用合并；其他 rank 不重复上报。
metrics = training_metric_payload(
    global_step=global_step,
    total_steps=total_steps,
    global_batch_size=actual_global_batch_size,
    steady=restore_replay_and_warmup_finished,
)
swanlab.log(metrics)
```

函数读取 DLRover 环境变量，不接受用户自己生成的代次；未取得有效代次时报错。
辅助函数默认也生成 `train/observed_at`，Master 可按需请求该 key。
若使用自定义 key，训练端也配置相同的 `DLROVER_SWANLAB_KEYS`，或传入 `keys` 参数。
`steady` 默认 false，不能为通过收益校验而在恢复或重放期间写死为 true。

服务端必须为一次日志批次保留相同的 `(epoch, createTime)`，并返回该批次的所有被请求指标。
适配层以此关联 generation 与进度，绝不按数组位置、最近时间或最新 generation 给历史点补标签。
不同 key 的 index 可以不同。如服务不保留共同批次标识，需先调整服务端存储/返回逻辑；
此时客户端会返回 unknown，不推测数据关联。

窗口仅选取 Master 当前 generation 的数据，校验完整批次、稳态标志、单调进度、
一致的总步数和实际 global batch size。重复相同点去重，冲突点、过期或缺失数据拒绝使用。
探索时还限定窗口在恢复完成之后、且不与已接受窗口重叠；前后 global batch size 不一致拒绝比较。
模型、数据集和并行语义的可比性仍由证据服务核实，不能仅凭这些 scalar 证明。

Master 重启后不会从 SwanLab 历史数据猜测当前 generation；未建立新的训练 rendezvous 时，
无法获得当前代次，收益评估返回 unknown。未完成探索继续受 journal 保护，需按恢复流程处理。

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
Prometheus 新增 `dlrover_master_vertical_benefit_evaluations_total{mode,result,reason}`，复用现有 Master 标签。
最新请求及结果保存在 guard.last_request / guard.last_result（进程内，非持久化账本）。
强制拒绝在通知 checkpoint 之前结束计划，记录 skipped/benefit_unknown 或 skipped/benefit_negative。
未开启探索时，被门禁拒绝的计划不会替换 worker。受控探索的准入与回退见下文。

针对性测试覆盖时间正收益但卡时负收益、缩容队列收益、重放、deadline、非法/缺失数据、HTTP 超时、身份校验、三种运行模式及 checkpoint 前拦截。


## 可选：无候选性能时的受控探索

仅 `enforce` 下开启；默认关闭。首期支持 `time` / `gpu_hours` 自动探索。
`cluster_jct` 仍要求事前候选性能与队列预测，不对未知的集群影响进行试错。
每个 job_uid 最多一次探索、一次回退，不引入新服务或通用工作流框架。

### 新增配置

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| DLROVER_BENEFIT_EXPLORE | false | 是否启用探索，建议只注入白名单作业 |
| DLROVER_BENEFIT_STATE_DIR | 空 | 必填，挂载持久化共享 POSIX 卷的绝对路径；必须支持跨进程/Pod flock 与原子 rename |
| DLROVER_BENEFIT_TRIAL_SECONDS | 600 | 从准入到完成观测的总预算，含 checkpoint、替换、恢复和预热 |
| DLROVER_BENEFIT_TRIAL_POLL_SECONDS | 10 | HTTP 观测轮询间隔 |
| DLROVER_BENEFIT_TRIAL_GPU_HOURS | 0 | 必须显式设置的探索总卡时预算，包含预留回退资源和回退开销 |
| DLROVER_BENEFIT_TRIAL_COOLDOWN_SECONDS | 1800 | 探索终态后的资源调整冷却时间 |

状态目录不能用 Pod emptyDir，也不能通过删除 journal 或取消挂载来绕过未完成计划。
使用文件锁是部署前提，不声称支持不具备可靠 POSIX 锁语义的对象存储或任意 NFS 配置。

### 开关控制

在 DLRover Master 容器中设置 `DLROVER_BENEFIT_EXPLORE`，默认 `false`。
只有 `true`（忽略大小写及首尾空格）允许探索；其他值均按关闭处理。
开关在 Master 初始化时读取，不支持运行中热更新。

```yaml
env:
  - name: DLROVER_BENEFIT_MODE
    value: "enforce"
  - name: DLROVER_BENEFIT_EXPLORE
    value: "true"
  - name: DLROVER_BENEFIT_STATE_DIR
    value: "/var/lib/dlrover/benefit"  # 挂载持久化共享卷
  - name: DLROVER_BENEFIT_TRIAL_GPU_HOURS
    value: "10"  # 示例值，按作业配置预算
```

上述配置需配合前文 HTTP 接口配置及准入条件使用。关闭探索时，将
`DLROVER_BENEFIT_EXPLORE` 改为 `"false"` 并在 Master 下次启动时生效。

| 收益门禁模式 | 探索开关 | 行为 |
| --- | --- | --- |
| off | 任意 | 不执行收益门禁，不进入受控探索 |
| shadow | 任意 | 记录预测结果，不因预测拒绝调整，不进入受控探索 |
| enforce | false / 未设置 | 缺少候选吞吐时拒绝调整；有候选吞吐时按收益门禁判断 |
| enforce | true | 明确缺少候选吞吐且通过全部探索准入条件时，允许受控探索 |

关闭开关不会自动回退已有探索或清除其状态。若重启时存在未完成探索，
仍保留状态保护并阻止新的自动调整；关闭状态下不自动恢复探索观测。
应完成当前探索后关闭；对遗留未完成状态，需核对实际资源与 checkpoint 后处理，
或恢复原配置继续可恢复的观测阶段。

### 准入响应扩展

只有明确返回 `candidate: {"status": "unavailable"}` 才可能进入探索。
字段缺失、HTTP 故障、过期数据、跨代次或其他验证错误都不触发探索。
原有 training、transition、scheduling、limits 仍需完整且可信。
另外提供：

```json
{
  "candidate": {"status": "unavailable"},
  "exploration": {
    "allowed": true,
    "checkpoint_compatible": true,
    "rollback_supported": true,
    "quiescent_checkpoint": true,
    "gpu_tiers": [1, 2, 4, 8],
    "rollback_seconds_upper": 300,
    "rollback_gpu_seconds_upper": 2400,
    "rollback_reservation": {
      "id": "scheduler-reservation-123",
      "confirmed": true,
      "expires_at": 1790010000
    }
  }
}
```

调度适配服务必须真正建立预留，并提供其 ID 和到期时间；代码不会凭布尔字段创建预留。
没有真实预留能力时不要返回 confirmed=true，自动探索应保持关闭。
预留覆盖去程、观测以及一次回退；资源占用核算包含预留资源。
本地还检查相邻合法 GPU 档位、worker 全部 Running 且数量和 GPU 配置一致、CPU/内存等其他配置未变。
quiescent_checkpoint 表示框架在 checkpoint 边界停止更新，且最新 checkpoint 可跨档位恢复；不支持该协议的框架不能探索。
当前任务剩余时间必须大于探索及回退预算，允许延后时间和 deadline 余量也必须覆盖两者。

### 执行、观测与反馈

状态为 PREPARING → SWITCHING → OBSERVING，然后进入 ACCEPTED / RETAINED，
或者 ROLLING_BACK → VERIFYING_ROLLBACK → ROLLED_BACK。
FAILED / INCONCLUSIVE 会封禁后续自动伸缩，等待对账处置。

Master 在同一个伸缩线程中持锁直到观测或回退终结；共享文件锁进一步防止另一个 Master 同时执行。
进入替换 worker 前先持久化 checkpoint 和 SWITCHING 状态，再重新向调度接口确认预留。
HTTP 请求新增 exploration，包含 id、status、reservation_id、checkpoint_path、applied_plan_id。
授权资源变更请求带 authorize_mutation=true；服务需回传身份、最新 observed_at、exploration_id、调度约束和有效预留。
正常观测请求中 current 为正在测试的配置，target 为原配置，便于检查回退可行性。

观测响应沿用 training 时间窗口，并新增：

```json
{
  "exploration_id": "探索计划ID",
  "execution": {
    "applied_plan_id": "当前实际生效的计划ID",
    "restored_checkpoint": "实际加载的checkpoint路径",
    "all_workers_ready": true,
    "generation": "恢复代次",
    "trial_gpu_seconds": 2400
  },
  "exploration": {
    "rollback_reservation": {
      "id": "scheduler-reservation-123",
      "confirmed": true,
      "expires_at": 1790010000
    }
  }
}
```

trial_gpu_seconds 从探索开始累计，包含切换、实际训练和预留资源，必须由分配记录计算，不能用 GPU 利用率换算。
适配服务从真实框架/调度事件验证 applied_plan_id、恢复 checkpoint、全部 worker readiness 和训练代次，不能简单回显这些字段。
至少两个不重叠的稳态窗口，使用其中较低吞吐作为保守测量；重复、旧代次、跨代次、预热前数据均不满足观测条件。
窗口必须发生在控制完成之后；稳态及可比性仍由适配服务结合原始框架指标验证。

评估同时记录两笔账：

1. 从探索开始算整体预测收益，扣除已经发生的时间与实际资源开销。
2. 从当前进度算保持或回退的未来代价，不以已经发生的开销强迫回退。

整体收益达标则 ACCEPTED。退回更划算或当前配置突破约束、且回退满足时限和预算时，才回退一次。
整体收益不足但回退更差时为 RETAINED，明确记录为未验证正收益，不能算作成功降本。
回退重新生成独立 plan ID，并通知当前 worker 保存最新 checkpoint；不加载探索前的旧 checkpoint。
框架 ready 回报必须携带精确 plan ID。回退也必须验证新代次的训练推进。
若训练无法保存新 checkpoint，不自动强制删除 worker；记录 FAILED，等待恢复处置。

终态 HTTP 请求携带 release_reservation=true、result（实测候选吞吐、整体收益、保持/回退估计）、candidate_observed_at 与 candidate_generation。
服务按探索 ID 幂等保存证据并释放预留，返回 request_id、exploration_id、recorded=true。
失败时保留本地结果；后续进入或重启时重发终态通知，预留服务必须保证 TTL 到期释放，不能依赖一次通知。
该回传可作为后续 `candidate.evidence=trial` 的数据来源；不包含完整跨作业收益数据库。

### 重启与失败处置

状态文件原子写入并 fsync，原配置、基线、预算、checkpoint、回退次数和结果一并保存。
Master 重启可继续 OBSERVING / VERIFYING_ROLLBACK，先重新核实真实执行状态，再允许回退或确认完成。
PREPARING / SWITCHING / ROLLING_BACK 中断时无法证明 Kubernetes 副作用是否完成，不自动重放删除/创建。
此时阻止初始 worker 自动拉起并禁用自动 relaunch，需运维核对现存 Pod、checkpoint、预留和训练进度后处理 journal，并重启 Master。
观测失败/超时不等于可以安全回退：没有可信进度或有效回退资源时进入 INCONCLUSIVE，停止自动调整；原训练进程不被强制终止。
上述失败状态、结构化日志和现有 inflight 指标应配置告警；新增 `dlrover_master_vertical_explorations_total{result}` 记录正常观测终态。
