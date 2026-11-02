# DLRover 垂直弹性收益计算指标清单

整理日期：2026-10-16。

本文按当前代码实现整理收益计算使用的指标、字段、数据来源、计算公式和适用边界。整体规划见[收益指标体系设计方案](vertical-elastic-benefit-metrics-design-cn.md)，接口及部署配置见[收益门禁与 HTTP 数据契约](vertical-elastic-benefit-http-cn.md)。

## 1. 当前计算范围

当前收益计算比较完成相同剩余训练工作量时，保持原配置与执行资源调整两种方案的预计时间和 GPU 卡时。

- 支持时间收益、GPU 卡时收益和集群加权预计完成时间收益。
- 通用计算器支持 steps、samples、tokens；当前 SwanLab scalar 适配器使用 **steps 口径**。
- steps 指已完成的 optimizer step，比较前后应保持每个 step 的训练工作量可比。
- 无候选吞吐时，不按 GPU 数量线性推算性能；可在显式开启且满足准入条件时进行受控探索。
- 当前结果主要用于预测和探索决策，不等同于完整的作业生命周期实际收益结算。

## 2. 直接参与计算的基础指标

| 指标 | 当前字段或默认 key | 含义与单位 | 当前来源 |
| --- | --- | --- | --- |
| 已完成训练步数 | `train/global_step` → `training.start/end.work` | 已完成的 optimizer step | SwanLab 接口 |
| 总训练步数 | `train/total_steps` → `training.target_work` | 本次训练的固定目标，step | SwanLab 接口 |
| 训练采样时间 | `createTime` → `start/end.observed_at` | 窗口起止时间，转换为 Unix 秒 | SwanLab 接口；已确认是采样时间 |
| 当前吞吐 q0 | `current_throughput` | 原配置稳定训练速度，steps/s | 根据进度和时间差计算 |
| 候选吞吐 q1 | `candidate.throughput_lower` | 候选配置的保守吞吐，steps/s | 收益证据接口；探索时采用调整后实测值 |
| 调整前 worker 数 | `current.workers` | 原配置 worker 数量 | DLRover |
| 调整前每 worker 卡数 | `current.gpus_per_worker` | 原配置单 worker GPU 数量 | DLRover |
| 调整后 worker 数 | `target.workers` | 目标配置 worker 数量 | DLRover 资源计划 |
| 调整后每 worker 卡数 | `target.gpus_per_worker` | 目标配置单 worker GPU 数量 | DLRover 资源计划 |
| 切换时间上界 D | `transition.lost_seconds_upper` | 预计无有效进展的切换耗时，秒 | 收益证据接口 |
| 重放工作量上界 R | `transition.replay_work_upper` | 恢复后需要重复执行的工作量，step | 收益证据接口 |
| 切换资源开销上界 C | `transition.gpu_seconds_upper` | 切换阶段累计资源占用，GPU·秒 | 收益证据接口 |

三个切换代价字段当前仍是外部输入，尚未自动根据 DLRover 历史执行记录估算。接口可用不意味着数据已经在生产完成采集，具体来源仍需证据服务对接。

### 2.1 基础计算公式

```text
当前吞吐 q0 = (结束 step - 开始 step) / (结束采样时间 - 开始采样时间)
剩余工作量 W = 总训练步数 - 结束 step

调整前总卡数 G0 = 原 worker 数 × 原每 worker 卡数
调整后总卡数 G1 = 目标 worker 数 × 目标每 worker 卡数

保持原配置的预计剩余时间 T0 = W / q0
执行调整的预计剩余时间 T1 = D + (W + R) / q1

原配置预计剩余卡时 H0 = G0 × T0 / 3600
调整后预计剩余卡时 H1 = [C + G1 × (W + R) / q1] / 3600

预计时间收益 = T0 - T1
预计卡时收益 = H0 - H1
```

重放工作量只增加执行开销，不增加训练目标和净产出。切换卡时 C 应覆盖切换期间的资源占用，包括必要的新旧资源重叠；不能遗漏，也不能与稳态运行部分重复计算。

当前仅支持 worker 数不变、GPU 资源类型一致的垂直调整，不进行异构 GPU 性能或价格折算。

### 2.2 收益判定

```text
时间目标：gain = T0 - T1，baseline = T0
卡时目标：gain = H0 - H1，baseline = H0

正收益条件：gain > 0 且 gain ≥ 最小收益比例 × baseline
```

此外，数据必须有效，资源必须可调度，延迟和截止时间约束必须满足。最小收益比例默认 5%，可通过配置修改。

## 3. 数据有效性与执行约束

这些字段多数不直接进入收益公式，但决定计算结果能否用于执行。

| 指标或条件 | 字段 | 用途与判断方式 |
| --- | --- | --- |
| 实际 global batch size | `train/global_batch_size` | 确保 step 工作量可比；检查窗口内一致，探索时还检查前后一致 |
| 训练运行代次 | DLRover 内部 generation 与开始时间 | 按代次时间边界过滤历史，合格窗口附加当前代次 |
| 是否处于稳定阶段 | DLRover 推断 → `training.steady` | 恢复确认、预热及连续子窗口吞吐稳定性检查 |
| 训练配置是否可比 | `training.comparable` | 由证据服务确认模型、数据、并行方式等训练语义可比 |
| GPU 资源类型 | `current/target.gpu_type` | 当前要求前后资源类型一致；该字符串不能独立证明硬件性能一致 |
| 候选吞吐证据 | `candidate.evidence`、`candidate.evidence_id` | 要求来自可追溯的 trial、profile 或 history |
| 数据年龄 | 当前时间减 `observed_at` | 默认最大 60 秒，可配置；同时检查证据快照及训练窗口末端 |
| 观测窗口长度 | `end.observed_at - start.observed_at` | 默认至少 30 秒，可配置 |
| 目标资源可调度 | `scheduling.target_feasible` | 必须明确为 true |
| 调度约束满足 | `scheduling.constraints_satisfied` | 必须明确为 true |
| 最大允许额外延迟 | `limits.max_delay_seconds` | 要求 T1 - T0 不超过该值 |
| 任务截止时间 | `limits.deadline_at` | 若设置，要求当前时间 + T1 ≤ deadline |
| 最小收益比例 | `DLROVER_BENEFIT_MIN_GAIN_RATIO` | 默认 5%，相对于基线量判断 |
| 数据归属与完整性 | `taskId`、`request_id`、任务及资源信息 | 防止串任务、错配置、冲突重复点或不完整采样 |

实际 global batch size 当前主要用于校验，没有在收益公式中自动将 step 转换成 samples。正常预测时，候选配置的可比性还依赖证据服务核实。

当前没有将采集覆盖率、上报延迟、基线可信度实现为独立评分指标；已有的是时效、完整性、代次和证据合法性检查。

## 4. 只观测探索专用指标

仅在enforce、探索开启、候选明确unavailable及其他准入条件满足时执行，支持time/gpu_hours。策略固定observe_only，切换后记录候选实测收益，不自动回退。

| 指标或条件 | 字段 | 用途 |
| --- | --- | --- |
| 候选保守吞吐 | measured_lower | 至少两个独立完整窗口吞吐的最小值 |
| 已耗时间 | 当前时间-started_at | 包含切换、恢复、预热、观测和人工等待 |
| 累计GPU秒 | execution.trial_gpu_seconds | 真实资源计量，不用GPU利用率代替 |
| 探索时间上限 | DLROVER_BENEFIT_TRIAL_SECONDS | 默认600秒，有效时长=min(配置值,基线剩余秒数) |
| 探索卡时预算 | DLROVER_BENEFIT_TRIAL_GPU_HOURS | 需显式配置，不额外要求回退预留 |
| 新配置执行确认 | execution.applied_plan_id/all_workers_ready | 实际生效计划和全部worker就绪 |
| checkpoint及代次 | execution.restored_checkpoint/generation | 实际恢复来源、新代次且work超过checkpoint |
| 准入能力 | policy/allowed/checkpoint_compatible/quiescent_checkpoint/gpu_tiers | observe_only授权、恢复能力与相邻档位 |

### 4.1 整体收益

设基线预计剩余时间Tbase、已耗时间E、当前剩余工作R、候选实测吞吐q1、累计GPU秒S、原/候选总GPU数G0/G1：

```text
keep_seconds = max(0,R)/q1
时间收益 = Tbase - E - keep_seconds
资源收益 = G0*Tbase - S - G1*keep_seconds
```

不再计算未来回退收益。有效观测且约束满足时，正收益达到阈值为ACCEPTED，否则RETAINED并保存负向结果。违反延迟/deadline则INCONCLUSIVE并保留保护；证据或预算不足不等于已经测得负收益。所有分支不自动回退。

### 4.2 探索输出

| 字段 | 含义 |
| --- | --- |
| candidate_throughput / baseline_throughput | 候选保守实测/原基线吞吐 |
| overall_gain / gain_unit | 整体收益及seconds或gpu_seconds单位 |
| gain_ratio | 整体收益相对于基线时间或基线GPU秒的比例 |
| keep_seconds / elapsed_seconds | 继续候选预计剩余秒数/已耗秒数 |
| remaining_work / trial_gpu_seconds | 剩余工作/已耗卡秒 |
| evaluation_status / constraints_satisfied | 收益评价/延迟与deadline是否满足 |
| unit / data_source / windows | 工作量单位、mock或http来源、窗口数 |
| candidate_resource | 本次实际观测的候选资源配置 |
| candidate_generation / candidate_observed_at | journal中的候选代次和窗口末端时间 |

资源目标收益需除3600才能转换为GPU小时。result持久化后通过report_result回传；INCONCLUSIVE/FAILED也回传，但无有效测量时result为空。结果保存不等于已建立跨作业候选数据库，服务仍需自行做可比性与复用判断。

## 5. 集群收益额外指标

当前 `cluster_jct` 计算受影响任务的加权预计完成时间改善，需要外部调度服务提供影响预测。

| 指标 | 字段 | 用途 |
| --- | --- | --- |
| 队列影响是否完整 | `scheduling.impacts_complete` | 必须明确覆盖受影响任务 |
| 当前任务权重 | `scheduling.job_weight` | 当前任务的收益权重 |
| 其他受影响任务标识 | `scheduling.other_jobs[].job_id` | 防止重复计入或再次计入当前任务 |
| 其他任务基线剩余完成时间 | `other_jobs[].baseline_remaining_seconds` | 不调整时预计还需多久完成 |
| 其他任务调整后剩余完成时间上界 | `other_jobs[].candidate_remaining_seconds_upper` | 本次调整影响下预计还需多久完成 |
| 其他任务权重 | `other_jobs[].weight` | 反映优先级或业务价值 |

```text
集群加权时间收益
= 当前任务权重 × 当前任务时间收益
  + Σ[其他任务权重 × (基线剩余完成时间 - 调整后剩余完成时间)]

集群加权基线时间
= 当前任务权重 × 当前任务基线剩余时间
  + Σ[其他任务权重 × 其他任务基线剩余完成时间]
```

正收益要求集群加权时间收益为正，并达到最小收益比例乘以集群加权基线时间。当前任务自身的延迟、截止时间和调度约束仍需满足。

涉及排队任务时，剩余完成时间应包含等待时间。当前没有直接计算“相同卡资源下每小时多完成多少任务”，也没有直接用队列长度推算收益。

## 6. 常规预测结果

| 结果字段 | 含义 | 单位 |
| --- | --- | --- |
| `current_throughput` | 当前配置吞吐 | 当前适配为 steps/s |
| `candidate_throughput` | 候选配置吞吐 | 当前适配为 steps/s |
| `remaining_work` | 剩余工作量 | 当前适配为 step |
| `baseline_seconds` | 保持原配置的预计剩余时间 | 秒 |
| `candidate_seconds` | 执行调整后的预计剩余时间，包含切换 | 秒 |
| `time_gain_seconds` | 预计节省时间，T0 - T1 | 秒 |
| `baseline_gpu_hours` | 原配置预计剩余卡时 | GPU·小时 |
| `candidate_gpu_hours` | 调整后预计剩余卡时 | GPU·小时 |
| `gpu_hours_saved` | 预计节省卡时，H0 - H1 | GPU·小时 |
| `cluster_jct_gain_seconds` | 受影响任务的加权预计时间收益 | 权重×秒 |
| `status` | `positive`、`negative` 或 `unknown` | 分类值 |
| `reason` | 结果原因，例如收益不足、超时限或证据缺失 | 分类值 |

基线/候选吞吐、收益比例、时间收益、GPU秒收益、有效性和证据时间已接入Prometheus；探索还暴露窗口数、累计卡秒、状态和回传确认。完整名称与查询见[收益Prometheus指标](vertical-elastic-benefit-prometheus-cn.md)。其他计算字段仍保存在评估结果或journal中。证据缺失时也不保证返回完整结果字段；例如候选吞吐明确不可用时，只返回当前吞吐、剩余工作量和基线时间等已知结果。

## 7. SwanLab 接口采集清单

当前平台固定 batch 模式开启 `DLROVER_SWANLAB_ASSUME_FIXED_BATCH=true` 后，请求
`global_step`、`max_steps`、`train_speed`（step/s）。不需要在线 global_batch_size，
但事件明确记录 `batch_size_source=assumed_fixed`。窗口吞吐继续由进度增量与采样时间计算，
train_speed 用于交叉核对。以下为未开启该假设时的原指标模式。

当前默认请求以下三个 key，实际 key 可通过 `DLROVER_SWANLAB_KEYS` 配置：

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

已确认 `createTime` 为采样时间，默认 `DLROVER_SWANLAB_TIME_SOURCE=sample`，直接使用各采样点的 `createTime`。
不使用请求时间或入库时间代替采样时间，也不请求 train/observed_at。
同批三个指标需要保留共同的 `(epoch, createTime)`，不能按数组位置关联。

generation 及其开始时间由 DLRover 内部提供，用于过滤旧数据；steady 由恢复确认、预热、
连续进度和多个窗口吞吐波动推断。SwanLab 不需要返回 generation 或 steady。
详细配置见 [HTTP 契约的代次边界与推断稳态](vertical-elastic-benefit-http-cn.md#代次边界与推断稳态)。

## 8. 当前未直接参与计算的指标与实现边界

| 指标或能力 | 当前情况 |
| --- | --- |
| `train/remaining_time` | 未直接用于当前收益公式；基线剩余时间由固定剩余工作量和当前吞吐计算 |
| `train/token_acc` | 不作为训练产出；准确率不等同于累计 token 数 |
| loss、准确率等质量指标 | 尚未接入收益决策中的自动质量退化判断 |
| GPU 利用率、显存利用率 | 不直接进入收益公式；不能据此认定收益为正 |
| 累计有效 samples/tokens | 通用计算器支持单位，但当前 SwanLab 客户端没有实现对应的工作量适配 |
| 货币成本 | 当前计算 GPU 卡时，尚未引入卡型单价、计费规则或金额折算 |
| 集群任务完成吞吐量 | 尚未计算单位时间完成任务数及其相对增益 |
| 实际全生命周期收益账本 | 尚未实现跨重启的完整资源、工作量和收益结算 |
| 数据覆盖率、上报延迟、基线可信度评分 | 尚未作为独立统计指标实现 |

## 9. 实现位置

- [收益评估及 HTTP 证据获取](../../dlrover/python/master/resource/benefit.py)
- [SwanLab scalar 接口适配](../../dlrover/python/master/resource/swanlab.py)
- [探索准入、观测与结果记录](../../dlrover/python/master/resource/exploration.py)
- [训练端 scalar 上报辅助函数](../../dlrover/python/common/training_metrics.py)
- [Master 训练代次生成](../../dlrover/python/master/elastic_training/rdzv_manager.py)
- [资源调整执行入口](../../dlrover/python/master/node/job_resource_scaler.py)
