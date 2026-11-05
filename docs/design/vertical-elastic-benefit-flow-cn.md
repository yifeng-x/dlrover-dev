# 垂直弹性收益评估整体流程说明

整理日期：2026-10-30。本文根据当前仓库实现说明资源调整前的收益评估、SwanLab 数据获取、只观测探索流程。文中的配置值是代码默认值，不代表线上任务的实际配置。逐事件说明与完整日志分支见 [BenefitFlow 完整流程](vertical-elastic-benefitflow-reference-cn.md)。

当前流程先生成资源计划，在通知训练保存 checkpoint 前评估收益。有可信候选性能时进行事前预测；明确缺少候选性能时，可以通过受控探索实测新配置，记录正收益或收益不足，保留候选配置。收益门禁默认关闭，探索也默认关闭。

## 1. 整体流程

### 1.1 架构图

```mermaid
flowchart TB
    REQ(["垂直弹性扩缩容请求"])

    subgraph CONTROL["① 弹性决策控制层"]
        direction LR
        CHECK["前置检查<br/>开关 / 并发锁 / 冷却时间 / 次数"]
        PLAN["扩缩容计划生成<br/>当前资源 → 目标资源"]
        GATE{"收益门禁<br/>off / shadow / enforce"}
        DECISION{"执行决策"}
        CHECK --> PLAN --> GATE
    end

    subgraph BENEFIT["② 收益评估层"]
        direction LR
        CONTEXT["训练上下文<br/>Generation / Checkpoint"]
        METRIC["SwanLab 指标查询<br/>历史稳定窗口"]
        ESTIMATE["收益预测服务<br/>时间 / GPU卡时 / 集群收益"]
        CONTEXT --> METRIC --> ESTIMATE
    end

    subgraph EXPLORE["③ 受控探索管理层"]
        direction LR
        ADMIT{"探索准入<br/>预算 / 约束 / 授权"}
        JOURNAL[("探索状态持久化<br/>原配置 / 基线 / 预算 / 策略")]
        ADMIT -->|通过| JOURNAL
    end

    subgraph EXEC["④ 资源调整执行层"]
        direction LR
        CKPT["Checkpoint<br/>保存并确认就绪"]
        AUTH["目标资源与<br/>调度授权校验"]
        RESIZE["Worker 替换<br/>资源与训练参数调整"]
        RECOVER["Rendezvous<br/>训练恢复与进度跟踪"]
        CKPT --> AUTH --> RESIZE --> RECOVER
    end

    subgraph VERIFY["⑤ 实测收益验证层"]
        direction LR
        OBSERVE["周期采集证据<br/>至少两个独立稳定窗口"]
        CALC["整体收益计算<br/>包含切换与恢复开销"]
        VERDICT{"探索结果判定"}
        OBSERVE --> CALC --> VERDICT
    end

    subgraph RESULT["⑥ 状态管理与反馈层"]
        direction LR
        ACCEPT["ACCEPTED<br/>验证收益达标"]
        RETAIN["RETAINED<br/>保留但未验证正收益"]
        PROTECT["INCONCLUSIVE / FAILED<br/>保护状态与人工核查"]
        DONE["常规执行完成"]
        OUTPUT[("状态与证据持久化<br/>事件 / 日志 / 结果反馈")]
        ACCEPT --> OUTPUT
        RETAIN --> OUTPUT
        PROTECT --> OUTPUT
        DONE --> OUTPUT
    end

    REQ --> CHECK

    GATE -->|off：直接放行| DECISION
    GATE -->|shadow / enforce| CONTEXT
    ESTIMATE --> DECISION

    DECISION -->|允许执行| CKPT
    DECISION -->|缺少候选吞吐且允许探索| ADMIT
    DECISION -->|拒绝 / 无效| STOP(["结束本次计划"])

    ADMIT -->|拒绝| STOP
    JOURNAL --> CKPT

    RECOVER -->|常规扩缩容| DONE
    RECOVER -->|受控探索| OBSERVE

    VERDICT -->|正收益达标| ACCEPT
    VERDICT -->|收益不足但约束满足| RETAIN
    VERDICT -->|证据、预算或约束异常| PROTECT

    classDef control fill:#E8F1FF,stroke:#4472C4,color:#17365D
    classDef benefit fill:#E7F4EA,stroke:#3F8F58,color:#20522D
    classDef explore fill:#FFF1D9,stroke:#D89A2B,color:#764B08
    classDef exec fill:#E9EEFF,stroke:#6476C8,color:#293A84
    classDef verify fill:#F0E7FC,stroke:#9264C4,color:#563183
    classDef success fill:#DDF4E4,stroke:#349854,color:#225A34
    classDef warn fill:#FFF0DD,stroke:#D48A2A,color:#81500B
    classDef error fill:#FCE7E7,stroke:#CB6262,color:#832F2F

    class CHECK,PLAN,GATE,DECISION control
    class CONTEXT,METRIC,ESTIMATE benefit
    class ADMIT,JOURNAL explore
    class CKPT,AUTH,RESIZE,RECOVER exec
    class OBSERVE,CALC,VERDICT verify
    class ACCEPT,OUTPUT success
    class RETAIN,DONE warn
    class PROTECT error
```

图中展示已配置独立 SwanLab 客户端的路径。没有配置 scalar URL 且没有开启 mock 时，训练窗口由收益证据服务的完整快照提供。采集或校验失败会得到 `unknown`，不会按缺失值为零继续计算。

### 1.2 核心决策流程图

相比总体架构图，这张图突出三个关键业务分支：off 直接执行、shadow 观察放行、enforce 收益约束与受控探索。

```mermaid
flowchart TD
    START(["触发垂直扩缩容"])

    START --> PRE["前置校验与扩缩容计划"]
    PRE --> VALID{"计划有效且 GPU 数变化？"}

    VALID -->|否| END(["结束"])
    VALID -->|是| MODE{"收益门禁模式"}

    MODE -->|off| NORMAL["常规执行"]
    MODE -->|shadow| EVAL["获取历史稳定窗口与收益证据"]
    MODE -->|enforce| EVAL

    EVAL --> CALC["预测时间、卡时与集群收益"]
    CALC --> POLICY{"门禁模式与评估结果"}

    POLICY -->|shadow：配置有效| LOG["记录评估结果"]
    LOG --> NORMAL

    POLICY -->|enforce：正收益| NORMAL
    POLICY -->|enforce：明确缺少候选吞吐| EXP{"满足探索准入条件？"}
    POLICY -->|enforce：负收益或其他未知| END

    EXP -->|否| END
    EXP -->|是| SAVE[("持久化探索上下文")]
    SAVE --> EXEC

    NORMAL --> EXEC["Checkpoint → 调度校验<br/>Worker调整 → Rendezvous"]

    EXEC --> OK{"执行与训练恢复成功？"}
    OK -->|否| PROTECT["进入异常保护与恢复处理"]
    OK -->|是| IS_EXP{"是否受控探索？"}

    IS_EXP -->|否| DONE(["常规执行完成"])
    IS_EXP -->|是| OBS["采集至少两个独立稳定窗口"]

    OBS --> PROFIT["计算整体实测收益"]
    PROFIT --> RESULT{"收益验证结果"}

    RESULT -->|正收益达标| ACCEPT["ACCEPTED"]
    RESULT -->|收益不足且约束满足| RETAIN["RETAINED"]
    RESULT -->|证据、预算或约束异常| PROTECT

    ACCEPT --> PERSIST[("持久化结果并反馈")]
    RETAIN --> PERSIST
    PROTECT --> PERSIST

    classDef action fill:#E7F0FF,stroke:#5C83C3,color:#1C4270
    classDef decision fill:#FFF0D7,stroke:#C99337,color:#6B470A
    classDef success fill:#E1F5E6,stroke:#4D9E65,color:#236339
    classDef failure fill:#FBE5E5,stroke:#C66B6B,color:#813333

    class PRE,EVAL,CALC,NORMAL,EXEC,OBS,PROFIT,LOG,SAVE action
    class VALID,MODE,POLICY,EXP,OK,IS_EXP,RESULT decision
    class ACCEPT,RETAIN,DONE,PERSIST success
    class PROTECT failure
```

## 2. 资源计划何时进入收益评估

资源调整入口检查功能开关、并发执行保护及探索状态保护。执行线程随后检查冷却时间、优化次数等条件，调用优化器生成目标资源计划。

计划为空、目标 GPU 数无效或与当前相同时，不进入后续资源替换。有效计划进入 `_check_resource_benefit()`，通过后才执行 `_execute_resource_plan()`，通知训练保存 checkpoint。

因此，收益门禁拒绝发生在 checkpoint 通知之前，当前计划不会因为收益评估失败而先替换 worker。普通路径不会在该计划内持续轮询等待训练窗口变稳；`enforce` 下证据不足会结束本次计划，后续需要新的调整触发。

拒绝会将执行阶段和指标记录为 `SKIPPED`，保持原 Worker 训练；仅进入 `REPLACING_WORKERS` 后设置的 `_replacement_started` 标记与 `FAILED` 同时成立时，才触发“Vertical replacement failed”的提前停止分支。其他训练故障停止规则继续生效。

收益门禁的三个模式：

| 模式 | 默认情况及行为 |
| --- | --- |
| `off` | 默认值；跳过收益评估，沿原资源调整路径执行 |
| `shadow` | 配置有效时记录评估结果，不因收益为负或未知阻止调整 |
| `enforce` | 正收益才放行；明确缺少候选吞吐时可尝试探索准入 |

## 3. SwanLab 数据什么时候采集

### 3.1 训练端上报与 Master 查询

训练端上报与 Master 查询是两个独立动作。

`training_metric_payload()` 只生成日志字段，不自动调用 SwanLab，也不向训练脚本注入采样逻辑。其使用约定是在 global rank 0 完成 optimizer step 后，将指标同批上报。实际每隔多少 step 或多少秒上报，由训练脚本的日志配置决定。

Master 从 scalar 接口拉取已经存在的历史采样点，在本地选取满足要求的窗口；不会在一次请求中现场等待 30 秒采样。

| 场景 | 查询时机 | 行为 |
| --- | --- | --- |
| 普通调整前 | 有效计划生成后、checkpoint 通知前 | 一次收益评估中先查询 SwanLab，再请求证据服务 |
| 普通调整完成后 | 执行完成与恢复跟踪阶段 | 普通路径不自动进入持续收益评估循环 |
| 受控探索 | 切换完成进入 `OBSERVING` 后 | 每轮观测后默认等待 10 秒再查询；实际周期还包含请求耗时 |
| 替换前资源变更授权 | checkpoint 就绪后、资源变更前 | 只请求证据服务，不查询 SwanLab |
| 探索终态回传结果 | 探索结束后 | 只请求证据服务，不查询 SwanLab |
| 门禁关闭 | `DLROVER_BENEFIT_MODE=off` | 该收益路径不查询 SwanLab |

### 3.2 查询身份与指标

SwanLab 的 `taskId` 仅来自当前任务顶层标签：

```yaml
metadata:
  labels:
    low_code_platform_task_id: "实际平台任务 ID"
```

该 ID 在扩缩容和 training generation 变化期间保持不变。缺失时不使用 job name、job UID 或进程环境中的其他任务 ID 猜测身份。

| 模式 | 默认查询 key | step 工作量可比性 |
| --- | --- | --- |
| 默认模式 | `train/global_step`、`train/total_steps`、`train/global_batch_size` | 检查窗口内 global batch size 一致 |
| 固定 batch 模式 | `global_step`、`max_steps`、`train_speed` | 显式假设扩缩容前后每个 optimizer step 的工作量固定 |

固定 batch 模式需要设置 `DLROVER_SWANLAB_ASSUME_FIXED_BATCH=true`，不伪造 batch size，而是记录 `batch_size_source=assumed_fixed`。指标 key 可通过 `DLROVER_SWANLAB_KEYS` 映射。

请求示例，采用固定 batch 模式：

```json
{
  "taskId": "实际平台任务 ID",
  "taskScalarList": [
    {"key": "global_step"},
    {"key": "max_steps"},
    {"key": "train_speed"}
  ]
}
```

### 3.3 稳定窗口如何生成

```mermaid
flowchart TD
    A[SwanLab 历史 scalar] --> B[校验任务身份、指标、数值与采样时间]
    B --> C[按 epoch 与 createTime 对齐同批指标]
    C --> D[确认当前 generation 和恢复状态]
    D --> E[采样下界：代次开始与恢复确认的较晚时间，加预热时间]
    E --> F[探索额外限制：不早于开始观测时间与上一窗口终点]
    F --> G[过滤旧数据，按采样时间排序]
    G --> H[检查进度、目标、batch 与采样间隔]
    H --> I[从最新点向前选择子窗口，默认两段各至少 15 秒]
    I --> J{每段吞吐为正且波动不超过 10%？}
    J -->|否| K[unknown：窗口不可用]
    J -->|是| L[输出至少 30 秒的窗口，steady=true]
```

具体规则如下：

1. 使用采样点的 `createTime`，不使用请求时间或入库时间。无时区字符串默认按 `+08:00` 解析。时间语义配置为非 `sample` 时拒绝窗口。
2. 同批指标按 `(epoch, createTime 对应时间戳)` 精确对齐，不按数组下标拼接。缺项或冲突重复值不可用，相同重复点不增加证据。
3. Master 在训练 rendezvous 完成时记录 generation 与开始时间。历史数据先按时间边界过滤，合格窗口才附加当前 generation。
4. 恢复未确认时不接受窗口。默认下界为 `max(代次开始时间, 恢复确认时间) + 60 秒`。探索还要求不早于 `observation_after` 和 `last_window_end`。
5. 最新采样点默认不得早于当前时间 60 秒；未来采样时间非法。进度回退视为异常，目标或 batch 改变、相邻采样间隔超过 60 秒时重新积累稳定序列。
6. 默认选择两个不重叠子窗口，每段至少 15 秒，总跨度至少 30 秒，至少需要三个独立采样点。稀疏采样可能使实际窗口更长。
7. 每段吞吐必须为正，且 `(最大吞吐 - 最小吞吐) / 最小吞吐 ≤ 10%`。
8. 查询过程中 generation 或恢复上下文发生变化，本次评估作废。

例如代次在 `10:00:00` 开始，且没有更晚的恢复确认，默认预热后可使用 `10:01:00` 起的数据。如果 `10:01:00`、`10:01:15`、`10:01:30` 三个点齐全且稳定，理论上 `10:01:30` 起才具备第一个合格窗口，还需等待数据实际上报并可查询。

`steady=true` 是根据恢复、预热和进度推断的稳定状态。模型、数据集和并行语义是否一致，仍需证据服务确认。

## 4. 收益计算的数据来源

| 数据 | 来源 | 用途 |
| --- | --- | --- |
| 已完成 step、总 step、采样时间 | SwanLab 或完整证据快照 | 计算当前吞吐和剩余工作量 |
| 当前、目标 worker 数与每 worker 卡数 | DLRover 资源状态和计划 | 计算前后总卡数 |
| 候选吞吐下界 | 收益证据服务 | 预测目标配置性能；需 `trial/profile/history` 及证据 ID |
| 切换耗时上界 | 收益证据服务 | 扣除无有效训练进展时间 |
| 重放工作量上界 | 收益证据服务 | 计入恢复后的重复执行开销 |
| 切换 GPU·秒上界 | 收益证据服务 | 计入切换期资源占用 |
| 可调度性、约束、允许延迟、deadline | 收益证据服务 | 判断能否执行 |
| 训练语义可比性 | 收益证据服务 | 确认模型、数据和并行语义可比较 |

候选吞吐不按 GPU 数量线性推算。切换成本目前仍由外部输入，尚未自动根据历史执行记录估计。独立 SwanLab 客户端启用后，本次观测会覆盖快照中的训练指标，但仍要求证据服务返回 `training.comparable=true`。

未配置独立客户端时使用完整快照模式；配置后采集失败不会回退使用证据服务中的旧训练数据。`DLROVER_BENEFIT_HTTP_URL` 和 `DLROVER_SWANLAB_HTTP_URL` 是不同接口。

## 5. 常规收益计算

### 5.1 变量与公式

| 符号 | 含义 | 单位 |
| --- | --- | --- |
| q0 | 当前配置稳定吞吐 | steps/s |
| q1 | 候选配置保守吞吐 | steps/s |
| W | 剩余训练工作量 | step |
| G0、G1 | 调整前、后总 GPU 数 | 卡 |
| D | 切换无有效进展时间上界 | 秒 |
| R | 恢复重放工作量上界 | step |
| C | 切换阶段资源占用上界 | GPU·秒 |

```text
q0 = (窗口结束 step - 窗口开始 step) / (窗口结束时间 - 窗口开始时间)
W = 总训练 step - 窗口结束 step
G0 = 当前 worker 数 × 当前每 worker GPU 数
G1 = 目标 worker 数 × 目标每 worker GPU 数

保持当前配置：
T0 = W / q0
H0 = G0 × T0 / 3600

执行调整：
T1 = D + (W + R) / q1
H1 = [C + G1 × (W + R) / q1] / 3600

时间收益 = T0 - T1
GPU 卡时收益 = H0 - H1
```

`H0/H1` 单位是 GPU·小时。重放增加执行开销，不增加净训练目标。`C` 应覆盖切换期间资源占用，包括必要的新旧资源重叠，且不能与稳态训练部分重复计入。

固定 batch 模式也使用进度增量除以采样时间差计算吞吐。最新 `train_speed` 仅用于核对：

```text
speed_relative_difference = |最新 train_speed - q0| / q0
```

该差异本身不直接决定放行。当前 scalar 适配器采用 steps 口径，通用计算器还支持 samples、tokens。

### 5.2 判定与约束

正收益要求 `gain > 0` 且 `gain ≥ min_gain_ratio × baseline`，默认最小收益比例为 5%。

| 目标 | gain | baseline |
| --- | --- | --- |
| `time` | T0 - T1 | T0 |
| `gpu_hours` | H0 - H1 | H0 |
| `cluster_jct` | 受影响任务加权预计剩余完成时间减少量 | 同一组任务的加权基线剩余完成时间 |

所有目标均检查 `T1 - T0 ≤ max_delay_seconds`；若有 deadline，还要求 `当前时间 + T1 ≤ deadline`。当前仅支持 worker 数不变、GPU 资源类型一致的垂直收益比较。

| 结果 | 含义 |
| --- | --- |
| `positive` | 数据有效、约束满足、收益达标 |
| `negative` | 收益不足，或明确不可调度、违反时限、无剩余工作量等 |
| `unknown` | 数据缺失、过期、不稳定、身份不一致、配置错误或接口失败等 |

### 5.3 集群目标

```text
集群收益 = 当前任务权重 × (T0 - T1)
         + Σ 其他任务权重 × (基线剩余完成时间 - 调整后剩余完成时间)

集群基线 = 当前任务权重 × T0
         + Σ 其他任务权重 × 其他任务基线剩余完成时间
```

证据服务必须声明受影响集合完整，并提供其他任务的预测，不能遗漏受到负面影响的任务、重复任务或再次包含当前任务。排队任务的剩余完成时间应包括等待时间。当前不支持未知候选性能下的 `cluster_jct` 自动探索。

### 5.4 计算示例

假设总卡数从 4 卡增加到 8 卡，当前 30 秒完成 60 step，剩余 6000 step，候选吞吐为 3.5 step/s，切换耗时 120 秒、重放 20 step、切换资源占用 960 GPU·秒：

```text
q0 = 60 / 30 = 2 step/s
T0 = 6000 / 2 = 3000 秒
T1 = 120 + 6020 / 3.5 = 1840 秒
时间节省 = 1160 秒，约 38.7%

H0 = 4 × 3000 / 3600 ≈ 3.33 GPU·小时
H1 = (960 + 8 × 1720) / 3600 ≈ 4.09 GPU·小时
卡时节省 ≈ -0.76 GPU·小时
```

在其他约束满足时，时间目标可通过，卡时目标不通过。加速与节省卡时是不同目标。

## 6. 资源调整执行过程

门禁放行后进入保存 checkpoint 阶段，等待训练上报完成。探索模式会在替换前持久化 checkpoint 和切换状态，并重新确认目标调度授权。

随后等待旧 worker 退出、提交资源计划、等待目标 worker 参数调整就绪，完成 rendezvous 协调。代码记录执行完成并启动恢复跟踪；执行完成不等于训练已经稳定恢复，恢复还需后续进度确认。

普通计划到此不再自动进入收益观测循环。探索计划转入 `OBSERVING`，等待新代次恢复、预热并形成有效窗口。

## 7. 候选性能未知时的受控探索

### 7.1 准入与预算

仅enforce、time/gpu_hours、探索开启、明确candidate_unavailable且当前worker健康时申请探索。服务须显式返回policy=observe_only、allowed/checkpoint_compatible/quiescent_checkpoint=true；只允许GPU相邻档位变化，其他资源形状不变。无需回退能力、回退预算或预留。

共享持久POSIX目录保证文件锁与原子保存；每个job_uid最多一次探索。有效时长D=min(trial_seconds,Tbase)，必须>2×min_window，max_delay≥D，deadline若存在需≥now+Tbase+D，max(G0,G1)×D≤GPU秒预算。D包含checkpoint、切换、恢复、预热和观测，不是纯采样时间。

### 7.2 切换与观测

保存checkpoint后持久化SWITCHING，替换前重新核验目标可调度、约束与observe_only授权。控制切换完成后进入OBSERVING，持续校验真实applied_plan_id、恢复checkpoint、worker readiness、新generation、单位/目标/batch和累计GPU秒。

观测请求current/target均为候选配置，原资源在exploration.original。进度超过checkpoint，窗口不早于observation_after和上一窗口末端；至少接受两个独立完整窗口，候选保守吞吐q1取最小值。每个SwanLab窗口内部的稳定子窗口与探索的两个完整窗口是不同层次，重复文件不能凑数。

### 7.3 记录整体收益

保存原吞吐q0和基线剩余时间Tbase。令E为已耗时间，R为当前剩余工作，S为累计GPU秒，G0/G1为原/候选总GPU数：

```text
K = max(0,R)/q1
整体时间收益 = Tbase - E - K
整体资源收益 = G0*Tbase - S - G1*K  （GPU秒）
```

result保存吞吐、整体收益及比例、保持预计时间、elapsed、remaining、累计卡秒、evaluation_status、constraints_satisfied、窗口数和候选资源；另存candidate_generation/observed_at。仍是实测吞吐外推收益，不是整个作业实际收益结算。

### 7.4 决策与保护

| 条件 | 结果 |
| --- | --- |
| 两窗口有效、满足延迟/deadline、整体正收益达到阈值 | ACCEPTED/positive_observed_gain，保留候选 |
| 两窗口有效、满足约束但收益不足 | RETAINED/insufficient_observed_gain，保留候选，evaluation_status=negative |
| 有效结果违反延迟/deadline | INCONCLUSIVE/constraint_violation，保存负向结果并保护后续调整 |
| 时间或卡秒耗尽、训练完成前不足两窗口 | INCONCLUSIVE，保留保护，不冒充有效正收益 |
| checkpoint或切换执行失败 | FAILED，保留保护 |

以上均不自动回退，不生成反向计划；异常不代表训练健康，需要核对现场。限额采用基线和当前更严格值。详细协议和升级兼容见[HTTP契约](vertical-elastic-benefit-http-cn.md)。

## 8. 配置与默认值

| 配置 | 默认值 | 作用 |
| --- | --- | --- |
| `DLROVER_BENEFIT_MODE` | `off` | 收益门禁模式 |
| `DLROVER_BENEFIT_OBJECTIVE` | `time` | 时间、卡时或集群目标 |
| `DLROVER_BENEFIT_HTTP_URL` | 空 | 收益证据接口 |
| `DLROVER_SWANLAB_HTTP_URL` | 空 | 独立 scalar 接口 |
| `DLROVER_BENEFIT_TIMEOUT_SECONDS` | 3 | HTTP 请求超时；并非两个接口合计的总超时 |
| `DLROVER_BENEFIT_MAX_AGE_SECONDS` | 60 | 快照与最新采样时效；也用于检查采样间隔 |
| `DLROVER_BENEFIT_MIN_WINDOW_SECONDS` | 30 | 最小完整训练窗口 |
| `DLROVER_BENEFIT_MIN_GAIN_RATIO` | 0.05 | 最小收益比例 |
| `DLROVER_SWANLAB_ASSUME_FIXED_BATCH` | `false` | 是否显式假设 step 工作量固定 |
| `DLROVER_SWANLAB_TIME_SOURCE` | `sample` | createTime 时间语义 |
| `DLROVER_SWANLAB_UTC_OFFSET` | `+08:00` | 无时区采样时间的偏移 |
| `DLROVER_SWANLAB_WARMUP_SECONDS` | 60 | 代次或恢复后的预热时间 |
| `DLROVER_SWANLAB_STEADY_WINDOWS` | 2 | 单个完整窗口内的稳定子窗口数 |
| `DLROVER_SWANLAB_MAX_THROUGHPUT_VARIATION` | 0.1 | 子窗口吞吐相对波动上限 |
| `DLROVER_BENEFIT_EXPLORE` | `false` | 受控探索开关 |
| `DLROVER_BENEFIT_STATE_DIR` | 空 | 探索持久化状态目录，启用探索时必需 |
| `DLROVER_BENEFIT_TRIAL_SECONDS` | 600 | 探索时间预算 |
| `DLROVER_BENEFIT_TRIAL_POLL_SECONDS` | 10 | 观测轮次之间的等待时间 |
| `DLROVER_BENEFIT_TRIAL_GPU_HOURS` | 0 | 探索卡时预算，需显式设置 |
| `DLROVER_BENEFIT_TRIAL_COOLDOWN_SECONDS` | 1800 | 探索终态后的冷却时间 |

## 9. 结果保存、恢复与能力边界

普通评估记录结构化日志、事件及 `dlrover_master_vertical_benefit_evaluations_total` 计数。最近请求和结果位于 guard 的进程内状态，不是持久化收益账本。

探索结果持久化到journal。ACCEPTED/RETAINED/INCONCLUSIVE/FAILED均尝试通过report_result幂等回传；确认需匹配request_id/exploration_id且recorded=true，失败在下次进入或重启时重试。证据不足的result可能为空，不能作为有效候选收益。

仅新策略OBSERVING可恢复观测；PREPARING/SWITCHING和旧策略未完成状态均不自动重放，FAILED/INCONCLUSIVE保留保护。旧版已完成ROLLED_BACK仍可识别，必要时发送兼容预留释放通知，但不执行任何新回退。

当前计算是预计剩余训练收益，以及探索中计入已发生开销的预测收益。尚未提供跨重启、覆盖完整作业生命周期的实际收益结算账本。普通扩缩容执行成功，不等于已经证明获得正收益；`RETAINED` 也不应计为成功降本。

## 10. 实现与相关文档

| 文件 | 关键入口或职责 |
| --- | --- |
| [job_resource_scaler.py](../../dlrover/python/master/node/job_resource_scaler.py) | `_try_execute_optimization`、`_check_resource_benefit`、`_execute_resource_plan`、`_observe_exploration` |
| [benefit.py](../../dlrover/python/master/resource/benefit.py) | `BenefitGuard` 请求编排、`evaluate` 收益计算与约束校验 |
| [swanlab.py](../../dlrover/python/master/resource/swanlab.py) | `fetch` 查询 scalar、`window` 筛选并验证稳定窗口 |
| [exploration.py](../../dlrover/python/master/resource/exploration.py) | `prepare` 准入、`inspect` 观测决策、状态持久化和结果回传 |
| [vertical_metrics.py](../../dlrover/python/master/monitor/vertical_metrics.py) | 执行与恢复跟踪、`training_observation_status` |
| [rdzv_manager.py](../../dlrover/python/master/elastic_training/rdzv_manager.py) | 训练 generation 与时间边界 |
| [training_metrics.py](../../dlrover/python/common/training_metrics.py) | 训练端同批上报字段辅助函数 |

相关资料：

- [收益指标清单](vertical-elastic-benefit-current-metrics-cn.md)
- [收益门禁与 HTTP 数据契约](vertical-elastic-benefit-http-cn.md)
- [收益日志说明](vertical-elastic-benefit-logging-cn.md)
- [垂直弹性执行指标](vertical-elastic-execution-metrics-cn.md)
- [SwanLab mock 验证](vertical-elastic-benefit-mock-cn.md)
