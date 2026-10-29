# 收益体系关键日志

Master 日志中，`BenefitFlow` 后为单行 JSON，用于数据获取、收益判断、探索和执行诊断。原有 `VerticalElasticEvent` 继续记录阶段转换、控制完成、训练恢复与最终资源状态。

## 关联与检索

- `plan_id`：当前资源调整计划；探索中的状态日志以原计划 ID 关联，回退时另含 `rollback_plan_id`。
- `request_id`：单次收益证据 HTTP 请求 ID，同时传入 SwanLab 获取日志。它与触发资源调整的 RPC 请求 ID 不同。
- `task_id`、`generation`：SwanLab 任务与训练代次。
- `component`：`swanlab`、`guard`、`exploration` 或 `execution`。

```bash
rg 'BenefitFlow|VerticalElasticEvent' master.log
rg '具体的plan_id' master.log
rg 'fetch_failed|observation_wait|gate_decision|finished' master.log
```

## 关键阶段

| 阶段 | 日志事件 | 主要内容 |
| --- | --- | --- |
| 发起获取（DEBUG） | `fetch_started` | 超时、允许数据年龄、最小窗口、观察时间下界 |
| 数据获取失败（WARNING） | `fetch_failed` | configuration/http/decode/window 阶段、状态码、错误类型、原因、耗时 |
| 选中训练窗口 | `window_selected` | 指标序列数、返回采样点总数、窗口起止时间与步数、吞吐、目标步数、global batch size、steady_source=inferred、time_source、稳态子窗口数及预热时长 |
| 开始收益评估（DEBUG） | `evaluation_started` | 当前/目标资源、模式、目标、请求 ID |
| 证据 HTTP | `evidence_response` / `evidence_failed` | 状态码、响应耗时或异常类型；成功响应为 DEBUG，失败为 WARNING |
| 完成收益评估 | `evaluation_finished` | status/reason、估算值、最小收益比例、总耗时、operation |
| 最终门控 | `gate_decision` | allow/reject/explore、模式、收益结论、探索开关与 worker 健康条件 |
| 探索入口与准入 | `entry` / `admission` | 恢复/新请求是否准入、拒绝原因、基线吞吐、资源档位、时间和 GPU 秒预算 |
| 执行 | `execution_started` / `checkpoint_ready` / `replacement_submitted` | 计划、是否回退、目标 GPU 数、检查点步数、目标 worker ID |
| 修改资源前确认 | `mutation_authorized` | 预留复核结果、是否回退、证据请求 ID |
| 探索状态落盘 | `state_saved` | 状态、窗口数、保守实测吞吐、已用 GPU 秒、预算、结果是否已回传 |
| 观察暂不能推进 | `observation_wait` | 数据失败、执行/窗口不匹配、旧 generation、进度不足、等待更多窗口等原因 |
| 实测收益判断 | `observed_benefit` | 吞吐、整体收益、保留/回退耗时、未来回退收益、约束是否满足 |
| 探索结束与回传 | `finished` / `result_reported` | 终态及原因、有效窗口数、资源消耗、外部结果确认 |
| 控制完成与训练恢复 | `VerticalElasticEvent` | 原有 control_completed、training_recovered、finished 等事件，区分控制完成和有效训练恢复 |

`window_selected.sample_count` 是 HTTP 响应内所有指标序列的采样点总数，不是最终选中窗口的训练样本数。吞吐单位为 optimizer steps/s。`observed_benefit.overall_gain` 在 time 目标下为秒，在 gpu_hours 目标下沿用探索计算的 GPU 秒。

预留授权/释放请求也经过证据检查，`evaluation_finished.operation` 会标识 `authorize_mutation` 或 `release_reservation`。这些请求是否成功，应以 `mutation_authorized.allowed` 或 `result_reported.acknowledged` 为准，不能单看收益 status。

## 日志量与数据保护

持续相同的获取失败、重复窗口、收益结论和观察等待，按事件的最近结果最多每 60 秒记录一次；原因或关联计划变化立即记录。`suppressed` 表示同一结果上次记录后被抑制的次数。请求开始的 DEBUG 日志可用于逐次排查。

新增日志只记录选择过的字段，不记录 HTTP token、Authorization、URL、完整请求/响应体、完整采样历史或异常原文。日志输出失败不会改变收益准入或执行结果。既有业务事件与其他模块日志保持原有行为。

## 验证

`test_benefit_logging.py` 覆盖请求关联、成功窗口、HTTP/超时/解析/窗口失败、敏感数据不输出、重复结果限频、日志异常隔离以及探索等待和最终结果。

本次完整收益及生命周期回归：267 passed，3 subtests passed。JUnit：`/tmp/dlrover-benefit-validation/logging-junit.xml`。
