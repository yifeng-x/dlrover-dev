# 垂直弹性执行链路指标

Master 的现有 `/metrics` 端点导出执行指标，公共标签为 `cluster`、
`namespace`、`job_uid`、`strategy`。已覆盖控制面执行、Pod 操作耗时和训练恢复确认。
参数就绪或 `COMPLETED` 仍仅代表控制面完成，训练恢复通过独立指标确认。

以下名称均带前缀 `dlrover_master_vertical_`：

| 名称 | 类型 | 附加标签与含义 |
| --- | --- | --- |
| `requests_total` | Counter | `action,result,reason`；受理、拒绝、启动异常 |
| `plans_started_total` | Counter | `action`；创建计划次数 |
| `plans_finished_total` | Counter | `action,result,reason`；completed/failed/skipped/cancelled |
| `plan_duration_seconds` | Histogram | `action,result`；创建到终结 |
| `phase_duration_seconds` | Histogram | `action,phase,outcome`；阶段退出时记录 |
| `phase` | Gauge | `phase`；当前状态独热值 |
| `inflight` | Gauge | 后台执行线程占用执行权时为 1 |
| `phase_elapsed_seconds` | Gauge | 当前活动阶段已等待时间，终结后为 0 |
| `plan_elapsed_seconds` | Gauge | 当前活动计划已持续时间，终结后为 0 |
| `workers` | Gauge | `state=target/param_ready`；就绪数仅计算目标集合交集 |
| `gpus_per_worker` | Gauge | `value_type=before/target`；活动计划每 worker GPU 数 |
| `reports_total` | Counter | `report_type=checkpoint/param`，`result` 为回报分类 |

`action` 为 add_gpus/reduce_gpus/manual，非法请求归为 unknown。
请求在首次拒绝或启动后台线程时计数一次，受理成功不等于计划完成。
无 job manager 的 RPC 无可归属的执行指标记录器，不产生本组指标。

结果原因使用固定枚举，不包含异常原文或 checkpoint 路径。
限频、达到次数限制、空计划、资源无变化分别记录 cooldown、max_attempts、
empty_plan、same_resource，并归类为 skipped；沿用状态机的 FAILED 状态。
checkpoint_failed、checkpoint_timeout、param_timeout、scale_failed、
invalid_plan、no_target_workers、internal_error 归类为 failed。
等待期间关闭弹性记录 cancelled/disabled。重复结束保留第一次的结果和原因。

阶段计时使用 monotonic。TUNE_PARAM 包含生成替换计划后的 Pod 替换、启动和
参数就绪等待，不能当成纯调参耗时。RENDEZVOUS_READY 当前会立即进入 COMPLETED，
其耗时不能当成真实组网耗时。Histogram 在阶段退出时累计，不依赖抓取频率。
资源比较值在计划结束后移除，worker 计数归零，终态 phase 保留到下一次计划。

指标仅保存在 Master 内存中，重启后重置，崩溃未完成计划不会补记失败。
第二阶段复用这些内存指标及已有进度 RPC，不增加 Redis、持久化账本或新通信协议。

## 查询示例

执行失败次数（跳过与取消不计入）：

```promql
sum by (cluster, namespace, reason) (
  increase(dlrover_master_vertical_plans_finished_total{result="failed"}[30m])
)
```

建议看板并排展示以下两项，并保持筛选条件、分组和时间窗口一致。

最近一小时完成次数（与耗时直方图使用同一组样本）：

```promql
sum by (cluster, namespace, action) (
  increase(dlrover_master_vertical_plan_duration_seconds_count{result="completed"}[1h])
)
```

成功计划耗时 P95：

```promql
histogram_quantile(0.95,
  sum by (cluster, namespace, action, le) (
    rate(dlrover_master_vertical_plan_duration_seconds_bucket{result="completed"}[1h])
  )
)
```

完成次数为 0 时，P95 无有效值属于正常现象，看板应显示“无完成样本”，
不要把 NaN 或空值映射成 0 秒。采样不足或抓取失败引起的无数据，也不能当成零次完成。
`increase()` 是窗口估算，可能返回小数。

Master 初始化指标时，为 add_gpus/reduce_gpus/manual 与
completed/failed/skipped/cancelled 的 12 个组合预创建耗时 Histogram，
使其 `_count`、`_sum` 和各 `_bucket` 在首次结束前即可导出为 0。
仅调用 `labels()`，不会执行 `observe(0)`，因此不会产生虚假的耗时样本。
同时预初始化 `plans_finished_total{result="completed",reason="none"}`；
失败、跳过及取消原因仍按实际发生创建，不生成无意义的原因组合。

初始化不保证 Prometheus 一定抓到零值：若计划在首次抓取前就结束，
第一次增长仍可能无法被 `rate()`/`increase()` 识别。不会为了等待抓取而阻塞执行。
该改动部署后的新进程生效，不能补回历史缺失的零值。

活动阶段等待时间：

```promql
dlrover_master_vertical_phase_elapsed_seconds
  * on (cluster, namespace, job_uid, strategy, instance) group_left(phase)
    (dlrover_master_vertical_phase == 1)
```

告警阈值应与作业等待配置匹配，checkpoint 和参数就绪目前共用
`ckpt_save_status_check_max_wait_time`。同时监控 `up`，避免 Master 消失后
等待告警随时间序列消失。

## Pod 操作耗时与训练恢复

以下指标同样使用 `dlrover_master_vertical_` 前缀：

| 名称 | 类型 | 标签和口径 |
| --- | --- | --- |
| `worker_operation_duration_seconds` | Histogram | `operation,outcome`；仅本次计划目标 worker |
| `recovery_total` | Counter | `action,result`；recovered/timeout/unverified |
| `recovery_duration_seconds` | Histogram | `action`；控制面完成至确认训练进度 |
| `resume_duration_seconds` | Histogram | `action`；进入 SAVE_CHECKPOINT 至确认训练进度 |
| `recovery_pending` | Gauge | 正在等待恢复确认为 1 |
| `recovery_elapsed_seconds` | Gauge | 控制面完成后的等待时间，确认结束后归零 |

Pod 操作按下列边界记录：

- `schedule_wait`：Pod creationTimestamp 到 PodScheduled=True 的 lastTransitionTime。
- `startup_wait`：调度完成到训练容器 running.startedAt。多容器 Pod 优先选择
  main 容器，旧模板无法识别 main 时沿用首个容器的时间。

这两项使用 Kubernetes 时间戳而非 watch 到达时间，每个目标节点每项只记录一次。
没有时间戳或时间逆序时不产生样本，不用 0 代替未知耗时。
`outcome=success` 表示对应阶段已完成；直方图不包含仍在等待的 Pod，
不能仅用其 P95 判断是否存在卡点。尚未完成的执行继续通过
`phase_elapsed_seconds`、目标/就绪节点数及 Pod 状态排查。
当前不采集 Master 内部创建队列及创建 API 耗时。
Pod 名称、节点 ID 不进入指标标签。节点明细仅在内存中关联当前计划的目标 ID，
下一次计划开始时清理；Master 重启不恢复这些观测。

### 恢复确认规则

1. 控制面 COMPLETED 后开启独立确认窗口，不延迟原执行流程返回。
2. 复用 GlobalStep RPC 请求里的 node_type/node_id，只接收当前替换目标 worker。
   Master 内新 worker 分配递增 ID，旧 worker 延迟回报不会确认新计划恢复。
3. 同一目标节点连续两次有效回报中，timestamp 和 step 均增长，且新 step 大于
   checkpoint step，才记为 recovered。第一条回报只建立基线，避免恢复文件里的
   历史 step 被误认为新进度；乱序和重复 timestamp 不推进基线。
4. 超时复用本次执行的 `ckpt_save_status_check_max_wait_time`，由已有心跳循环
   每约 15 秒检查，进度回报时也检查。超时后回报不能改写已结束的结果。
   已有有效目标节点回报但未确认增长记为 timeout；完全没有有效回报记为 unverified。
5. 新计划开始或作业停止时，尚未超时的旧确认窗口结束为 unverified，避免跨计划串账。
   未知目标、缺少 checkpoint 阶段起点同样记为 unverified。

确认窗口里的进度要求 step 非负、timestamp 为正，且两者均为有限数值。
没有接入 GlobalStep 的框架会显示 unverified。只有 recovered 记录恢复耗时直方图；
训练进度采样周期会增加观测延迟，指标代表“确认恢复”的时间，不代表首个训练算子时间。
`resume_duration_seconds` 包含 checkpoint 保存等待，不等于训练停机时间。

恢复失败与覆盖率应分别观察：

```promql
sum by (cluster, namespace, result) (
  increase(dlrover_master_vertical_recovery_total[1h])
)
```

等待恢复但尚未结束的作业：

```promql
dlrover_master_vertical_recovery_elapsed_seconds
  and on (cluster, namespace, job_uid, strategy, instance)
    (dlrover_master_vertical_recovery_pending == 1)
```
