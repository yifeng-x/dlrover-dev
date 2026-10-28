# DLRover 核心监控指标与 PromQL 查询说明

## 文档信息

| 项目 | 内容 |
| --- | --- |
| 文档版本 | V1.0 |
| 文档状态 | 当前实现说明 |
| 适用阶段 | 第一阶段：基础可见 |
| 适用组件 | Controller、Master、Brain、Agent |
| 采集模式 | 应用暴露 `/metrics`，Prometheus 主动 Pull |
| 指标依据 | 当前代码实际注册和暴露的指标 |

## 1. 使用约定

本文只列入生产基础 Dashboard 和告警所需的核心指标。PromQL 中使用的
`service`、`job`、`namespace` 和 `pod` 是 Prometheus 服务发现附加的 target
标签，具体值应以目标集群 `/targets` 页面为准。

### 1.1 指标类型与聚合原则

| 指标类型或场景 | 正确聚合方式 |
| --- | --- |
| Counter | 查询速率使用 `sum(rate(metric[5m]))`；查询窗口增量使用 `sum(increase(metric[1h]))` |
| Histogram | 使用 `histogram_quantile()`，聚合 bucket 时必须保留 `le` |
| 作业级 Gauge | 按 `job_uid` 或 `job_uuid` 使用 `max`，避免 Pod 切换时重复计算 |
| 多副本二值健康 Gauge | 使用 `min`，任一副本异常时结果为 `0` |
| 多副本共享视图 Gauge | 使用 `max`，不能按副本求和 |
| 多副本局部数量 Gauge | 需要总量时使用 `sum`，排查热点时保留 `pod` |
| Prometheus `up` | 表示抓取成功，不表示组件内部业务逻辑健康 |

组件健康应至少同时观察：

```text
up
+ dlrover_metrics_collector_ready（Python 组件）
+ 组件关键业务状态
```

## 2. Controller

Controller 通过 controller-runtime 的既有 `/metrics` 端点暴露指标。Helm 默认通过
`dlrover-controller-manager-metrics-service` 的 HTTPS 端口抓取。

### 2.1 核心指标

| 指标 | 含义 | 正确 PromQL |
| --- | --- | --- |
| `up` | Prometheus 是否能够抓取 Controller | `min by (namespace) (up{service="dlrover-controller-manager-metrics-service"})` |
| `controller_runtime_reconcile_total{controller,result}` | Reconcile 吞吐及 success、error、requeue 等结果 | `sum by (controller,result) (rate(controller_runtime_reconcile_total{controller="elasticjob"}[5m]))` |
| `controller_runtime_reconcile_errors_total{controller}` | Reconcile 异常速率 | `sum by (controller) (rate(controller_runtime_reconcile_errors_total{controller="elasticjob"}[5m]))` |
| `controller_runtime_reconcile_time_seconds` | Reconcile 耗时 Histogram | 见“Reconcile P95” |
| `workqueue_depth{name}` | 当前工作队列积压数量 | `max by (namespace,pod,name) (workqueue_depth{name="elasticjob"})` |
| `workqueue_retries_total{name}` | 工作队列重试速率 | `sum by (name) (rate(workqueue_retries_total{name="elasticjob"}[5m]))` |
| `workqueue_longest_running_processor_seconds{name}` | 当前最长任务已经执行的时间 | `max by (namespace,pod,name) (workqueue_longest_running_processor_seconds{name="elasticjob"})` |
| `workqueue_unfinished_work_seconds{name}` | 尚未完成任务累计执行时间；持续增长可能表示 Worker 卡住 | `max by (namespace,pod,name) (workqueue_unfinished_work_seconds{name="elasticjob"})` |
| `process_cpu_seconds_total` | Controller CPU 使用 | `sum by (namespace,pod) (rate(process_cpu_seconds_total{service="dlrover-controller-manager-metrics-service"}[5m]))` |
| `process_resident_memory_bytes` | Controller 常驻内存 | `max by (namespace,pod) (process_resident_memory_bytes{service="dlrover-controller-manager-metrics-service"})` |

Reconcile 错误比例：

```promql
sum(
  rate(controller_runtime_reconcile_total{
    controller="elasticjob",
    result="error"
  }[5m])
)
/
clamp_min(
  sum(
    rate(controller_runtime_reconcile_total{
      controller="elasticjob"
    }[5m])
  ),
  0.001
)
```

Reconcile P95：

```promql
histogram_quantile(
  0.95,
  sum by (le,controller) (
    rate(controller_runtime_reconcile_time_seconds_bucket{
      controller="elasticjob"
    }[5m])
  )
)
```

### 2.2 当前限制

当前使用的 controller-runtime v0.12.2 没有在本项目中注册
`leader_election_master_status`。虽然 Controller 启用了 `--leader-elect`，当前也不能
依赖该指标判断 Leader 状态。

## 3. Master

每个 ElasticJob 拥有独立的 Master Service，名称格式为：

```text
elasticjob-<job-name>-dlrover-master
```

推荐使用以下 target selector：

```promql
{service=~"elasticjob-.*-dlrover-master"}
```

Master 作业指标主要包含：

```text
cluster, namespace, job_uid, strategy
```

### 3.1 核心指标

| 指标 | 含义 | 正确 PromQL |
| --- | --- | --- |
| `up` | Master `/metrics` 是否可以抓取 | `min by (namespace,pod) (up{service=~"elasticjob-.*-dlrover-master"})` |
| `dlrover_metrics_collector_ready` | 最近一次 Master 指标快照是否成功 | `min by (namespace,pod) (dlrover_metrics_collector_ready{service=~"elasticjob-.*-dlrover-master"})` |
| `dlrover_metrics_collection_errors_total` | 指标快照采集异常速率 | `sum by (namespace,pod) (rate(dlrover_metrics_collection_errors_total{service=~"elasticjob-.*-dlrover-master"}[5m]))` |
| `dlrover_component_ready{component="master"}` | Master 业务指标快照能够生成 | `min by (namespace,pod) (dlrover_component_ready{component="master"})` |
| `dlrover_job_state{state}` | 作业阶段 one-hot 状态 | `max by (cluster,namespace,job_uid,strategy,state) (dlrover_job_state) == 1` |
| `dlrover_training_global_step` | Master 已观察到的最大训练 Step | `max by (cluster,namespace,job_uid,strategy) (dlrover_training_global_step)` |
| `dlrover_job_last_progress_timestamp_seconds` | Master 最近一次观察到 Step 递增的 Unix 时间 | `max by (cluster,namespace,job_uid,strategy) (dlrover_job_last_progress_timestamp_seconds)` |
| `dlrover_training_running_workers` | PerfMonitor 当前记录的运行 Worker 数 | `max by (cluster,namespace,job_uid,strategy) (dlrover_training_running_workers)` |
| `dlrover_training_target_workers` | 当前目标 Worker 数 | `max by (cluster,namespace,job_uid,strategy) (dlrover_training_target_workers)` |
| `dlrover_master_workers{role,status}` | 按角色和状态统计的节点数 | `max by (cluster,namespace,job_uid,strategy,role,status) (dlrover_master_workers)` |
| `dlrover_master_pending_nodes` | Pending Worker 数 | `max by (cluster,namespace,job_uid,strategy) (dlrover_master_pending_nodes)` |
| `dlrover_master_heartbeat_age_seconds` | 已有心跳节点中最老心跳距当前时间 | `max by (cluster,namespace,job_uid,strategy) (dlrover_master_heartbeat_age_seconds)` |
| `process_cpu_seconds_total` | Master CPU 使用 | `sum by (namespace,pod) (rate(process_cpu_seconds_total{service=~"elasticjob-.*-dlrover-master"}[5m]))` |
| `process_resident_memory_bytes` | Master 常驻内存 | `max by (namespace,pod) (process_resident_memory_bytes{service=~"elasticjob-.*-dlrover-master"})` |

### 3.2 训练无进展

必须排除尚未收到首个 Step、时间戳仍为 `0` 的作业：

```promql
(
  time()
  -
  max by (cluster,namespace,job_uid,strategy) (
    dlrover_job_last_progress_timestamp_seconds
  )
) > 600
and on (cluster,namespace,job_uid,strategy)
max by (cluster,namespace,job_uid,strategy) (
  dlrover_job_last_progress_timestamp_seconds
) > 0
and on (cluster,namespace,job_uid,strategy)
max by (cluster,namespace,job_uid,strategy) (
  dlrover_job_state{state="running"}
) == 1
```

### 3.3 Worker 缺口

```promql
clamp_min(
  max by (cluster,namespace,job_uid,strategy) (
    dlrover_training_target_workers
  )
  -
  max by (cluster,namespace,job_uid,strategy) (
    dlrover_training_running_workers
  ),
  0
)
```

### 3.4 训练速度

当前代码没有实际暴露 `dlrover_training_speed_steps_per_second`，该指标在 Master
collector 中已被注释。可以临时通过 `global_step` Gauge 估算：

```promql
clamp_min(
  deriv(
    (
      max by (cluster,namespace,job_uid,strategy) (
        dlrover_training_global_step
      )
    )[5m:30s]
  ),
  0
)
```

该结果是 Prometheus 根据抓取样本估算的速度，不等价于训练框架内部的实时速度。

### 3.5 指标解释限制

- `dlrover_master_heartbeat_age_seconds=0` 既可能表示心跳刚发生，也可能表示当前没有
  有效心跳，必须结合 `dlrover_master_workers` 判断。
- `dlrover_master_workers` 只为当前实际存在的 `role,status` 组合生成时序；数量为零的
  状态组合可能直接不存在。

## 4. Brain

Brain 默认通过 `dlrover-brain-metrics` Service 抓取，每个 Brain Pod 都是独立 target。
Brain 多实例部署时，不能直接对所有 Gauge 求和。

### 4.1 基础健康与报告处理

| 指标 | 含义 | 正确 PromQL |
| --- | --- | --- |
| `up` | 所有 Brain Pod 是否可抓取 | `min by (namespace) (up{service="dlrover-brain-metrics"})` |
| `dlrover_metrics_collector_ready` | 所有副本最近一次快照是否成功 | `min by (namespace) (dlrover_metrics_collector_ready{service="dlrover-brain-metrics"})` |
| `dlrover_metrics_collection_errors_total` | Brain 快照采集异常速率 | `sum by (namespace) (rate(dlrover_metrics_collection_errors_total{service="dlrover-brain-metrics"}[5m]))` |
| `dlrover_component_ready{component="brain"}` | Brain 业务指标能够生成 | `min by (namespace) (dlrover_component_ready{component="brain"})` |
| `dlrover_brain_runtime_processor_ready` | 所有副本的报告处理线程是否存活 | `min by (namespace) (dlrover_brain_runtime_processor_ready)` |
| `dlrover_brain_runtime_reports_total{result}` | accepted、rejected 报告速率 | `sum by (result) (rate(dlrover_brain_runtime_reports_total[5m]))` |
| `dlrover_brain_runtime_reports_processed_total` | 报告处理速率 | `sum(rate(dlrover_brain_runtime_reports_processed_total[5m]))` |
| `dlrover_brain_runtime_report_processing_errors_total` | 报告处理异常速率 | `sum(rate(dlrover_brain_runtime_report_processing_errors_total[5m]))` |
| `dlrover_brain_report_queue_size` | 每个 Brain 副本的当前队列长度 | `max by (namespace,pod) (dlrover_brain_report_queue_size)` |
| `dlrover_brain_report_queue_capacity` | 每个 Brain 副本的队列容量 | `max by (namespace,pod) (dlrover_brain_report_queue_capacity)` |
| `process_cpu_seconds_total` | Brain CPU 使用 | `sum by (namespace,pod) (rate(process_cpu_seconds_total{service="dlrover-brain-metrics"}[5m]))` |
| `process_resident_memory_bytes` | Brain 常驻内存 | `max by (namespace,pod) (process_resident_memory_bytes{service="dlrover-brain-metrics"})` |

最差副本队列利用率：

```promql
max by (namespace,pod) (
  dlrover_brain_report_queue_size
  /
  clamp_min(dlrover_brain_report_queue_capacity, 1)
)
```

报告拒绝比例：

```promql
sum(
  rate(dlrover_brain_runtime_reports_total{result="rejected"}[5m])
)
/
clamp_min(
  sum(rate(dlrover_brain_runtime_reports_total[5m])),
  0.001
)
```

### 4.2 Redis 与多实例所有权

| 指标 | 含义 | 正确 PromQL |
| --- | --- | --- |
| `dlrover_brain_redis_available` | 是否所有 Brain 副本都可以访问 Redis | `min by (namespace) (dlrover_brain_redis_available)` |
| `dlrover_brain_redis_last_check_timestamp_seconds` | 各副本最近 Redis 探测时间 | `max by (namespace,pod) (time() - dlrover_brain_redis_last_check_timestamp_seconds)` |
| `dlrover_brain_redis_checks_total{result}` | Redis 探测成功、失败速率 | `sum by (namespace,result) (rate(dlrover_brain_redis_checks_total[5m]))` |
| `dlrover_brain_owner_allocator_instance_registered` | 是否所有 Brain 都已注册到 allocator | `min by (namespace) (dlrover_brain_owner_allocator_instance_registered)` |
| `dlrover_brain_owner_allocator_active_instances` | allocator 看到的活跃 Brain 实例数 | `max by (namespace) (dlrover_brain_owner_allocator_active_instances)` |
| `dlrover_brain_owner_allocator_quarantined_instances` | 被隔离、不接收新作业的 Brain 数 | `max by (namespace) (dlrover_brain_owner_allocator_quarantined_instances)` |
| `dlrover_brain_owned_jobs` | 所有 Brain 副本实际拥有的作业总数 | `sum by (namespace) (dlrover_brain_owned_jobs)` |

`active_instances` 和 `quarantined_instances` 是多个副本看到的共享视图，必须使用
`max`；`owned_jobs` 是副本局部状态，需要使用 `sum`。

### 4.3 弹性决策链路

| 指标 | 含义 | 正确 PromQL |
| --- | --- | --- |
| `dlrover_brain_distributed_lock_total{result}` | 分布式锁 acquired、contended、error、unavailable、lost 速率 | `sum by (namespace,result) (rate(dlrover_brain_distributed_lock_total[5m]))` |
| `dlrover_brain_elastic_decisions_total{result}` | 决策 triggered、no_action 以及各失败原因速率 | `sum by (namespace,result) (rate(dlrover_brain_elastic_decisions_total[5m]))` |
| `dlrover_brain_elastic_triggers_total{result}` | 调用 Master 执行弹性动作的结果速率 | `sum by (namespace,result) (rate(dlrover_brain_elastic_triggers_total[5m]))` |
| `dlrover_brain_job_dispatch_total{result}` | 多实例本地处理、转发、重复、Owner 变化等结果速率 | `sum by (namespace,result) (rate(dlrover_brain_job_dispatch_total[5m]))` |

分布式锁异常：

```promql
sum by (namespace,result) (
  rate(dlrover_brain_distributed_lock_total{
    result=~"error|unavailable|lost"
  }[5m])
)
```

`contended` 是 Brain 多实例正常锁竞争，不应单独作为故障。

弹性触发成功率：

```promql
sum(
  rate(dlrover_brain_elastic_triggers_total{
    result=~"success|retry_success"
  }[15m])
)
/
clamp_min(
  sum(rate(dlrover_brain_elastic_triggers_total[15m])),
  0.001
)
```

### 4.4 垂直弹性

垂直弹性指标的业务标签为：

```text
job_name, job_uuid, cluster, namespace, pod_name
```

GPU 快照额外包含 `gpu_count`。

| 指标 | 含义 | 正确 PromQL |
| --- | --- | --- |
| `dlrover_brain_vertical_max_gpu_utilization_percent` | Worker Pod 最大 GPU 计算利用率 | `max by (cluster,job_uuid,job_name,pod_name,gpu_count) (dlrover_brain_vertical_max_gpu_utilization_percent)` |
| `dlrover_brain_vertical_max_gpu_used_memory_mb` | Worker Pod 最大已用显存 | `max by (cluster,job_uuid,job_name,pod_name,gpu_count) (dlrover_brain_vertical_max_gpu_used_memory_mb)` |
| `dlrover_brain_vertical_max_gpu_memory_utilization_percent` | Worker Pod 最大显存利用率 | `max by (cluster,job_uuid,job_name,pod_name,gpu_count) (dlrover_brain_vertical_max_gpu_memory_utilization_percent)` |
| `dlrover_brain_vertical_metric_update_timestamp_seconds` | 最近一次 GPU 数据更新时间 | `max by (cluster,job_uuid,job_name,pod_name) (dlrover_brain_vertical_metric_update_timestamp_seconds)` |
| `dlrover_brain_vertical_decision_avg_gpu_utilization_percent` | 最近决策窗口平均 GPU 计算利用率 | `max by (cluster,job_uuid,job_name,pod_name) (dlrover_brain_vertical_decision_avg_gpu_utilization_percent)` |
| `dlrover_brain_vertical_decision_avg_gpu_memory_utilization_percent` | 最近决策窗口平均显存利用率 | `max by (cluster,job_uuid,job_name,pod_name) (dlrover_brain_vertical_decision_avg_gpu_memory_utilization_percent)` |
| `dlrover_brain_vertical_decision_sample_count` | 最近决策使用的样本数 | `max by (cluster,job_uuid,job_name,pod_name) (dlrover_brain_vertical_decision_sample_count)` |
| `dlrover_brain_vertical_decision_emergency` | 最近决策是否走紧急路径 | `max by (cluster,job_uuid,job_name,pod_name) (dlrover_brain_vertical_decision_emergency)` |
| `dlrover_brain_vertical_decision_timestamp_seconds` | 最近一次垂直弹性决策时间 | `max by (cluster,job_uuid,job_name,pod_name) (dlrover_brain_vertical_decision_timestamp_seconds)` |
| `dlrover_brain_vertical_decisions_total{action,emergency}` | 垂直弹性决策次数 | `sum by (cluster,job_uuid,job_name,pod_name,action,emergency) (increase(dlrover_brain_vertical_decisions_total[1h]))` |

GPU 数据新鲜度：

```promql
time()
-
max by (cluster,job_uuid,job_name,pod_name) (
  dlrover_brain_vertical_metric_update_timestamp_seconds
)
```

作业级 Gauge 必须使用 `max` 去除 Brain target 副本维度，不能对所有 Brain Pod 求和。

### 4.5 当前限制

- 告警配置中引用的 `dlrover_brain_leader_election_enabled` 和
  `dlrover_component_leader{component="brain"}` 当前没有实现。
- 业务代码会累计 `owner_not_registered` dispatch 结果，但当前 exporter 未将该结果
  加入 `dlrover_brain_job_dispatch_total` 的输出列表。

## 5. Agent

Agent 默认监听 `9092/metrics`，但当前没有默认 Agent ServiceMonitor 或 PodMonitor。
下面假设用户补充的抓取任务使用 `job="dlrover-agent"`；实际查询必须替换为集群中的
真实 `job` 标签。

Agent 应用指标只有 `component`、`role`，必须保留 Prometheus target 提供的 `pod` 或
`instance`，不能只按 `role` 判断单个 Agent 状态。

### 5.1 核心指标

| 指标 | 含义 | 正确 PromQL |
| --- | --- | --- |
| `up` | 所有 Agent target 是否可抓取 | `min by (namespace) (up{job="dlrover-agent"})` |
| `dlrover_metrics_collector_ready` | 各 Agent 最近一次快照是否成功 | `min by (namespace,pod) (dlrover_metrics_collector_ready{job="dlrover-agent"})` |
| `dlrover_metrics_collection_errors_total` | Agent 指标采集异常速率 | `sum by (namespace,pod) (rate(dlrover_metrics_collection_errors_total{job="dlrover-agent"}[5m]))` |
| `dlrover_component_ready{component="agent",role}` | Agent 指标逻辑是否正常 | `min by (namespace,pod,role) (dlrover_component_ready{component="agent"})` |
| `dlrover_agent_master_connected{role}` | 最近一次到 Master 的心跳是否成功 | `min by (namespace,pod,role) (dlrover_agent_master_connected)` |
| `dlrover_agent_last_heartbeat_timestamp_seconds{role}` | 最近一次成功心跳时间 | `max by (namespace,pod,role) (dlrover_agent_last_heartbeat_timestamp_seconds)` |
| `dlrover_agent_process_restarts_total{role}` | Agent 管理的训练进程重启次数 | `sum by (namespace,role) (increase(dlrover_agent_process_restarts_total[1h]))` |

Agent 心跳新鲜度：

```promql
time()
-
max by (namespace,pod,role) (
  dlrover_agent_last_heartbeat_timestamp_seconds
)
```

异常 Agent 数量：

```promql
count by (namespace,role) (
  dlrover_agent_master_connected == 0
)
```

### 5.2 Python 运行时指标

Agent 默认不注册 `process_*`、`python_gc_*`。只有设置以下环境变量后才会暴露：

```text
DLROVER_AGENT_RUNTIME_METRICS_ENABLED=true
```

启用后的 CPU 查询：

```promql
sum by (namespace,pod) (
  rate(process_cpu_seconds_total{job="dlrover-agent"}[5m])
)
```

启用后的常驻内存查询：

```promql
max by (namespace,pod) (
  process_resident_memory_bytes{job="dlrover-agent"}
)
```

## 6. Namespace 标签冲突

Master 和 Brain 的应用指标定义了 `namespace`，Prometheus Operator 同样会为 target
附加 `namespace`。当 ServiceMonitor 使用默认 `honorLabels: false` 时，应用侧标签可能
被保存为 `exported_namespace`。

Brain 固定部署在 `dlrover` Namespace，却可能处理其他 Namespace 的训练作业，因此应
首先检查实际存储标签：

```promql
count by (namespace,exported_namespace) (
  dlrover_brain_vertical_metric_update_timestamp_seconds
)
```

如果存在 `exported_namespace`，作业级垂直弹性查询应使用：

```promql
max by (
  cluster,exported_namespace,job_uuid,job_name,pod_name
) (
  dlrover_brain_vertical_max_gpu_utilization_percent
)
```

## 7. 生产最低监控闭环

| 组件 | 必须进入 Dashboard 和告警的指标 |
| --- | --- |
| Controller | `up`、Reconcile 错误率、Reconcile P95、WorkQueue 深度/重试/最长处理时间、CPU、内存 |
| Master | `up`、collector ready、作业状态、Global Step、最后推进时间、运行/目标 Worker、Pending、心跳年龄、CPU、内存 |
| Brain | `up`、collector ready、processor ready、队列利用率、报告拒绝/处理异常、Redis、Owner 注册/隔离、锁异常、决策与触发结果、GPU 数据新鲜度、CPU、内存 |
| Agent | `up`、collector ready、Master 连接、心跳新鲜度、训练进程重启次数 |

所有告警均应设置合理的 `for` 持续时间，避免单次抓取失败、短暂锁竞争或瞬时队列峰值
直接触发生产告警。
