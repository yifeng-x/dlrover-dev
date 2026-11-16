# 弹性收益聚合服务

独立 MySQL 服务，为 React 看板提供事件、收益和 JCT 查询。部署步骤见 [部署说明](../../../deploy/elastic-dashboard/README.md)，字段定义及数据缺口见 [接口文档](../../../docs/design/elastic-dashboard-api-cn.md)。

## 启动

```bash
python -m dlrover.dashboard.elastic --host 0.0.0.0 --port 8080
```

只保留 `--host`（默认 0.0.0.0）和 `--port`（默认 8080）。服务固定使用 MySQL，数据库表提前执行 `schema.sql` 创建，启动过程不执行 DDL。MySQL 连接使用 `DLROVER_MYSQL_HOST/PORT/DATABASE/USER/PASSWORD` 环境变量；可选 `DLROVER_MYSQL_SSL_CA` 启用证书及主机名验证。

原 `serve`、`init-db`、`import-events`、`import-job`、`export-events`、`--storage`、`--db` 已移除。SQLite 仅保留内部测试及旧 Master 本地日志兼容，不再是服务启动选项。

## 数据接入

内部接口不使用服务令牌。Master 配置 `DLROVER_ELASTIC_EVENT_URL` 后，通过有界队列异步发送事件；网络失败不阻塞弹性控制，原始日志保留供补录。

```text
GET  /healthz
GET  /readyz
GET  /api/v1/elastic/jobs
GET  /api/v1/elastic/jobs/{url-encoded-job-uid}/dashboard
GET  /api/v1/elastic/jct-statistics
POST /api/v1/elastic/events
POST /api/v1/elastic/job-snapshots
```

补录日志时，由采集器提取 `VerticalElasticEvent ` 后的原始 JSON，逐条 POST events。必须保留 event_id：重复记录返回 HTTP 200 且 inserted=false，内容冲突返回 422，临时存储故障返回 503，可重试。单事件最大 1 MiB。服务不提供独立日志导入或 SQLite 导出命令。

平台通过 job-snapshots 同步真实生命周期，例如：

```json
{
  "job_uid": "actual-job-uid",
  "job_name": "train-001",
  "cluster": "cluster-a",
  "namespace": "elastic-training",
  "status": "completed",
  "submitted_at": 1793941440,
  "completed_at": 1793947200,
  "lifecycle_observed_at": 1793947201
}
```

状态支持 running/completed/failed/unknown；运行中 completed_at=null。过期快照不覆盖新状态，同时间冲突被拒绝。实际 JCT 为完成减提交时间，运行中耗时为本次查询减提交时间；失败或无效时间不计入成功 JCT。汇总支持 cluster、namespace 精确过滤，以及均值、最小/最大值、P50/P95。JCT 改善仍需冻结基线账本。

## 连接管理与可选数据源

MySQL 使用 SQLAlchemy Engine 和独立 Session：pool_size=20、max_overflow=10、pool_recycle=200、pool_pre_ping=True、pool_timeout=3 秒。Engine 创建时不访问数据库；请求结束归还连接，退出释放连接池。URL.create 处理凭据特殊字符，异常响应不包含连接信息。`models.py` 定义 ElasticJob、ElasticEvent，与预建表结构一致；插入使用 session.add()/flush()，查询使用 select()/session.get()，生命周期更新使用模型属性赋值。并发作业创建使用 MySQL insert().on_duplicate_key_update()，查询锁使用 with_for_update()；事件冲突回滚事务，重复插入使用 savepoint。服务启动仍不自动建表。

可选设置 `DLROVER_PROMETHEUS_URL` 和 `DLROVER_PROMETHEUS_TOKEN`。收益评估仅在没有持久化记录时查询 Prometheus；GPU 历史曲线始终查询 Prometheus，按作业标识匹配；不覆盖已记录的负收益或无效评估。此配置不再提供命令行参数。

## 测试与边界

```bash
python -m pip install -r deploy/elastic-dashboard/requirements.txt
python -m pytest dlrover/dashboard/tests/test_elastic_*.py -q
```

真实 MySQL 测试需配置独立测试库的连接变量，并设置 `DLROVER_TEST_MYSQL=1`；测试账号需建表和 DELETE 权限，未开启时 4 项测试跳过。

尚未自动同步平台生命周期，未实现 JCT 冻结基线、连续吞吐采集、真实停训区间、可靠投递队列和历史分页。单作业超过 10000 条原始事件返回 503，不静默截断；统计完整性标记仍为 false。缺失字段返回 null 或空数组，不生成模拟数据。

## 运行 Worker GPU 时序

Master 每 10 秒单独读取 Kubernetes Pod 列表，以任务 owner UID 校验归属，按 Pod UID 去重。仅统计 Running、未标记删除、训练容器处于 running 状态的 Worker，将当前 Pod spec 的加速器分配数量逐个求和，支持不同 Worker 配置。包括空闲但仍运行的 Worker；不等同于 GPU 利用率或参与训练的 rank 数。NVIDIA MIG 数值是分配的实例数，不折算整卡。

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
