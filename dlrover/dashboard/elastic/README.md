# 弹性收益查询服务（MySQL / Kubernetes）

这个独立服务为 `web/elastic-dashboard` 提供真实 API。它复用现有 `VerticalElasticEvent`，不改变收益决策或执行流程。生产存储为 MySQL，聚合服务以无状态 Kubernetes Pod 运行；SQLite 保留作本地开发兼容。MySQL 驱动见 `deploy/elastic-dashboard/requirements.txt`。

部署入口：[MySQL + Kubernetes 操作说明](../../../deploy/elastic-dashboard/README.md)。服务默认使用 MySQL；提供 `--db` 且未设置 storage 时兼容旧 SQLite 命令。

已实现：

- Master 通过有界队列和后台线程异步发送到聚合 Service，后端用 MySQL 事务持久化；同时保留原日志；同一事件重放去重、冲突 ID 拒绝、UID 跨集群/命名空间冲突拒绝。
- 原始日志 / JSONL 导入，按作业和计划聚合事件、执行阶段、收益快照、实际资源观测。
- 作业列表、看板查询、内部采集写入接口；独立服务可在 Master 退出后继续查询。
- 没有事件收益时可查询 Prometheus 七个收益 Gauge；持久化事件评估优先，包括无效/负收益，不用旧的正收益覆盖。
- 平台生命周期快照导入，旧快照不覆盖新状态；没有明确来源时状态为 `unknown`。

未实现：自动同步 Kubernetes/平台生命周期、不可变 JCT 基线账本、连续实际 GPU/吞吐采集、真实停训区间、可靠发送队列、历史分页、用户与租户鉴权。当前统计仅为已记录下界，`summary.complete=false`。`series.points=[]`，JCT 基线和中断耗时为 null，不使用生成数据。

## 本地 SQLite 兼容模式（非 K8s 生产配置）

在仓库根目录运行。先为 Master 配置可写的**本地持久卷**，不是容器临时目录：

```bash
export DLROVER_ELASTIC_EVENT_DB=/var/lib/dlrover/elastic/events.sqlite3
# 使用原有命令启动 Master，保持该环境变量。
```

在同一台机器或挂载同一节点本地卷的 sidecar 中启动服务：

```bash
python -m dlrover.dashboard.elastic \
  --db /var/lib/dlrover/elastic/events.sqlite3 serve \
  --host 127.0.0.1 --port 8080
```

另开终端启动 React：

```bash
cd web/elastic-dashboard
npm ci
npm run dev
```

打开 `http://127.0.0.1:5173/?mode=api`，或者手动切换“真实 API”。Vite 已将 `/api` 代理到本机 8080。此端口与旧 `dlrover.dashboard.run_dashboard` 默认端口相同，二者需要选择不同端口；本服务不依赖旧 Master 内嵌看板。

Master 没有发生事件时，数据库不会凭空创建作业。可以导入下述真实平台快照，使尚无弹性事件的作业出现在列表中。

SQLite 使用短事务、1 秒锁等待。写入失败仍保留日志，异常由现有 best-effort 包装隔离，不改变资源决策。此模式会在事件路径中执行本地落盘，不是异步可靠消息系统；低延迟执行链路建议用集中日志采集模式。**不要让多节点通过 NFS/RWX 共享 SQLite 文件**。

## 内部采集

部署与 MySQL 配置统一见 [部署说明](../../../deploy/elastic-dashboard/README.md)，表由部署前提前创建。内部 GET/POST API 不使用服务令牌；Master 只需配置 `DLROVER_ELASTIC_EVENT_URL`，不需要数据库密码。网络调用不在控制线程中执行；队列满或发送失败不影响弹性流程，原始日志用于补导。队列上限及故障行为见部署说明。

日志采集器可提取 `VerticalElasticEvent ` 后的完整 JSON，逐条 POST `/api/v1/elastic/events`。HTTP 200 `{recorded:true, inserted:false}` 表示重复事件已存在；同 ID 不同内容返回 422，503 可重试。重试必须保留原 event_id。平台通过 `/api/v1/elastic/job-snapshots` 同步生命周期。

## 导入已有日志

生产使用 `--storage mysql` 并从环境变量读取连接配置；下面的 `--db` 示例仅用于本地 SQLite 开发。旧 SQLite 可先只读导出再导入 MySQL：

```bash
python -m dlrover.dashboard.elastic --storage sqlite --db /path/to/old.sqlite3 \
  export-events /path/to/events.jsonl
python -m dlrover.dashboard.elastic --storage mysql import-events /path/to/events.jsonl
```

导出不会覆盖已有文件，生命周期需从平台重新同步。

```bash
python -m dlrover.dashboard.elastic --db /var/lib/dlrover/elastic/events.sqlite3 \
  import-events /path/to/master.log
```

支持前缀日志与纯 JSONL；非 JSON 的无关日志会跳过。损坏的候选事件会报告行号并停止，之前已成功写入的事件保留；修正后重复导入不会重复计数。工具仅识别 `VerticalElasticEvent`，不把缺少身份/时间的旧 `BenefitFlow` 单独当成弹性事件。

持续读取已有日志采集输出：

```bash
tail -F /path/to/master.log | python -m dlrover.dashboard.elastic \
  --db /var/lib/dlrover/elastic/events.sqlite3 import-events -
```

此管道没有独立 offset 检查点；重启需重新导入日志以补齐，去重由数据库完成。生产可靠投递由日志采集器的 offset、队列及重试保障。

## 平台生命周期快照

以下仅说明格式，部署时填写平台真实值，不要将示例当作实际记录：

```json
{
  "job_uid": "actual-job-uid",
  "job_name": "train-001",
  "cluster": "cluster-a",
  "namespace": "elastic-training",
  "status": "running",
  "submitted_at": 1793941440,
  "completed_at": null,
  "lifecycle_observed_at": 1793941500
}
```

```bash
python -m dlrover.dashboard.elastic --db /var/lib/dlrover/elastic/events.sqlite3 \
  import-job /path/to/real-job-snapshot.json
```

状态支持 running/completed/failed/unknown。完成时间只允许与 completed 配套，不从计划终态推导。`lifecycle_observed_at` 是平台状态快照的时间，用于拒绝过期覆盖；相同时间的冲突状态也会拒绝。平台生命周期接入后，聚合服务返回实际 JCT（completed_at - submitted_at）及运行中已耗时（本次查询时间 - submitted_at）。成功任务时长固定，失败/未知/无效时间返回 null。`GET /api/v1/elastic/jct-statistics` 提供成功任务均值、最小/最大值、P50/P95，可按 cluster、namespace 精确过滤；没有样本返回 null。JCT 改善仍需第二阶段基线账本。

## 可选 Prometheus 回退

```bash
python -m dlrover.dashboard.elastic --db /var/lib/dlrover/elastic/events.sqlite3 \
  serve --prometheus-url http://prometheus.monitoring:9090
```

可通过 `DLROVER_PROMETHEUS_TOKEN` 配置上游令牌。作业身份须先由事件或平台快照注册，不能从任意 Prometheus series 枚举作业。查询按 cluster/namespace/job_uid 精确匹配，各 stage/objective/work_unit/data_source/strategy 独立分组；重复抓取目标导致同组同字段重复时拒绝猜测，请在上游去重。多个组按观测时间选最新，未知观测时间按本次样本时间选择无效结果，避免显示旧收益。

Prometheus 回退不补造计划 ID、窗口数、状态、阈值或回传确认；这些字段未知即 null。现有持久化事件的最新评估优先（包括无效评估）；该版本不将 Prometheus 实时值覆盖到历史事件，也不轮询归档 Gauge。

## 查询与边界

```text
GET  /healthz                       # 进程探针
GET  /readyz                        # 连接与表结构就绪
GET  /api/v1/elastic/jobs
GET  /api/v1/elastic/jct-statistics  # 可选 cluster、namespace 过滤
GET  /api/v1/elastic/jobs/{url-encoded-job-uid}/dashboard
POST /api/v1/elastic/events          # 采集器专用，单事件 ≤ 1 MiB
POST /api/v1/elastic/job-snapshots   # 平台专用
```

当前无自动清理，容量由部署方监控。单作业超过 10000 条原始事件返回 503，不静默截断；更大规模需分页/预聚合后再开放。生产 MySQL 需纳入备份与容量管理；SQLite 仅用于开发，文件路径不可作为远端共享数据库方案。

GET 返回字段见 [接口文档](../../../docs/design/elastic-dashboard-api-cn.md)。checkpoint 路径等原始审计字段只保存在受控事件库，不出现在页面 JSON。

## 后端测试

```bash
python -m pip install -r deploy/elastic-dashboard/requirements.txt
python -m pytest dlrover/dashboard/tests/test_elastic_api.py \
  dlrover/dashboard/tests/test_elastic_mysql.py \
  dlrover/dashboard/tests/test_elastic_deployment.py -q
```

真实 MySQL 测试需配置独立测试库的 `DLROVER_MYSQL_*` 变量，并设置 `DLROVER_TEST_MYSQL=1`。测试会初始化表并清理本次随机记录，账号需要建表和 DELETE 权限；未开启时 4 项数据库测试跳过。
