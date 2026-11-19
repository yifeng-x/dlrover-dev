# 真实弹性任务收益看板部署手册：集群后端与本地 VS Code 前端

本手册适用于 DLRover 和聚合服务运行在 Kubernetes、聚合 API 通过 NodePort 暴露、前端在开发者电脑运行的环境。当前联调运行真实 ElasticJob，**当前吞吐和候选吞吐均使用 mock 数据**。Master 使用测试输入执行收益评估，前端通过真实聚合 API 展示结果。按作业 UID 核对评估和执行事件，可以验证计算、上报与展示链路；生产收益需要用实测吞吐另行验证。

如果集群已经部署好，且浏览器能通过节点 IP 和端口访问后端，可以直接从**第 4 节**确认任务上报，再按**第 5 节**启动本地前端。

## 1. 确认部署位置与地址

| 调用方 | 目标 | 示例或用途 |
|---|---|---|
| 任务 Master | 集群内聚合 Service | `http://elastic-dashboard.dlrover:8080`，上报弹性事件 |
| 任务 Master | SwanLab mock 文件 | 从容器内的 `/mock/swanlab.json` 读取模拟训练进度窗口，计算当前吞吐 q0 |
| 任务 Master | 收益证据 HTTP fixture（测试服务） | 向 `/benefit/snapshot` 请求测试场景设定的候选吞吐 q1、调整开销等证据 |
| 聚合服务 | MySQL / Prometheus | MySQL 保存作业和事件；Prometheus 提供 GPU 历史及可用的收益指标 |
| 本地 Vite | 聚合服务 NodePort | 例如 `http://192.0.2.10:30080`，替换为已能访问的实际地址 |
| 本地浏览器 | 本地 Vite | 通常为 `http://127.0.0.1:5173/?mode=api` |

第 2 至 4 节中的管理命令在有集群权限的 Bash 终端中执行，从仓库根目录开始。示例以 `dlrover` 为控制器和后端命名空间，训练任务可在其他命名空间。第 5 节在开发者自己的 VS Code 集成终端执行。

## 2. 部署或核对 DLRover

1. 准备集群可拉取的控制器和 Master 镜像。Master 镜像应包含本版本的收益评估、弹性事件上报代码；使用临时生命周期上报时还需包含对应实现。训练镜像沿用已验证的真实训练任务镜像。
2. 在实际使用的 Helm values 中设置 `controller.image.name/tag` 和 `dlroverMaster.image.name/tag`。镜像名包含实际仓库前缀。如果任务模板显式指定 Master 镜像，也要更新模板。沿用现网的 RBAC 和监控配置。前端在本地运行，`dashboardWeb.enabled` 保持 `false`。
3. 首次安装使用下面的命令。更新已有安装时沿用原 release 名、命名空间和完整 values 文件。已有健康部署可直接做后两项检查。

```bash
helm upgrade --install dlrover ./helm_install -n dlrover --create-namespace \
  -f /path/to/your-dlrover-values.yaml --wait
kubectl -n dlrover rollout status deployment/dlrover-controller-manager
kubectl api-resources --api-group=elastic.iml.github.io
```

确认控制器可用、API 资源中能看到 ElasticJob。Chart 默认创建 ServiceMonitor；使用 Prometheus Operator 时，核对其 CRD 已安装、监控选择器能选中相应 ServiceMonitor。若集群已有其他抓取方式，可在 values 中关闭 `controller.metrics.serviceMonitor.enabled` 和 `controller.metrics.masterServiceMonitor.enabled`，通过已有配置抓取 Master 的 `/metrics`。[Helm 配置](../../helm_install/values.yaml)

## 3. 部署聚合服务并验证 NodePort

**初始化存储。** 准备 MySQL 8.x 数据库，由具备建表权限的账号执行下面的命令；将主机、用户和库名改为实际值。运行时账号需要对 `elastic_jobs`、`elastic_events` 有 SELECT、INSERT、UPDATE 权限。[建表脚本](../../dlrover/dashboard/elastic/schema.sql)

```bash
mysql -h MYSQL_HOST -u SCHEMA_USER -p dlrover_dashboard \
  < dlrover/dashboard/elastic/schema.sql
```

**准备镜像和部署清单。** 从仓库根目录构建后端镜像。复制部署清单再填写环境参数，避免将实际数据库凭据提交到仓库。

```bash
IMAGE=YOUR_REGISTRY/dlrover/elastic-dashboard:YOUR_TAG
docker build -f deploy/elastic-dashboard/Dockerfile -t "$IMAGE" .
docker push "$IMAGE"
DEPLOY_DIR=$(mktemp -d)
cp deploy/elastic-dashboard/{configmap,deployment,service}.yaml "$DEPLOY_DIR/"
```

- 在部署副本 `deployment.yaml` 中将 `api` 容器的 `image` 改为刚推送的完整地址。
- 在 `configmap.yaml` 中填写 MySQL 主机、端口、数据库和用户。密码按平台现有 Secret 流程注入 `DLROVER_MYSQL_PASSWORD`；可在 Deployment 容器的 `env` 中用 `valueFrom.secretKeyRef` 引用已有 Secret，并从 ConfigMap 移除密码项。
- 要展示实际 GPU 曲线，在 ConfigMap 设置 Pod 能访问的 `DLROVER_PROMETHEUS_URL`。在 Prometheus Targets 中确认真实任务的 Master 抓取目标为 UP；任务启动后再检查也可以。

**暴露 NodePort。** 已有可访问 NodePort 的环境沿用现有 Service 和端口，避免用仓库的 ClusterIP 模板覆盖它。首次部署时，将部署副本 `service.yaml` 改为以下内容。省略 `nodePort` 可由集群自动分配端口。该 API 同时提供内部写入接口，NodePort 应位于有访问控制的调试网络。

```yaml
apiVersion: v1
kind: Service
metadata:
  name: elastic-dashboard
spec:
  type: NodePort
  selector:
    app.kubernetes.io/name: elastic-dashboard
  ports:
    - name: http
      port: 8080
      targetPort: http
```

```bash
kubectl -n dlrover apply -f "$DEPLOY_DIR/configmap.yaml" -f "$DEPLOY_DIR/deployment.yaml"
# 首次部署 Service 时执行；已有 NodePort 可跳过这一行
kubectl -n dlrover apply -f "$DEPLOY_DIR/service.yaml"
kubectl -n dlrover rollout status deployment/elastic-dashboard --timeout=300s
kubectl -n dlrover get svc elastic-dashboard
kubectl -n dlrover get endpointslices -l kubernetes.io/service-name=elastic-dashboard
```

在客户端浏览器打开 `http://节点IP:实际NodePort/readyz`，预期返回 `{"status":"ready"}`。再访问 `/api/v1/elastic/jobs`，预期得到含 `items` 的 JSON。首次部署返回空数组也能证明 API 可读。若更新了现有 ConfigMap 中通过环境变量注入的配置，执行 `kubectl -n dlrover rollout restart deployment/elastic-dashboard` 后重新检查。

## 4. 接入一项真实弹性训练任务

**沿用一份能正常训练和弹性调整的 ElasticJob 模板。** 保留原有 Worker、数据集、模型、checkpoint、ServiceAccount、资源和平台的调整触发方式。收益面板需要任务真正经历一次收益评估；仅启动训练、部署看板或刷新页面都不会触发评估。

### 配置 Master 上报与两路 mock 输入

把下列环境变量合并到任务模板的 `spec.replicaSpecs.dlrover-master.template.spec.containers` 中实际运行 Master 的容器。保留已有 `env`。示例假设测试服务 `benefit-fixture` 与 Master 同命名空间；跨命名空间时使用实际 Service DNS。[收益 HTTP 契约](vertical-elastic-benefit-http-cn.md)

```yaml
env:
  - name: DLROVER_ELASTIC_EVENT_URL
    value: "http://elastic-dashboard.dlrover:8080/api/v1/elastic/events"
  - name: DLROVER_BENEFIT_MODE
    value: "shadow"
  - name: DLROVER_BENEFIT_OBJECTIVE
    value: "time"
  - name: DLROVER_BENEFIT_HTTP_URL
    value: "http://benefit-fixture:8080/benefit/snapshot"
  - name: DLROVER_SWANLAB_MOCK
    value: "true"
  - name: DLROVER_SWANLAB_MOCK_FILE
    value: "/mock/swanlab.json"
```

示例为首次联调选择 `shadow`：Master 对已有候选计划做收益评估，评估结论不拦截原有调整。原调整仍可能实际执行。已使用 `enforce` 的任务沿用既有策略；`off` 会跳过收益评估。证据服务需要能返回与真实作业、计划及当前训练阶段匹配的证据。若该服务缺失，任务和事件可以展示，收益数值可能为空。

触发评估前，按以下顺序准备两路输入：

1. 部署 `benefit-fixture` 及其场景 ConfigMap，设置本次用例的候选吞吐 q1 和调整开销。从 Master 访问 fixture 的 `GET /scenario` 核对生效值；评估时 Master 会向 `POST /benefit/snapshot` 请求证据。
2. 给 ElasticJob 的 `metadata.labels.low_code_platform_task_id` 填平台任务 ID，并给 Master 挂载可写的 `/mock` 目录。Master 启动、当前训练阶段预热完成后，在触发调整前生成 `/mock/swanlab.json`。文件中的 task ID 要与标签一致，样本字段、稳定窗口和时间范围要符合 Master 配置。每次评估会重新读取文件，过期样本需重新生成。
3. 记录 fixture 场景、mock 样本时间和本次计划 ID。面板上的“API 数据”表示浏览器正在读取集群后端；本次记录中的当前吞吐、候选吞吐及其派生收益均按模拟输入解读。

fixture 部署清单、相互匹配的 Master 环境变量、样本生成命令及触发时机，见 [Kubernetes mock 测试手册第 3、4 节](vertical-elastic-benefit-k8s-mock-test-cn.md)。

### 接入任务生命周期

平台已同步生命周期时，继续由平台向 `/api/v1/elastic/job-snapshots` 上报真实 UID、状态和时间。平台尚未接入时，可在**新建联调任务**的 Master 环境中追加下面两项，由 Master 上报运行和终态。同一 UID 只选一个来源，避免快照互相覆盖。

```yaml
- name: DLROVER_MASTER_LIFECYCLE_ENABLED
  value: "true"
- name: DLROVER_MASTER_LIFECYCLE_URL
  value: "http://elastic-dashboard.dlrover:8080/api/v1/elastic/job-snapshots"
```

### 提交真实任务并记录本轮 mock 评估

```bash
JOB_NS=elastic-training
JOB_NAME=your-real-job
kubectl -n "$JOB_NS" apply -f /path/to/your-real-elasticjob.yaml
kubectl -n "$JOB_NS" get elasticjob "$JOB_NAME" -o jsonpath='{.metadata.uid}{"\n"}'
kubectl -n "$JOB_NS" get pods
```

记下这里的 **ElasticJob UID**，它是后续对账用的 `job.uid`。找到该作业的 Master Pod。等训练进入稳定阶段，由任务原有策略产生候选计划，或使用该任务已验证且获准的调试入口触发一次评估。记录时间和计划 ID，查看两侧日志：

```bash
MASTER_POD=your-master-pod
kubectl -n "$JOB_NS" logs "$MASTER_POD" -c main --since=30m | \
  grep -E 'evaluation_finished|VerticalElasticEvent'
kubectl -n dlrover logs -l app.kubernetes.io/name=elastic-dashboard \
  -c api --prefix=true --since=30m --tail=-1 | grep -E 'events|job-snapshots'
```

Master 容器名按实际模板填写。先查 `evaluation_finished` 中的 `status`、`reason`、`plan_id`，再查弹性事件及聚合服务的写入结果。如果 Master 已生成结论而页面没有对应记录，应优先检查事件上报 URL、网络和存储。已在运行的 Master 使用启动时的环境；更改模板后，用带新配置的下一项任务验证。

## 5. 在本地 VS Code 启动前端

用 VS Code 打开仓库根目录，选择 Windows PowerShell 或 Linux Bash 集成终端。安装 Node.js 22 和 npm。两套命令中的示例地址都要替换为浏览器已能访问的**协议、节点 IP 和 NodePort**，例如地址末尾无需附加 `/api`。首次运行或锁文件变化时执行 `npm ci`。Vite 在启动时读取环境变量，更换后端时先停止原进程，再在设置变量的终端启动。

### Windows PowerShell

```powershell
Set-Location .\web\elastic-dashboard
$env:DLROVER_ELASTIC_API_URL = 'http://192.0.2.10:30080'
Invoke-RestMethod "$env:DLROVER_ELASTIC_API_URL/readyz"
(Invoke-RestMethod "$env:DLROVER_ELASTIC_API_URL/api/v1/elastic/jobs").items | Select-Object uid,name,namespace,cluster
npm.cmd ci
npm.cmd run dev
```

### Linux Bash

```bash
cd web/elastic-dashboard
export DLROVER_ELASTIC_API_URL='http://192.0.2.10:30080'
curl -fsS "$DLROVER_ELASTIC_API_URL/readyz"
curl -fsS "$DLROVER_ELASTIC_API_URL/api/v1/elastic/jobs"
npm ci
npm run dev
```

保持终端运行，在浏览器打开 **http://127.0.0.1:5173/?mode=api**。若 Vite 输出其他端口，使用实际端口。右上角应显示“API 数据”。浏览器请求本地 `/api/`，Vite 将其代理到 NodePort。直接在 VS Code 修改 `web/elastic-dashboard/src/` 并保存即可热更新。[Vite 代理配置](../../web/elastic-dashboard/vite.config.ts)

## 6. 验收收益评测面板

1. 打开“任务分析”，选择真实任务。在浏览器开发者工具的 Network 中找到 `/api/v1/elastic/jobs/<uid>/dashboard`，确认 HTTP 200 且响应 `job.uid` 与第 4 节取得的 Kubernetes UID 完全一致。重名或重建过的任务尤其需要核对 UID。
2. 查看“最近一次弹性评估”。将响应 `latest_evaluation` 与时间最接近的 Master 评估记录核对，检查阶段、观测时间和有效性，并将原吞吐与 mock 文件计算出的 q0、候选吞吐与 fixture 的 q1 分别核对。有持久化收益时优先展示该记录，缺少记录时聚合服务才尝试查询 Prometheus。
3. 对照下表核验数值。负收益保留负号。记录过期时页面会提示历史观测；训练结束后的实际耗时可在 JCT 区域独立核对。
4. 在事件表选择对应计划，核对计划 GPU、执行状态和恢复状态。预测收益、资源调整成功和训练完成分别有独立证据。一次预测可产生正、负或证据不足的结果。

| 面板项目 | 核验方法 |
|---|---|
| 预计节省时间 | 有效估算中 `time_gain_seconds / 60`，页面单位为分钟。 |
| 预计节省 GPU 小时 | 有效估算中 `gpu_seconds_saved / 3600`，页面单位为 GPU·h，负数表示预计额外消耗。 |
| 收益比例 | 将 `gain_ratio × 100` 与百分比对照，并检查本次目标和门槛。 |
| 已记录调整尝试 | 对照 `summary`、实际事件和 Master 日志；异步事件上报可能有缺口。 |
| 实际 GPU 曲线 | 对照同一 UID 的 Prometheus 采样。缺少采样时保留断点；计划 GPU 数单独展示。 |

**验收完成的依据**是同一作业 UID 下，Master 有本轮 mock 输入驱动的评估记录，详情 API 可读，页面展示与接口及事件一致。若 `latest_evaluation` 为空或无效，可确认前端接入和空状态正确，本轮收益数值的核验需等两路 mock 输入均有效后再进行。集群平均 JCT 前后对比还需要足够的已完成作业及可比的提交日期窗口。[接口字段和数据口径](elastic-dashboard-api-cn.md)

## 7. 按症状定位

| 现象 | 优先检查 |
|---|---|
| NodePort 无法访问 | 核对节点 IP、实际分配端口、Service endpoints、客户端到节点的网络，以及 `/readyz`。读库未就绪时查后端日志、MySQL 连通性和表权限。 |
| NodePort 直连正常，本地页面 API 失败 | 在浏览器打开本地 Vite 的 `/api/v1/elastic/jobs`。检查启动终端中的环境变量和 Vite 代理日志，确认修改地址后已重启。 |
| 页面显示“示例数据” | 使用 `/?mode=api` 或在右上角切换“真实 API”。 |
| API 返回任务列表，但没有目标作业 | 核对 UID、Master 镜像、生命周期来源、训练命名空间到聚合 Service 的连通性，以及是否已经收到事件或快照。 |
| 有作业，显示“尚无评估”或“数据不完整” | 核对是否实际产生候选计划、收益模式、证据服务响应及时间窗口。查看 Master 的 `evaluation_finished.reason`、对应事件和详情响应的 `missing_data`。 |
| 有收益数值，GPU 曲线为空 | 核对 Prometheus 地址、Master 抓取目标及对应 UID 的采样时间范围。 |
