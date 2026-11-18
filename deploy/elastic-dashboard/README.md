# 弹性聚合服务部署

内部链路：`Master → ClusterIP Service → 聚合 Pod → MySQL`。使用已有 namespace；默认两个副本，无接口令牌、建表 Job 和数据卷。

## 准备

1. 在已有 MySQL 8.x 数据库执行 [schema.sql](../../dlrover/dashboard/elastic/schema.sql)。运行账号需要 elastic_jobs、elastic_events 的 SELECT、INSERT、UPDATE 权限。GPU 周期采样仅由 Prometheus 保存。
2. 在 [configmap.yaml](configmap.yaml) 填写数据库连接参数和 Prometheus 地址，实际密码按现有内部配置流程注入，不提交到仓库。
3. 构建并推送镜像，将 [deployment.yaml](deployment.yaml) 的 `image` 改为实际地址：

```bash
docker build -f deploy/elastic-dashboard/Dockerfile \
  -t YOUR_REGISTRY/dlrover/elastic-dashboard:0.1.0 .
docker push YOUR_REGISTRY/dlrover/elastic-dashboard:0.1.0
```

## 部署与接入

启动入口直接写在 `deployment.yaml` 的 `/bin/bash -c` 多行 `command` 中：

```bash
python -m dlrover.dashboard.elastic --host 0.0.0.0 --port 8080
```

日志通过 `tee` 同时输出到容器标准输出和 `/data/logs/elastic-dashboard/elastic-dashboard.log`（容器本地文件）。命令使用 `exec` 保留 Python 退出码并直接接收 SIGTERM。

以下使用已有 `dlrover` namespace：

```bash
kubectl -n dlrover apply \
  -f deploy/elastic-dashboard/configmap.yaml \
  -f deploy/elastic-dashboard/deployment.yaml \
  -f deploy/elastic-dashboard/service.yaml
kubectl -n dlrover rollout status deployment/elastic-dashboard --timeout=300s
```

Master 镜像需包含异步事件发送代码，配置：

```text
DLROVER_ELASTIC_EVENT_URL=http://elastic-dashboard.dlrover.svc.cluster.local:8080/api/v1/elastic/events
```

Master 在独立后台线程每 10 秒只读查询本任务 Worker Pod 并更新内存指标，Prometheus 拉取 `/metrics`。此流程不依赖上述事件 URL。GPU 曲线需要在 ConfigMap 设置 `DLROVER_PROMETHEUS_URL` 并配置 Prometheus 抓取 Master，建议间隔 10 秒；长期回溯需保证历史保留时间。

平台向 `/api/v1/elastic/job-snapshots` 同步生命周期；前端 `/api` 同源代理到上述 Service 的 8080 端口，页面使用 `?mode=api`。更换 namespace 时同步修改 Service 地址。内部调用无需 Authorization 请求头。

## 故障隔离

Master 先记录事件日志，再非阻塞入队；单个后台守护线程负责 HTTP，每次 I/O timeout 为 1 秒，最多尝试两次。队列最多 32 条、单条最多 1 MiB；网络失败、队列满或超限只影响 HTTP 投递，日志仍保留。即使 DNS/网络调用挂起，也不会让控制线程等待网络、无限增加线程或阻止 Master 退出。退出时不等待队列清空，缺失事件需从日志补导。

Brain、Master 的启动、健康检查和扩缩容决策不依赖聚合服务就绪。MySQL 使用 SQLAlchemy 连接池（每 Pod 20 个常驻、最多 10 个溢出连接，等待上限 3 秒）。聚合服务的 `/readyz` 检查 MySQL 和表，失败仅摘除聚合 Pod；`/healthz` 不依赖数据库。不要将此服务加入其他组件的启动等待或健康探针。未配置事件 URL 时不创建 HTTP 发送线程。

接口与数据缺口见 [接口文档](../../docs/design/elastic-dashboard-api-cn.md)，本地调试、补导和测试见 [开发说明](../../dlrover/dashboard/elastic/README.md)。

## 随 DLRover Helm release 安装前端

先按上文创建聚合服务。在仓库根目录构建前端镜像并推送到集群可拉取的仓库：

```bash
IMAGE=YOUR_REGISTRY/dlrover/elastic-dashboard-web:0.1.0
docker build -f deploy/elastic-dashboard/Dockerfile.frontend -t "$IMAGE" .
docker push "$IMAGE"
```

在现有 DLRover 安装用的 values 文件中加入以下字段，并替换镜像地址：

```yaml
dashboardWeb:
  enabled: true
  image: YOUR_REGISTRY/dlrover/elastic-dashboard-web:0.1.0
  backend: elastic-dashboard:8080
```

沿用原 release 名称、命名空间和 values 文件，例如：

```bash
helm upgrade --install dlrover ./helm_install -n dlrover \
  -f helm_install/custom-values.yaml --wait
kubectl -n dlrover port-forward svc/elastic-dashboard-web 18080:80
```

浏览器打开 `http://127.0.0.1:18080/?mode=api`。前端 Service 类型为 ClusterIP，平台网关可以将已鉴权的页面入口转发到其 80 端口。Nginx 启动时需要能够解析后端 Service；若聚合服务在另一命名空间，将 `backend` 改为 `elastic-dashboard.<后端命名空间>:8080`。

## 使用已有源码卷在集群中调试前端

在 [frontend.yaml](frontend.yaml) 中将 `claimName` 改为部署命名空间中已有的源码 PVC 名称。
`subPath` 指向卷内包含前端 `package.json` 和 `package-lock.json` 的目录。默认值
`web/elastic-dashboard` 假设卷根就是仓库根目录。如果卷根就是前端目录，删除 `subPath` 行。

```bash
kubectl -n dlrover apply -f deploy/elastic-dashboard/frontend.yaml
kubectl -n dlrover rollout status deployment/elastic-dashboard-web-dev --timeout=300s
kubectl -n dlrover port-forward svc/elastic-dashboard-web-dev 18080:80
```

Pod 启动后在挂载的源码目录执行 `npm ci` 并启动 Vite，依赖保存在 Pod 的临时卷中。
Pod 需要能拉取 Node 镜像并访问项目使用的 npm registry。浏览器打开
`http://127.0.0.1:18080/?mode=api`。直接编辑共享卷上的前端源码，Vite 会检测改动并热更新。
更新依赖清单后，执行 `kubectl -n dlrover rollout restart deployment/elastic-dashboard-web-dev` 重新安装依赖。
