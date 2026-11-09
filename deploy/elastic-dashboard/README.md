# 弹性聚合服务部署

内部链路：`Master → ClusterIP Service → 聚合 Pod → MySQL`。使用已有 namespace；默认两个副本，无接口令牌、建表 Job 和数据卷。

## 准备

1. 在已有 MySQL 8.x 数据库执行 [schema.sql](../../dlrover/dashboard/elastic/schema.sql)。运行账号需要两张表的 SELECT、INSERT、UPDATE 权限。
2. 在 [configmap.yaml](configmap.yaml) 填写数据库连接参数，实际密码按现有内部配置流程注入，不提交到仓库。
3. 构建并推送镜像，将 [deployment.yaml](deployment.yaml) 的 `image` 改为实际地址：

```bash
docker build -f deploy/elastic-dashboard/Dockerfile \
  -t YOUR_REGISTRY/dlrover/elastic-dashboard:0.1.0 .
docker push YOUR_REGISTRY/dlrover/elastic-dashboard:0.1.0
```

## 部署与接入

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

平台向 `/api/v1/elastic/job-snapshots` 同步生命周期；前端 `/api` 同源代理到上述 Service 的 8080 端口，页面使用 `?mode=api`。更换 namespace 时同步修改 Service 地址。内部调用无需 Authorization 请求头。

## 故障隔离

Master 先记录事件日志，再非阻塞入队；单个后台守护线程负责 HTTP，每次 I/O timeout 为 1 秒，最多尝试两次。队列最多 32 条、单条最多 1 MiB；网络失败、队列满或超限只影响 HTTP 投递，日志仍保留。即使 DNS/网络调用挂起，也不会让控制线程等待网络、无限增加线程或阻止 Master 退出。退出时不等待队列清空，缺失事件需从日志补导。

Brain、Master 的启动、健康检查和扩缩容决策不依赖聚合服务就绪。聚合服务的 `/readyz` 检查 MySQL 和表，失败仅摘除聚合 Pod；`/healthz` 不依赖数据库。不要将此服务加入其他组件的启动等待或健康探针。未配置事件 URL 时不创建 HTTP 发送线程。

接口与数据缺口见 [接口文档](../../docs/design/elastic-dashboard-api-cn.md)，本地调试、补导和测试见 [开发说明](../../dlrover/dashboard/elastic/README.md)。
