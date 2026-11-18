# 在集群上部署和调试前端，并展示真实数据

前端可以在本地 VS Code 或集群 Pod 中运行，两种方式都通过 Vite 的 `/api/` 代理读取真实聚合服务。前端在本地、后端已通过 NodePort 暴露时，按下面的本地调试步骤操作即可。需要在集群中运行前端时，从第 1 节开始。

## 本地 VS Code 调试，连接集群 NodePort

在 VS Code 打开仓库根目录，通过终端面板的配置文件下拉菜单新建 **PowerShell（Windows）** 或 **Bash（Linux）** 终端，执行对应的一组命令。把示例地址替换为浏览器可访问的后端地址，保留协议、节点 IP 和 NodePort。

首次启动或依赖锁文件更新后执行安装命令 `npm ci`（Windows 使用 `npm.cmd ci`），日常启动可跳过这一行。代理地址在 Vite 启动时读取。如果已有前端进程在运行，先按 Ctrl+C 停止，再在设置了环境变量的同一终端启动。

### Windows PowerShell

```powershell
Set-Location .\web\elastic-dashboard
$env:DLROVER_ELASTIC_API_URL = 'http://192.0.2.10:30080'
Invoke-RestMethod "$env:DLROVER_ELASTIC_API_URL/api/v1/elastic/jobs" | ConvertTo-Json -Depth 10
npm.cmd ci
npm.cmd run dev
```

保持 Vite 运行，新开一个 PowerShell 终端，通过本地代理查询任务。把 5173 替换为 Vite 实际输出的端口。

```powershell
Invoke-RestMethod 'http://127.0.0.1:5173/api/v1/elastic/jobs' | ConvertTo-Json -Depth 10
```

### Linux Bash

```bash
cd web/elastic-dashboard
export DLROVER_ELASTIC_API_URL='http://192.0.2.10:30080'
curl -fsS "$DLROVER_ELASTIC_API_URL/api/v1/elastic/jobs"
npm ci
npm run dev
```

保持 Vite 运行，新开一个 Bash 终端，通过本地代理查询任务。端口同样以 Vite 输出为准。

```bash
curl -fsS http://127.0.0.1:5173/api/v1/elastic/jobs
```

### 核对浏览器中的真实作业

打开 Vite 输出的本地地址并加上 `/?mode=api`，例如 **http://127.0.0.1:5173/?mode=api**。确认右上角显示“API 数据”。

直连 NodePort 和通过本地代理的两次查询都应返回包含 `items` 的 JSON。在“任务分析”中选择一个已知真实作业，通过浏览器 Network 面板核对详情响应中的 `job.uid` 与查询结果一致。若 `items` 为空，说明接口可达，后端当前还没有可展示的作业。若仅本地代理失败，检查启动 Vite 的终端中的 `DLROVER_ELASTIC_API_URL`，修改后重启 Vite。

在 VS Code 中修改本地 `web/elastic-dashboard/src/` 下的 TSX 或 CSS 并保存，浏览器会热更新；数据请求继续使用集群后端。

## 1. 指定已有源码卷和后端

在终端设置前端命名空间，示例使用 `dlrover`。确认当前集群，并找到保存源码的 PVC。

```bash
export NS=dlrover
kubectl config current-context
kubectl -n "$NS" get pvc
```

打开部署清单，根据实际挂载位置调整以下三项。[前端部署清单](../../deploy/elastic-dashboard/frontend.yaml)

| 字段 | 怎么填 |
|---|---|
| `persistentVolumeClaim.claimName` | 填上一步中保存源码且状态为 `Bound` 的 PVC 名称，前端 Pod 部署在该 PVC 所在命名空间。 |
| `volumeMounts` 中源码卷的 `subPath` | 填卷内包含前端 `package.json` 和 `package-lock.json` 的目录。卷根若是仓库根目录，保留 `web/elastic-dashboard`；卷根若直接包含这两个文件，删除 `subPath` 行。 |
| `DLROVER_ELASTIC_API_URL` | 同命名空间时保留 `http://elastic-dashboard:8080`。例如后端在 `monitoring` 时，填 `http://elastic-dashboard.monitoring:8080`。 |

Pod 需要能拉取 `node:22-alpine` 并访问项目使用的 npm registry。源码挂载到 `/workspace`，依赖安装在 Pod 自己的 `/workspace/node_modules` 临时卷中。

## 2. 确认真实数据来源

已有聚合服务时，检查其 Pod 是否就绪。命令中的 `dlrover` 替换为**后端**命名空间。

```bash
kubectl -n dlrover get deployment/elastic-dashboard service/elastic-dashboard
kubectl -n dlrover get endpointslices -l kubernetes.io/service-name=elastic-dashboard
```

Deployment 应有可用副本，Service 应有后端地址。首次搭建时，先按部署说明建 MySQL 表、填写连接配置并部署聚合服务。[聚合服务部署说明](../../deploy/elastic-dashboard/README.md)

准备一个能在平台或 Kubernetes 中确认身份的真实作业，记下作业名称和 UID，供第 4 步核对。页面各部分使用的数据来源如下。

| 页面内容 | 真实数据从哪里来 |
|---|---|
| 任务状态与集群 JCT | 平台把真实生命周期同步到 `/api/v1/elastic/job-snapshots`。联调时若平台尚未接入，可给新测试任务启用临时 Master 生命周期上报；同一任务只使用一个生命周期来源。 |
| 弹性事件 | Master 的 `DLROVER_ELASTIC_EVENT_URL` 指向聚合服务的 `/api/v1/elastic/events`。发生并成功上报的事件才会出现在任务页中。 |
| 实际 GPU 曲线 | Prometheus 抓取 Master 的 `/metrics`，聚合服务通过 `DLROVER_PROMETHEUS_URL` 查询该任务时间段的采样。 |

生命周期上报的具体环境变量和启用条件见[聚合服务开发说明](../../dlrover/dashboard/elastic/README.md)。

## 3. 启动前端并打开页面

```bash
kubectl -n "$NS" apply -f deploy/elastic-dashboard/frontend.yaml
kubectl -n "$NS" rollout status deployment/elastic-dashboard-web-dev --timeout=300s
kubectl -n "$NS" logs deployment/elastic-dashboard-web-dev --tail=30
kubectl -n "$NS" port-forward svc/elastic-dashboard-web-dev 18080:80
```

Pod 会在 `/workspace` 执行 `npm ci`，然后用 `npm run dev` 启动 Vite，监听 `0.0.0.0:8080`。保持端口转发终端运行，在浏览器打开 **http://127.0.0.1:18080/?mode=api**。右上角应显示“API 数据”，数据模式应为“真实 API”。

## 4. 从浏览器入口核对一个真实作业

在另一个终端执行以下命令。请求走浏览器使用的同一个前端端口，能同时验证端口转发、Vite 代理和后端查询。

```bash
curl -fsS http://127.0.0.1:18080/api/v1/elastic/jobs
```

1. 在返回的 `items` 中找到已知真实作业，核对 `uid`、`name`、`namespace` 和 `cluster`。若结果是 `{"items":[]}`，说明接口可达，但当前没有可展示的任务。先检查第 2 步的真实作业上报。
2. 在“任务分析”中选择该作业。打开浏览器开发者工具的“网络（Network）”，找到 `/api/v1/elastic/jobs/<uid>/dashboard` 请求，确认 HTTP 200、返回的 `job.uid` 与真实作业一致，再对照页面上的状态和事件。
3. 在“集群总览”中选择接口返回的集群，设置基线和实验的两个不重叠的**提交日期**窗口。日期按上海时区计算，结束日期不含当天。选择覆盖已知任务提交时间的窗口，核对“有效完成样本”；窗口里有任务但没有有效完成样本时，平均 JCT、P50 和 P95 会显示缺失。

## 5. 修改源码并验证热更新

在已有的源码卷上打开前端目录，临时修改 `src/App.tsx` 中一处可见文字并保存。保持页面和端口转发打开，检查浏览器是否自动显示新文字，再还原该改动。也可以在前端容器内编辑同一份文件：

```bash
# 新终端中使用实际前端命名空间
kubectl -n dlrover exec -it deployment/elastic-dashboard-web-dev -- vi /workspace/src/App.tsx
```

清单已开启文件轮询，以便 Vite 检测共享存储上的修改。修改 TSX 或 CSS 后可直接查看页面。若更新了依赖，请同步更新 `package.json` 和 `package-lock.json`，再执行 `kubectl -n dlrover rollout restart deployment/elastic-dashboard-web-dev`。Pod 更换后重新运行端口转发命令。

## 常见问题

| 现象 | 定位方法 |
|---|---|
| Pod 一直 Pending 或挂载失败 | 运行 `kubectl -n dlrover describe pod -l app.kubernetes.io/name=elastic-dashboard-web-dev`，根据 Events 核对 PVC 名称、命名空间、卷的挂载限制以及 subPath。 |
| 容器启动后反复重启 | 运行 `kubectl -n dlrover logs deployment/elastic-dashboard-web-dev --previous`。找不到依赖清单时检查 subPath；下载超时时检查 npm registry 连通性。 |
| 页面能打开，但任务列表请求报错 | 在前端日志中查看 Vite 代理错误。核对后端 Service 地址、8080 端口、就绪副本，以及前端 Pod 到后端的网络连通性。 |
| 任务能查到，但 GPU 曲线为空 | 检查 Prometheus 是否抓取该任务的 Master，采样时间是否覆盖页面所选范围，以及聚合服务是否配置了 Prometheus 地址。 |
