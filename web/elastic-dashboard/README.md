# DLRover 弹性分析前端

React + TypeScript + Vite 独立工程。按 UI 原型实现 JCT 复盘、最近收益评估、GPU/吞吐时序、事件筛选和事件详情。原生 SVG 绘图，无外部字体或 CDN 依赖。

```bash
cd web/elastic-dashboard
npm ci
npm run dev
```

打开终端输出的本地地址（默认 http://127.0.0.1:5173）。

- 默认**演示模式**，所有数据均明确标识为示例。`train-001` 展示完成作业，`train-002` 展示运行中及缺失数据。
- 切换**真实 API**读取 `/api/v1/elastic/jobs` 和作业 dashboard 接口。聚合服务已实现，按[服务说明](../../dlrover/dashboard/elastic/README.md)启动并导入真实事件；可用 `?mode=api` 直接进入真实模式。失败时显示错误，绝不自动混入 mock。
- 本地 `/api` 由 Vite 代理到 `http://127.0.0.1:8080`，可修改 `vite.config.ts`。生产构建需要部署侧配置同源反向代理和登录鉴权，浏览器不保存 SwanLab/Prometheus 服务令牌。
- 也可在启动时设置 `DLROVER_ELASTIC_API_URL=http://127.0.0.1:18080` 指向其他本地后端端口；这是 Vite 服务端配置，不注入浏览器。
- 时间范围只筛选事件与趋势；顶部 JCT、最新评估及全生命周期次数保持任务级口径。
- `GPU 变化`是计划值；实际 GPU 独立展示。收益有效性不等价于执行、恢复或收益达标。
- 采样时间使用 Unix 秒，界面以 Asia/Shanghai 显示。图表保留 null 断点，负收益不截断。

```bash
npm test
npm run build
npm run preview
```

`npm test` 中的跨语言协议测试需要 PATH 中存在 Python 3，仅使用 Python 标准库，不需要安装额外 Python 依赖。

构建输出 `dist/`。接口的可执行类型校验位于 `src/model.ts`，示例数据位于 `src/demo.ts`。后续接入无需改动展示组件。后端入口为 `python -m dlrover.dashboard.elastic --host 0.0.0.0 --port 8080`。尚未自动部署、执行资源调整或实现完整 JCT 收益结算。缺少生命周期显示“未知”，不完整统计显示“已记录次数”。

生产聚合服务使用 MySQL，并通过 K8s ClusterIP Service 暴露；部署方法见 [MySQL + Kubernetes](../../deploy/elastic-dashboard/README.md)。聚合服务为内部接口，无需 read/ingest token；前端通过平台同源代理访问。
