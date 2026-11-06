# DLRover 弹性分析前端

React + TypeScript + Vite 独立工程。按 UI 原型实现 JCT 复盘、最近收益评估、GPU/吞吐时序、事件筛选和事件详情。原生 SVG 绘图，无外部字体或 CDN 依赖。

```bash
cd web/elastic-dashboard
npm ci
npm run dev
```

打开终端输出的本地地址（默认 http://127.0.0.1:5173）。

- 默认**演示模式**，所有数据均明确标识为示例。`train-001` 展示完成作业，`train-002` 展示运行中及缺失数据。
- 切换**真实 API**读取 `/api/v1/elastic/jobs` 和作业 dashboard 接口。当前仓库尚无该聚合服务，需按[接口文档](../../docs/design/elastic-dashboard-api-cn.md)实现。失败时显示错误，绝不自动混入 mock。
- 本地 `/api` 由 Vite 代理到 `http://127.0.0.1:8080`，可修改 `vite.config.ts`。生产构建需要部署侧配置同源反向代理和登录鉴权，浏览器不保存 SwanLab/Prometheus 服务令牌。
- 时间范围只筛选事件与趋势；顶部 JCT、最新评估及全生命周期次数保持任务级口径。
- `GPU 变化`是计划值；实际 GPU 独立展示。收益有效性不等价于执行、恢复或收益达标。
- 采样时间使用 Unix 秒，界面以 Asia/Shanghai 显示。图表保留 null 断点，负收益不截断。

```bash
npm test
npm run build
npm run preview
```

构建输出 `dist/`。接口的可执行类型校验位于 `src/model.ts`，示例数据位于 `src/demo.ts`。后续接入无需改动展示组件。当前未实现后端、发布部署、资源调整操作或真实收益结算。
