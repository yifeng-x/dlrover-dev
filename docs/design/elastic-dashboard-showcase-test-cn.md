# 弹性看板 MVP Showcase 测试说明

## 1. 目标与验收边界

覆盖“集群总览”和“任务分析”两个页签，使用本地 HTTP mock 返回所有页面接口数据，展示加载、日期过滤、JCT 对比、任务详情、事件联动、负收益及异常状态。不需要真实训练、GPU、Kubernetes、MySQL、Prometheus 或 SwanLab。

**本方案使用真实 API 请求路径，但返回合成数据。不是开启真实弹性算法的收益验证，也不是 MySQL/Prometheus 集成验收。** 页面顶部的“API 数据”表示传输模式，不表示真实生产数据。集群名 `showcase-mock`、任务名 `MOCK-*`、任务完整性提示和演示开场说明均应保留。

真实后端目前不生成任务冻结基线账本、连续吞吐和真实停训区间。mock 为展示这些 UI 字段而提供合成数据，不应据此宣称生产采集已完成。任务页已有内部演示模式不经过 HTTP，本次 showcase 不使用该模式。

配套文件：

- [Mock HTTP 服务](../../web/elastic-dashboard/scripts/showcase-mock.mjs)
- [前端启动说明](../../web/elastic-dashboard/README.md)
- [真实 API 契约](elastic-dashboard-api-cn.md)
- [真实后端说明](../../dlrover/dashboard/elastic/README.md)

## 2. 架构及接口

```text
浏览器 ?mode=api
    → Vite /api 同源代理
    → 127.0.0.1:18080 本地 mock HTTP 服务
        ├─ 20 个任务生命周期 fixture → JCT 分组统计
        └─ 任务 dashboard fixture → 收益、曲线、事件
```

| 接口 | Mock 行为 |
| --- | --- |
| GET /healthz | 返回 mock=true 和当前 scenario |
| GET /api/v1/elastic/jobs | 返回全部 mock 任务，包括非弹性任务 |
| GET /api/v1/elastic/jct-statistics | 根据 cluster、namespace 和提交时间窗口实际筛选 fixture，再计算统计；不是固定返回卡片数值 |
| GET /api/v1/elastic/jobs/{uid}/dashboard | 返回匹配 UID 的任务详情；不存在返回 404 |
| POST /__scenario?name=… | 仅用于本地切换场景，不属于生产 API |

后端依赖平台、事件及 Prometheus 的输出在此统一替换为最终 HTTP 响应；不再额外模拟下游服务。若要验收真实聚合逻辑，运行第 9 节测试，不能用本 mock 的统计结果替代真实后端测试。

## 3. 环境与启动

准备项目依赖支持的 Node.js/npm 和浏览器。示例使用两个终端，从仓库根目录执行。首次安装需要依赖源可访问；之后本地演示无需访问外部数据服务。

终端 A：

```bash
cd web/elastic-dashboard
npm ci
node scripts/showcase-mock.mjs
```

预期打印 `MOCK ONLY http://127.0.0.1:18080`。服务仅监听本机；端口占用时先停止占用者，不连接真实后端代替。

终端 B：

```bash
cd web/elastic-dashboard
DLROVER_ELASTIC_API_URL=http://127.0.0.1:18080 npm run dev -- --port 5173 --strictPort
```

浏览器打开：

```text
http://127.0.0.1:5173/?mode=api
```

连通检查：

```bash
curl --fail http://127.0.0.1:18080/healthz
curl --fail http://127.0.0.1:5173/api/v1/elastic/jobs
```

在开发者工具 Network 中确认 jobs、jct-statistics、dashboard 请求通过本地 `/api`，没有生产平台、Prometheus、SwanLab 或外部 CDN 请求。浏览器不要切换到“演示模式”。

## 4. 固定数据与可手算预期

数据截止时间固定为 **2026-11-12 10:00:00，Asia/Shanghai**。当前系统日期不同不影响 fixture。日期控件默认值随系统日期变化，必须手工设为下表窗口。

| 项目 | 基线 | 实验 |
| --- | --- | --- |
| 集群 | showcase-mock | showcase-mock |
| namespace | showcase | showcase |
| 页面开始日期 | 2026-10-26 | 2026-11-02 |
| 页面结束日期（不含） | 2026-11-02 | 2026-11-09 |
| 实际提交时间 | 2026-10-27 13:04 | 2026-11-03 13:04 |
| 任务 UID | mock-baseline-01…10 | mock-experiment-01…10 |
| JCT 分钟 | 9 个 90，1 个 190 | 9 个 65，1 个 215 |

complete 场景所有任务成功完成，合计 20 个：

| 指标 | 基线 | 实验 | 变化 |
| --- | --- | --- | --- |
| 平均 JCT | 100 min | 80 min | 下降 20.0% |
| P50 | 90 min | 65 min | 下降 27.8% |
| P95 | 190 min | 215 min | 增加 13.2% |
| 有效完成样本 | 10/10 | 10/10 | 不显示暂定原因 |

均值分别为 `(9×90+190)/10` 与 `(9×65+215)/10`。nearest-rank 下，10 个任务的 P95 是最大值。该数据刻意展示“平均更快，但长尾变慢”，不能讲成所有任务均获益。

HTTP 查询示例（秒值分别对应上海午夜）：

```bash
curl --fail 'http://127.0.0.1:18080/api/v1/elastic/jct-statistics?cluster=showcase-mock&submitted_from=1792944000&submitted_to=1793548800'
curl --fail 'http://127.0.0.1:18080/api/v1/elastic/jct-statistics?cluster=showcase-mock&submitted_from=1793548800&submitted_to=1794153600'
```

如需核对日期转换，可在浏览器控制台运行 `Date.parse('2026-10-26T00:00:00+08:00')/1000`。

## 5. 集群页用例

每次切换 scenario 后刷新页面数据；如任务列表变化，重新加载浏览器页面。

| ID | 操作 | 验收标准 |
| --- | --- | --- |
| C01 | complete 场景，设置第 4 节范围 | 三张卡片及柱图符合手算结果，单位分钟；P95 增加用警示色 |
| C02 | 查看 Network 两次统计请求 | 分别携带提交窗口；两组均为同一集群，不从单任务收益累加 |
| C03 | 基线改成 10/28–11/02 | 基线无样本，显示 —；变化率不可计算，不显示 0% |
| C04 | 基线设 10/26–10/27 | 10/27 13:04 提交的任务被排除 |
| C05 | 基线设 10/27–10/28 | 10 个任务被纳入，验证按提交日期选择 |
| C06 | 输入结束早于开始或两个阶段重叠 | 显示日期错误，不展示旧结果；恢复合法范围可重新加载 |
| C07 | partial 场景，恢复标准窗口 | 实验第 10 个任务运行中；样本 9/10，平均/P50/P95 都为 65 min，显示暂定提示；不能称长尾改善已验证 |
| C08 | invalid 场景 | 响应格式错误，出现错误状态，不退回内部示例 |
| C09 | outage 场景 | HTTP 503，出现错误状态，不继续显示旧收益卡片 |
| C10 | empty 场景，重新加载页面 | 无可选任务集群，显示空状态，无虚构卡片 |
| C11 | 返回 complete，重新加载并设置日期 | 完整恢复，结果符合 C01，无需清空数据库 |

阶段名称“关闭/开启弹性”是人工选择窗口的语义，当前不会验证真实集群开关，不应演示为自动识别。

## 6. 任务页用例

complete 场景下进入“任务分析”，选择 **MOCK-experiment-10**。其他短任务没有构造弹性事件，适合演示空状态。

| ID | 操作 | 验收标准 |
| --- | --- | --- |
| T01 | 选择 MOCK-experiment-10 | UID 与请求一致；提交 11/03 13:04，完成 16:39；实际 JCT 215 min |
| T02 | 查看任务 JCT 复盘 | mock 完整基线 240 min，节省 25 min，改善约 10.4%；来源标识为 MOCK 估算，不是实际后端账本 |
| T03 | 查看最近一次收益 | 时间收益 +24 min、GPU收益约 +0.37 GPU·h、剩余时间目标收益率 +75%，至少两个观测窗口；不要求与全生命周期 JCT 改善率相同 |
| T04 | 查看事件列表 | 3 条：扩容成功、缩容跳过、扩容失败；实际调整尝试 2，执行完成 1，失败 1 |
| T05 | 点击成功/跳过/失败事件 | 右侧详情与选中事件一致，展示计划/实际 GPU、执行/恢复/收益结论和阶段时间 |
| T06 | 事件筛选与搜索 | 扩容 2 条、缩容 1 条、失败 1 条、跳过 1 条；搜索 evt-003 仅 1 条 |
| T07 | 趋势及时间范围 | 全生命周期可见 mock GPU/吞吐与事件；“结束前10分钟”可为空，因为事件发生在更早时间，不能强行补点 |
| T08 | 选择 MOCK-baseline-01 | 实际 JCT 90 min；无基线、收益和事件时显示未知/—/空状态 |
| T09 | partial 场景，重新加载，选择实验第10个任务 | 显示运行中耗时，实际完成时间为空；无有效收益和事件，不误显示成功完成 |
| T10 | negative 场景，刷新任务 | 最新收益 -5 min、-0.17 GPU·h、-12%；收益不足保留配置，执行状态仍完成；负收益不能截成0 |
| T11 | outage 场景，刷新任务 | 错误提示，不自动回退到内部示例；恢复后能重新加载 |
| T12 | 切换回集群页再返回 | 页签清楚分离；不将单任务最新预测值显示为集群累计收益 |

提示：fixture 的时间基准固定，收益观测早于 generated_at，会显示“历史观测”；这是正确的时效提示。任务页的 GPU 曲线、停训区间、收益数值用于独立 UI 字段演示，不是由训练执行器产生的整套物理仿真，也不能用于验证成本模型。

## 7. 场景切换与复位

终端 C 按需执行，命令仅操作本机 mock：

```bash
curl --fail -X POST 'http://127.0.0.1:18080/__scenario?name=complete'
curl --fail -X POST 'http://127.0.0.1:18080/__scenario?name=partial'
curl --fail -X POST 'http://127.0.0.1:18080/__scenario?name=negative'
curl --fail -X POST 'http://127.0.0.1:18080/__scenario?name=empty'
curl --fail -X POST 'http://127.0.0.1:18080/__scenario?name=outage'
curl --fail -X POST 'http://127.0.0.1:18080/__scenario?name=invalid'
```

只执行当前需要的一条；场景互斥，重启服务默认 complete。outage 不阻断 `/__scenario` 和 `/healthz`。negative 仅改变任务收益，集群生命周期及统计不变；invalid 仅破坏统计响应。scenario 不修改生产配置或文件。

## 8. 建议的 6 分钟 Showcase 流程

1. **30秒：说明边界。** 本次接口全部来自本地 mock；展示功能和数据口径，不展示真实算法收益。
2. **90秒：集群结果。** complete 场景、标准窗口，说明平均100→80，P95却190→215，必须同时看平均与长尾。
3. **60秒：不完整样本。** 切 partial 并刷新，解释慢任务未完成会让已完成均值更好看，页面因此提示暂定。
4. **90秒：任务复盘。** 恢复 complete，选实验第10个任务，展示完整JCT、最近评估、成功/跳过/失败事件，强调三个状态维度不同。
5. **45秒：负收益。** 切 negative，展示保留负值、执行完成不等于收益达标。
6. **45秒：容错与复位。** 切 outage 展示 API 错误不回退；最后恢复 complete、标准日期、集群页。

建议保存三张截图：集群完整对比、partial暂定提示、任务负收益与事件详情。故障页截图作为附录即可，不在故障状态结束演示。

## 9. 自动验证与真实后端边界

前端与跨语言契约测试：

```bash
cd web/elastic-dashboard
npm test
npm run build
```

真实聚合服务 JCT、窗口边界、HTTP 参数验证（仓库根目录，Python 环境需安装 pytest）：

```bash
python -m pytest dlrover/dashboard/tests/test_elastic_jct.py dlrover/dashboard/tests/test_elastic_api.py -q
```

后端测试使用隔离临时存储及本地 socket；不依赖生产数据库。测试包括左闭右开窗口、窗口后完成仍计入、缺少提交时间、无样本、非法窗口及接口错误。MySQL部署和真实Prometheus查询另按服务说明验收，不在本地mock showcase通过结论内。

完整验收记录应包含：代码版本、浏览器版本、启动命令、场景、窗口、接口响应、截图、预期/实际、通过/失败。自动测试通过不能替代 C/T 页面用例的人工确认。

## 10. 常见问题和收尾

| 现象 | 排查 |
| --- | --- |
| 集群没有样本 | 检查是否 mode=api、集群showcase-mock、固定日期窗口；默认日期随系统时间变化 |
| 看到 demo-cluster | 使用了内部演示模式，改为 ?mode=api |
| API请求到8080 | Vite启动前未设置 DLROVER_ELASTIC_API_URL，需重启Vite |
| 503一直不恢复 | POST场景切回complete，再刷新；healthz可查看当前场景 |
| 任务无曲线 | 选择实验第10个任务、complete场景、全生命周期 |
| 浏览器/API跨域报错 | 使用Vite同源/api代理，不让前端直接请求mock端口 |
| 基线被误当成真实数据 | 保留MOCK命名、来源说明和开场声明；生产服务返回基线null是当前正确行为 |
| 启动报端口占用 | 停止旧本地进程，勿复用未知监听服务 |

完成后在终端A/B分别 Ctrl+C 停止 mock 和 Vite。本方案不创建数据库、不会提交训练任务，无需清理集群资源。不要将 mock 服务打包进生产容器或对公网开放。

### 配套 mock 自检

启动终端 A 的服务后，在前端目录执行：

```bash
node scripts/check-showcase.mjs
```

检查六种场景、60 个任务响应的前端 schema、集群统计预期值，结束时自动复位 complete。
脚本会切换场景，不要在现场演示过程中同时执行。它不操作生产接口。

已部署真实 DLRover、仅 SwanLab 使用 mock 的联调，请改用
[真实集群验证说明](elastic-dashboard-real-cluster-validation-cn.md)，不要继续使用本地 showcase API 替代真实后端。
