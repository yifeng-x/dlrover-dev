# 真实环境端到端测试手册：Master 临时采集 + SwanLab mock

更新日期：2026-11-17。适用于已经部署 DLRover 的 Kubernetes 环境。本手册重新组织完整测试顺序；不需要重装已有 DLRover，不使用 showcase HTTP mock。

## 1. 本次到底验证什么

### 1.1 验证范围

1. 真实训练任务的生命周期，由 Master 临时上报到真实看板聚合服务。
2. 真实 checkpoint、Worker 替换、训练恢复和弹性事件。
3. SwanLab mock 输入驱动的门禁评估及看板收益展示。
4. 真实运行 Worker GPU 数，经 Prometheus 展示曲线。
5. 两个提交时间窗口的平均 JCT、P50、P95及样本完整性。

**JCT 的实际口径是 ElasticJob 创建至 Master 确认成功，不含平台创建 CR 前的等待。** 前端目前未自动标注此来源，演示与报告必须说明。两阶段均使用同一来源。

SwanLab 为 mock，候选收益服务若使用 fixture 也属于合成输入。因此结果可以证明执行、采集、计算、展示链路正确，不能证明算法基于真实吞吐取得了真实收益。实际耗时差异可以记录，不要求一定改善才算功能通过。

### 1.2 三条独立数据链路

```text
SwanLab mock + 收益证据HTTP服务
  → Master门禁/可选探索
  → 真实弹性执行
  → 结构化事件 → POST /events → MySQL → 任务收益和事件详情
  → /metrics → Prometheus → 执行监控、收益查询回退

ElasticJob创建时间 + Master明确结束判定
  → POST /job-snapshots → MySQL elastic_jobs
  → /jct-statistics → 集群平均/P50/P95
  → /jobs/{uid}/dashboard → 单任务实际JCT

真实运行Worker GPU分配
  → Master周期采样 → Prometheus
  → 聚合服务历史查询 → 任务GPU曲线
```

聚合服务优先使用持久化收益记录，无记录时尝试 Prometheus。GPU 历史独立查询 Prometheus。集群 JCT 不依赖 SwanLab、不累加收益 Gauge。连续吞吐与真实停训时长当前尚未完整接入，缺失必须如实展示。

## 2. 资源、账号与数据准备

### 2.1 开始前检查表

| 项目 | 准备内容 | 通过要求 |
| --- | --- | --- |
| 集群访问 | kubectl上下文、测试namespace、执行人 | 仅操作独立测试范围 |
| 镜像 | 当前Master、Agent、训练镜像digest | Master包含临时生命周期模块，训练镜像支持垂直checkpoint/恢复 |
| GPU资源 | 型号、总预算、初始/目标卡数 | 至少可调度一个1→2卡场景，基线/实验预算相同 |
| 模型与数据 | 已跑通的模型、数据集、完整训练命令 | 不用未验证的新训练栈排查监控功能 |
| 工作量 | 目标steps/tokens、batch、序列长度、训练质量要求 | 对照两阶段保持一致；足够长以完成预热和恢复 |
| 共享存储 | 模型、数据、checkpoint路径 | 跨节点恢复可读写 |
| Brain与触发 | 已验证的手动资源调整路径 | 隔离测试，不影响其他任务 |
| 聚合服务 | 真实API地址、MySQL表、当前镜像 | 支持/events、job-snapshots、时间窗口统计 |
| Prometheus | 抓取配置、查询地址、保留周期 | Master目标UP，保留期覆盖实验 |
| 前端 | 当前构建、同源/api代理 | 使用?mode=api，不回退内部示例 |
| 收益证据 | 真实适配服务或明确标注fixture | SwanLab mock不能单独代替完整收益证据 |

不要记录数据库密码、token或完整Secret。当前服务部署步骤见[部署说明](../../deploy/elastic-dashboard/README.md)。

### 2.2 任务清单用于对账，不是输入配置

维护CSV/Excel或复用平台导出，记录：

```text
stage, lifecycle_source, job_uid, job_name, namespace, cluster,
platform_task_id, image_digest, model_version, dataset_version,
target_work, initial_gpu_count, elastic_enabled,
cr_created_at, master_result, completed_at, collection_status
```

- lifecycle_source统一填master_temporary。
- UID使用真实ElasticJob UID，taskId只用于SwanLab身份校验。
- 清单包含成功、失败、运行中、Master未启动和未上报的任务。
- 系统不自动读取该清单；用于核对“应该有多少任务”和“实际采到多少”。
- 不把未采到任务从清单删除，不把failed或running填成completed。

### 2.3 目录和变量

在仓库根目录设置。所有REPLACE值必须换成真实环境值再执行：

```bash
export REPO_DIR="$PWD"
export NS='REPLACE_TEST_NAMESPACE'
export CLUSTER='REPLACE_REAL_CLUSTER_LABEL'
export JOB_NAME='REPLACE_CURRENT_ELASTICJOB_NAME'
export MASTER='REPLACE_MASTER_POD'
export MASTER_CONTAINER='main'
export TASK_ID='REPLACE_PLATFORM_TASK_ID'
export DASHBOARD_API='http://127.0.0.1:18080'
export MANIFEST_DIR='/tmp/benefit-k8s-manifests'
export RUN_DIR="/tmp/dlrover-validation-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RUN_DIR"
kubectl config current-context
kubectl get namespace "$NS"
```

DASHBOARD_API应是你批准访问的真实聚合服务地址；使用18080时应是其本地转发，绝不能启动showcase-mock占用该端口。新终端需要重新设置变量。日志跟踪、port-forward分别占用终端，Ctrl+C只停止对应本地命令。

## 3. 部署前置检查：通过后再提交任务

### 步骤3.1 检查看板真实后端

```bash
curl --fail "$DASHBOARD_API/healthz"
curl --fail "$DASHBOARD_API/readyz"
curl --fail "$DASHBOARD_API/api/v1/elastic/jobs" > "$RUN_DIR/jobs-before.json"
```

预期：健康和就绪成功，jobs返回JSON。失败先排查Service、MySQL和同源代理，不通过时不开始正式测试。

Prometheus查询地址配置在聚合服务 `DLROVER_PROMETHEUS_URL`。抓取Master默认/metrics端口按实际metrics_port确认；建议10秒间隔。部署聚合服务不会自动创建抓取配置。

### 步骤3.2 配置Master上报

将以下片段合并到新任务的Master容器env，保留原有变量，替换真实Service地址：

```yaml
env:
  - name: DLROVER_MASTER_LIFECYCLE_ENABLED
    value: "true"
  - name: DLROVER_MASTER_LIFECYCLE_URL
    value: "http://elastic-dashboard.dlrover.svc.cluster.local:8080/api/v1/elastic/job-snapshots"
  - name: DLROVER_ELASTIC_EVENT_URL
    value: "http://elastic-dashboard.dlrover.svc.cluster.local:8080/api/v1/elastic/events"
```

基线与实验均启用生命周期上报。`DLROVER_MASTER_LIFECYCLE_ENABLED`不控制资源弹性。

当前实现读取args.raw_job，UID需匹配，创建时间缺失保留null；进入Master运行循环后上报running，每60秒补报相同快照。明确成功/失败后上报终态，中断不伪造成功。每次最多2次发送，退出最多等待2秒，强杀/网络故障可能漏终态。重试保留原快照时间，不随每次补报更新时间。

同一UID只能有一个生命周期写入者。暂停该测试范围的平台同步；后端当前无来源优先级和终态不可回退保护。新镜像/配置须作用到新Master，修改ConfigMap不热更新已有进程。

### 步骤3.3 准备弹性测试模板

复用[真实K8s SwanLab mock手册](vertical-elastic-benefit-k8s-mock-test-cn.md)第2–4节的环境前提、模板提取和部署命令，生成并检查：

- `01-manual-idle.yaml`：scale_action=not_adjust。
- `02-job.yaml`：已跑通的真实训练任务，初始1卡。
- `03-trigger.yaml`：目标2卡，scale_action=manual。
- SwanLab环境、benefit-fixture或真实适配服务。

将本手册步骤3.2的env加入Master模板。替换镜像、PVC、namespace、模型/数据路径；模板中的job_name、taskId与实际任务保持一致。先执行server dry-run检查，但它不能证明训练或恢复可用。

**手动ConfigMap可能影响同namespace的多个Master，不依赖job_names隔离。** 每个测试namespace只能有一个监听该配置的测试任务，或采用已经验证的隔离方式。禁止在共享生产namespace直接apply手册触发模板。

## 4. 基线阶段：不调整资源的真实训练

### 步骤4.1 禁止调整入口

使用现有部署的策略配置关闭自动调整，并将隔离手动配置置于not_adjust；探索关闭。确认没有其他控制器触发资源变化。

**benefit mode=off仅关闭收益检查，并非关闭弹性。** 不能用它代替基线资源固定。baseline/env、策略配置和实际GPU记录都应归档。

### 步骤4.2 提交并登记任务

使用平台原有提交方式或经过检查的ElasticJob YAML提交。两阶段任务使用不同名称和真实UID，避免旧资源复用。每个任务创建后执行：

```bash
kubectl get elasticjobs.elastic.iml.github.io "$JOB_NAME" -n "$NS" -o json > "$RUN_DIR/elasticjob.json"
export JOB_UID=$(kubectl get elasticjobs.elastic.iml.github.io "$JOB_NAME" -n "$NS" -o jsonpath='{.metadata.uid}')
kubectl get pods -n "$NS" -o wide
```

从归属关系确认真实Master名称后设置MASTER，不要按模糊名称随意选Pod。记录CR创建时间、UID、初始资源和stage=baseline。

### 步骤4.3 验证运行快照

确认真实训练日志step持续增长，再查询：

```bash
kubectl logs -n "$NS" "$MASTER" -c "$MASTER_CONTAINER" --timestamps > "$RUN_DIR/master-running.log"
rg 'Temporary Master lifecycle|lifecycle delivery' "$RUN_DIR/master-running.log"
curl --fail "$DASHBOARD_API/api/v1/elastic/jobs/$JOB_UID/dashboard" > "$RUN_DIR/dashboard-running.json"
```

预期：启用日志出现；dashboard中job.uid匹配、status=running、submitted_at等于CR创建时间、completed_at=null。actual_seconds为空，elapsed_seconds随刷新增长。

若UID匹配或URL校验失败，会有unavailable日志且训练继续；如果任务仅由事件登记、却没有submitted_at，仍不算生命周期采集通过。网络恢复后等待下一次补报并查询，不用固定sleep认定成功。

### 步骤4.4 自然结束并核对JCT

等待训练自然成功结束，不删除Pod来制造结束。完成后保存：

```bash
kubectl logs -n "$NS" "$MASTER" -c "$MASTER_CONTAINER" --timestamps > "$RUN_DIR/master-final.log"
kubectl get elasticjobs.elastic.iml.github.io "$JOB_NAME" -n "$NS" -o json > "$RUN_DIR/elasticjob-final.json"
curl --fail "$DASHBOARD_API/api/v1/elastic/jobs/$JOB_UID/dashboard" > "$RUN_DIR/dashboard-final.json"
```

预期：status=completed，completed_at有值，actual_seconds=completed_at−submitted_at；与真实训练成功日志时刻相符（上报时钟及日志粒度可能不同）。界面刷新后耗时不再增长。

基线不应有真实资源调整；GPU采样缺失时不能仅凭“无事件”证明无调整，需结合配置、Pod资源和训练记录。若Master已被清理，应提前采集日志或使用已有日志系统，不虚构日志文件。

## 5. 实验阶段：mock收益驱动真实扩容

### 步骤5.1 隔离并创建实验任务

基线任务全部结束后再启用调整最清晰。使用同模型、数据、目标工作量、初始资源和提交节奏的新任务。保持同样集群资源预算。标记stage=experiment。

先应用idle配置再创建实验任务，避免遗留manual在监听初始化时触发。任务创建后重复4.2、4.3，确认临时上报与真实训练均正常。

### 步骤5.2 确认mock与收益服务

Master最低配置如下，其余keys/batch配置与引用手册配套：

```text
DLROVER_SWANLAB_MOCK=true
DLROVER_SWANLAB_MOCK_FILE=/mock/swanlab.json
DLROVER_BENEFIT_MODE=enforce
DLROVER_BENEFIT_OBJECTIVE=time
DLROVER_BENEFIT_EXPLORE=false
DLROVER_BENEFIT_HTTP_URL=实际完整收益证据接口
```

普通正收益算例可使用引用手册G01：mock q0=1、剩余3600，候选q1=2、切换120秒、重放0、切换240GPU秒；初始1卡目标2卡，预计节省1680秒，GPU收益−240GPU秒。只有证据参数完全匹配时才采用这些预期；actual JCT无需等于预测剩余时间。

若用benefit-fixture，明确候选、成本和调度也为mock。该fixture不能用于探索执行验证。不要将feasible=true当作真实GPU已经可调度。

### 步骤5.3 满足时间窗口再写mock

采用引用手册的隔离测试参数warmup=30秒、max_age=600秒、window=60秒。真实step确认增长后记一次Master时间：

```bash
BASELINE_READY_AT=$(kubectl exec -n "$NS" "$MASTER" -c "$MASTER_CONTAINER" -- python -c 'import time; print(time.time())')
kubectl exec -n "$NS" "$MASTER" -c "$MASTER_CONTAINER" -- python -c \
  'import math,sys,time; print(max(0,math.ceil(float(sys.argv[1])+95-time.time())))' "$BASELINE_READY_AT"
```

第二条可反复检查；输出0且训练继续推进后才生成。期间若重启、重新rendezvous或调整，重新确认进展并计时。若实际warmup不是30秒，按实际值重新计算，不能照搬95秒。

```bash
kubectl exec -i -n "$NS" "$MASTER" -c "$MASTER_CONTAINER" -- python - \
  --task-id "$TASK_ID" --assume-fixed-batch \
  --window-seconds 60 --work-start 100 --work-end 160 --total-steps 3760 \
  --output /mock/swanlab.json \
  < "$REPO_DIR/examples/pytorch/resource_elastic/benefit_validation/generate_swanlab_mock.py"
kubectl exec -n "$NS" "$MASTER" -c "$MASTER_CONTAINER" -- cat /mock/swanlab.json > "$RUN_DIR/swanlab.json"
```

该例要求固定每step工作量假设，与配置一致。taskId必须与真实CR标签一致。模拟100→160不改变真实训练step或checkpoint。生成成功不代表窗口被接受，也不会自动触发扩容。

### 步骤5.4 触发一次并核对四层结果

在文件生成后5分钟内，确保配置max_age=600仍有效：

```bash
kubectl get pods -n "$NS" -o json > "$RUN_DIR/pods-before.json"
kubectl apply -f "$MANIFEST_DIR/03-trigger.yaml"
kubectl logs -n "$NS" "$MASTER" -c "$MASTER_CONTAINER" -f --timestamps
```

用另一终端观察并分别核验：

1. **评估**：window_selected、gate_decision表明输入被接受，positive且允许；参数匹配G01时预计时间收益1680秒。
2. **执行**：真实checkpoint成功、目标Worker被替换、实际资源变化符合目标；仅计划目标值不算执行成功。
3. **恢复**：真实训练进度恢复增长，记录恢复确认；执行completed不等于恢复confirmed。
4. **展示**：事件入库、任务页展示prediction及执行阶段；未开启探索不要求ACCEPTED或exploration结果。

```bash
kubectl get pods -n "$NS" -o json > "$RUN_DIR/pods-after.json"
kubectl logs -n "$NS" "$MASTER" -c "$MASTER_CONTAINER" --timestamps > "$RUN_DIR/master-after.log"
curl --fail "$DASHBOARD_API/api/v1/elastic/jobs/$JOB_UID/dashboard" > "$RUN_DIR/dashboard-after.json"
```

用例结束后把手动配置复位idle，避免影响下一任务；复位前确认不会影响共享任务。让训练自然结束，重复4.4归档实际JCT。不得为了匹配预期数字修改生命周期。

## 6. 任务页逐项验收

使用真实前端 `?mode=api` → 任务分析 → 选真实任务。建议保留Network中的dashboard响应。

| 检查项 | 操作 | 预期 |
| --- | --- | --- |
| 任务身份 | 对比UID、名称、namespace | 与CR和接口一致，无跨任务串数据 |
| 运行中耗时 | 未完成时刷新 | elapsed增长，完成时间为空 |
| 实际JCT | 成功后刷新 | 对应真实快照，不再增长 |
| 任务基线 | 查看JCT复盘 | 当前真实后端baseline=null，显示待补充，不强求任务事后改善率 |
| 最新收益 | 对照门禁记录 | 数值、单位、objective、stage、来源一致，允许负卡时收益 |
| 事件列表 | 选中事件 | 计划/实际GPU、执行、恢复、收益结论分别展示 |
| GPU曲线 | 对比Prometheus和Pod | 运行Worker分配量吻合；无效/过期采样有缺口，不用目标值补齐 |
| 连续吞吐/中断 | 未接入真实证据 | 空值是预期，不把mock scalar自动当作真实历史 |
| 时间范围/筛选 | 切换时间范围和事件类型 | 仅改变任务事件/趋势，不改变生命周期JCT |
| 数据源 | 查看模式及记录 | API不回退demo；报告标明SwanLab为mock |

Prometheus重点核对 `dlrover_master_running_worker_allocated_gpus`、`dlrover_master_worker_gpu_observation_valid`、`dlrover_master_worker_gpu_observed_timestamp_seconds`，使用真实cluster/namespace/job_uid筛选。收益Gauge不做rate/increase，执行Counter不作为永久总账。

## 7. 集群页完整流程

### 步骤7.1 安排可分组的时间范围

当前UI仅支持上海时区日期午夜，基线与实验不重叠。正式展示需要两组CR创建时间分别落在不同日期窗口。若同一天冒烟，可用API秒级窗口核验，但当前UI不能分开显示同日两组；不允许改真实时间迁就页面。

建议先完成1+1冒烟，再完成两组各10个同配置任务；10个样本的P95就是最大JCT。周级生产对比则单独报告任务构成和提交负载差异。

### 步骤7.2 查询两个窗口

设置真实秒级边界，下列REPLACE值替换后执行：

```bash
export BASELINE_FROM='REPLACE_BASELINE_START_UNIX_SECONDS'
export BASELINE_TO='REPLACE_BASELINE_END_UNIX_SECONDS'
export EXPERIMENT_FROM='REPLACE_EXPERIMENT_START_UNIX_SECONDS'
export EXPERIMENT_TO='REPLACE_EXPERIMENT_END_UNIX_SECONDS'
curl --fail --get "$DASHBOARD_API/api/v1/elastic/jct-statistics" \
  --data-urlencode "cluster=$CLUSTER" \
  --data-urlencode "submitted_from=$BASELINE_FROM" \
  --data-urlencode "submitted_to=$BASELINE_TO" > "$RUN_DIR/baseline-statistics.json"
curl --fail --get "$DASHBOARD_API/api/v1/elastic/jct-statistics" \
  --data-urlencode "cluster=$CLUSTER" \
  --data-urlencode "submitted_from=$EXPERIMENT_FROM" \
  --data-urlencode "submitted_to=$EXPERIMENT_TO" > "$RUN_DIR/experiment-statistics.json"
```

日期转秒示例（仅演示转换，选择你实际测试日期）：

```bash
python -c 'from datetime import datetime; print(int(datetime.fromisoformat("2026-11-17T00:00:00+08:00").timestamp()))'
```

UI当前按cluster统计所有已登记namespace。若用API额外加namespace，则不能直接与未过滤namespace的页面比较。应隔离后端实例或核算该cluster全部记录，不伪造cluster标签来隐藏其他任务。

### 步骤7.3 独立核算

从清单及真实快照取各窗口内成功、时间合法的任务，JCT=completed_at−submitted_at。只限制提交时间[from,to)，不限制完成时间是否在窗口内。

- 平均=所有有效JCT之和/样本数。
- P50/P95=排序后第ceil(n×p)个，按nearest-rank。
- 变化=(实验−基线)/基线；正数为耗时增加，负数为下降。
- 无样本返回null；基线0或缺失不生成百分比。
- failed、running、unknown及无效完成记录不进入成功JCT，但必须展示计数。

### 步骤7.4 页面验收

| 编号 | 操作 | 通过标准 |
| --- | --- | --- |
| C1 | 选择真实集群和两个窗口 | 发出两次真实jct-statistics请求，无mock API |
| C2 | 对比三张卡片和柱图 | 与独立核算一致；秒转分钟仅显示舍入 |
| C3 | 对比有效样本分子/分母 | 与清单核算一致，未采集任务另外记录覆盖缺口 |
| C4 | 查看运行中、失败、未知 | 均如实显示，不把慢任务漏采当成收益 |
| C5 | 窗口内任务在窗口后完成 | 补报后仍被纳入，刷新统计更新 |
| C6 | 选择无任务窗口 | —及不可计算提示，不伪造0耗时或100%收益 |
| C7 | 输入重叠或倒置日期 | 前端阻止并提示，不保留旧结果冒充新范围 |
| C8 | P95比基线更大 | 保留增加及警示色，不截断负向结论 |
| C9 | 有未完成/无效数据 | 暂定提示；窗口结束不等于所有任务完成 |

## 8. 必测异常与可选探索

每类使用新的隔离测试任务及独立RUN_DIR。不要在正式基线/实验统计集合混入故意故障任务，必要时使用独立采集实例。

| 场景 | 操作方式 | 验收证据 |
| --- | --- | --- |
| 负收益门禁 | 采用引用手册G03，使用新任务和新有效mock窗口 | enforce拒绝，无checkpoint/替换，原训练继续；页面跳过/原因正确 |
| 过期或不匹配taskId | 按引用手册S系列构造输入 | unknown/拒绝，不能构造正收益 |
| 临时采集默认关闭 | 新任务不设置生命周期开关 | 不发送快照；事件仍可能登记任务身份，不等于生命周期接入 |
| 真实任务失败 | 使用获准的隔离失败测试任务 | failed且completed_at为空，不进入成功JCT |
| 任务中断 | 已获准的中断测试，不影响共享任务 | 不误报completed；可能残留running，登记终态缺口 |
| 临时上报不可达 | 隔离任务使用不可达测试URL | 训练不等待HTTP；失败日志；不能承诺最终快照一定补达 |
| 重复快照/事件 | 保留原时间和原event_id重放 | 不重复计数；同时间冲突422 |
| 真实前端API故障 | 在独立测试前端变更代理后恢复 | 错误不回退demo，恢复后可刷新 |

探索仅在执行证据适配服务、持久化journal、真实恢复上下文齐备后按引用手册第8节执行。普通benefit-fixture拒绝探索控制请求，不能沿用。探索需要两轮独立有效窗口，满足新代次、checkpoint进度下限和时效；mock窗口不能不断重写同一进度冒充新证据。RETAINED保留负值且不自动回退。

## 9. 终态缺失补录和平台切换

### 9.1 临时补录

先确认该UID旧Master已停止发送，依据真实CR和训练成功/失败证据生成完整快照，不能猜测完成时间：

```json
{
  "job_uid": "REPLACE_REAL_UID",
  "job_name": "REPLACE_REAL_NAME",
  "cluster": "REPLACE_REAL_CLUSTER",
  "namespace": "REPLACE_REAL_NAMESPACE",
  "status": "completed",
  "submitted_at": 0,
  "completed_at": 0,
  "lifecycle_observed_at": 0
}
```

三个0是类型占位，必须替换真实Unix秒，不可原样提交。若无法确认成功时间，应保持缺失，不制造有效JCT。

```bash
curl --fail -H 'Content-Type: application/json' --data-binary @job-snapshot.json \
  "$DASHBOARD_API/api/v1/elastic/job-snapshots"
```

完整快照会替换生命周期字段，省略submitted_at会清空原值。新时间覆盖旧时间；同时间同内容幂等、同时间冲突422。当前无终态保护，新running可能覆盖completed。重试保留原快照，不为旧状态重新盖新时间。

事件缺失可从Master日志提取 `VerticalElasticEvent ` 后原始JSON逐条补录 `/events`，必须保留event_id。不用生命周期补录代替事件，也不重新生成ID重复计数。

### 9.2 平台就绪后的切换

1. 模板关闭 `DLROVER_MASTER_LIFECYCLE_ENABLED`，防止新任务临时上报。
2. 等旧Master结束，确认后台线程不再发送；修改环境变量不热更新旧进程。
3. 平台接入逻辑查询真实提交/完成信息，向同一job-snapshots补报完整快照。
4. 按UID核对实际写入；不修改或删除历史弹性事件。
5. 报告口径从CR-to-Master改为平台提交至完成，重新核算对比，不混用两类基线。

job-snapshots由DLRover聚合服务提供，被动接收；它不会自动调用平台。平台同步器仍需单独接入。

## 10. 故障定位顺序

| 现象 | 先检查 |
| --- | --- |
| 无任务/无submitted_at | Master镜像和开关、是否进入run、raw_job UID、HTTP完整URL、发送日志 |
| 完成后仍running | Master是否强杀、终态是否发送失败、是否有其他写入者；不靠刷新推断成功 |
| 集群无样本 | CR创建时间是否在窗口、cluster标签、采集覆盖、unassigned_jobs |
| 任务收益为空 | 门禁是否触发、benefit模式、SwanLab窗口校验、收益HTTP证据、事件入库 |
| 有事件无GPU曲线 | Prometheus target、job_uid标签、有效性/时效、历史保留、聚合查询地址 |
| 无吞吐/停训时间 | 当前真实采集能力限制，不能复制showcase合成数据 |
| API与页面不同 | 先保存响应UID和数值，再查前端筛选、刷新状态和单位 |
| 只改ConfigMap仍旧行为 | 新环境变量需作用于新进程；核对实际Pod配置而非本地YAML |

## 11. 归档、验收记录和收尾

每个任务保存独立目录，集群汇总另存顶层：

```text
validation/
  environment.md                 # 镜像、资源、数据来源、策略配置
  task-manifest.csv              # 全部任务，包括采集缺口
  baseline-statistics.json
  experiment-statistics.json
  job-UID/
    elasticjob.json
    elasticjob-final.json
    master-running.log
    master-final.log
    swanlab.json
    pods-before.json
    pods-after.json
    dashboard-running.json
    dashboard-final.json
  prometheus/                    # 抓取状态和历史查询证据
  screenshots/
  acceptance.md
```

验收表至少记录：用例、任务UID、预期、实际、接口/日志/截图路径、通过/失败/未执行、原因。分别给出生命周期采集、真实执行、mock收益展示、集群统计四项结论，不以“页面有绿色收益”代替真实执行证据。

真实接口尚无任务冻结基线账本；任务基线null、连续吞吐缺失不应阻塞本次范围验收。没有完成的探索前提应标未执行，不能填通过。实际JCT恶化也可以意味着测试成功发现负向结果，不是为了通过而修改数据。

收尾：复位隔离手动配置为idle，停止本地日志/转发，按测试资源清单回收；不要删除共享namespace或清空生产数据库。Master临时上报在平台切换前按第9节停用。本手册是操作说明，不代表已在你的集群执行。

## 12. 本地回归与参考

在已安装依赖的Python环境中，从仓库根目录执行：

```bash
python -m pytest dlrover/python/tests/test_master_lifecycle.py -q
python -m pytest dlrover/dashboard/tests/test_elastic_jct.py dlrover/dashboard/tests/test_elastic_api.py -q
```

前端目录执行 `npm test`、`npm run build`。HTTP测试需要本地监听权限；自动测试不替代现场用例和截图。

- [完整K8s mock门禁与探索步骤](vertical-elastic-benefit-k8s-mock-test-cn.md)
- [收益监控指标与口径](vertical-elastic-benefit-prometheus-cn.md)
- [真实看板接口契约](elastic-dashboard-api-cn.md)
- [Master临时上报及服务说明](../../dlrover/dashboard/elastic/README.md)
- [聚合服务部署](../../deploy/elastic-dashboard/README.md)
- [仅UI演示的mock手册，不能替代本文](elastic-dashboard-showcase-test-cn.md)
