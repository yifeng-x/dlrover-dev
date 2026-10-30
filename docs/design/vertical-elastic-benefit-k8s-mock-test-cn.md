# Kubernetes 集群上的 SwanLab mock 收益场景测试手册

本文依据当前仓库代码编写，日期为 2026-10-30。目标是在真实 Kubernetes 垂直弹性链路中，用可控 SwanLab scalar 数据验证收益判断、checkpoint 前拦截、资源替换，以及具备执行证据服务时的探索与回退。

本文提供完整的测试基础设施 YAML、ElasticJob YAML、手动触发 YAML、探索配置 YAML、操作步骤与验收矩阵。镜像、StorageClass、Brain 地址、模型和数据路径必须替换为本集群已有值。文档编写不代表已在你的集群执行或通过验收。

## 1. 测试范围和部署关系

```text
kubectl exec：生成模拟 scalar JSON
                   ↓
Master /mock/swanlab.json → SwanLab 客户端真实校验 → 当前吞吐 q0
                   ↓
benefit-fixture HTTP 服务 → 候选吞吐 q1、切换成本、调度/时限测试输入
                   ↓
BenefitGuard → 拒绝：保持当前 worker
            → 放行：真实 checkpoint → 真实 worker 替换 → 训练恢复
```

- `DLROVER_SWANLAB_MOCK=true` 只替换 SwanLab 的 HTTP 获取，仍检查任务身份、训练代次边界、预热、稳定窗口、数据时效和工作量。它不模拟 Kubernetes、Brain 或训练框架。
- 本文的 `benefit-fixture` 是**普通门禁测试桩**，候选性能、切换成本和调度字段均为合成输入。它拒绝探索控制请求，也不提供回退预留，不能用于验收探索执行。
- 正收益和 shadow/off 场景会真实替换 worker。checkpoint、资源调度和恢复必须由已集成垂直弹性的训练镜像完成。
- 模拟吞吐与真实训练速度无关。因此 `positive`、基于 mock 得到的 `ACCEPTED` 都只能证明控制行为，不能证明真实加速、降本或训练质量。

建议先完成普通门禁矩阵，再执行第 8 节探索扩展。仅验证门禁不需要对接真实 SwanLab，也不需要探索预留服务。

### 1.1 哪些是真实状态，哪些是测试输入

| 内容 | 本文来源 | 操作者如何使用 |
| --- | --- | --- |
| 初始/目标 GPU、Pod UID、worker 进程、checkpoint、恢复 | 真实 Kubernetes 和训练框架 | 必须查看实际状态，mock 不会代替这些动作 |
| generation、代次开始时间、恢复是否确认 | 正在运行的 Master | 由 DLRover 产生；不能往 mock JSON 填一个 generation 来绕过校验 |
| `work-start/work-end/total-steps` | 手工设计的模拟累计 optimizer step | 普通门禁用固定算例；不是 GPU 数、micro step，也不是让训练从此步数启动 |
| `window-seconds`、createTime | 生成器构造的模拟采样时间 | 生成器用 Master 时钟一次性构造过去的窗口，不会采样或等待60秒 |
| 候选吞吐 `q1` | 普通场景的 HTTP fixture 配置 | 与 mock 基线 q0 分开设定；2卡不自动意味着 q1=2 |
| 探索后的候选吞吐 | 切换后提供的两轮有效 mock 窗口 | 由窗口计算，须满足新代次、恢复、checkpoint 和连续窗口约束 |

**普通门禁和探索的数据准备不能混用。** G01–G23 用 `100→160/60秒` 得到 q0=1；E01–E10 需要从探索基线起设计整条累计进度轨迹，并保证观测末端 work 超过真实 checkpoint step。本文第8节给出单独的数值模板。

### 1.2 一次普通测试的执行顺序

```text
准备镜像/卷/Brain → 部署 fixture 和配置 → manual 保持 not_adjust
→ 创建1卡 worker → 确认真实 optimizer step 持续增长
→ 从此刻再留足预热+完整窗口的时间
→ 生成一次 mock 基线 → 立即提交 manual 触发
→ 看 window_selected / gate_decision
→ 拒绝：核对未执行checkpoint；放行：继续核对2卡恢复训练
→ 归档 → 复位idle → 结束本用例 → 新建下一用例
```

`kubectl apply -f 02-job.yaml` 创建的是初始1卡训练；写 mock 文件不触发扩容；真正触发调整的是把手动 ConfigMap 从 `not_adjust` 改成 `manual`。`03-trigger.yaml` 中目标2卡不会在创建作业时自动执行。

### 1.3 时间配置分析与本文采用的手动测试档位

原来的60秒有效期要求生成文件后很快触发，600秒探索还要包含真实checkpoint、Pod重建、恢复与人工看日志，300秒回退也容易被恢复耗时耗尽。另一个问题是默认1800秒冷却让同一作业的后续观察等待较长。因此本文采用以下**隔离mock测试配置**，代码默认值不变：

| 项目 | 原示例 | 本文设置 | 手动测试效果 |
| --- | --- | --- | --- |
| 数据/收益快照有效期 | 60秒 | 600秒（10分钟） | 有时间查看文件和日志；建议生成后5分钟内触发 |
| HTTP请求超时 | 3秒 | 10秒 | 容忍集群短暂抖动；失败仍会较快返回 |
| mock稳态预热 | 60秒 | 30秒 | 保留预热边界，减少初次等待；真实稳态仍需框架证据 |
| 模拟采样窗口 | 60秒 | 保留60秒 | 继续使用60 step/60秒算例，不把人工操作时间误当成吞吐窗口 |
| 初始训练确认后的保守等待 | 125秒 | 95秒 | 30秒预热+60秒窗口+5秒余量 |
| 探索HTTP轮询 | 10秒 | 保留10秒 | 新文件通常下轮就读到；不是10秒内必须手工更新文件 |
| 第二候选窗口的提交操作区间 | 30秒 | 5分钟 | 更方便先确认journal再提交，仍受探索到期时间限制 |
| 正常探索总预算 | 600秒 | 1800秒（30分钟） | 包含真实切换和人工操作；满足条件会提前结束，无需等满 |
| 回退预算（服务端字段） | 300秒 | 900秒（15分钟） | 给真实恢复及两轮验证留出时间 |
| 探索终态冷却 | 1800秒 | 60秒 | 减少后续普通调整等待；不允许同UID重复探索，不解除失败保护 |

只延长总预算仍不够：探索基线剩余工作同步改为7200 step、允许延后3600秒、预留TTL至少5400秒、卡时预算5 GPU·h，具体约束见8.3。普通G系列仍用原3600 step算例及1800秒允许延后，不改变其收益期望。

正常测试留足操作时间；故意测过期、HTTP超时或探索到期时，使用第7节和8.6的**专用短预算用例**，不用为了看失败等10或30分钟。上述参数只针对本手册mock控制流程，不能直接当成生产数据时效/稳态标准。

## 2. 集群与镜像前提

1. 已安装 `elastic.iml.github.io/v1alpha1` ElasticJob CRD/controller，且 controller 管理测试命名空间。
2. 已部署当前版本 Brain，支持手动资源优化器，能读取测试命名空间的 `brain-manual-scale-config` 并访问节点资源信息。本文不重装 controller/Brain。
3. Master 镜像包含当前仓库代码及依赖；worker 镜像包含匹配版本 Agent、`dlrover-run` 和已接入垂直 checkpoint/恢复协议的 Swift。普通上游 Swift 镜像不保证支持该协议。
4. 至少有节点能承载单 worker 2 张同类 NVIDIA GPU。示例初始 1 卡，目标 2 卡，CPU=12、内存=48Gi 保持不变；缩容对照场景为 2→1 卡。
5. 已有可用的 RWX 存储类，卷上准备好 `/model/Qwen2.5-3B-Instruct`、`/dataset/train.jsonl`。可以替换为已跑通任务的模型、数据及完整启动命令。首次填充空 PVC 后才能启动作业。
6. `elastic-training` 中只运行一个开启手动 ConfigMap 监听的作业。若该命名空间已有其他作业，先确认它们没有监听这个手动 ConfigMap；不能因为作业名不同就认为互不影响。当前 Master 回调不按 `job_names` 隔离事件，不要依赖该字段防止其他作业被触发。
7. Master、节点及操作机时钟同步。以下命令在仓库根目录执行，客户端安装 `kubectl`、Python 3、curl、rg（日志筛选）；静态校验和探索YAML生成另外需要PyYAML。

部署前先准备替换值，不能原样提交 `REPLACE_...`：

| 替换项 | 从哪里取得 | 检查条件 |
| --- | --- | --- |
| MASTER_IMAGE | 本次代码构建的Master镜像地址/digest | 容器可import当前benefit/swanlab模块，具备mock开关 |
| TRAINING_IMAGE | 已跑通垂直弹性的训练镜像 | 有dlrover-run、Swift、checkpoint回报和恢复支持；与本机架构/GPU匹配 |
| PYTHON_3_IMAGE | 集群可拉取的Python3基础镜像 | 提供python命令；fixture只用标准库，不需要DLRover依赖 |
| RWX_STORAGE_CLASS / claimName | 本集群存储配置或已有PVC | 和任务同在elastic-training，访问模式满足跨节点训练/checkpoint |
| BRAIN_ADDRESS / PROTOCOL | 已部署Brain的Service和协议 | Master能访问；Brain能读取elastic-training中的手动ConfigMap |
| 模型/数据路径 | 已填充卷的内容 | 容器挂载点是/training，卷内/model对应/training/model；示例PVC不会自动填数据 |

预检查：

```bash
kubectl get crd elasticjobs.elastic.iml.github.io
kubectl get nodes -o wide
kubectl get storageclass
kubectl get deployment,svc -n dlrover
```

先用本环境已有任务确认普通垂直调整可完成 checkpoint 和恢复。否则“收益已放行但执行失败”应归入执行链路问题，不能判为收益公式错误。

## 3. 完整 YAML

将本节各代码块分别保存为指定文件。先替换所有 `REPLACE_...`，再执行。RBAC 只授予测试命名空间内 Master 所需的常用资源操作；集群 controller/Brain 的权限沿用已验证部署。

也可从仓库根目录直接提取本文的 11 个 YAML 代码块，输出到本地测试目录后再修改环境值：

```bash
python - <<'PY'
import re
from pathlib import Path
doc = Path('docs/design/vertical-elastic-benefit-k8s-mock-test-cn.md').read_text()
blocks = re.findall(r'```yaml\n(.*?)\n```', doc, re.S)
names = ['00-namespace.yaml', '00-rbac.yaml', '00-storage.yaml',
         '00-benefit-env.yaml', '00-fixture-scenario.yaml',
         '00-fixture-code.yaml', '00-fixture-service.yaml',
         '01-manual-idle.yaml', '02-job.yaml', '03-trigger.yaml',
         '04-exploration-env.yaml']
assert len(blocks) == len(names)
out = Path('/tmp/benefit-k8s-manifests')
out.mkdir(parents=True, exist_ok=True)
for name, block in zip(names, blocks):
    (out / name).write_text(block + '\n')
print(out)
PY
```

提取后在操作终端 A 设置下列变量；`REPO_DIR` 必须指向本仓库根目录。后续使用绝对路径，不需要在仓库目录和 YAML 目录之间来回切换：

```bash
export REPO_DIR="$PWD"
export MANIFEST_DIR=/tmp/benefit-k8s-manifests
export NS=elastic-training
export CASE_ID=G01
export CASE_DIR="/tmp/benefit-k8s-test/$CASE_ID"
mkdir -p "$CASE_DIR"
```

重新打开操作终端时重新设置上述变量，以及后文的 MASTER/WORKER。终端 B 用于持续日志或 port-forward（命令会占住终端，Ctrl+C 只停止日志跟踪/转发）；不要误以为命令没返回就是任务卡住。文件提取脚本会覆盖本地同名 YAML，只在最初执行，避免覆盖已修改的镜像和场景配置。提交前可用 `kubectl apply --dry-run=server -f 文件路径` 校验本集群 CRD/RBAC/准入规则，但 dry-run 不验证 checkpoint、GPU 可调度性和训练恢复。

### 3.1 基础设施：按功能分别部署

基础设施拆分为以下七个文件，按下列顺序部署。RBAC 与服务工作负载中属于同一功能的对象保留在各自文件内。

#### 3.1.1 `00-namespace.yaml`：测试命名空间

命名空间不存在时创建；已有 `elastic-training` 时直接复用。后续所有测试资源都部署在其中，不删除该命名空间中的其他资源。

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: elastic-training
```

#### 3.1.2 `00-rbac.yaml`：Master 身份与权限

包含 ServiceAccount、Role 和 RoleBinding，仅授予测试命名空间内的权限；controller/Brain 沿用现有部署。

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: benefit-master
  namespace: elastic-training
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: benefit-master
  namespace: elastic-training
rules:
  - apiGroups: [""]
    resources: [pods, services, configmaps, events]
    verbs: [get, list, watch, create, update, patch, delete]
  - apiGroups: ["elastic.iml.github.io"]
    resources: [elasticjobs, elasticjobs/status, elasticjobs/finalizers]
    verbs: [get, list, watch, update, patch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: benefit-master
  namespace: elastic-training
subjects:
  - kind: ServiceAccount
    name: benefit-master
    namespace: elastic-training
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: benefit-master
```

#### 3.1.3 `00-storage.yaml`：训练数据与 checkpoint 持久卷

创建模型、数据、checkpoint 及可选探索状态共用的 PVC。已有合适的数据卷时可跳过此文件，并同步修改任务中的 claimName。

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: benefit-data
  namespace: elastic-training
spec:
  accessModes: [ReadWriteMany]
  storageClassName: REPLACE_RWX_STORAGE_CLASS
  resources:
    requests:
      storage: 100Gi
```

#### 3.1.4 `00-benefit-env.yaml`：Master 收益与 SwanLab mock 参数

供 Master 的 envFrom 引用。修改参数后需创建新 Master 进程才生效。

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: benefit-test-env
  namespace: elastic-training
data:
  DLROVER_BENEFIT_MODE: "enforce"
  DLROVER_BENEFIT_OBJECTIVE: "time"
  DLROVER_BENEFIT_HTTP_URL: "http://benefit-fixture:8080/benefit/snapshot"
  DLROVER_BENEFIT_TIMEOUT_SECONDS: "10"
  DLROVER_BENEFIT_MAX_AGE_SECONDS: "600"
  DLROVER_BENEFIT_MIN_WINDOW_SECONDS: "30"
  DLROVER_BENEFIT_MIN_GAIN_RATIO: "0.05"
  DLROVER_BENEFIT_EXPLORE: "false"
  DLROVER_SWANLAB_MOCK: "true"
  DLROVER_SWANLAB_MOCK_FILE: "/mock/swanlab.json"
  DLROVER_SWANLAB_ASSUME_FIXED_BATCH: "true"
  DLROVER_SWANLAB_KEYS: '{"work":"global_step","target_work":"max_steps","train_speed":"train_speed"}'
  DLROVER_SWANLAB_TIME_SOURCE: "sample"
  DLROVER_SWANLAB_UTC_OFFSET: "+08:00"
  DLROVER_SWANLAB_WARMUP_SECONDS: "30"
  DLROVER_SWANLAB_STEADY_WINDOWS: "2"
  DLROVER_SWANLAB_MAX_THROUGHPUT_VARIATION: "0.1"
```

#### 3.1.5 `00-fixture-scenario.yaml`：收益场景输入

定义 HTTP 测试服务返回的候选吞吐、成本和约束。切换用例时只更新此 ConfigMap，并通过 /scenario 确认加载结果。

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: benefit-fixture-scenario
  namespace: elastic-training
data:
  scenario.json: |
    {"name":"time-positive","q1":2,"delay":120,"replay":0,"overhead":240,"max_delay":1800}
```

#### 3.1.6 `00-fixture-code.yaml`：收益 HTTP 测试服务代码

保存测试服务的 Python 实现。修改代码后需重启 benefit-fixture Deployment；普通场景切换无需修改此文件。

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: benefit-fixture-code
  namespace: elastic-training
data:
  server.py: |
    import json
    import time
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from pathlib import Path

    def scenario():
        return json.loads(Path("/scenario/scenario.json").read_text())

    class Handler(BaseHTTPRequestHandler):
        def send_json(self, status, data):
            body = json.dumps(data, allow_nan=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_GET(self):
            if self.path == "/healthz":
                self.send_json(200, {"ok": True})
            elif self.path == "/scenario":
                self.send_json(200, scenario())
            else:
                self.send_json(404, {"error": "not_found"})

        def do_POST(self):
            if self.path != "/benefit/snapshot":
                self.send_json(404, {"error": "not_found"})
                return
            req = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if req.get("exploration"):
                self.send_json(409, {"error": "real_execution_adapter_required"})
                return
            cfg = scenario()
            print(json.dumps({"scenario": cfg["name"],
                              "request_id": req["request_id"],
                              "plan_id": req["plan_id"]}), flush=True)
            time.sleep(cfg.get("sleep_seconds", 0))
            if cfg.get("http_status", 200) != 200:
                self.send_json(cfg["http_status"], {"error": "injected"})
                return
            now = time.time()
            data = {k: req[k] for k in ("request_id", "job", "current", "target")}
            data.update(
                schema_version=1,
                observed_at=now - cfg.get("snapshot_age", 0),
                training={"comparable": cfg.get("comparable", True)},
                candidate={"throughput_lower": cfg.get("q1", 2),
                           "evidence": "profile", "evidence_id": "mock-" + cfg["name"]},
                transition={"lost_seconds_upper": cfg.get("delay", 120),
                            "replay_work_upper": cfg.get("replay", 0),
                            "gpu_seconds_upper": cfg.get("overhead", 240)},
                scheduling={"target_feasible": cfg.get("feasible", True),
                            "constraints_satisfied": cfg.get("constraints", True)},
                limits={"max_delay_seconds": cfg.get("max_delay", 1800),
                        "deadline_at": None},
            )
            if "deadline_in" in cfg:
                data["limits"]["deadline_at"] = now + cfg["deadline_in"]
            if cfg.get("candidate") == "unavailable":
                data["candidate"] = {"status": "unavailable"}
            elif cfg.get("candidate") == "missing":
                del data["candidate"]
            if "impacts_complete" in cfg:
                data["scheduling"].update(
                    impacts_complete=cfg["impacts_complete"], job_weight=1,
                    other_jobs=cfg.get("other_jobs", []))
            if cfg.get("identity_mismatch"):
                data["request_id"] = "wrong-request"
            self.send_json(200, data)

    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
```

#### 3.1.7 `00-fixture-service.yaml`：收益 HTTP 服务部署与访问入口

包含 Deployment 和 Service。服务每次请求读取投影卷中的 scenario.json，场景更新无需重启，但要等待 ConfigMap 投影传播；不要将配置卷改为 subPath 挂载。

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: benefit-fixture
  namespace: elastic-training
spec:
  replicas: 1
  selector:
    matchLabels:
      app: benefit-fixture
  template:
    metadata:
      labels:
        app: benefit-fixture
    spec:
      automountServiceAccountToken: false
      containers:
        - name: server
          image: REPLACE_PYTHON_3_IMAGE
          command: [python, -u, /code/server.py]
          ports:
            - containerPort: 8080
          readinessProbe:
            httpGet:
              path: /healthz
              port: 8080
          resources:
            requests: {cpu: "100m", memory: "64Mi"}
            limits: {cpu: "1", memory: "256Mi"}
          volumeMounts:
            - {name: code, mountPath: /code, readOnly: true}
            - {name: scenario, mountPath: /scenario, readOnly: true}
      volumes:
        - name: code
          configMap:
            name: benefit-fixture-code
        - name: scenario
          configMap:
            name: benefit-fixture-scenario
---
apiVersion: v1
kind: Service
metadata:
  name: benefit-fixture
  namespace: elastic-training
spec:
  selector:
    app: benefit-fixture
  ports:
    - {name: http, port: 8080, targetPort: 8080}
```

### 3.2 `01-manual-idle.yaml`：初始禁止触发

先创建此对象，再启动任务，避免监听初始化时收到遗留的 `manual` 事件。

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: brain-manual-scale-config
  namespace: elastic-training
data:
  config.json: |
    {"scale_action":"not_adjust","customize_worker_resource":{"cpu":12,"memory":48,"gpus":2,"gpu_type":"nvidia.com/gpu"},"job_names":"benefit-mock"}
```

### 3.3 `02-job.yaml`：完整 ElasticJob

示例采用仓库现有 Swift 垂直弹性任务的启动形式，使用预先构建的镜像，不在启动时安装依赖。`REPLACE_TRAINING_IMAGE` 必须是本环境已通过 checkpoint/恢复测试的集成镜像；如果本环境不是 Swift，替换整个 worker 启动命令为已验证入口。

实际训练 `max_steps=100000` 用于保持作业运行，mock 的 `max_steps=3760` 是独立的收益测试输入。两者不同是有意的，不能据此做真实收益验收。跨档位 global batch 一致性需由框架调整梯度累积保证；示例初始 global batch 为 `1×2×32=64`，扩到 2 卡后应使用梯度累积 16，保持 `2×2×16=64`。固定 batch 开关只是测试假设，不会自动修改训练参数；需核对框架实际调整结果。

```yaml
apiVersion: elastic.iml.github.io/v1alpha1
kind: ElasticJob
metadata:
  name: benefit-mock
  namespace: elastic-training
  labels:
    low_code_platform_task_id: "123"
spec:
  distributionStrategy: AllreduceStrategy
  optimizeMode: cluster
  replicaSpecs:
    dlrover-master:
      template:
        spec:
          restartPolicy: Never
          serviceAccountName: benefit-master
          containers:
            - name: main
              image: REPLACE_MASTER_IMAGE
              imagePullPolicy: IfNotPresent
              command: [python, -m, dlrover.python.master.main]
              args:
                - --platform
                - pyk8s
                - --namespace
                - elastic-training
                - --job_name
                - benefit-mock
                - --port
                - "50001"
                - --metrics_port
                - "9090"
                - --xpu_type
                - nvidia
                - --elastic_dimension
                - vertical
                - --resource_monitor_switch
                - "off"
                - --configmap_manual_scale_switch
                - "on"
              envFrom:
                - configMapRef:
                    name: benefit-test-env
              env:
                - name: POD_IP
                  valueFrom:
                    fieldRef:
                      fieldPath: status.podIP
                - {name: DLROVER_LOG_LEVEL, value: DEBUG}
                - {name: DLROVER_BRAIN_SERVICE_ADDR, value: REPLACE_BRAIN_ADDRESS}
                - {name: DLROVER_BRAIN_PROTOCOL, value: http}
              ports:
                - {name: grpc, containerPort: 50001}
                - {name: metrics, containerPort: 9090}
              resources:
                requests: {cpu: "1", memory: 2Gi}
                limits: {cpu: "2", memory: 4Gi}
              volumeMounts:
                - {name: mock, mountPath: /mock}
                - {name: training, mountPath: /training}
          volumes:
            - name: mock
              emptyDir: {}
            - name: training
              persistentVolumeClaim:
                claimName: benefit-data
    worker:
      replicas: 1
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: main
              image: REPLACE_TRAINING_IMAGE
              imagePullPolicy: IfNotPresent
              command: [/bin/bash, -lc]
              args:
                - |
                  exec dlrover-run --nnodes=1 --nproc_per_node=1 --max_restarts=1 \
                    swift sft --model /training/model/Qwen2.5-3B-Instruct \
                    --dataset /training/dataset/train.jsonl \
                    --output_dir /training/output/benefit-mock \
                    --model_type qwen2 --torch_dtype bfloat16 --train_type lora \
                    --max_steps 100000 --per_device_train_batch_size 2 \
                    --gradient_accumulation_steps 32 --learning_rate 1e-4 \
                    --max_length 1024 --logging_steps 1 --save_steps 50 \
                    --save_total_limit 10 --lora_rank 8 --lora_alpha 32 \
                    --target_modules all-linear --use_flash_ckpt true
              resources:
                requests: {cpu: "12", memory: 48Gi, nvidia.com/gpu: 1}
                limits: {cpu: "12", memory: 48Gi, nvidia.com/gpu: 1}
              volumeMounts:
                - {name: training, mountPath: /training}
                - {name: dshm, mountPath: /dev/shm}
          volumes:
            - name: training
              persistentVolumeClaim:
                claimName: benefit-data
            - name: dshm
              emptyDir:
                medium: Memory
                sizeLimit: 4Gi
```

`REPLACE_BRAIN_ADDRESS` 与协议配套，例如已有 HTTP Brain 的完整 `http://服务:端口`。如果本集群使用 gRPC，把协议改成 `grpc` 并填 `服务:端口`，不能只改地址不改协议。

### 3.4 `03-trigger.yaml`：执行 1→2 卡

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: brain-manual-scale-config
  namespace: elastic-training
data:
  config.json: |
    {"scale_action":"manual","customize_worker_resource":{"cpu":12,"memory":48,"gpus":2,"gpu_type":"nvidia.com/gpu"},"job_names":"benefit-mock"}
```

`memory` 单位是 GiB，`gpus` 是每 worker 的卡数。2→1 场景需把 `02-job.yaml` 初始 requests/limits GPU 与 `--nproc_per_node` 改为 2，梯度累积改为 16；把 idle 和 trigger 中目标 `gpus` 改为 1。

## 4. 首个场景的逐步操作

### 4.1 部署并记录环境

```bash
mkdir -p "$CASE_DIR"
git -C "$REPO_DIR" rev-parse HEAD > "$CASE_DIR/commit.txt"
kubectl apply -f "$MANIFEST_DIR/00-namespace.yaml"
kubectl apply -f "$MANIFEST_DIR/00-rbac.yaml"
kubectl apply -f "$MANIFEST_DIR/00-storage.yaml"
kubectl apply -f "$MANIFEST_DIR/00-benefit-env.yaml"
kubectl apply -f "$MANIFEST_DIR/00-fixture-scenario.yaml"
kubectl apply -f "$MANIFEST_DIR/00-fixture-code.yaml"
kubectl apply -f "$MANIFEST_DIR/00-fixture-service.yaml"
kubectl rollout status -n "$NS" deployment/benefit-fixture --timeout=300s
kubectl get pvc -n elastic-training
kubectl apply -f "$MANIFEST_DIR/01-manual-idle.yaml"
```

确认 PVC Bound 且模型/数据已填充，再创建作业。新建 PVC 不会自动下载模型和数据；本环境已有数据卷时可将所有 `claimName` 改为该卷，并跳过 `00-storage.yaml`。

```bash
kubectl apply -f "$MANIFEST_DIR/02-job.yaml"
kubectl get pods -n elastic-training -o wide
```

根据 `kubectl get pods` 输出识别本作业的 Master 和初始 worker；可先用下面的只读命令查看标签。不要把 `benefit-fixture` Pod 当成 Master，也不要选择同命名空间其他作业的 worker。

```bash
kubectl get pods -n "$NS" --show-labels
kubectl get elasticjob benefit-mock -n "$NS" -o jsonpath='{.metadata.uid}{"\n"}'
```

填写实际 Pod 名，后续在终端 A 使用这些变量；worker 被替换后需重新选新 Pod，旧 `$WORKER` 不会自动更新：

```bash
MASTER=REPLACE_MASTER_POD_NAME
WORKER=REPLACE_INITIAL_WORKER_POD_NAME
kubectl logs -n "$NS" "$MASTER" -c main --timestamps
```

确认日志 `training_metrics_configured` 中 `task_id=123`、`task_id_source=job_label`、`data_source=mock`、`valid=true`。不设置 `LOW_CODE_PLATFORM_TASK_ID` 环境变量，客户端身份唯一来自 ElasticJob 顶层 label。

在生成有时效的mock之前，先确认运行中Master的**实际环境**及HTTP服务场景；只看ConfigMap内容不能证明进程已加载。下面只读取白名单配置，不输出token：

```bash
kubectl exec -n "$NS" "$MASTER" -c main -- python -c \
  'import os,json; keys=["DLROVER_BENEFIT_MODE","DLROVER_BENEFIT_OBJECTIVE","DLROVER_BENEFIT_EXPLORE","DLROVER_BENEFIT_HTTP_URL","DLROVER_SWANLAB_MOCK","DLROVER_SWANLAB_MOCK_FILE","DLROVER_SWANLAB_WARMUP_SECONDS","DLROVER_BENEFIT_MAX_AGE_SECONDS","DLROVER_BENEFIT_TIMEOUT_SECONDS"]; print(json.dumps({k:os.getenv(k) for k in keys},indent=2))'
kubectl exec -n "$NS" "$MASTER" -c main -- python -c \
  'from urllib.request import urlopen; print(urlopen("http://benefit-fixture:8080/scenario",timeout=3).read().decode())'
```

G01应看到 enforce/time、explore=false、mock=true、timeout=10、max_age=600、warmup=30、文件路径/mock/swanlab.json，且HTTP返回name=time-positive、q1=2、overhead=240。这里的GET只检查连通性/配置，不触发收益评估或资源变更。请求失败先排查DNS、Service endpoints或网络策略，不要先生成会过期的mock。


### 4.2 等待基线时间边界并生成 mock 文件

本节针对 G01 的正常首次运行，初始 worker 为1卡，没有经历过一次垂直切换。S01/S06 等故意制造数据缺失或预热不足的异常用例，按第7节改变执行顺序。

#### 4.2.1 先确认真实 worker 正在使用初始配置训练

**是的，先等初始1卡 worker 正常输出并推进训练 step，再执行后面的等待和数据生成。** Pod Running 可能仍在安装依赖、加载模型或等待 rendezvous，不是这里的继续条件。

```bash
kubectl get pod -n "$NS" "$WORKER" -o jsonpath='{.metadata.uid}{"\n"}{.status.phase}{"\n"}{.spec.containers[?(@.name=="main")].resources}{"\n"}'
kubectl logs -n "$NS" "$WORKER" -c main --tail=200 --timestamps
```

应确认：

1. 本次新作业只有一个初始 worker，`main` 容器的 GPU requests/limits 都是1，未发生意外替换或反复重启。
2. worker 已完成 rendezvous 并开始真正的 optimizer step。观察至少两个不同时间的训练日志，**累计 optimizer/global step 持续增长**，例如12→13→14。不是仅看到 `local_rank=0`、模型加载完成或一次性打印训练总步数。
3. 当前没有 checkpoint 等待、恢复中、OOM 或 rendezvous 重建。训练日志字段由集成框架决定，不能保证所有镜像都叫 `global_step`；以实际 trainer 的累计更新步数为准。

若无法看到有效 step，先解决训练启动问题，不要靠生成100→160的 mock 假装真实训练已经就绪。普通门禁的真实 step 可以是14、200或其他值，**不要求真实训练恰好跑到100或160**。

#### 4.2.2 时间边界如何等待

生成器立即写出三个模拟采样点，不会先睡60秒收集数据。设执行时刻为 T，`--window-seconds 60` 会生成 `[T−60, T−30, T]`。代码要求窗口起点满足：

```text
普通基线下界 L = max(当前generation开始时间, 最近恢复确认时间) + warmup
窗口起点 T−60 >= L
末端年龄 0 <= 评估时刻−T <= max_age
```

本文手动测试设置 warmup=30、max_age=600，所以首次训练最早在 generation 开始约90秒后才能容纳这段60秒窗口。**90秒是预热30秒加窗口60秒，不是要等到第90个训练step；max_age=600是生成后的有效期，不计入生成前的等待。** 如果训练慢，步数少也不影响这条时间条件。

当前文档不假设有一个可用 HTTP 接口直接返回 Master 内存中的 started_at/recovered_at。为了避免猜时间，首次普通测试采用保守操作：**在4.2.1已经确认真实step持续增长后，用Master时钟记下一个时刻，再等待95秒。** 该时刻晚于本轮初始 rendezvous；额外5秒用于留出边界余量。期间若 worker 重启、重新 rendezvous 或发生资源调整，重新确认训练推进并重新记时。

```bash
BASELINE_READY_AT=$(kubectl exec -n "$NS" "$MASTER" -c main -- \
  python -c 'import time; print(time.time())')
printf '%s\n' "$BASELINE_READY_AT" > "$CASE_DIR/baseline-ready-at.txt"
```

该命令只是记时，不会停止训练，也不会立刻开始95秒的阻塞等待。在等待期间可以核对 fixture 场景、保存 worker 日志和配置。下面命令可随时检查剩余等待秒数：

```bash
kubectl exec -n "$NS" "$MASTER" -c main -- python -c \
  'import math,sys,time; print("remaining_wait_seconds=", max(0, math.ceil(float(sys.argv[1])+95-time.time())))' \
  "$BASELINE_READY_AT"
```

只有输出0且 worker 仍持续训练，才执行4.2.4。不要每次检查都重新赋值 BASELINE_READY_AT，否则会不断重新计时。此保守方法用于**首次基线**；探索切换后的恢复必须另看 `training_recovered` 和 `observation_after`，见8.4。

例：14:00:00建立generation，14:00:20已看到真实step增长并记时，则14:01:55以后生成。生成的窗口为14:00:55–14:01:55，完整位于14:00:30的预热下界之后。14:03:30评估时末端仅旧95秒，仍有效；超过14:11:55才评估则过期。

#### 4.2.3 `100`、`160`、`3760` 是怎么选的

它们是为**可手算、可重复的门禁测试**设计的模拟累计计数，不是随机数，也不从 worker 日志自动读取：

| 参数 | 示例 | 为什么这样设置 / 改动影响 |
| --- | --- | --- |
| `--work-start` | 100 | 模拟窗口起点的累计optimizer step；100本身无业务含义，提供一个非零起点 |
| `--work-end` | 160 | 与起点差60，配合60秒窗口得到 `q0=(160−100)/60=1 step/s` |
| `--window-seconds` | 60 | 三个点相隔30秒，两个子窗口均长于本文15秒下限，整段也满足30秒最低窗口 |
| `--total-steps` | 3760 | 模拟训练目标；`R=3760−160=3600 step`，按q0=1还需3600秒，便于手算 |
| `--task-id` | 123 | 必须等于 ElasticJob 顶层 label，不是任意字符串 |
| `--assume-fixed-batch` | 开启 | 输出global_step/max_steps/train_speed三个key，匹配本文客户端模式 |
| `--output` | /mock/swanlab.json | **Master容器内**路径，与 MOCK_FILE 相同，不是操作机路径 |
| `--now` | 本节不传 | 默认读取Master当前时间；本节不要填写固定历史时间或未来时间 |

默认会生成work=[100,130,160]，target_work=[3760,3760,3760]，train_speed=[1,1,1]。收益公式使用 `Δwork/Δtime`，不是直接用 train_speed。

这是三个采样时刻、每个时刻三个指标，所以日志中的聚合sample_count可能为9，而不是3；不要把它理解为9个独立采样时刻。

可以换成200→260，但要保持本手册手算结果，还须把 total_steps 改为3860；只改start/end而不改total，会改变剩余工作量和收益。换成100→220则q0=2，本矩阵的期望不能继续照抄。生成器在中点取整数，建议step差为偶数；例如差1会出现一个子窗口零进度，被稳态校验拒绝。有效计数还需满足 `0<=start<end<total<2**53`（生成器允许end=total，但此时没有剩余工作，不能测试正常正收益）。

mock中的100→160**不会**让真实worker从100恢复到160，也不向checkpoint写入该步数。真实训练的 `--max_steps 100000` 与mock `--total-steps 3760` 属于两条独立链路。这个独立性仅适用于普通门禁算例；探索窗口还有真实checkpoint步数下限和跨窗口一致性要求。

#### 4.2.4 在时间条件满足后生成一次基线

下面代码先检查95秒是否到期；未到期则打印剩余秒数并跳过生成。到期后通过stdin把本地生成器送到Master运行，使用Master时钟原子写文件；无需镜像中有examples目录，也无需kubectl cp/tar。

```bash
if kubectl exec -n "$NS" "$MASTER" -c main -- python -c \
  'import math,sys,time; left=max(0,math.ceil(float(sys.argv[1])+95-time.time())); print("remaining_wait_seconds=",left); sys.exit(1 if left else 0)' \
  "$BASELINE_READY_AT"
then
  kubectl exec -i -n "$NS" "$MASTER" -c main -- python - \
  --task-id 123 --assume-fixed-batch \
  --window-seconds 60 --work-start 100 --work-end 160 --total-steps 3760 \
  --output /mock/swanlab.json \
  < "$REPO_DIR/examples/pytorch/resource_elastic/benefit_validation/generate_swanlab_mock.py"
fi
```

必须看到 `Mock scalar response written to /mock/swanlab.json`，否则不要进入触发步骤。它只表示文件写入成功，**不表示运行中的Master已接受窗口或收益已放行**。`window_selected` 在真正触发收益评估后才出现，不能在此处等它出现。

生成成功后保存输入，再执行4.3；建议5分钟内触发，留出其余5分钟给人工核查、事件传播及请求。无需抢在10秒内操作，最终判定以评估时刻的末端年龄≤600秒为准。数据越早使用越好；10分钟是容忍上限，不是要求等待10分钟。

```bash
kubectl exec -n "$NS" "$MASTER" -c main -- cat /mock/swanlab.json \
  > "$CASE_DIR/swanlab.json"
```

如果因操作中断超过600秒，确认仍是同一初始代次后重新运行生成命令，再触发。重新生成是为新的**独立普通评估**重建同一合成算例，不是把旧的真实数据刷新成新观测。探索不能循环写100→160来冒充持续进度，也不能在等待第一轮窗口被接受前覆盖文件。

### 4.3 触发并收集证据

执行本节前应已完成：worker仍为1卡并持续训练、fixture加载正确、mock刚生成、手动ConfigMap仍为not_adjust。先保存基线Pod UID与资源，再只触发一次：

```bash
kubectl get pods -n "$NS" -o json > "$CASE_DIR/pods-before.json"
kubectl apply -f "$MANIFEST_DIR/03-trigger.yaml"
kubectl logs -n "$NS" "$MASTER" -c main -f --timestamps
```

终端持续跟踪日志，另开终端观察 Pod。完成后导出：

```bash
kubectl logs -n "$NS" "$MASTER" -c main --timestamps > "$CASE_DIR/master.log"
kubectl logs -n "$NS" deployment/benefit-fixture --timestamps > "$CASE_DIR/fixture.log"
kubectl get pods -n "$NS" -o json > "$CASE_DIR/pods-after.json"
kubectl get events -n "$NS" --sort-by=.metadata.creationTimestamp > "$CASE_DIR/events.txt"
kubectl get elasticjob benefit-mock -n "$NS" -o yaml > "$CASE_DIR/job.yaml"
rg 'BenefitFlow|VerticalElasticEvent|benefit_' "$CASE_DIR/master.log"
```

可在另一个终端执行 `kubectl get pods -n elastic-training -w`。对被删除的旧 worker 提前保存日志，或使用集群日志平台。只看最终 Pod Running 不足以证明 checkpoint 已恢复。

预期：`positive/gain_above_threshold`，`gate_decision.decision=allow`，随后出现 checkpoint、替换和恢复过程；目标 worker 为 2 卡，进度在恢复后继续。普通门禁放行不会进入探索 `OBSERVING`，也不会自动产生 `ACCEPTED`。

按以下阶段验收，不能把某个中间阶段当成整个用例通过：

| 阶段 | 必须看到的证据 | 缺失时下一步 |
| --- | --- | --- |
| 触发已被处理 | 本次新plan_id，BenefitFlow evaluation_started | 查ConfigMap事件、Brain是否产生目标计划、same_resource/busy/冷却；不要反复apply同一trigger |
| mock窗口已被采用 | window_selected，data_source=mock，work_start=100、work_end=160、throughput=1、target_work=3760 | 查fetch_failed；若是过期/边界错误，本次不能记为收益算法失败 |
| 门禁结果正确 | evaluation_finished为positive/gain_above_threshold，gate_decision=allow | 比对实际objective、q1和成本；普通成功场景必须出现而非仅HTTP 200 |
| 真实调整已执行 | checkpoint已保存，worker替换，实际main资源GPU变成2 | 检查checkpoint协议、调度和替换执行；fixture的feasible只是测试输入 |
| 训练已恢复 | 新worker的累计step继续推进，Master出现training_recovered或相应恢复完成证据 | Pod Running、服务ready、控制执行completed都不能代替训练进度恢复 |

可在本次日志中按精确plan_id筛选关联记录；注意JSON日志含空格，不要假定字段紧贴冒号。拒绝场景不应该等待 `training_recovered`，其结束条件是本次gate_decision=reject且对应计划结束，并核对未发checkpoint/未换Pod。

新worker名字/UID可能改变。用 `kubectl get pods -n "$NS" --show-labels` 重新选择该作业worker，更新WORKER后再执行4.2.1的资源/日志检查。训练恢复后核对global batch仍为64；不要只从旧ElasticJob模板中的1卡配置推断实际Pod配置。

记录提交trigger的时间。若迟迟没有收益评估，先归类为“触发/优化链路未进入门禁”；超过mock有效期后即使后来进入评估也可能先报过期。不要在日志仍有进行中plan时反复改文件和触发，避免难以关联输入。检查清楚后结束该用例，按第5节用新作业重测。


### 4.4 手算核对

当前 1 卡、目标 2 卡；q0=1，R=3760−160=3600，q1=2，切换 120 秒、重放 0、切换卡秒 240：

这里的q1=2来自fixture，是为了构造时间正收益；不是因为GPU从1变2就按比例推算性能。120秒也是人为给定的切换成本上界，240卡秒按本例2卡×120秒构造，不是本次K8s实际测量值。普通场景默认保留这些成本，只改变矩阵明确列出的字段；例如q1=0.8故意模拟“扩卡反而变慢”的候选，用于检查拒绝逻辑。真实服务应使用测量/profile/调度证据替换这些合成值。

| 项目 | 计算 | 结果 |
| --- | --- | --- |
| 原配置剩余时间 T0 | R/q0 | 3600 秒 |
| 新配置剩余时间 T1 | 120+R/q1 | 1920 秒 |
| 时间收益 | T0−T1 | 1680 秒，46.67% |
| 原配置卡时 H0 | 1×T0/3600 | 1 GPU·h |
| 新配置卡时 H1 | (240+2×R/q1)/3600 | 1.066667 GPU·h |
| 卡时收益 | H0−H1 | −0.066667 GPU·h |

同一输入在 `time` 下放行，在 `gpu_hours` 下拒绝。改变 `DLROVER_BENEFIT_OBJECTIVE` 后必须创建新 Master 才生效。

### 4.5 当前任务总步数为1044时如何生成mock

本节提供**total_steps=1044的替代算例**。第4.2/4.4与第6节原始矩阵使用3760，第8节探索另用107260；它们是独立的合成测试输入，不要把其数值预期混用于本节。

真实训练的max_step/max_steps=1044，对应生成器的`--total-steps 1044`。在固定batch模式下，生成器会把它写成SwanLab指标`max_steps`，不是新增名为max_step的指标。若按本文YAML启动这个1044步任务，worker命令中的`--max_steps 100000`也应改为`--max_steps 1044`；已有实际任务则以其训练配置为准。mock文件不会改变真实训练目标，也不会让任务延迟结束。

#### 4.5.1 只验证普通收益门禁：保留100→160，修改总步数

在确认初始1卡worker持续训练、完成4.2的95秒保守等待后，使用下面命令替换4.2.4中的生成命令，task_id仍为123：

```bash
kubectl exec -i -n "$NS" "$MASTER" -c main -- python - \
  --task-id 123 --assume-fixed-batch \
  --window-seconds 60 --work-start 100 --work-end 160 --total-steps 1044 \
  --output /mock/swanlab.json \
  < "$REPO_DIR/examples/pytorch/resource_elastic/benefit_validation/generate_swanlab_mock.py"
kubectl exec -n "$NS" "$MASTER" -c main -- cat /mock/swanlab.json \
  > "$CASE_DIR/swanlab-1044.json"
```

这仍是模拟100→160的进度，不要求实际worker刚好处于这些step。窗口三点分别为100、130、160；max_steps三个点全部为1044；train_speed为1。`work-end`是窗口末端的已完成步数，**不能因为总目标1044就设work-end=1044**，否则remaining=0，不再是正常收益评估场景。

保持fixture的q1=2、delay=120、overhead=240时：

| 项目 | 1044步算例结果 |
| --- | --- |
| 当前吞吐q0 | (160−100)/60=1 step/s |
| 剩余工作量R | 1044−160=884 step |
| 原配置剩余时间T0 | 884秒 |
| 目标2卡剩余时间T1 | 120+884/2=562秒 |
| 时间收益 | 322秒，约36.43%，time目标放行 |
| 原配置卡时H0 | 884/3600≈0.245556 GPU·h |
| 目标配置卡时H1 | (240+2×442)/3600≈0.312222 GPU·h |
| 卡时收益 | −0.066667 GPU·h，gpu_hours目标拒绝 |

相应日志断言改为`target_work=1044`、`remaining_work=884`、`baseline_seconds=884`、`candidate_seconds=562`；不再断言3760、3600、1920。

#### 4.5.2 如果要让模拟进度接近当前真实进度

应取两个实际采样时刻及对应累计global_step，而不是仅凭总目标推算起止step。例如当前step约600，若实际60秒前为540，可使用`--work-start 540 --work-end 600 --total-steps 1044`；这时q0=1，R=444，预测结果需要重新计算。

只有当前step=600、不知道60秒前步数时，不能宣称540→600是真实吞吐；它只能是人为构造的q0=1场景。实际两段吞吐可能不稳定，真实接口模式应让SwanLab客户端从历史点选窗口，而不是用生成器的线性中点替代测量。

当前进度越靠近1044，可回收的切换成本越少。例如mock改成840→900，仍为q0=1，但R=144，目标耗时120+144/2=192秒，比保持1卡的144秒更慢，应拒绝。固定q0=1、q1=2、切换120秒、5%时间收益阈值时，需要剩余至少267 step，即work_end≤777；不能因为希望扩容通过就忽略真实剩余时间。

#### 4.5.3 第6节矩阵如何使用1044步版本

其他条件不变时，大多数正负/未知分类仍成立，但所有涉及剩余工作量的数字都要重算，以下三项还要修改注入值才能保留原测试目的：

| 用例 | 1044步版本修改 | 原因 |
| --- | --- | --- |
| G03：正收益但低于5% | fixture q1从1.05改为1.18 | T1≈869.153秒，收益约1.68%；原1.05在短任务中已变为负收益，无法单独覆盖“正但不足阈值” |
| G13：delay_budget | max_delay从500改为200，q1仍0.8 | 延后为341秒，超过200但没有超过500 |
| G14：deadline | deadline_in从1800改为500，q1仍2 | 预计耗时562秒，超过500；1800秒不会触发deadline拒绝 |

例如G05的2→1缩容，q1=0.75时T1≈1298.667秒，H0≈0.491111、H1≈0.427407 GPU·h，仍为卡时正收益，但不是原表30%的节省。报告中必须标记使用“1044步版本”，保存实际mock和fixture输入。

#### 4.5.4 1044步任务不能直接照抄长预算探索

用100→160、total=1044时，基线剩余884秒，小于正常探索1800秒加回退900秒的2700秒，`prepare()`会以`insufficient_remaining_time`拒绝；开启探索开关也不会放行。这是预期保护，不是mock文件格式错误。

如果该任务真实吞吐较低，1044步仍可能有足够剩余时间，应使用合理的实际进度/吞吐重新判断。例如模拟100→106/60秒得到q0=0.1，R=938，基线9380秒，可满足时间准入；但这表示另一个慢训练场景，不能据此声称真实1 step/s的任务也适合30分钟探索。

探索还要求候选和回退窗口的work超过各自真实checkpoint_step，并始终≤1044、total_steps始终为1044。第8节100000以上的偏移轨迹不能用于1044总目标；只把其total_steps换成1044会因work>target直接失败。需根据触发时真实step、checkpoint进度及剩余时间单独设计连续轨迹，并协调缩短探索/回退预算；总时间仍须覆盖真实切换、恢复和两轮窗口，不能只为准入通过随意缩小。

实际任务若会在操作期间接近或达到1044步，应优先完成普通门禁测试，或另开长任务验证探索回退。延长mock有效期不能延长真实训练生命期。

## 5. 场景切换方法

每个用例建议重新创建测试作业，初始资源恢复为该用例要求的 1 卡或 2 卡。停止旧用例前先归档证据；将手动 ConfigMap 复位为 idle，再删除旧 ElasticJob，等待其所属 Pod 退出后创建新作业。删除同名后重建会产生新 job_uid。不要把删除 Master Pod 当作“干净的新用例”；探索恢复尤其必须保留原 UID 和 journal。

```bash
kubectl apply -f "$MANIFEST_DIR/01-manual-idle.yaml"
kubectl delete elasticjob benefit-mock -n elastic-training --wait=true
kubectl get pods -n elastic-training
```

此处删除的是本测试作业；`--wait=true` 等待ElasticJob对象删除，不应据此假定所有子Pod已立即清理。用标签/ownerReferences确认本作业的旧Master和worker全部退出后再继续，不要求同命名空间的其他Pod或fixture退出。保留数据 PVC 和历史输出，每个新训练用例最好换一个 `--output_dir`，避免训练框架自动加载上次输出。

### 5.1 修改证据场景

先切换CASE_ID和CASE_DIR并创建新目录，避免覆盖上一用例证据。以下示例为G02，执行前已完成上面的旧作业归档、idle复位和删除：

```bash
export CASE_ID=G02
export CASE_DIR="/tmp/benefit-k8s-test/$CASE_ID"
mkdir -p "$CASE_DIR"
```


将矩阵中的 JSON 保存为 `scenario.json`，例如负收益：

```bash
cat > "$CASE_DIR/scenario.json" <<'JSON'
{"name":"time-negative","q1":0.8}
JSON
kubectl create configmap benefit-fixture-scenario -n elastic-training \
  --from-file=scenario.json="$CASE_DIR/scenario.json" --dry-run=client -o yaml | kubectl apply -f -
```

在终端B单独启动转发（已运行同一转发时无需重复启动）：

```bash
kubectl port-forward -n elastic-training svc/benefit-fixture 18080:8080
```

该port-forward在终端B持续运行；在终端A查询，直到与预期JSON一致后才继续创建/准备新作业。第一次返回旧配置属于投影传播尚未完成，不能立即触发：

```bash
curl --fail http://127.0.0.1:18080/scenario
```

HTTP 测试桩未指定的字段使用 3.1.6 节代码默认值。`name` 必填，仅用于日志和证据 ID。切换场景不改变当前 worker 配置，也不改变 Master 收益目标。

### 5.2 修改 Master 模式/目标

每个普通用例从基准env开始，再覆盖该用例模式和目标，避免沿用上一用例的阈值或探索设置。若本地00-benefit-env.yaml也已被修改，先核对其内容与第3.1.4节一致。

例如G04（时间有收益、卡时无收益）：先按5.1将scenario设置回 `{"name":"gpu-cost-negative","q1":2}` 并确认服务已加载；然后在创建新作业之前执行：

```bash
kubectl apply -f "$MANIFEST_DIR/00-benefit-env.yaml"
kubectl patch configmap benefit-test-env -n elastic-training --type merge \
  -p '{"data":{"DLROVER_BENEFIT_MODE":"enforce","DLROVER_BENEFIT_OBJECTIVE":"gpu_hours"}}'
kubectl apply -f "$MANIFEST_DIR/02-job.yaml"
```

更改 ConfigMap 不会热更新已运行 Master 的环境变量。模式、目标、开关、阈值都需要新进程；scalar JSON 文件则每次读取。每次切换填写所有本用例相关值，避免继承上一用例的 `shadow`、探索开关或修改过的阈值。

重新 apply `00-benefit-env.yaml` 会重置 Master 环境配置，重新 apply `00-fixture-scenario.yaml` 会重置场景输入。切换用例时只更新对应文件或 ConfigMap，避免覆盖当前设置。

### 5.3 三个最容易混淆的场景如何落地

| 用例 | 初始作业与目标 | Master配置 | fixture配置 | mock数据 | 具体期望 |
| --- | --- | --- | --- | --- | --- |
| G02 | 新1卡作业，触发2卡 | enforce/time | q1=0.8 | 仍是100→160/60秒，total=3760 | 只降低目标配置预测吞吐，得到negative；**不要同时降低mock q0** |
| G04 | 新1卡作业，触发2卡 | enforce/gpu_hours | q1=2 | 与G01完全相同 | 仅切换优化目标；卡时多0.066667，拒绝 |
| G05 | 新2卡作业，触发1卡 | enforce/gpu_hours | q1=0.75 | 仍设q0=1、R=3600 | 初始进程数2、累积16；idle/trigger目标1；H0=2、H1=1.4，放行缩容 |

每次创建新作业后都重新填写MASTER/WORKER、核对环境、等待初始训练推进并建立新的BASELINE_READY_AT；上一作业的Pod名、时间戳和mock文件不能复用。普通负收益/未知用例虽然不换Pod，仍建议用新作业执行下一项，避免外层冷却和优化次数干扰。

G05完成后执行扩容场景时，先把02-job.yaml改回GPU=1、nproc_per_node=1、累积32，把idle/trigger改回目标2；不能只修改fixture q1。模式off/shadow虽然用于观测，也可能实际更换worker，不是无副作用的dry-run。

## 6. 普通收益门禁验收矩阵

未特别说明时：初始 1→2 卡、`enforce/time`、探索关闭；SwanLab 基线按 4.2 节生成。每个用例依次执行：设置 JSON → 确认服务加载 → 设置 Master 环境并创建新任务 → 等待代次/预热 → 生成 mock → 保存 Pod 基线 → 触发 → 归档断言。

| ID | 模式/目标/资源 | `scenario.json` 完整内容 | 期望 |
| --- | --- | --- | --- |
| G01 | enforce/time，1→2 | `{"name":"time-positive","q1":2}` | positive，时间收益 1680 秒，allow |
| G02 | enforce/time，1→2 | `{"name":"time-negative","q1":0.8}` | negative/insufficient_gain，T1=4620，reject |
| G03 | enforce/time，1→2 | `{"name":"below-threshold","q1":1.05}` | negative/insufficient_gain，收益约 51.43 秒，仅 1.43%，reject |
| G04 | enforce/gpu_hours，1→2 | `{"name":"gpu-cost-negative","q1":2}` | negative/insufficient_gain，卡时增加约 0.066667 |
| G05 | enforce/gpu_hours，2→1 | `{"name":"gpu-saving","q1":0.75}` | positive，H0=2，H1=1.4，节省 30%；T1=4920，延后 1320≤1800 |
| G06 | enforce/time，2→1 | `{"name":"shrink-time-negative","q1":0.75}` | negative/insufficient_gain，同一缩容配置不节省时间 |
| G07 | enforce/cluster_jct，2→1 | `{"name":"queue-positive","q1":0.75,"impacts_complete":true,"other_jobs":[{"job_id":"waiting-fixture","weight":1,"baseline_remaining_seconds":6000,"candidate_remaining_seconds_upper":2520}]}` | positive，集群收益 2160 秒，22.5% |
| G08 | enforce/cluster_jct，1→2 | `{"name":"queue-unknown","q1":2}` | unknown/invalid_or_missing_evidence，无完整队列证据，reject |
| G09 | enforce/time | `{"name":"candidate-unavailable","candidate":"unavailable"}` | unknown/candidate_unavailable，探索关闭，reject |
| G10 | enforce/time | `{"name":"candidate-missing","candidate":"missing"}` | unknown/invalid_or_missing_evidence，reject，不等同于 G09 |
| G11 | enforce/time | `{"name":"infeasible","feasible":false}` | negative/target_infeasible，reject |
| G12 | enforce/time | `{"name":"constraints","constraints":false}` | negative/scheduler_constraints，reject |
| G13 | enforce/time | `{"name":"delay-budget","q1":0.8,"max_delay":500}` | negative/delay_budget，延后 1020>500 |
| G14 | enforce/time | `{"name":"deadline","q1":2,"deadline_in":1800}` | negative/deadline，预计耗时 1920 秒 |
| G15 | enforce/time | `{"name":"stale-evidence","snapshot_age":1200}` | unknown/invalid_or_missing_evidence |
| G16 | enforce/time | `{"name":"wrong-identity","identity_mismatch":true}` | unknown/invalid_or_missing_evidence |
| G17 | enforce/time | `{"name":"incomparable","comparable":false}` | unknown/invalid_or_missing_evidence |
| G18 | enforce/time | `{"name":"http-error","http_status":503}` | unknown/http_error |
| G19 | enforce/time | `{"name":"http-timeout","sleep_seconds":15}` | unknown/http_or_payload_error，HTTP 超时失败日志，reject |
| G20 | shadow/time | `{"name":"shadow-negative","q1":0.8}` | negative 但 allow，真实执行调整 |
| G21 | shadow/time | `{"name":"shadow-unknown","candidate":"unavailable"}` | unknown 但 allow，不进入探索 |
| G22 | off/time | `{"name":"off-http-error","http_status":503}` | 跳过检查，allow；不调用该收益请求、不读取 mock |
| G23 | enforce/time | `{"name":"replay","q1":2,"replay":100}` | positive，T1=1970 秒，重放成本被计入 |

G07 的队列收益是合成输入，不能作为真实集群 JCT 改善的证据。G15 是收益快照过期；SwanLab 原始窗口仍应新鲜，避免在更早阶段失败而覆盖目标分支。

拒绝用例共同断言：本 plan 的 `gate_decision=reject`；评估结果与原因符合表格；在没有其他故障/重启干扰时 worker UID 和 GPU 数不变；该 plan 没有 checkpoint 请求、Pod 替换。`skipped/benefit_unknown` 或 `skipped/benefit_negative` 是伸缩计划结果，不代表训练任务失败。

放行用例分开记录两项：①收益判断正确；②checkpoint→资源替换→训练恢复成功。①通过但②失败，不能填“集群端到端通过”。

## 7. SwanLab mock 异常和首次运行

本节固定收益服务为 G01 的正常 JSON，保证异常来自训练数据源。每次变更先保存原始 JSON；用同一生成命令重新生成可恢复正常数据。

| ID | 操作 | 期望 |
| --- | --- | --- |
| S01 | 新 Master 启动后不生成 `/mock/swanlab.json` 就触发 | unknown/swanlab_mock_file_error（前提是本轮generation已建立），checkpoint前拒绝；不访问真实 SwanLab 兜底 |
| S02 | 生成命令改 `--task-id wrong-task` | unknown/task_identity_mismatch，checkpoint前拒绝 |
| S03 | 删除 ElasticJob 顶层 task label 后新建作业 | 客户端配置无效，unknown；环境变量不能补救 |
| S04 | 专用新作业将max_age改为60秒，生成后等65秒再触发（见下方快速异常配置） | unknown/stale_or_missing_generation，过期窗口拒绝 |
| S05 | 生成命令改 `--window-seconds 10 --work-start 150 --work-end 160` | unknown，最小 30 秒窗口不足 |
| S06 | 在 generation/预热下界之前生成窗口并立即触发 | unknown；先正常训练，等待下界后重新生成，并用新的调整触发重评 |
| S07 | 将 work 序列改为 `[100,101,160]`，时间仍等间隔 | unknown，子窗口吞吐波动过大 |
| S08 | 删除 `train_speed` 序列，或破坏某一同批点的 createTime | unknown，必需指标/采样对齐失败 |
| S09 | 新 Master 配置 `DLROVER_SWANLAB_TIME_SOURCE=ingestion` | unknown/untrusted_sample_time，mock 同样不能绕过 |
| S10 | 完整新鲜基线 + G09 缺候选，探索关闭 | 当前配置训练继续，本次调整拒绝；不会从 q0 或 GPU 比例推导 q1 |

异常用例每次只注入一种错误，其他条件保持有效。S01要在新作业确认generation后仍不生成文件；如果连generation都没有，先得到的是配置/代次错误，不是文件错误。S04必须先生成合格窗口再等其过期；S06则故意不执行正常95秒等待。S03/S09改label或环境后必须新建Master。

S04专用短有效期：在创建该用例的新Master之前，先加载正常env再覆盖max_age。生成正常60秒窗口后记录生成时刻，等65秒再触发即可；无需按正常配置等10分钟。

```bash
kubectl apply -f "$MANIFEST_DIR/00-benefit-env.yaml"
kubectl patch configmap benefit-test-env -n "$NS" --type merge \
  -p '{"data":{"DLROVER_BENEFIT_MAX_AGE_SECONDS":"60"}}'
```

之后按第4节新建作业、确认实际max_age=60、等待95秒、生成mock、再等待65秒后触发。下一用例重新apply正常env并新建Master，恢复max_age=600。只修改正在运行Master引用的ConfigMap不会生效。

G15直接让fixture返回1200秒前的快照，可立即验证正常600秒有效期下的拒绝；SwanLab文件仍应新鲜。G19使用sleep_seconds=15，大于本文10秒HTTP超时，预期约10秒请求超时后拒绝，不需要等15秒服务返回。这些注入值必须大于**本次Master实际加载**的阈值。

S07/S08顺序为：完成正常等待 → 生成新鲜基线 → 立即修改文件 → 立即触发。不要先让文件过期再注入畸形数据，否则更早的校验可能遮蔽要测的分支。异常注入后预期没有普通成功的window_selected；下一用例重新生成文件，不沿用已损坏JSON。

S07的可执行注入示例：

```bash
kubectl exec -i -n "$NS" "$MASTER" -c main -- python - <<'PY'
import json
import os
from pathlib import Path
p = Path('/mock/swanlab.json')
data = json.loads(p.read_text())
for series in data:
    if series['key'] == 'global_step':
        for point, work in zip(series['scalarMetricsList'], [100, 101, 160]):
            point['data'] = str(work)
tmp = p.with_suffix('.tmp')
tmp.write_text(json.dumps(data))
os.replace(tmp, p)
PY
```

注入必须在窗口仍新鲜时触发。普通门禁结束本次计划后不自行轮询；如果重复 apply 相同 trigger 内容，Kubernetes 可能没有产生变化事件。先复位 idle，等待配置更新与已有伸缩线程结束，再触发 manual；也应检查外层冷却/优化次数。最易复现的方式仍是每个用例新建作业。

## 8. 无候选数据的受控探索与回退

### 8.1 与普通测试桩的边界

G09/S10 已覆盖“首次无候选且不开探索”的集群行为。要覆盖自动探索，需要将 `DLROVER_BENEFIT_HTTP_URL` 切换到本环境真实的执行/调度证据适配服务。本文测试桩没有预留资源，也不观察 checkpoint，不能将其响应改为 `confirmed=true` 后宣称完成了探索验收。

真实适配服务需支持 [收益 HTTP 契约](vertical-elastic-benefit-http-cn.md) 中四种交互：

1. **准入**：明确 `candidate={"status":"unavailable"}`；提供完整训练可比性、成本、调度约束、limits，以及真实 checkpoint 兼容性、静止 checkpoint 支持、GPU 档位、回退预算和已建立的回退资源预留。
2. **变更授权**：处理 `exploration.authorize_mutation=true`，再次核验本次请求身份、资源、预留 ID 与到期时间。不能要求此时必须取得训练窗口，训练可能已暂停。
3. **观测**：提供真实 `applied_plan_id`、`restored_checkpoint`、`all_workers_ready`、generation、累计 `trial_gpu_seconds` 和预留状态。观测请求的 current 是试运行配置、target 是原配置。
4. **终态回传**：处理 `release_reservation=true`，按探索 ID 幂等保存结果、释放预留，并返回匹配的 request_id、exploration_id、`recorded=true`。

没有该服务时，第 8 节记录为“依赖未具备，未执行”，普通门禁矩阵仍可独立执行。现有本地 mock 单测可覆盖算法分支，但不能替代此集群验收。

### 8.2 `04-exploration-env.yaml`：完整探索 ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: benefit-exploration-env
  namespace: elastic-training
data:
  DLROVER_BENEFIT_MODE: "enforce"
  DLROVER_BENEFIT_OBJECTIVE: "time"
  DLROVER_BENEFIT_HTTP_URL: "http://REPLACE_EXECUTION_ADAPTER/benefit/snapshot"
  DLROVER_BENEFIT_EXPLORE: "true"
  DLROVER_BENEFIT_STATE_DIR: "/training/benefit-state"
  DLROVER_BENEFIT_TRIAL_SECONDS: "1800"
  DLROVER_BENEFIT_TRIAL_POLL_SECONDS: "10"
  DLROVER_BENEFIT_TRIAL_GPU_HOURS: "5"
  DLROVER_BENEFIT_TRIAL_COOLDOWN_SECONDS: "60"
```

沿用 02-job.yaml 的持久卷，必须额外验证该卷支持跨进程/Pod `flock`、原子 rename 和持久化；RWX 声明本身不证明锁可靠。`/mock` 可以使用 emptyDir，但探索状态目录绝不能放在那里。

为避免手工遗漏，使用以下命令生成**完整**探索 ElasticJob 文件；输入仍为替换好环境值的 02-job.yaml：

```bash
python - <<'PY'
from pathlib import Path
import os
import yaml
manifest_dir = Path(os.environ['MANIFEST_DIR'])
job = yaml.safe_load((manifest_dir / '02-job.yaml').read_text())
master = job['spec']['replicaSpecs']['dlrover-master']['template']['spec']['containers'][0]
master['envFrom'].append({'configMapRef': {'name': 'benefit-exploration-env'}})
(manifest_dir / '05-exploration-job.yaml').write_text(yaml.safe_dump(job, sort_keys=False))
PY
kubectl apply -f "$MANIFEST_DIR/04-exploration-env.yaml"
```

后面的 envFrom 覆盖前面的同名字段；显式 `env` 又优先于 envFrom，不要在其他地方残留同名配置。先归档并结束之前的普通测试作业，复位idle，再部署生成的探索作业：

```bash
kubectl apply -f "$MANIFEST_DIR/01-manual-idle.yaml"
kubectl apply -f "$MANIFEST_DIR/05-exploration-job.yaml"
```

重新选择MASTER/WORKER并核对实际环境。不能仅apply探索ConfigMap后继续使用旧普通Master；也不要在本节沿用4.2的100→160参数，改用8.4的探索轨迹。

### 8.3 准入准备和预算

本节正常探索采用30分钟总预算、15分钟回退预算；这些是上限，达到有效观测后会立即决策，不会固定等满。初始1卡、目标2卡，GPU tiers为`[1,2]`。

以下字段需要在**真实证据适配服务的测试任务配置**中设置，不属于Master env；只改04-exploration-env.yaml不足以完成配置：

| 适配服务字段 | 本文正常探索值 | 配置来源/含义 |
| --- | --- | --- |
| exploration.rollback_seconds_upper | 900秒 | 回退切换、恢复和验证的预算；代码也用它估计回退代价 |
| exploration.rollback_gpu_seconds_upper | 2700卡秒 | 本例按最多3卡占用（2卡候选+1卡预留）×900秒准备；实际资源账本仍必须真实计量 |
| limits.max_delay_seconds | 3600秒 | 准入允许至少2700秒附加时间；普通fixture仍用1800秒，不改普通门禁算例 |
| limits.deadline_at | null | 常规探索先不加deadline；专门测deadline时再设，不能用过近deadline遮蔽其他场景 |
| exploration.rollback_reservation.expires_at | 准入时刻之后至少5400秒 | 真实预留90分钟，并满足服务自身释放/续期策略；不是JSON声明就完成资源预留 |

联动检查：

- 当前worker健康，只有GPU档位变化；基础数据仍要求steady/comparable。
- 探索基线按8.4设`T0=7200`秒，严格大于`1800+900=2700`秒。
- `max_delay_seconds=3600 >= 2700`；如果额外设deadline，准入时至少需覆盖`7200+2700=9900`秒并留余量。
- 准入卡秒为`max(1,2)×1×1800+2700=6300`，小于`5×3600=18000`。5 GPU·h是上限，不是固定消费；累计实际占用含预留仍由服务核算。
- 预留不能只勉强覆盖去程和观测。代码在回退验证阶段仍检查“预留过期时间>当前时间+rollback_seconds”；示例5400秒覆盖最迟1800秒请求回退、900秒回退验证，再加900秒检查余量，另留1800秒缓冲。
- 1800秒包含checkpoint、替换、恢复、30秒预热、两轮60秒窗口、轮询与人工核对。调试过程中不要人为等满预算再测正收益；建议候选决策在20分钟内完成。

### 8.4 探索数据：基线、候选、回退分别何时生成

#### 8.4.1 先设计整条模拟累计进度，不能中途换算例

普通测试的100→160不直接用于本节。探索代码会比较mock末端work和**真实checkpoint_step**，并要求target_work、单位、batch假设与探索基线一致。若真实checkpoint已到500，提供work_end=160即使时间新鲜也不会通过。

为避免真实训练在准备期间超过小计数器，本节采用固定偏移的合成轨迹：假设真实训练保持本文 `--max_steps 100000`，所有相关真实checkpoint step都小于100000，设置：

| 阶段 | 模拟work_start | 模拟work_end | total_steps | 吞吐 |
| --- | --- | --- | --- | --- |
| 探索基线（初始1卡） | 100000 | 100060 | 107260 | 1 step/s |
| E01候选窗口1（目标2卡） | 100060 | 100180 | 107260 | 2 step/s |
| E01候选窗口2 | 100180 | 100300 | 107260 | 2 step/s |
| E02候选窗口1 | 100060 | 100090 | 107260 | 0.5 step/s |
| E02候选窗口2 | 100090 | 100120 | 107260 | 0.5 step/s |
| E03候选窗口1 | 100060 | 100120 | 107260 | 1 step/s |
| E03候选窗口2 | 100120 | 100180 | 107260 | 1 step/s |

每个窗口均60秒。探索基线改为 `R=107260−100060=7200`，对应2小时的模拟剩余工作；与普通门禁的3600不同，用于容纳较长人工操作并保持收益分支可复现。偏移100000不是测得的训练进度；它只用于隔离测试的合成计数，使观测计数满足checkpoint下限。先核对真实checkpoint确实低于该值。若训练目标/框架计数不符合这个前提，需在**开始探索前**选更大的固定偏移B，基线B→B+60、总目标B+7260，并整体平移后续work；不能看到checkpoint后临时把候选work跳到一个大数，否则会虚增完成工作量、污染整体收益。

表格刻意把从基线末端到候选窗口起点之间的切换/恢复/等待视为无有效模拟进展，使这段时间计入成本。两个候选窗口之间则严格连续。真实worker仍按真实进度训练，表中的合成计数不修改真实checkpoint。只能用于控制分支测试，不能据此验证真实收益。

#### 8.4.2 初始1卡阶段生成探索基线并触发

使用05-exploration-job.yaml创建新作业，确认实际enforce/time、explore=true、HTTP URL指向真实适配服务。按4.2.1–4.2.2确认初始训练推进并等待95秒，然后把4.2.4的生成命令参数替换为：

```bash
kubectl exec -i -n "$NS" "$MASTER" -c main -- python - \
  --task-id 123 --assume-fixed-batch \
  --window-seconds 60 --work-start 100000 --work-end 100060 --total-steps 107260 \
  --output /mock/swanlab.json \
  < "$REPO_DIR/examples/pytorch/resource_elastic/benefit_validation/generate_swanlab_mock.py"
kubectl exec -n "$NS" "$MASTER" -c main -- cat /mock/swanlab.json \
  > "$CASE_DIR/exploration-baseline.json"
kubectl apply -f "$MANIFEST_DIR/03-trigger.yaml"
```

适配服务应明确返回candidate_unavailable以及有效准入/预留。依次确认admission.allowed=true、gate_decision=explore、PREPARING、checkpoint和SWITCHING。此时不要再提交第二次manual，也不要一直刷新基线文件；Master会在切换后进入探索轮询。

#### 8.4.3 读取当前探索状态并等待真实恢复

从gate_decision/admission日志记录根plan_id（探索id），设置PLAN_ID。下面只读journal，不修改状态，也不按“最新文件”猜测作业：

```bash
PLAN_ID=REPLACE_EXPLORATION_ROOT_PLAN_ID
kubectl exec -i -n "$NS" "$MASTER" -c main -- python - "$PLAN_ID" <<'PY' > "$CASE_DIR/trial-state.json"
import json
import sys
from pathlib import Path
matches = []
for path in Path('/training/benefit-state').glob('*.json'):
    state = json.loads(path.read_text())
    if state.get('id') == sys.argv[1]:
        matches.append(state)
if len(matches) != 1:
    raise SystemExit('Expected exactly one journal matching PLAN_ID')
print(json.dumps(matches[0], indent=2))
PY
```

本地查看trial-state.json，确认id、status、checkpoint_step、observation_after、expires_at、windows、last_window_end。第一次候选观测前应为OBSERVING；还需看到该plan的真实 `training_recovered` 事件和新2卡worker持续推进step。OBSERVING本身不证明训练已经恢复，SwanLab mock也不会把recovery_ready从false改成true。

只读命令退出非零或文件为空时，先检查PLAN_ID、挂载路径和日志，不要继续从该文件计算下一轮时间。目录可能保留其他作业的journal，这也是必须按根plan_id筛选的原因。

在**OBSERVING和真实恢复两项都已确认之后**，用Master时钟记 `CANDIDATE_READY_AT`，按4.2.2同样方法留足95秒：

```bash
CANDIDATE_READY_AT=$(kubectl exec -n "$NS" "$MASTER" -c main -- \
  python -c 'import time; print(time.time())')
```

这样第一个完整60秒窗口会晚于observation_after、代次/恢复预热下界。此时检查expires_at是否还能覆盖95秒等待、第二窗口60秒、至少300秒人工操作及30秒请求余量（合计至少485秒）；不足则本轮无法按此流程完成观测，记录原因，不靠伪造时间或修改journal延长预算。下一个独立用例应在准入前协调增大trial预算、delay/deadline和预留TTL。

#### 8.4.4 第一个候选窗口：以E01为例

确认仍是同一2卡代次、checkpoint_step<100060且总目标仍为107260。执行以下带时间检查的命令；E02/E03只按8.4.1表格替换work参数：

```bash
if kubectl exec -n "$NS" "$MASTER" -c main -- python -c \
  'import math,sys,time; left=max(0,math.ceil(float(sys.argv[1])+95-time.time())); print("remaining_wait_seconds=",left); sys.exit(1 if left else 0)' \
  "$CANDIDATE_READY_AT"
then
  kubectl exec -i -n "$NS" "$MASTER" -c main -- python - \
  --task-id 123 --assume-fixed-batch \
  --window-seconds 60 --work-start 100060 --work-end 100180 --total-steps 107260 \
  --output /mock/swanlab.json \
  < "$REPO_DIR/examples/pytorch/resource_elastic/benefit_validation/generate_swanlab_mock.py"
fi
```

确认文件生成成功后立即归档：

```bash
kubectl exec -n "$NS" "$MASTER" -c main -- cat /mock/swanlab.json \
  > "$CASE_DIR/candidate-window-1.json"
```

等待Master下一次轮询后，重新执行8.4.3的只读journal命令，确认 `windows=1`、E01的`measured_lower=2`（E02为0.5，E03为1）、`last_window_end>0`。仅看到SwanLab window_selected还不够：探索还会检查真实执行/预留证据，只有journal计数增加才证明本轮被探索接受。

若一直为0，查observation_wait.reason，例如execution_or_window_mismatch、original_generation、checkpoint_not_exceeded、training_recovery_pending。不要生成下一轮覆盖问题输入；先保存日志和当前JSON。相同文件反复读取不会增加有效窗口数。

#### 8.4.5 第二个窗口：固定连续时间，不重复第一窗口

从刚保存且windows=1的journal取第一窗口末端，再计算第二末端=第一末端+60秒：

```bash
SECOND_END=$(python - "$CASE_DIR/trial-state.json" <<'PY'
import json
import sys
s = json.load(open(sys.argv[1]))
assert s['status'] in ('OBSERVING', 'VERIFYING_ROLLBACK') and s['windows'] == 1
print(s['last_window_end'] + 60)
PY
)
```

等Master时钟到达SECOND_END后，可以在之后5分钟内生成第二窗口，无需抢在30秒内操作。下面检查同时要求0≤age≤300，且journal所记录的本阶段expires_at至少还剩30秒。5分钟是本文操作窗口；数据有效期为10分钟，剩余余量供下一轮HTTP观测。总预算是独立限制，即使数据新鲜，探索也可能已经结束。

先从本轮已保存的journal取本阶段到期时间（候选OBSERVING和回退VERIFYING_ROLLBACK分别使用各自最新journal）：

```bash
TRIAL_EXPIRES_AT=$(python -c 'import json,sys; print(json.load(open(sys.argv[1]))["expires_at"])' "$CASE_DIR/trial-state.json")
```

确认当前phase仍是需要提交窗口的观测阶段，再运行：

```bash
if kubectl exec -n "$NS" "$MASTER" -c main -- python -c \
  'import sys,time; now=time.time(); age=now-float(sys.argv[1]); remaining=float(sys.argv[2])-now; print("second_window_end_age=",age,"remaining_trial_seconds=",remaining); sys.exit(0 if 0<=age<=300 and remaining>=30 else 1)' \
  "$SECOND_END" "$TRIAL_EXPIRES_AT"
then
  kubectl exec -i -n "$NS" "$MASTER" -c main -- python - \
  --task-id 123 --assume-fixed-batch --now "$SECOND_END" \
  --window-seconds 60 --work-start 100180 --work-end 100300 --total-steps 107260 \
  --output /mock/swanlab.json \
  < "$REPO_DIR/examples/pytorch/resource_elastic/benefit_validation/generate_swanlab_mock.py"
fi
```

这里显式传--now是为了让两个**模拟**窗口在同一时间点衔接，不是把旧真实数据更新时间。E02第二窗口改为100090→100120，E03改为100120→100180；total_steps和SECOND_END不变。用同样的cat重定向命令保存为 `$CASE_DIR/candidate-window-2.json`，然后等待windows=2和最终决策；普通文件生成器不会替你等待或重复执行。

若错过模板时间段，不要把相同step计数配到一个随意的新时间；应记录当前轮未按预定输入完成，并按实际journal/预算处理。后续独立用例重新设计时间安排，或显式记录窗口间空档的模拟进度及重新计算预期。不要修改journal的last_window_end/windows来强行通过。

#### 8.4.6 回退后还需要第三、第四轮窗口

E02第二个候选窗口后应进入回退分支。此时真实框架要保存**当前最新**checkpoint并恢复原1卡配置，不是重新加载探索开始时的旧checkpoint。等待新1卡worker、回退新generation、真实training_recovered和VERIFYING_ROLLBACK，再重新按恢复后的时间下界提供两个q=1窗口。

按上述E02轨迹，模拟回退等待期间暂停进展，可用 `100120→100180`、`100180→100240`，total_steps始终107260；先确认新checkpoint_step也低于这些work，单位/batch假设没有变化。两轮之间同样采用上一已接受窗口末端+60秒衔接，并确认journal windows先为1再为2。回退进入观测时windows会清零，不能把候选阶段的两轮计入回退验证。

回退预算从请求回退时重新按rollback_seconds计时，本文900秒同时容纳真实回退切换、恢复、95秒保守预热/窗口等待、第二窗口60秒及人工操作。可按真实切换/恢复300秒、等待95秒、第二窗口60秒、人工核对300秒、请求余量30秒规划，共785秒，还余115秒。若本环境切换明显超过5分钟，需提前调整适配服务预算和预留，而不是在运行中改journal。只看到资源回到1卡不算ROLLED_BACK，必须完成两轮新代次进度验证。

### 8.5 探索场景矩阵与步骤

每个独立探索用例使用新 job_uid。当前实现每个作业最多一次探索、一次回退，冷却到期不意味着允许第二次探索；不要通过删除 journal 重试。

| ID | 场景及操作 | 验收 |
| --- | --- | --- |
| E01 | time，基线 q0=1，候选两个窗口 q1=2；剩余工作量和预算充足 | `ACCEPTED/positive_observed_gain`，保留 2 卡；结果回传成功，预留释放 |
| E02 | time，候选两个窗口 q1=0.5；确认回退的未来时间更短、约束/预算仍满足 | `ROLLBACK_REQUESTED`，使用**当前最新** checkpoint 回退到 1 卡；再提供回退新代次两个窗口 q=1，且 work 超过回退 checkpoint，最终 `ROLLED_BACK` |
| E03 | time，候选 q1=1，保持可行且回退更慢 | 整体收益通常因切换成本不达标，但回退要再付成本，`RETAINED/rollback_not_beneficial`；不能记成正收益 |
| E04 | 专用新作业设trial_seconds=300，确认在240秒内进入OBSERVING；此后不提供有效新窗口，直到300秒总预算耗尽 | `INCONCLUSIVE/observation_budget_exhausted`；不自动回退，不证明目标配置有收益，后续自动调整被保护阻止 |
| E05 | 准入回退预留未确认/TTL不足，或 max_delay<2700（正常1800秒探索+900秒回退配置） | 准入拒绝，不 checkpoint、不替换；检查 admission.reason |
| E06 | 明确 unavailable，但目标设为 cluster_jct | 不进入探索，拒绝；当前仅支持 time/gpu_hours 探索 |
| E07 | 缺 candidate 字段、HTTP 错误或基线未稳 | unknown，不进入探索，不能把所有 unknown 当作试运行许可 |
| E08 | OBSERVING 中重启 Master，保留同一 ElasticJob UID、持久卷和原配置 | 恢复 journal，不重复首次资源替换；重新建立可信代次/执行上下文后提供新窗口，继续观测；预算超时则 INCONCLUSIVE |
| E09 | 在隔离用例的 SWITCHING/ROLLING_BACK 状态中断 Master | 重启后保护阻止自动重放变更，需要核对真实资源/checkpoint；不删除 journal 强行继续 |
| E10 | 变更前预留授权拒绝 | 不执行后续 worker 替换，记录失败/拒绝；检查 checkpoint 与当前 worker 实际状态 |

E01/E02 的吞吐只是输入设计，最终分支还依赖 elapsed、remaining、卡秒、deadline，不能只凭 q1 的大小预断结果。time 目标的关键计算为：

```text
整体收益 = 基线剩余时间 - 已消耗时间 - 当前剩余工作量/q1
继续耗时 = 当前剩余工作量/q1
回退耗时 = 回退时间上界 + 当前剩余工作量/q0
```

整体收益为正且超过阈值、继续满足约束时 ACCEPTED；回退更划算或继续违反约束且回退可行时才回退；继续可行但回退不划算可 RETAINED。观测超时或预留失效时不保证自动回退。

以8.4的新基线和三套输入为例，假定第二轮决策时从准入起实际已过600秒，回退上界900秒、max_delay=3600、无deadline且预留/卡时满足要求，可手算：

| 用例 | 第二轮剩余work | 保持预计耗时 | 整体收益 | 回退预计耗时 | 预期分支依据 |
| --- | --- | --- | --- | --- | --- |
| E01 | 6960 | 6960/2=3480秒 | 7200−600−3480=3120秒 | 900+6960=7860秒 | 整体收益43.33%超过5%，ACCEPTED |
| E02 | 7140 | 7140/0.5=14280秒 | 7200−600−14280=−7680秒 | 900+7140=8040秒 | 回退更快，且600+8040≤7200+3600，申请回退 |
| E03 | 7080 | 7080秒 | 7200−600−7080=−480秒 | 900+7080=7980秒 | 整体未盈利，但回退多花900秒，RETAINED |

600秒是说明用的elapsed假设，不是伪造时钟。采用更大的7200秒基线后，即使E01在1800秒预算临近结束时才取得两轮窗口，整体收益仍约1920秒，超过360秒阈值；不会因为正常人工操作稍慢就轻易改变预期分支。真正到期前未完成有效观测仍是INCONCLUSIVE，不可修改时间绕过。

E08 不是简单“重启后马上能用旧文件”：Master 新进程可能尚无可信训练代次边界，`/mock` 也可能已丢失；必须观察实际恢复上下文并重建新窗口。重启不会重置原 trial expires_at。

### 8.6 专门验证探索超时，不等30分钟

E04在独立新作业中把trial_seconds改为300秒，其余正常探索配置保留；下面覆盖必须在创建新Master之前执行：

```bash
kubectl apply -f "$MANIFEST_DIR/04-exploration-env.yaml"
kubectl patch configmap benefit-exploration-env -n "$NS" --type merge \
  -p '{"data":{"DLROVER_BENEFIT_TRIAL_SECONDS":"300"}}'
```

使用新UID的05-exploration-job.yaml，按正常探索基线触发；进入OBSERVING后不提供新候选窗口，观察expires_at及最终INCONCLUSIVE。这里不需要生成两轮窗口，也不用等待候选恢复后95秒再操作。

短预算的前提是本环境从准入到OBSERVING能在240秒内完成，至少留下60秒观察超时。若未进入OBSERVING就超时，应记录“切换阶段超时”，不能算E04观测超时通过；下个新用例把trial_seconds设为实测切换时间加至少120秒。不能运行中修改journal来缩短或延长当前倒计时。

正常探索结束后的60秒冷却不是E04的恢复手段：FAILED/INCONCLUSIVE会保留保护，需核对资源、checkpoint与预留。下一正常用例先重新apply04-exploration-env.yaml恢复1800秒，并使用新UID；保留失败journal供排查。

## 9. 收益控制参数说明

以下默认值来自代码，不代表集群部署值。除 mock 文件内容与 fixture 场景文件外，参数都在初始化时读取。

### 9.1 门禁参数（Master 环境变量）

| 参数 | 代码默认 | 本文值/意义 |
| --- | --- | --- |
| `DLROVER_BENEFIT_MODE` | off | enforce：正收益放行、未知/负收益拒绝；shadow：配置有效时记录后放行；off：不评估。非法值不会正常放行 |
| `DLROVER_BENEFIT_OBJECTIVE` | time | time / gpu_hours / cluster_jct；不同目标可能对同一配置得出相反结论 |
| `DLROVER_BENEFIT_HTTP_URL` | 空 | 完整收益快照接口；mock SwanLab 下依然需要，与 SwanLab URL 不同 |
| `DLROVER_BENEFIT_HTTP_TOKEN` | 空 | 可选 Bearer token；需要时通过 Secret 注入，不写入测试报告 |
| `DLROVER_BENEFIT_TIMEOUT_SECONDS` | 3 | 本文设10秒，减少集群抖动导致的误失败；有限且>0，沿用到SwanLab HTTP客户端 |
| `DLROVER_BENEFIT_MAX_AGE_SECONDS` | 60 | 本文设600秒（10分钟），快照/末端采样最大年龄，也影响采样间隔校验；有限且>0 |
| `DLROVER_BENEFIT_MIN_WINDOW_SECONDS` | 30 | 当前吞吐稳定窗口总时长下限，有限且>0 |
| `DLROVER_BENEFIT_MIN_GAIN_RATIO` | 0.05 | 收益比例至少 5%，范围[0,1]；同时要求收益严格>0，设为0也不会放行零收益 |

### 9.2 SwanLab mock 与稳态参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `DLROVER_SWANLAB_MOCK` | false | 仅显式 true（忽略大小写及首尾空格）启用本地文件 |
| `DLROVER_SWANLAB_MOCK_FILE` | 空 | Master 内可读的 JSON 文件；每次请求重读，文件错误返回 unknown |
| `DLROVER_SWANLAB_HTTP_URL` | 空 | mock=true 时不需要；切回真实数据时填写完整 scalar URL |
| `DLROVER_SWANLAB_HTTP_TOKEN` | 空 | 真实 scalar 接口的 Bearer token，与收益服务 token 分离 |
| `DLROVER_SWANLAB_ASSUME_FIXED_BATCH` | false | 本文 true，显式假设 optimizer step 工作量跨档位固定，不伪造 batch 数值 |
| `DLROVER_SWANLAB_KEYS` | {} | 本文 work=global_step、target_work=max_steps、train_speed=train_speed；默认非固定 batch 模式查询 train/global_step、train/total_steps、train/global_batch_size |
| `DLROVER_SWANLAB_TIME_SOURCE` | sample | createTime 作为采样时间；unknown/ingestion 不参与收益判断 |
| `DLROVER_SWANLAB_UTC_OFFSET` | +08:00 | 仅用于无时区时间戳；本文生成器输出 UTC 显式时区 |
| `DLROVER_SWANLAB_WARMUP_SECONDS` | 60 | 本文mock测试设30秒；从代次开始与恢复确认的较晚时间起算。允许0，但本文保留预热校验 |
| `DLROVER_SWANLAB_STEADY_WINDOWS` | 2 | 推断稳态的子窗口数，整数≥2；不是探索所需观测轮数 |
| `DLROVER_SWANLAB_MAX_THROUGHPUT_VARIATION` | 0.1 | 子窗口(最大吞吐−最小吞吐)/最小吞吐上限，范围[0,1] |

任务身份通过 `metadata.labels.low_code_platform_task_id` 注入，不能用 Master 环境中的同名/相关 ID 代替。每个 scalar 序列 taskId 均要匹配。

### 9.3 探索参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `DLROVER_BENEFIT_EXPLORE` | false | 只有 true 开启，且要求 enforce、time/gpu_hours 和明确 candidate_unavailable |
| `DLROVER_BENEFIT_STATE_DIR` | 空 | 持久化 POSIX 状态目录绝对路径；支持 flock、原子替换和持久化 |
| `DLROVER_BENEFIT_TRIAL_SECONDS` | 600 | 本文正常探索设1800秒（30分钟）；从准入开始，包含切换、恢复与人工操作，非纯测量时间 |
| `DLROVER_BENEFIT_TRIAL_POLL_SECONDS` | 10 | 两次观测间等待时间，实际周期还包括请求耗时 |
| `DLROVER_BENEFIT_TRIAL_GPU_HOURS` | 0 | 本文设5 GPU·h；默认不具备探索预算，计入回退预算和预留占用 |
| `DLROVER_BENEFIT_TRIAL_COOLDOWN_SECONDS` | 1800 | 本文设60秒；减少结束后的观察等待，不解除每job_uid最多一次探索或失败保护 |

时长、轮询、冷却、卡时预算均要求有限且>0。关闭探索开关不会清除已有 journal 或自动回退；存在未完成状态时仍保留保护。

### 9.4 HTTP 证据参数：不是 Master 环境变量

| 响应字段 | 单位/意义 | 本文普通场景默认 |
| --- | --- | --- |
| candidate.throughput_lower | 目标配置吞吐，与 training.unit 一致；需 trial/profile/history 与 evidence_id | 2 step/s，测试 profile |
| transition.lost_seconds_upper | 切换期间无有效进展时间上界 | 120秒 |
| transition.replay_work_upper | 需重放的训练工作量 | 0 step |
| transition.gpu_seconds_upper | 切换资源消耗上界 | 240卡秒 |
| scheduling.target_feasible | 目标资源可调度性 | 合成 true |
| scheduling.constraints_satisfied | 调度约束满足 | 合成 true |
| limits.max_delay_seconds | 相对原配置最多允许延后 | 1800秒 |
| limits.deadline_at | 绝对 Unix 秒截止时间，可空 | null |
| scheduling.impacts_complete / other_jobs | cluster_jct 的完整影响集合及权重/前后剩余时间 | 默认不提供，故 cluster_jct 默认 unknown |

这些字段在真实适配服务中必须来自可信证据。本文的 q1、delay、replay、overhead、max_delay、deadline_in 等只是 fixture 配置键，不是生产配置接口。

### 9.5 几个容易混淆的时间和“窗口”

| 名称 | 本文数值 | 实际含义 |
| --- | --- | --- |
| warmup | 30秒 | 本文mock测试的预热期；不是Pod启动后固定等30秒 |
| generator window-seconds | 60秒 | 一次文件中模拟采样点覆盖的时间跨度；运行生成器不会等待60秒 |
| min_window | 30秒 | 校验要求的窗口总时长下限；本文选择60秒留出余量，不要求恰好30秒 |
| max_age | 600秒 | 允许手动操作和查看日志；不是要求文件放10分钟后读取 |
| 本文首次等待 | 95秒 | 确认真实step增长后保守等待warmup30+窗口60+余量5；不是代码新增配置项 |
| steady_windows | 2个子窗口 | 一轮SwanLab窗口内部的两个稳定区间；本文三个点构成两段30秒 |
| 探索journal windows | 至少2轮 | 两次独立、时间不重叠的合格观测；一次文件里的两个子窗口不等于两轮探索 |
| trial poll | 10秒 | 探索重新请求的间隔；轮询同一旧文件不会自动产生新采样 |
| trial seconds | 1800秒 | 从探索准入起计时的总预算，包含切换、恢复、预热、观测及操作等待 |

需要加快测试时，应先说明要缩短哪一段以及改变后仍要满足哪些校验。不能只把trial设得很短，却保留原有checkpoint耗时、30秒预热和两轮60秒窗口，并期待探索成功。

### 9.6 执行链路还有独立超时，收益参数不会覆盖它们

当前代码还从Brain配置读取执行参数；这些不是本文的`DLROVER_BENEFIT_*`环境变量。先检查Master日志里的`vertical elastic control params`，确认实际值，不要自行创造同名环境变量并假定生效：

| 参数 | 代码默认 | 本文处理 |
| --- | --- | --- |
| ckpt_save_status_check_max_wait_time | 900秒 | 保留，供checkpoint及执行阶段使用；增大trial不会自动增大它 |
| ckpt_save_status_check_interval | 1秒 | 保留，方便及时发现checkpoint完成，不是人工操作期限 |
| log_interval_check_nums | 10次 | 保留，等待过程约每10次检查记录日志 |
| vertical_elastic_opt_interval | 36000秒 | 本文手动监听on时，代码会记录间隔不足但继续手动调整，不需等10小时；探索状态保护和次数限制仍有效 |

探索中的参数就绪等待还会受剩余trial预算限制；控制完成后的训练恢复确认也有执行侧超时。因此把trial设为1800秒不代表每个执行阶段都能等1800秒。若真实checkpoint/恢复超过执行侧900秒，应先定位框架或通过本环境Brain配置机制调整，不要单独增加mock有效期。

预估操作节奏：首次确认真实step后约95秒可生成基线；普通门禁在触发链路正常时通常一个HTTP请求内返回判断，真实扩容耗时另算。正常探索建议10–20分钟内完成候选观测，回退另预留15分钟。期间可持续观察日志、Pod、journal，正常分支完成即结束，无需等预算耗尽；收益结果始终使用实际elapsed扣除人工等待时间。

## 10. 日志、指标与问题定位

使用日志中的 plan_id/request_id 串联 `BenefitFlow`、`VerticalElasticEvent` 和 fixture 请求记录。DEBUG 便于保留窗口细节；重复轮询日志有抑制机制，日志条数不等于实际请求次数。

可选指标采集：

```bash
kubectl port-forward -n "$NS" pod/"$MASTER" 19090:9090
```

另一个终端：

```bash
curl --fail http://127.0.0.1:19090/metrics > /tmp/benefit-k8s-test/metrics.prom
rg 'dlrover_master_vertical_' /tmp/benefit-k8s-test/metrics.prom
```

重点检查 `dlrover_master_vertical_benefit_evaluations_total` 的 mode/result/reason，以及探索终态计数。off 路径不增加收益评估计数，不应强求出现 positive/negative。Master 重启后计数器会重置，跨进程不能直接相减。

| 现象 | 优先检查 |
| --- | --- |
| 没有 evaluation_started | 手动 ConfigMap 事件、Master watcher、Brain 地址/权限、空计划、same_resource、busy、外层冷却和次数限制 |
| 文件存在却 unknown | task label、JSON taskId、窗口时效、代次/恢复下界、稳定性、key 对齐；读取失败不会回退真实接口 |
| 明明设置正收益却拒绝 | 环境变量是否新进程生效、HTTP 服务实际 scenario、objective、delay/deadline、当前是否仍为预期初始卡数 |
| 修改场景后仍是旧结果 | ConfigMap 投影传播；GET /scenario；不要使用 subPath；确保没有重新 apply 初始的 `00-fixture-scenario.yaml` 覆盖当前场景 |
| 收益放行后卡住 | checkpoint 协议、持久路径、真实节点可调度性、框架参数调整、rendezvous/恢复；fixture 的 feasible=true 不会创建 GPU |
| 无候选但没有探索 | enabled、mode/objective、baseline 是否合格、PVC/文件锁、预算、预留、worker 健康、是否已有探索 journal |
| 一直 more_windows_required | 是否重复同一文件，第二窗口是否在上次终点之后；至少两轮有效窗口 |
| 探索回退没有 ROLLED_BACK | 回退真实恢复及新的两个窗口、最新 checkpoint step、generation、预留和剩余时间 |
| INCONCLUSIVE 后仍在2卡 | 当前实现不在证据不足时强制回退；核对实际资源、checkpoint、预留并处理状态，不能直接删 journal |

## 11. 报告模板与测试结束

每个用例至少记录以下内容：

| 字段 | 内容 |
| --- | --- |
| 用例身份 | ID、执行时间、操作者、namespace、job name/UID、taskId |
| 版本 | 仓库 commit、Master/worker 镜像 digest、controller/Brain 版本 |
| 输入 | 最终 job YAML、env ConfigMap、实际加载的 scenario、每轮 scalar JSON、初始与目标 GPU |
| 关联 | plan_id、request_id、generation；探索另加 reservation_id 和 rollback_plan_id |
| 预期/实际 | evaluation status/reason、gate decision、公式估计值、探索终态 |
| 执行证据 | checkpoint 路径/step、前后 Pod UID、GPU 配置、恢复后的持续进度 |
| 结论 | 门禁是否通过、集群执行是否通过、未执行项/依赖和失败原因分别记录 |

归档最终日志和配置后，先将手动 ConfigMap 复位 idle，再停止本测试作业及 fixture：

```bash
kubectl apply -f "$MANIFEST_DIR/01-manual-idle.yaml"
kubectl delete elasticjob benefit-mock -n elastic-training --wait=true
kubectl delete deployment benefit-fixture -n elastic-training
kubectl delete service benefit-fixture -n elastic-training
```

探索用例先确认真实预留已释放或按约定 TTL 到期，保存 journal；不要通过清空状态目录消除失败记录。PVC 包含模型、数据和 checkpoint，以上清理命令保留该卷。

## 12. 文档校验记录

本次文档生成时已在本地完成：11 个 YAML 代码块共 14 个 Kubernetes 对象的 YAML 解析、必需标识检查、内嵌 HTTP 服务的 Python 语法检查、相对文件链接检查，以及将测试服务生成的 20 组场景快照传入当前 `evaluate()` 核对结果与表格一致。503 响应分支也已检查。

本次细化后额外校验了全部shell代码块的Bash语法和嵌入Python语法；直接运行仓库mock生成器及SwanLab窗口校验，确认100→160基线得到q0=1、R=3600，预热不足和过期样本被拒绝，E01/E02/E03两轮示例在时间与累计work上连续且吞吐符合表格。

调整手动测试时间后，重新用max_age=600验证20组普通收益场景；通过实际生成器和窗口校验确认生成5分钟后可用、601秒后过期，S04专用60秒有效期下65秒后过期。还以模拟时钟/执行证据直接调用当前Exploration.prepare/inspect，验证新预算准入、E01接受、E02请求回退并设置900秒回退预算、E03保留，以及elapsed=1790秒时E01仍满足收益阈值。这是本地状态机验证，不包含真实K8s回退执行。

这些校验未部署 Kubernetes，没有执行真实 HTTP 超时、checkpoint、GPU 调度或探索回退。当前本地 Python 环境未安装 pytest，未运行现有 pytest 回归集。集群结果按第 11 节逐项填写，不能将上述静态/函数级校验当作端到端通过。

相关实现与已有材料：

- [收益门禁计算](../../dlrover/python/master/resource/benefit.py)
- [SwanLab mock/稳态校验](../../dlrover/python/master/resource/swanlab.py)
- [受控探索状态机](../../dlrover/python/master/resource/exploration.py)
- [mock 生成器](../../examples/pytorch/resource_elastic/benefit_validation/generate_swanlab_mock.py)
- [整体流程说明](vertical-elastic-benefit-flow-cn.md)
- [HTTP 与探索契约](vertical-elastic-benefit-http-cn.md)
- [已有分层测试方案](vertical-elastic-benefit-validation-cn.md)
