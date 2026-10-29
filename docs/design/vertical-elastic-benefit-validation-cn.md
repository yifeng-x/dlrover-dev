# 垂直弹性收益体系测试验证方案

本文对应当前仓库实现，提供本地逻辑验证、HTTP 接口联调、Kubernetes 真实训练测试和集群收益对照实验。配置示例以单 worker、每 worker 4 → 8 张同类 GPU 为例；实际部署按资源档位替换。

## 1. 验证范围与通过条件

| 层次 | 要回答的问题 | 通过条件 |
| --- | --- | --- |
| 计算正确性 | 相同输入是否产生正确收益与决策？ | 固定数据集的时间、卡时、集群 JCT 结果与手算一致 |
| 数据正确性 | SwanLab 数据是否可用于计算？ | taskId、key、采样批次、generation、窗口和进度校验通过；错误数据不被放行 |
| 控制正确性 | 拒绝、调整、探索和回退是否符合判断？ | enforce 拒绝发生在 checkpoint 前；探索仅在明确缺候选吞吐且准入通过时执行 |
| 可观测性 | 能否还原一次调整？ | 事件关联身份、资源四态、收益依据和执行结果；控制完成与训练恢复可区分 |
| 实际收益 | 相同工作量或同一任务集合是否确实改善？ | 实测完成时间、资源积分及集群 JCT/任务吞吐满足预设目标，训练质量不退化 |

前四项通过不能代替第五项。模拟数据只能验证程序行为，不能证明生产收益。`positive` 是预测结论；`ACCEPTED` 是基于实测吞吐的剩余收益投影；`RETAINED` 和 `INCONCLUSIVE` 均不能计为已实现正收益。

当前实现不提供独立收益数据库、看板、完整集群调度模拟器或跨重启收益结算账本。实际对照实验需从训练、调度和日志平台导出数据进行核算。

## 2. 测试材料与前置条件

可直接复制修改的 YAML：

- [Master 收益与 SwanLab 配置](../../examples/pytorch/resource_elastic/benefit_validation/master-config.yaml)。
- [可选受控探索配置](../../examples/pytorch/resource_elastic/benefit_validation/exploration-config.yaml)。
- [手动资源调整触发](../../examples/pytorch/resource_elastic/benefit_validation/manual-scale.yaml)。

这些文件是 ConfigMap，不是完整训练任务。将配置合并进本环境已经跑通的 ElasticJob；参考现有 [手动垂直弹性任务](../../examples/pytorch/resource_elastic/dev/swift_sft_manual_resource_elastic.yaml)，保留本环境镜像、模型、数据集、启动命令、卷和 ServiceAccount，不直接照抄其中的内网地址。

真实训练测试前确认：

1. 集群已安装 ElasticJob CRD/controller、当前版本 Master/Agent 和可生成垂直资源计划的 Brain；调度器可分配目标档位。单节点每 worker 4 → 8 卡需要相应节点容纳 8 卡，不能仅检查全局空闲卡数。
2. 训练框架支持当前 checkpoint、参数调整与恢复协议；检查点可在两个档位加载。训练进度保留 optimizer step，实际 global batch size 在调整前后保持一致。
3. Master 能访问两个 HTTP 服务。收益证据服务不是仓库内自动部署的服务，必须由环境提供；SwanLab 接口只提供训练数据。
4. 使用独立测试命名空间 `dlrover-benefit-test`，其中只启用一个手动监听作业。**当前 Master 回调没有按 `job_names` 过滤；该字段不能隔离触发对象。** Brain 手动优化器读取固定名称 `brain-manual-scale-config`，仅修改 Master 监听名称不能完整隔离链路。
5. 探索额外要求真实回退资源预留和持久化 POSIX 卷，验证跨 Pod 文件锁及原子 rename；不能用 emptyDir，不能用返回 `confirmed=true` 的模拟接口冒充实际预留。
6. 保存测试代码提交号、镜像 digest、配置、GPU 型号、并行策略、训练数据版本和时钟同步状态。HTTP token 通过既有 Secret 注入，不写入测试报告。

## 3. 第一阶段：无需 GPU 的逻辑回归

在已安装项目测试依赖的 Python 环境，从仓库根目录运行：

```bash
mkdir -p /tmp/dlrover-benefit-validation
PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider \
  dlrover/python/tests/test_vertical_benefit.py \
  dlrover/python/tests/test_swanlab_metrics.py \
  dlrover/python/tests/test_training_generation.py \
  dlrover/python/tests/test_vertical_exploration.py \
  dlrover/python/tests/test_vertical_events.py \
  dlrover/python/tests/test_vertical_execution_metrics.py \
  dlrover/python/tests/test_vertical_recovery_metrics.py \
  dlrover/python/tests/test_job_resource_scaler.py \
  dlrover/python/tests/test_elasticity_policy.py \
  --junitxml=/tmp/dlrover-benefit-validation/junit.xml
```

环境缺依赖属于测试环境未准备好，不能记录为测试通过。这些用例使用模拟 HTTP、节点状态和可控时钟，不要求真实 Kubernetes/GPU，也不代表端到端测试完成。

### 3.1 固定输入与手算基准

输入：当前 4 卡，候选 8 卡；窗口 60 秒完成 60 step；当前累计 160 step，总目标 3760 step；候选吞吐 2 step/s；无有效进展时间 120 秒；重放量 0；切换资源消耗 960 卡秒；允许延后 1800 秒。

| 项目 | 计算 | 期望值 |
| --- | --- | --- |
| 当前吞吐 q0 | 60 / 60 | 1 step/s |
| 剩余工作量 R | 3760 − 160 | 3600 step |
| 基线剩余时间 T0 | R / q0 | 3600 秒 |
| 候选剩余时间 T1 | 120 + R / 2 | 1920 秒 |
| 时间收益 | T0 − T1 | 1680 秒，46.67% |
| 基线卡时 H0 | 4 × 3600 / 3600 | 4 GPU·h |
| 候选卡时 H1 | (960 + 8 × 1800) / 3600 | 4.266667 GPU·h |
| 卡时收益 | H0 − H1 | −0.266667 GPU·h |

同一配置在 `time` 目标下应为 positive，在 `gpu_hours` 下应为 negative。用以下独立片段调用当前计算函数，冻结时间避免固定日期变成过期数据：

```python
from copy import deepcopy
from math import isclose
from dlrover.python.master.resource.benefit import evaluate

now = 1000
request = {
    "schema_version": 1, "request_id": "validation-1",
    "job": {"job_uid": "validation-job"}, "generation": "123",
    "current": {"workers": 1, "gpus_per_worker": 4, "gpu_type": "nvidia.com/gpu"},
    "target": {"workers": 1, "gpus_per_worker": 8, "gpu_type": "nvidia.com/gpu"},
}
snapshot = {
    **deepcopy(request), "observed_at": now,
    "training": {
        "unit": "steps", "comparable": True, "steady": True,
        "global_batch_size": 64, "target_work": 3760,
        "start": {"generation": "123", "observed_at": 940, "work": 100},
        "end": {"generation": "123", "observed_at": 1000, "work": 160},
    },
    "candidate": {"throughput_lower": 2, "evidence": "profile", "evidence_id": "fixture-1"},
    "transition": {"lost_seconds_upper": 120, "replay_work_upper": 0, "gpu_seconds_upper": 960},
    "scheduling": {"target_feasible": True, "constraints_satisfied": True},
    "limits": {"max_delay_seconds": 1800, "deadline_at": None},
}
time_result = evaluate(snapshot, request, now, objective="time")
gpu_result = evaluate(snapshot, request, now, objective="gpu_hours")
assert time_result.status == "positive"
assert time_result.estimates["time_gain_seconds"] == 1680
assert gpu_result.status == "negative"
assert isclose(gpu_result.estimates["gpu_hours_saved"], -4 / 15)
print(time_result.to_dict())
print(gpu_result.to_dict())
```

集群算例：8 → 4 卡，q0=1、q1=0.75、R=3600，切换 120 秒；本任务延后 1320 秒。一个受影响任务从 6000 秒变为 2520 秒，各权重均为 1，则集群收益为 `−1320 + 3480 = 2160` 秒，归一化改善为 `2160 / (3600 + 6000) = 22.5%`。单任务时间为 negative，`cluster_jct` 为 positive。队列集合必须明确完整，不能把未知影响填为零。

## 4. 第二阶段：HTTP 与训练指标联调

### 4.1 训练端上报

在 global rank 0、optimizer step 完成后，与已有 SwanLab 日志合并为一次上报：

```python
from dlrover.python.common.training_metrics import training_metric_payload

metrics = training_metric_payload(
    global_step=global_step,
    total_steps=total_steps,
    global_batch_size=actual_global_batch_size,
    steady=restore_replay_and_warmup_finished,
)
swanlab.log(metrics)
```

变量由真实训练循环提供。generation 来自 Agent 设置的 `DLROVER_TRAIN_GENERATION`，不能在真实测试中固定为示例值。恢复、重放和预热期间 steady=false。若训练期间 global batch 改变，当前 steps 适配链路不能直接比较前后收益，应记录不可比并调整测试训练配置。

### 4.2 请求与返回检查

```bash
curl --fail-with-body -X POST \
  'http://REPLACE-METRICS-SERVICE/metrics/scalar/by-task-name' \
  -H 'Content-Type: application/json' \
  --data '{"taskId":"REPLACE-TASK-ID","taskScalarList":[{"key":"train/global_step"},{"key":"train/total_steps"},{"key":"train/global_batch_size"},{"key":"train/generation"},{"key":"train/steady"}]}'
```

有鉴权时按环境安全方式增加认证头。返回必须为按 key 分组的数组，每项包含对应 taskId/key/scalarMetricsList；每个点包含 data/createTime/epoch/index。同批所有 key 的 `(epoch, createTime)` 应相同，index 可不同，epoch 不等于 generation。

默认使用 createTime 作为训练采样时间，时区为 +08:00。如 createTime 是入库时间，需将配置中的 observed_at 映射到 `train/observed_at`，并确保训练端同批上报该 key。

验证顺序：

1. 第一次 rendezvous 后获取当前 generation 的完整数据；至少两个稳态采样点，间隔 ≥30 秒，窗口末端距当前 ≤60 秒。
2. 检查全局步数单调增加、总步数和实际 global batch 不变；剩余工作量为总步数减窗口末端步数。
3. 调整资源后 generation 改变，新窗口只使用新 generation；旧代次即使时间很近也不能混入。
4. 修改 work key 为 `trainer/step`，训练上报与 Master 映射同时修改，应正常获取。只改一侧应 unknown，不能补零。
5. 用测试接口代理或既有测试服务注入以下异常；不要污染生产训练数据。

| 注入条件 | enforce 期望 | 证据 |
| --- | --- | --- |
| taskId 不一致、缺 key、重复冲突 key | unknown，拒绝调整 | evaluated.reason_code 与接口日志 |
| 同批缺点、createTime/epoch 对不齐 | unknown，拒绝调整 | 不按最近时间拼接 |
| 旧 generation、代次混合、进度回退 | unknown，拒绝调整 | 不产生有效吞吐 |
| 窗口过短、末端过期、steady=0 | unknown，拒绝调整 | 等待后续完整稳态窗口 |
| NaN/Infinity、非法计数 | unknown，拒绝调整 | 事件仍可序列化，非法数值不隐藏拒绝结果 |
| 超时、非 200、非法 JSON | unknown，拒绝调整 | 不使用收益服务里的旧 training 兜底 |

### 4.3 收益证据服务

完整接口见 [HTTP 契约](vertical-elastic-benefit-http-cn.md)。服务响应需匹配本次 request_id/job/current/target，提供 candidate、transition、scheduling、limits 和新鲜 observed_at。启用 SwanLab 后 training 中仍需提供真实核实的 comparable=true，其余训练字段由 Master 本次 scalar 观测覆盖。

隔离的模拟测试可用第 3.1 节 snapshot 作为响应模板，按本次请求回填身份与资源，并使用受控时钟/实时生成的模拟采样序列；固定日期样本不能直接用于实时 Master。实际训练测试不得刷新旧观测时间伪装新数据，也不得简单回显请求就宣称 checkpoint 已恢复、资源已预留。

探索测试需另外覆盖三类请求：资源替换前 `authorize_mutation=true`；观测时真实 execution、reservation 和 trial_gpu_seconds；终态 `release_reservation=true` 的幂等入库及释放。没有这些能力，只验收普通门禁，探索标记为“未具备测试条件”。

## 5. 第三阶段：Kubernetes 配置与执行

### 5.1 部署配置

先替换 YAML 中的服务地址、taskId、命名空间和资源规格，再执行。创建 namespace 本身不会安装 controller、Brain 或 RBAC，它们必须已能管理测试命名空间。

```bash
kubectl create namespace dlrover-benefit-test --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f examples/pytorch/resource_elastic/benefit_validation/master-config.yaml
```

将以下结构合并到现有 ElasticJob 对应位置；这是**合并片段，不可单独 apply，也不要用 JSON merge patch 替换整个 containers 数组**。保留原 Master command、镜像、资源和其他 env：

```yaml
spec:
  replicaSpecs:
    dlrover-master:
      template:
        spec:
          containers:
            - name: main
              envFrom:
                - configMapRef:
                    name: benefit-validation-env
```

Master 原启动命令应包括 `--elastic_dimension vertical --configmap_manual_scale_switch on --metrics_port 9090`，namespace/job_name 与实际作业一致。已有显式 env 的同名值优先于 envFrom，应移除冲突配置；训练端自定义 key 时同步配置 worker。

配置在进程启动时读取。更改 ConfigMap 不会热更新正在运行的 Master；普通场景换用新的测试作业，探索重启场景保留原作业身份及 journal。不要误把配置变化当成已经生效。

### 5.2 开启受控探索的附加片段

确认预留服务与卷准备好后应用 exploration-config.yaml，并合并以下内容。`REPLACE-POSIX-PVC` 是当前命名空间已绑定、具备可靠锁语义的持久卷，不提供虚构 StorageClass：

```yaml
spec:
  replicaSpecs:
    dlrover-master:
      template:
        spec:
          volumes:
            - name: benefit-state
              persistentVolumeClaim:
                claimName: REPLACE-POSIX-PVC
          containers:
            - name: main
              envFrom:
                - configMapRef:
                    name: benefit-validation-env
                - configMapRef:
                    name: benefit-validation-exploration-env
              volumeMounts:
                - name: benefit-state
                  mountPath: /var/lib/dlrover/benefit
```

后一个 envFrom 覆盖前一个同名值。示例预算 600 秒、10 GPU·h；准入响应若给出回退时间 300 秒，允许延后至少覆盖 900 秒，当前剩余训练时间也需大于 900 秒，预留 TTL 覆盖两者并留出余量。真实预算按框架恢复耗时设置。

每个 job_uid 最多一次探索、一次回退。独立探索用例创建新测试作业以获得新 UID；重启恢复测试必须使用原 UID 和原持久目录，不能删除 journal 来重试。

### 5.3 触发与收集

初始 worker 为 4 卡，CPU=4、内存=8Gi（或同步修改目标 YAML 与任务）。先等待接口正常及稳态窗口形成，再应用手动触发文件：

```bash
kubectl apply -f examples/pytorch/resource_elastic/benefit_validation/manual-scale.yaml
kubectl get pods -n dlrover-benefit-test -o wide
```

该 ConfigMap 的 ADDED/MODIFIED 事件会触发调整，文件特意包含 `scale_action: manual`。目标 gpus 是**每个 worker 的卡数**，memory 是 GiB。探索时 CPU/内存/worker 数/GPU 资源类型均保持不变。相同 YAML 重复 apply 无变化时可能不产生事件，不将其作为重试机制；watch 重连可能再次收到事件，检查实际 request_id 与 busy/状态保护。

新用例启动前，将遗留 ConfigMap 的 config.json 中 scale_action 改为 `not_adjust` 并应用，防止监听初始化时立即调整；训练稳定后再改为 manual。探索测试不在观察期修改触发配置。

在各终端使用真实 Master Pod 名：

```bash
kubectl logs -n dlrover-benefit-test REPLACE-MASTER-POD -c main --timestamps \
  > /tmp/dlrover-benefit-validation/master.log
kubectl get pods -n dlrover-benefit-test -o json \
  > /tmp/dlrover-benefit-validation/pods.json
kubectl port-forward -n dlrover-benefit-test pod/REPLACE-MASTER-POD 19090:9090
```

在另一终端抓取指标，触发前后各保存一份：

```bash
curl --fail http://127.0.0.1:19090/metrics \
  > /tmp/dlrover-benefit-validation/metrics.prom
rg 'VerticalElasticEvent' /tmp/dlrover-benefit-validation/master.log
rg 'dlrover_master_vertical_' /tmp/dlrover-benefit-validation/metrics.prom
```

持续保存 Pod watch/调度器分配与释放记录、worker 日志及 SwanLab 原始采样，单次 pods.json 只能检查某一时刻状态，不能计算全程资源积分。Master 重启时一并保留旧容器/旧 Pod 日志与指标，计数器为进程内值，不直接跨重启相减。

## 6. 场景与验收矩阵

### 6.1 普通收益门禁

每项使用新作业或确认上次调整已完整终结；模式/目标配置在创建 Master 前设置。

| 编号 | 设置与动作 | 期望行为 |
| --- | --- | --- |
| G01 | off；证据服务不可达；触发有效计划 | 不调用收益接口，benefit 为 not_evaluated；继续原有资源调整，执行本身仍可能失败 |
| G02 | shadow；有效配置；模拟负收益/HTTP 超时 | 分别记录 negative/unknown、decision=allow；继续执行 |
| G03 | enforce/time；第 3.1 节正收益证据 | positive/allow；执行 checkpoint、替换、恢复 |
| G04 | enforce/gpu_hours；相同证据 | negative/reject；checkpoint 前 skipped，worker UID/资源不因该计划改变 |
| G05 | enforce；候选缺失；探索关闭 | unknown/reject；不替换 worker |
| G06 | enforce；截止时间不可满足或调度不可行 | negative/reject；对应 deadline/target_infeasible 等原因 |
| G07 | enforce；身份不匹配、证据过期、训练不可比 | unknown/reject，不进入探索 |
| G08 | cluster_jct；第 3.1 节队列算例 | 集群目标 positive；删除 impacts_complete 或重复 job_id 后拒绝 |
| G09 | 参数调整已完成但没有可信新进度 | 有 control_completed，恢复待定；超时 finished=unverified，不能记为训练恢复成功 |

G03/G04 中的固定数据仅用于模拟联调。真实训练时使用实际 q0/R/q1/开销计算对应预期，不要求真实任务恰好达到示例数值。

### 6.2 受控探索

| 编号 | 设置与测试动作 | 期望行为 |
| --- | --- | --- |
| E01 | enforce，探索开启，candidate 明确 unavailable，其他证据与预留完整 | unknown/explore；记录基线，持久化状态后执行调整 |
| E02 | 候选字段缺失、接口故障或数据失效 | 不满足 candidate_unavailable 条件；拒绝，不试跑 |
| E03 | 预留不确认、预算不足、非相邻 GPU 档位或 checkpoint 不兼容 | 准入失败，checkpoint 前结束 |
| E04 | 调整后至少两个独立稳态窗口，整体收益超过阈值 | ACCEPTED；记录实测 q1 与整体收益，保留目标配置并回传结果 |
| E05 | 新配置较差，回退未来代价更低且满足预算/期限 | 仅一次回退；新 plan_id，root/parent 关联；保存当前新 checkpoint，恢复原资源并验证新代次，最终 ROLLED_BACK |
| E06 | 整体收益不达标，但回退更贵，继续仍满足约束 | RETAINED；不宣称正收益，也不进行无收益回退 |
| E07 | 重复窗口、旧代次、预热数据或尚未越过 checkpoint | 不增加有效观测次数；有效数据到来前等待，耗尽预算后 INCONCLUSIVE |
| E08 | 观测期预留失效或资源预算耗尽 | INCONCLUSIVE；停止自动调整，不凭空承诺安全回退 |
| E09 | 回退 checkpoint/执行失败 | FAILED；不无限重试，不绕过保护强删训练进程 |
| E10 | OBSERVING / VERIFYING_ROLLBACK 时重启 Master | 使用原 journal 恢复观测；重新核实代次、执行和预留；不能猜测旧数据属于当前代次 |
| E11 | SWITCHING / ROLLING_BACK 时中断 | 防护状态阻止自动重放资源变更，保留现场供核对 |
| E12 | 未完成探索时关闭开关并重启 | 原状态保护仍生效，不恢复自动观测，也不清除 journal |
| E13 | 终态回传失败后恢复服务 | 保留本地结果，后续进入/重启重发；服务按探索 ID 幂等存证和释放预留 |

确定性分支可运行现有 `test_vertical_exploration.py` 对应用例，其中 `test_negative_measurement_rolls_back_once_and_verifies_new_checkpoint`、`test_sunk_cost_does_not_force_harmful_rollback` 分别覆盖回退和保留。真实集群测试可以用独立测试训练负载控制新配置下的计算/等待开销，但必须保持有效工作量和训练语义可比，不能修改标量来伪造性能；人为限速结果单独标记为故障注入实验。

### 6.3 事件与指标核对

以 [事件契约](vertical-elastic-events-cn.md) 为准：

1. requested 用 request_id 关联计划，不要求日志严格按阶段到达；被 busy 拒绝的请求不能关联正在运行的计划。
2. evaluated 同时保存 evaluation_status 与 decision；shadow 下 negative/allow、探索下 unknown/explore 均是合法组合。
3. 普通成功计划：control_completed 后确认进度，再有 training_recovered 和 finished。探索可能由稳态窗口验证恢复，不强制要求每条成功探索都有原生进度事件。
4. 资源包含 before/target/actual_after/final。目标不是实测；缺节点或混合配置时观测为未知，不能用 target 填补 final。
5. 回退原计划 finished=rollback_requested、final=null；回退子计划使用共同 root_plan_id，parent_plan_id 指向原计划，确认后记录最终资源。
6. 正常单进程内每个计划上下文只产生一次 finished。日志平台按 event_id 去重；重启可能产生不同 ID 的同业务状态事件，不能宣称全链路 exactly-once。
7. 比较 `benefit_evaluations_total`、`plans_finished_total`、`recovery_total`、`explorations_total` 与对应日志。plans_finished 表示控制流程终态，事件 finished 可能等恢复/探索完成才出现；二者时间与数量口径不必逐条同步。
8. Prometheus 不增加 plan_id/request_id 等高基数标签。日志写入失败不能改变资源控制决策；模拟 sink 故障由 `test_reporting_failure_does_not_break_metrics_or_controller` 验证。

## 7. 第四阶段：真实收益对照实验

### 7.1 单任务

从相同初始 checkpoint、模型、数据顺序和总训练目标运行两组：A 固定资源；B 开启收益门禁及所需探索。A 必须关闭自动和手动资源调整，**不能用 benefit mode=off 代替固定资源基线**，因为 off 仍允许原有弹性执行。

前后实际 global batch size 一致，GPU 型号/资源配额/其他负载受控，记录质量指标。建议至少 3 组成对重复，交替执行 A/B，报告每组和中位数；波动较大时扩大样本，不能只挑正收益组。

计算：

```text
训练总耗时 = 同一训练起点至固定有效工作量完成的墙钟时间
任务 JCT = 完成时间 - 提交时间（含排队；不要与纯训练耗时混用）
实际卡时 H = Σ[该区间分配卡数 × 区间秒数] / 3600
时间改善率 = (T_A - T_B) / T_A
卡时改善率 = (H_A - H_B) / H_A
预测误差 = 预测剩余完成时间 - 从同一预测时刻到实际完成的时间
```

资源积分覆盖暂停、恢复、重放、新旧 worker 重叠；预留资源消耗另记并与分配资源去重。GPU 利用率不能代替资源占用。异构 GPU 分型号核算，如需货币成本再乘各型号实际费率。

同一逻辑训练进度重放不重复计产出；不能累加每个 generation 的 step 增量作为总有效工作量。目标不一致、global batch 改变或提前终止的运行，不能直接比较总耗时与卡时并声称收益。

预先约定成功标准，例如时间目标改善≥5%、卡时目标改善≥5%，同时验证 loss/验证集质量、任务成功率及 deadline。程序阈值只约束预测/探索决策，不保证最终实测必达该值。

### 7.2 集群

固定相同 GPU 型号与数量、同一提交时间表、同一任务集合/工作量/优先级，A 采用固定资源策略，B 采用收益策略；控制其他租户干扰。所有受影响运行中和排队任务都纳入，不能只看发生弹性的作业。

| 指标 | 核算方式 | 注意事项 |
| --- | --- | --- |
| 平均/加权 JCT | 对相同任务集合统计完成−提交；权重实验前固定 | 纳入排队；同时报告 P95 与长任务变化 |
| 任务吞吐 | 固定观察窗内成功完成任务数 / 观察窗小时数 | 任务组合必须可比，同时报告失败和未完成任务 |
| 批次完成时间 | 最后任务完成−实验起点 | 适合固定任务集合，不必强行使用固定时长窗口 |
| 集群卡时 | 全部任务与独占预留的去重资源积分 | 不仅统计触发弹性的作业 |
| 时限与质量 | deadline 违约率、失败率、训练质量 | 不通过降低训练目标增加完成任务数 |

有限观察窗内未完成的任务不能从 JCT 报告中悄悄删除。优先让固定任务集合全部完成；否则报告未完成比例/进度，JCT 标记为不完整。`cluster_jct` 当前是队列情景预测，最终集群任务吞吐改善需上述实验验证，不能从单次 positive 直接推出。

## 8. 结果交付与验收记录

每个用例保存独立目录：配置 YAML（脱敏）、镜像/提交信息、JUnit、原始 scalar、证据请求响应（脱敏）、Master/worker 日志、Pod 生命周期/分配记录、指标快照、探索 journal 副本、对照计算表。journal 副本只读存档，不作为运行状态覆盖回写。

建议记录表：

| 用例 | job_uid / taskId | root_plan_id / plan_id | 输入与目标 | 预期 | 实际 | 原始证据路径 | 结论 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| G03 | 待填 | 待填 | time，4→8 | allow、恢复确认 | 待填 | 待填 | 未执行 |
| E05 | 待填 | 原计划/回退计划 | 负向观测且回退有利 | 一次回退、ROLLED_BACK | 待填 | 待填 | 未执行 |

最终报告分别列出：逻辑回归结论、接口联调结论、真实资源调整结论、探索故障恢复结论、实测单任务/集群收益结论。未部署的服务、未验证的预留能力、未执行的 GPU 实验写“未验证”，不以本地测试通过代替。

测试结束先核实任务终态、回退预留已释放和 journal 状态，再按环境流程清理测试资源；保留异常现场与原始证据。本文仅提供测试材料，不会自动部署或触发实际资源调整。
