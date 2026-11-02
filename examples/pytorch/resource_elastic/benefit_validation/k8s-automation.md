# 按场景自动执行 Kubernetes 收益测试

入口为 `run_k8s_cases.py`，场景来自 `docs/design/vertical-elastic-benefit-k8s-mock-test-cn.md`。
需要操作机 Python 3.8+、PyYAML、kubectl；Master 镜像须包含当前收益门禁及 SKIPPED 修复。
这里只提供脚本和本地验证，不代表任何集群已经通过验收。

## 1. 准备配置和 YAML

从仓库根目录执行：

```bash
python examples/pytorch/resource_elastic/benefit_validation/run_k8s_cases.py extract \
  --output /tmp/benefit-k8s-manifests
cp examples/pytorch/resource_elastic/benefit_validation/k8s-cases.example.yaml \
  /tmp/benefit-k8s-config.yaml
```

提取命令不覆盖已有文件。替换模板里的 `REPLACE_...`，配置 kubectl context、GPU 类型、实际训练日志的 `step_regex`、模型/数据/PVC 和已验证的训练入口。
`step_regex` 必须恰好有一个捕获组，捕获累计 optimizer/global step，不能匹配总步数、epoch 或 micro step。
脚本在两次日志采样间确认 step 增长，Pod Running 本身不能放行。
默认只支持一个 worker，与手册一致。非 Swift 的启动命令用 `worker_args` 显式提供；独立输出目录中应包含 `{run_id}`。

不要直接使用示例的集群占位值。示例 G01 默认复用命名空间、RBAC 和数据卷；首次搭建需要把相应项加回 G01 的 `infrastructure`。

## 2. 按场景选择 3.1 的 YAML

`infrastructure` 是严格有序列表，场景列表替换全局列表，不做追加，不自动补齐依赖。
未列出的 YAML 不会 apply。选用已有基础设施时，脚本仍检查 Master 的实际环境和 fixture 实际加载场景，避免误用旧配置。
空列表 `[]` 表示全部复用，但已有配置必须正好符合该场景。

```yaml
infrastructure:
  - 00-benefit-env.yaml
  - 00-fixture-scenario.yaml
cases:
  G01:
    infrastructure:
      - 00-namespace.yaml
      - 00-rbac.yaml
      - 00-storage.yaml
      - 00-benefit-env.yaml
      - 00-fixture-scenario.yaml
      - 00-fixture-code.yaml
      - 00-fixture-service.yaml
  G02:
    infrastructure: [00-benefit-env.yaml, 00-fixture-scenario.yaml]
  S04:
    infrastructure: [00-benefit-env.yaml, 00-fixture-scenario.yaml]
  E01:
    infrastructure: [00-benefit-env.yaml]
```

自定义文件有两种使用方式：

1. 用 `templates` 替换标准文件：例如 `templates: {00-storage.yaml: ./storage-npu.yaml}`。列表中仍写 `00-storage.yaml`。标准模板会调整 namespace；env/scenario 会叠加本场景输入。
2. 直接在 `infrastructure` 中写 `./extra-network-policy.yaml` 等路径。此类文件原样 apply，不修改 namespace、不叠加收益参数。

相对路径都以配置文件所在目录为基准。`02-job.yaml`、idle 和 trigger 也能通过 `templates` 分场景替换。
3.1 的选择只控制基础设施；脚本始终生成并部署本用例的 idle、ElasticJob 和 trigger。
E 系列把 8.2 的探索环境合并进 `benefit-test-env`，不必额外部署探索 ConfigMap，也不沿用普通 fixture。

## 3. 查看、生成和执行

```bash
# 列出 G01–G23、S01–S10、E01–E10。
python examples/pytorch/resource_elastic/benefit_validation/run_k8s_cases.py list

# 离线生成；不执行 kubectl，不连接集群。
python examples/pytorch/resource_elastic/benefit_validation/run_k8s_cases.py render \
  --config /tmp/benefit-k8s-config.yaml --cases G01,G02,S04 \
  --output /tmp/benefit-render

# 单场景执行；默认保留作业供检查。
python examples/pytorch/resource_elastic/benefit_validation/run_k8s_cases.py run \
  --config /tmp/benefit-k8s-config.yaml --cases G02 \
  --output /tmp/benefit-results

# 顺序执行整个 G/S 矩阵；每个用例归档后删除自己的 ElasticJob。
python examples/pytorch/resource_elastic/benefit_validation/run_k8s_cases.py run \
  --config /tmp/benefit-k8s-config.yaml --cases G,S --cleanup \
  --output /tmp/benefit-results
```

`run` 会真实修改 Kubernetes；放行场景会保存 checkpoint、替换 Worker。`render` 用于先检查具体 YAML。
多场景必须指定 `--cleanup`，确保每轮使用新 job_uid。脚本不删除已有作业，不删除 namespace、PVC、journal 或历史输出。
命名空间存在其他 ElasticJob 时拒绝执行，因为手动 ConfigMap 监听不按 job_names 隔离；请使用专用测试命名空间。
失败默认停止后续用例；`--continue-on-failure` 可继续，但清理不成功时下轮仍会被已有作业检查挡住。
中断或失败尽力归档并复位 idle；不带 `--cleanup` 时需按原手册归档、删除保留的测试作业后再跑下一轮。

## 4. 场景覆盖及断言

| 场景 | 自动操作与验收 |
| --- | --- |
| G01–G23 | 设置模式、目标、初始/目标 GPU、fixture；等待实际配置生效；生成新鲜基线；校验 decision/status/reason |
| 拒绝场景 | 终态必须 skipped；观察期内 UID/重启次数不变；真实 step 继续增长；无 checkpoint/替换阶段和 execution_started |
| 放行场景 | 必须出现本 plan 的 training_recovered；新 Worker 使用目标 GPU，UID 已变化，真实 step 继续增长 |
| S01 | 不生成文件，generation 建立后触发 |
| S02/S03 | 错 task ID／删除作业 label |
| S04 | 新 Master 的 max_age=60，先生成合格数据，再等65秒触发 |
| S05 | 10秒短窗口，不足最小稳态窗口 |
| S06 | 生成早于初始 Worker 创建时间的窗口，确定落在当前代次边界之前；避免靠启动速度碰巧命中预热期 |
| S07/S08 | 原子注入吞吐不稳定／删除 train_speed |
| S09/S10 | ingestion 时间源／候选 unavailable 且探索关闭 |
| E01–E03 | 对接真实服务后，等待 journal、真实恢复；提交两轮连续候选窗口；E02负收益仍保留候选2卡，不再提交回退窗口 |
| E04 | 300秒预算，要求240秒内进入 OBSERVING，不提供新窗口，等待 INCONCLUSIVE |
| E05–E07 | 真实服务准备对应拒绝条件，通用拒绝断言及服务侧验收 |
| E08–E10 | 环境相关故障由 drive_fault hook 执行，verify_adapter 必须提供对应真实证据；不能仅凭脚本退出0算通过 |

`total_steps: 1044` 自动替换 G03/G13/G14 的 fixture 参数。普通场景仍使用100→160，探索使用独立107260轨迹；不更改真实训练的总步数。
E01/E02/E03 的决策还依赖实际切换耗时，未达到预期会报告 FAIL，不将不同终态当成通过。
基线预热等待期间若 Pod UID/重启次数变化则中止；无法识别的同 Pod 内训练代次变化仍由 Master 的真实窗口校验拒绝。

## 5. 探索服务与 hook 契约

普通fixture不能提供真实调度授权和checkpoint恢复证据。E 场景未配置 `adapter_url`、`prepare_adapter`、`verify_adapter` 时会在集群变更前报告 `NOT_RUN`，退出非零。
E08–E10 还需要 `drive_fault`。hook 为本地 argv 数组，不经 shell 展开：

```yaml
adapter_url: http://real-adapter:8080/benefit/snapshot
hooks:
  prepare_adapter: [python3, ./adapter_hooks.py, prepare]
  verify_adapter: [python3, ./adapter_hooks.py, verify]
  drive_fault: [python3, ./adapter_hooks.py, fault]
```

hook 读取环境变量 `BENEFIT_TEST_CONTEXT` 指向的 JSON，字段为 case、namespace、job_name、job_uid、master、plan_id、artifact_dir。
`prepare_adapter` 在新 job_uid 建立后、触发前执行，设置手册中的observe_only授权/成本/约束或预定故障；E02准入及观测均须max_delay_seconds=20000，无deadline。此时 master/plan_id 可能为空。
`drive_fault` 在门禁返回 explore 后执行：E08 在 OBSERVING 重启并完成恢复观测；E09 在 SWITCHING 中断并核验保护；E10 配合 prepare 阶段预置授权拒绝并核验未替换。需要抢占瞬时状态的故障必须在 prepare 中预置，不能等轮询碰巧命中。
脚本不猜测 controller 的重启方式，不自动删除 Master，也不改 journal。E08–E10 的故障执行及相应检查由集群适配 hook 负责。

`verify_adapter` 保存真实服务/资源/checkpoint 证据后，写入 `<artifact_dir>/adapter-verification.json`，退出0：

```json
{
  "case": "E01",
  "job_uid": "与上下文一致",
  "plan_id": "与上下文一致",
  "checks": {"result_recorded": true, "candidate_retained": true, "no_reverse_plan": true},
  "evidence": ["adapter-response.json", "worker-resource-evidence.json"]
}
```

不可用硬编码 true 替代真实检查。脚本严格校验身份、必需 checks 和 evidence 非空：

| 场景 | 必需 checks |
| --- | --- |
| E01/E03 | result_recorded, candidate_retained, no_reverse_plan |
| E02 | result_recorded, candidate_retained, no_reverse_plan |
| E04 | fenced, result_recorded, no_reverse_plan |
| E05 | admission_rejected |
| E06/E07 | no_exploration_started |
| E08 | journal_recovered, no_duplicate_replacement, training_recovered |
| E09 | fenced, no_mutation_replayed, checkpoint_resources_checked |
| E10 | authorization_denied, no_worker_replacement, checkpoint_resources_checked |

E04的INCONCLUSIVE与FAILED是保护状态，也会回传。脚本要求reported=true；服务须幂等记录，即使result为空。candidate_retained和no_reverse_plan须依据实际worker/计划记录核验，不能由状态字符串推断。

## 6. 产物和本地验证

每个场景独立目录含渲染 YAML、plan.json、result.json、实际 Master 环境、输入 scalar、门禁日志、Worker 前后状态、Master/Worker/fixture 日志、Kubernetes events；探索额外保存 journal 和 hook 日志。
顶层 summary.json（带运行时间与随机后缀）汇总 PASS/FAIL/NOT_RUN/RENDERED，FAIL 或 NOT_RUN 返回非零。超时不会算通过；归档失败写 archive-errors.json。

不连接集群的测试：

```bash
python -m pytest -q examples/pytorch/resource_elastic/benefit_validation/test_k8s_automation.py
```
