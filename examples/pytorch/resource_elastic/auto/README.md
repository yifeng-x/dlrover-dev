# 启动时自动选择弹性维度

Master、Worker 均使用包含本次改动的镜像。该功能使用 Python PodScaler，Master 必须指定 `--platform pyk8s --elastic_dimension auto`。默认仍是 horizontal；不要同时填写与自动结果冲突的两个开关。

本目录两个 YAML 是配置片段，需合并到提交前的完整 ElasticJob，保留现有镜像、训练命令、挂载、服务账号和 Master 其他参数。不要向正在训练的作业热更新这些片段。

| 文件 | Worker 配置 | 自动结果 | Worker 启动要求 |
| --- | --- | --- | --- |
| `single-worker.yaml` | 1×4 GPU | vertical/off/on | `dlrover-run --nnodes=1 --nproc_per_node=4 ...`；支持 checkpoint 保存、恢复及卡数变化 |
| `multi-worker.yaml` | 2×4 GPU | horizontal/off/off | `dlrover-run --nnodes=1:2 --nproc_per_node=4 ...`；支持成员变化 |

自动垂直必须设置 `spec.optimizeMode: cluster` 并连接可用的 Brain。共享 checkpoint 存储和训练侧状态恢复需沿用已验证的配置。本功能不会给任意训练脚本自动添加 checkpoint 能力。

每实例只接受 1～8 卡。支持 `nvidia.com/gpu`（Master `--xpu_type nvidia`）和已适配的 `huawei.com/Ascend910B3`（`--xpu_type ascend`）；不适用于共享 GPU/MIG、PS、Ray 或 Workspace。多副本即使总卡数不超过 8，也选择水平模式。

启动自适应需要 ElasticJob get/patch 和任务命名空间内的 Pod list 权限，其他权限沿用既有训练执行链路。本功能不再为容量预检新增节点读取或跨命名空间 Pod list 需求。自定义服务账号需核对绑定。

检查启动日志中的 `Startup elasticity policy`，以及 ElasticJob 的 `dlrover.io/startup-elastic-policy` 注解。注解只保存版本、UID、维度及两个开关。Master 重启校验这五项；资源来源等诊断字段变化不会导致恢复冲突，旧版完整注解仍可读取。最终控制参数冲突会明确失败。不要删除运行中任务的决策注解。

扩卡继续复用既有 Brain 计划及 Kubernetes 调度，本功能不新增节点容量预检。资源不足时替换 Pod 仍可能 Pending，需沿用并验证既有超时与恢复流程；上线前应在目标集群验证扩卡、checkpoint 恢复和 Master 重启。
