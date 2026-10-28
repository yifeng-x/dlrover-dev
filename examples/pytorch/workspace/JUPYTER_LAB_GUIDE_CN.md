# DLRover Workspace Jupyter Lab 使用指引

本文档面向已经进入 DLRover Workspace Jupyter Lab 的用户。下面命令建议在
Jupyter Lab 的 Terminal 中执行。

## 1. 查看 Workspace 状态

查看所有 worker Pod 和当前训练进程状态：

```bash
dlrover-workspace status
```

查看更详细的 Pod、IP、Node 等调试信息：

```bash
dlrover-workspace status --cli-log on
```

只查看指定 worker：

```bash
dlrover-workspace status --ranks 0,1
```

## 2. 默认模式：不使用 DLRover 自动组网

默认情况下，`dlrover-workspace` 只负责把命令提交到目标 worker Pod 中运行，
不会自动包装 `dlrover-run`，也不会改写用户命令。

```bash
dlrover-workspace launch --framework-cmd \
  "python examples/pytorch/mnist/cnn_train.py --num_epochs 5"
```

适用场景：

- 单机或单 worker 调试。
- 用户脚本自己管理分布式启动。
- 希望命令严格按原样执行。

## 3. 全部节点使用 DLRover 自动组网

如果希望 workspace 中全部 worker Pod 参与同一个 DLRover 分布式训练，使用
`--network-mode all`。

```bash
dlrover-workspace launch \
  --network-mode all \
  --dlrover-run-args "--nnodes=\$NODE_NUM --nproc_per_node=1 --max_restarts=3" \
  --framework-cmd "python examples/pytorch/mnist/cnn_train.py --num_epochs 5"
```

说明：

- `--network-mode all` 表示选择全部 worker Pod 参与组网。
- `$NODE_NUM` 会由 workspace CLI 设置为当前 workspace 的 worker 总数。
- `--framework-cmd` 是用户原始训练命令。
- `--dlrover-run-args` 是传给 `dlrover-run` 的启动参数。

如果需要指定更多 DLRover 启动参数，继续放到 `--dlrover-run-args` 中：

```bash
dlrover-workspace launch \
  --network-mode all \
  --dlrover-run-args "--nnodes=\$NODE_NUM --nproc_per_node=8 --max_restarts=3 --network-check" \
  --framework-cmd "python train.py --epochs 10"
```

## 4. 部分节点使用 DLRover 自动组网

如果只希望部分 worker Pod 参与 DLRover 分布式训练，使用
`--network-mode ranks`，并通过 `--ranks` 指定 worker rank。
rank 是 worker 的系统标识；如果不确定实例与 rank 的对应关系，先执行：

```bash
dlrover-workspace status --cli-log on
```

输出中的 `[workspace]` 区域会展示 `rank`、`pod`、`namespace`、`ip` 和
`node`，用于确认要选择的实例。

```bash
dlrover-workspace launch \
  --network-mode ranks \
  --ranks 0,1 \
  --dlrover-run-args "--nnodes=\$NODE_NUM --nproc_per_node=1 --max_restarts=3" \
  --framework-cmd "python examples/pytorch/mnist/cnn_train.py --num_epochs 5"
```

说明：

- `--ranks 0,1` 表示只选择 rank 0 和 rank 1 两个 worker。
- `--ranks` 不能重复；指定的 rank 必须全部存在，且对应 Pod 必须为
  `Running`，否则本次 launch 会失败。
- `$NODE_NUM` 会被设置为选中 worker 数量，这里是 2。
- workspace CLI 会在本次 launch 中把选中的原始 rank 临时映射为连续 rank，
  避免原始 rank 不连续影响 rendezvous。

例如只使用 rank 2 和 rank 3：

```bash
dlrover-workspace launch \
  --network-mode ranks \
  --ranks 2,3 \
  --dlrover-run-args "--nnodes=\$NODE_NUM --nproc_per_node=8 --max_restarts=3" \
  --framework-cmd "bash train.sh"
```

## 5. 指定 nnodes、nproc_per_node 和其它 DLRover 参数

使用自动组网模式时，`dlrover-workspace` 会把命令包装为：

```bash
dlrover-run <dlrover-run-args> <framework-cmd>
```

因此所有 DLRover 启动参数都通过 `--dlrover-run-args` 指定：

```bash
dlrover-workspace launch \
  --network-mode all \
  --dlrover-run-args "--nnodes=\$NODE_NUM --nproc_per_node=8 --max_restarts=3 --network-check --node_unit=1" \
  --framework-cmd "python train.py --epochs 10 --batch_size 64"
```

常见参数：

- `--nnodes=\$NODE_NUM`：节点数，通常使用 workspace CLI 设置的 `$NODE_NUM`。
- `--nproc_per_node=8`：每个节点启动的训练进程数，通常等于每节点 GPU 数。
- `--max_restarts=3`：最大重启次数。
- `--network-check`：启用 DLRover 原有 network check。
- `--node_unit=1`：节点调度单元。

注意：`--dlrover-run-args` 只在 `--network-mode all/ranks` 且使用
`--framework-cmd` 时由 workspace CLI 自动拼接。如果使用 `--cmd`，请自己写完整命令。

## 6. 完全自定义启动命令

如果希望完全控制启动命令，使用 `--cmd`。这种方式下 workspace CLI 不会自动包装
`dlrover-run`，用户写什么就执行什么。

```bash
dlrover-workspace launch --cmd \
  "dlrover-run --nnodes=\$NODE_NUM --nproc_per_node=1 --max_restarts=3 python examples/pytorch/mnist/cnn_train.py --num_epochs 5"
```

这会保持 DLRover 原有启动和组网逻辑，不额外改变 `dlrover-run` 的行为。

临时调试单个 worker：

```bash
dlrover-workspace launch --foreground --ranks 0 --cmd "env | sort"
```

## 7. 查看训练日志

查看所有 worker 最近 100 行日志：

```bash
dlrover-workspace logs --tail 100
```

持续跟随日志：

```bash
dlrover-workspace logs --follow
```

只查看某个 worker：

```bash
dlrover-workspace logs --follow --ranks 0
```

训练日志默认保存在各 worker Pod 内：

```text
/tmp/dlrover-workspace/train.rank_<rank>.log
```

## 8. 停止训练

停止当前训练进程，但保留 workspace Pod、Jupyter 和已申请资源：

```bash
dlrover-workspace stop
```

设置优雅停止等待时间：

```bash
dlrover-workspace stop --grace-period 300 --kill-timeout 30
```

只停止指定 worker：

```bash
dlrover-workspace stop --ranks 0,1
```

## 9. 常用命令模板

不自动组网，直接运行脚本：

```bash
dlrover-workspace launch --framework-cmd "bash train.sh"
```

全部 worker 参与 DLRover 分布式训练：

```bash
dlrover-workspace launch \
  --network-mode all \
  --dlrover-run-args "--nnodes=\$NODE_NUM --nproc_per_node=8 --max_restarts=3" \
  --framework-cmd "bash train.sh"
```

只使用 worker 2 和 worker 3 参与 DLRover 分布式训练：

```bash
dlrover-workspace launch \
  --network-mode ranks \
  --ranks 2,3 \
  --dlrover-run-args "--nnodes=\$NODE_NUM --nproc_per_node=8 --max_restarts=3" \
  --framework-cmd "bash train.sh"
```

完全自定义 `dlrover-run`：

```bash
dlrover-workspace launch --cmd \
  "dlrover-run --nnodes=2 --nproc_per_node=8 --max_restarts=3 bash train.sh"
```

## 10. 排查问题

如果提示已有训练正在运行：

```bash
dlrover-workspace status
dlrover-workspace stop
```

如果部分 worker Pod 不是 Running：

```bash
dlrover-workspace status --cli-log on
```

如果命令引号很复杂，建议写成脚本后再启动：

```bash
dlrover-workspace launch \
  --network-mode all \
  --dlrover-run-args "--nnodes=\$NODE_NUM --nproc_per_node=8" \
  --framework-cmd "bash train.sh"
```

如果只想验证当前 Jupyter 环境变量：

```bash
dlrover-workspace launch --foreground --ranks 0 --cmd "env | sort"
```
