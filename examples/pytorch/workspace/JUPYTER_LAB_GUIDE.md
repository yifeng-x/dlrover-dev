# DLRover Workspace Jupyter Lab Guide

This guide is for users who have already opened Jupyter Lab in a DLRover
workspace. Run the commands below in a Jupyter terminal.

## 1. Check the Workspace

Show all worker pods and the current training process state:

```bash
dlrover-workspace status
```

Show extra pod/IP/node debug information:

```bash
dlrover-workspace status --cli-log on
```

Check selected workers only:

```bash
dlrover-workspace status --ranks 0,1
```

## 2. Run Without DLRover Auto Networking

This is the default mode. The command is launched on the target worker pods,
but `dlrover-workspace` does not wrap it with `dlrover-run`.

```bash
dlrover-workspace launch --framework-cmd \
  "python examples/pytorch/mnist/cnn_train.py --num_epochs 5"
```

Use this mode for single-node debugging, scripts that manage distributed
startup by themselves, or commands that should run exactly as written.

## 3. Run With All Workers Networked by DLRover

Use `--network-mode all` when all worker pods should join one DLRover
distributed launch.

```bash
dlrover-workspace launch \
  --network-mode all \
  --dlrover-run-args "--nnodes=\$NODE_NUM --nproc_per_node=1 --max_restarts=3" \
  --framework-cmd "python examples/pytorch/mnist/cnn_train.py --num_epochs 5"
```

`$NODE_NUM` is set by the workspace CLI to the number of selected workers. In
`all` mode, it is the total worker count of the workspace job.

Add other `dlrover-run` options inside `--dlrover-run-args`, for example:

```bash
dlrover-workspace launch \
  --network-mode all \
  --dlrover-run-args "--nnodes=\$NODE_NUM --nproc_per_node=8 --max_restarts=3 --network-check" \
  --framework-cmd "python train.py --epochs 10"
```

## 4. Run With Selected Workers Networked by DLRover

Use `--network-mode ranks` with `--ranks` when only some workers should join
the DLRover launch.

```bash
dlrover-workspace launch \
  --network-mode ranks \
  --ranks 0,1 \
  --dlrover-run-args "--nnodes=\$NODE_NUM --nproc_per_node=1 --max_restarts=3" \
  --framework-cmd "python examples/pytorch/mnist/cnn_train.py --num_epochs 5"
```

In `ranks` mode, `$NODE_NUM` is the number of selected ranks. The selected
worker pods are also mapped to contiguous launch ranks for this command.

## 5. Run a Fully Custom Command

Use `--cmd` when you want to control the full command yourself. This keeps
DLRover's existing behavior exactly as requested by your command.

```bash
dlrover-workspace launch --cmd \
  "dlrover-run --nnodes=\$NODE_NUM --nproc_per_node=1 --max_restarts=3 python examples/pytorch/mnist/cnn_train.py --num_epochs 5"
```

For quick debugging on one worker:

```bash
dlrover-workspace launch --foreground --ranks 0 --cmd "env | sort"
```

## 6. View Logs

Show the latest logs from all workers:

```bash
dlrover-workspace logs --tail 100
```

Follow logs continuously:

```bash
dlrover-workspace logs --follow
```

Follow one worker:

```bash
dlrover-workspace logs --follow --ranks 0
```

## 7. Stop Training

Stop the current training processes while keeping the workspace pods alive:

```bash
dlrover-workspace stop
```

Give the training process more time to exit gracefully:

```bash
dlrover-workspace stop --grace-period 300 --kill-timeout 30
```

Stop selected workers only:

```bash
dlrover-workspace stop --ranks 0,1
```

## 8. Common Patterns

Run on all workers without auto networking:

```bash
dlrover-workspace launch --framework-cmd "bash train.sh"
```

Run DLRover distributed training on all workers:

```bash
dlrover-workspace launch \
  --network-mode all \
  --dlrover-run-args "--nnodes=\$NODE_NUM --nproc_per_node=8 --max_restarts=3" \
  --framework-cmd "bash train.sh"
```

Run DLRover distributed training on workers 2 and 3:

```bash
dlrover-workspace launch \
  --network-mode ranks \
  --ranks 2,3 \
  --dlrover-run-args "--nnodes=\$NODE_NUM --nproc_per_node=8 --max_restarts=3" \
  --framework-cmd "bash train.sh"
```

## 9. Troubleshooting

If `launch` reports that training is already running:

```bash
dlrover-workspace status
dlrover-workspace stop
```

If a worker pod is not ready:

```bash
dlrover-workspace status --cli-log on
```

If quoting becomes complicated, put the training command in a script and launch
the script:

```bash
dlrover-workspace launch \
  --network-mode all \
  --dlrover-run-args "--nnodes=\$NODE_NUM --nproc_per_node=8" \
  --framework-cmd "bash train.sh"
```
