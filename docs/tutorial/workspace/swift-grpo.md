# Workspace Swift GRPO 角色适配

此入口是显式启用的独立路径。未使用 `--framework`、部署方式或角色资源等新增选项时，原有 `--cmd` / `--framework-cmd` 行为不变。适配器不限定 ms-swift 的版本号，按已安装的参数类字段校验实际命令。运行前记录每个角色 Pod 的实际版本，并检查跨 Pod 版本一致性、参数字段、模型元数据和设备范围；通过预检查不等于该 Swift/PyTorch/vLLM/Ascend 组合已完成训练验收。

已对照官方 ms-swift 4.0.0 发布包核对 `swift.cli.rlhf` 入口、`RLHFArguments`/`RolloutArguments` 参数类、多 endpoint 列表及 HTTP 探针协议。预检查只读取参数类字段，不实例化会连接服务或初始化设备的参数对象。Master 和所有 worker 需要部署同一版包含本功能的 DLRover。

## 输入与启动

脚本采用正常 Bash 语法；换行参数必须用 `\` 续行。支持常量、可确定变量引用、export、命令前环境赋值、注释和绝对路径 cd。不执行整份脚本，不解释循环、管道、条件、source、后台符号和命令替换。普通 shell 变量不会被擅自当作环境变量导出。

下面使用同一模型和模型类型。原示例中的 Qwen3-1.7B/qwen3 与 Qwen3.5-2B/qwen3_5 不一致，会被拒绝；`--max_step` 应改为完整的 `--max_steps`。

```bash
# grpo_server.sh，业务参数按实际任务补充
ASCEND_RT_VISIBLE_DEVICES=0 \
  swift rollout \
  --model /nas/disk1/Qwen3-1.7B \
  --model_type qwen3 \
  --port 8991

ASCEND_RT_VISIBLE_DEVICES=1,2 NPROC_PER_NODE=2 \
  swift rlhf \
  --rlhf_type grpo \
  --model /nas/disk1/Qwen3-1.7B \
  --model_type qwen3 \
  --use_vllm true \
  --vllm_mode server \
  --vllm_server_host 127.0.0.1 \
  --vllm_server_port 8991 \
  --dataset /data/train.jsonl \
  --reward_funcs accuracy format \
  --max_steps 4 \
  --output_dir /output/grpo
```

先查看无启动副作用的静态计划：

```bash
dlrover-workspace launch --framework-cmd 'bash grpo_server.sh' \
  --framework swift --rollout-management managed \
  --network-mode all --dry-run
```

确认后去掉 `--dry-run`。默认每个选中 Pod 使用同一角色布局：设备 0 启动 rollout，设备 1、2 启动两个 trainer 进程。所有 trainer 使用按原始 worker rank 排序的同一 endpoint 列表。训练进程只由 ElasticAgent 派生，自动选择 Ascend/CUDA 启动后端。脚本在提交端读取，无需分发脚本文件；模型、数据和工作目录仍需在角色 Pod 中存在。

跨 Pod 分角色时增加 `--trainer-ranks 0,1 --rollout-ranks 2,3`。两项必须一起指定，且并集恰好覆盖选中的 Pod。同 Pod 的角色设备必须互斥。显式 `--trainer-devices` / `--rollout-devices` 覆盖脚本布局；每个角色脚本需要提供对应后端的可见性变量。

共卡脚本只包含 trainer，使用 `--vllm_mode colocate`，启动时不指定 rollout-management。external 分离式脚本只包含 trainer 和已有服务地址，使用 `--rollout-management external`（分离式的默认值），Workspace 不启动或停止外部服务。

## 状态、停止与生命周期

```bash
dlrover-workspace status
dlrover-workspace logs --role trainer --follow
dlrover-workspace logs --role rollout --follow
dlrover-workspace stop
```

状态索引位于 `STATE_DIR/role-task.json`；初始化标记位于 `STATE_DIR/runs/RUN_ID/`；角色日志位于 `LOG_DIR/RUN_ID/ROLE/rank-N.log`。索引以 0600 权限原子写入。每个角色有独立 launch_id，清理只使用自身进程身份，包含脱离原进程组的子进程。

- 同一 workspace 只允许一个活动适配任务。所有 worker（包括不承担角色的 worker）都登记占用，防止与旧命令并发启动。
- 启动 CLI 等待任务进入 RUNNING 或结束；进入 RUNNING 后无需 CLI 存活。RUNNING 的条件是每个训练 worker 完成首个 optimizer step，不是仅端口开放。
- 服务启动和训练初始化各使用 `--rollout-ready-timeout`，默认 600 秒；控制失联宽限为 60 秒，trainer 完成窗口为 60 秒。
- Master 通过现有 RPC 聚合状态。Pod UID/IP 变化、角色失败、服务探针失效或启动超时触发整任务停止。
- Pod 内独立 guardian 在监管进程意外退出时清理角色。正常结束或失败均先清理 trainer，再清理 managed rollout。
- 只有任务清理屏障完成才释放任务占用。清理失败、控制结果不确定时保留占用；再次 stop 会重试孤儿角色清理。不能删除状态文件来绕过占用检查。
- Master 重启使旧代次失效；不透明恢复训练。新任务必须重新检查所有 worker 的进程和占用。永久丢失 Pod、无法证明清理完成时需要运维核对，不能报告成功。

## 当前明确限制

- 新适配路径暂不支持 release 和 foreground；启动前拒绝，旧入口的 release/foreground 不变。
- 不支持动态扩缩容、垂直弹性、正在自动扩缩容的 workspace、多任务并发、跨 Pod rollout TP、异构角色布局或 Megatron 适配器。
- 必须预先挂载模型目录。managed 两个角色使用相同模型路径、model_type 和 revision，并比较跨 Pod config/index 元数据；这不验证全部权重文件的字节一致性。
- 暂不支持 `--config` 配置文件、认证 rollout API 或 managed 异步 rollout。
- `network-mode=none` 的适配任务仅支持单 Pod，保留 Swift 原生派生进程，并通过插件记录首步信号。
- 初步 HTTP 就绪检查使用 `/health/` 和 `/get_world_size/`，同时确认监听 socket 属于本任务。首步回调确认训练已执行；真实采样、LoRA/全量权重同步正确性仍需目标硬件验收。
- Ascend 的 HCCL 接口、动态通信端口、驱动与 vLLM-Ascend 兼容性由实际环境提供。不会自动套用 CUDA/NCCL 网络设置。

## 验证

新增自动化测试覆盖脚本解析、三种部署布局、参数优先级、单 launcher 命令、任务代次、RPC 超时、占用冲突、部分停止拒绝、真实本地服务进程和失败补偿。旧 workspace launcher、process manager、state 和 release supervisor 回归也必须通过。

生产上线前还必须在目标 Ascend 镜像验证：单 Pod 两训练卡、同 Pod 分卡、两 Pod 同布局、跨 Pod 分角色、共卡、external，以及采样、权重同步、首步和故障清理。本地模拟服务测试不替代这些硬件验收。


### 本次开发验证记录

- 新增规划、脚本解析、进程监管和 Swift 能力测试，以及 Master RPC 测试；真实本地子进程覆盖正常结束、trainer 失败、rollout 启动失败、端口归属和外部进程保护。
- 旧 workspace launcher/process manager/state/release supervisor 共 54 项通过；Kubernetes watcher 共 9 项通过。
- 扩展回归中的三项既有失败已在修改前 HEAD 的独立目录复现：两个 release lifecycle 测试的 Master mock 缺少 `_job_ctx`/退出码，以及一个 job manager 测试传入了 `NodeEvent` 不接受的 `event_message`。此次没有修改这些旧测试或相关业务语义。
- 当前开发环境没有 PyTorch/Swift/Ascend 运行栈；完整训练代理测试和真实 GRPO 硬件验收未完成。以上结果不能视为生产训练验收完成。
