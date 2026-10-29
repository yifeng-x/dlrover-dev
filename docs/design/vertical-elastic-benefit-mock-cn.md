# SwanLab mock 模式验证

`DLROVER_SWANLAB_MOCK=true` 将 SwanLab HTTP 获取替换为本地 JSON 文件读取，默认关闭，只有显式 `true`（忽略大小写和首尾空白）开启。mock 模式无需配置 SwanLab URL。

```yaml
env:
  - name: DLROVER_SWANLAB_MOCK
    value: "true"
  - name: DLROVER_SWANLAB_TIME_SOURCE
    value: "sample"  # 本生成器的 createTime 确实是采样时间
  - name: DLROVER_SWANLAB_ASSUME_FIXED_BATCH
    value: "true"
  - name: DLROVER_SWANLAB_MOCK_FILE
    value: "/tmp/dlrover-benefit-validation/swanlab-mock.json"
```

文件路径必须在 Master 容器内可读。任务标识仍唯一来自 ElasticJob 的 `metadata.labels.low_code_platform_task_id`。文件中每个序列的 `taskId` 必须匹配该 label，不能用环境变量绕过。

## 生成测试数据

在仓库根目录运行（`xxxxxxx` 替换为任务 label）：

```bash
python examples/pytorch/resource_elastic/benefit_validation/generate_swanlab_mock.py \
  --task-id xxxxxxx --assume-fixed-batch \
  --work-start 100 --work-end 160 --total-steps 10000 \
  --window-seconds 60 \
  --output /tmp/dlrover-benefit-validation/swanlab-mock.json
```

生成器使用当前时间，输出真实 scalar 接口的 JSON 数组格式，并原子替换文件。默认模拟 60 秒推进 60 optimizer steps；可用 `--now` 指定测试时间。开启 --assume-fixed-batch 后输出 global_step、max_steps、train_speed（step/s），与当前平台配置一致；省略该参数则输出原含 global_batch_size 的指标格式。以 UTC `createTime` 表示采样时间。如配置了自定义 key，需相应修改 JSON。文件只含三个业务指标，不需要 generation、steady、observed_at scalar。

Master 每次获取重新读取文件，因此可以更新数据而无需重启；开关本身在初始化时读取。mock 仍执行 taskId、DLRover 内部代次边界、恢复与预热、采样对齐、计数器、吞吐稳定性、时效及进度校验。文件不存在、格式错误或数据无效时返回 unknown，不会自动访问真实 SwanLab，也不会跳过收益门控。

## 联调步骤

1. 设置任务 label，开启上述 mock 配置，确保文件在 Master 内可读。
2. 等待当前代次恢复及预热完成，生成完全位于时间下界之后的新鲜基线数据。保留现有收益证据服务配置；该服务仍需提供候选吞吐、切换开销、调度约束及 comparable 等证据。
3. 触发原有资源调整流程。查看 `BenefitFlow` 中 `data_source=mock`、窗口与吞吐、收益结果和 `gate_decision`。
4. 探索时，在新 worker generation 建立后更新文件。保持目标总步数和 global batch size 一致，按实际测试时钟推进时间及 work。至少提供两个独立的新窗口；反复读取同一文件不会增加有效窗口数。每个后续窗口的起点需不早于上一窗口终点，且在 observation_after 与预热下界之后。每个窗口内至少三个点，以便推断吞吐稳定性。
5. 验证负收益回退时，为回退后的 generation 再提供两个新窗口，并让 work 超过新的检查点步数。
6. 关闭 mock 后恢复真实 HTTP 获取，需要配置有效 SwanLab URL。

mock 只替换 SwanLab 数据源，不模拟收益证据服务、资源预留、Kubernetes、checkpoint 或真实训练。enforce 模式仍可能执行实际资源调整。模拟结果仅用于验证控制流程，不能当作真实收益。收益请求及业务事件的 training 中也携带 `data_source=mock`，用于识别模拟证据。

## 无集群回归

```bash
PYTHONDONTWRITEBYTECODE=1 python -m pytest -q -p no:cacheprovider \
  dlrover/python/tests/test_swanlab_mock.py
```

测试覆盖默认关闭与显式开启、文件和数据失败、label 必需、正负收益门控到执行、重复窗口拒绝、探索接受、负收益回退及回退后进度确认。收益证据 HTTP、checkpoint 和 worker 执行边界由测试模拟，其余走真实代码。
