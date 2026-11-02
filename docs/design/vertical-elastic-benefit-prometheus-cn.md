# 收益 Prometheus 指标与查询

Master通过现有`/metrics`暴露收益指标，由Prometheus主动抓取，不向Prometheus推送。默认端口9090，`--metrics_port=0`关闭端点。修改后需构建并部署包含新代码的Master镜像；仅修改ConfigMap不会更新代码。

## 1. 指标及单位

以下名称均带前缀`dlrover_master_vertical_`。

| 后缀 | 类型 | 含义 |
| --- | --- | --- |
| benefit_baseline_throughput | Gauge | 原配置吞吐，单位由work_unit指定，如steps/s |
| benefit_candidate_throughput | Gauge | 候选吞吐；探索取至少两个独立完整窗口的最小吞吐 |
| benefit_gain_ratio | Gauge | 当前objective的收益比例，0.05表示5%，允许负值 |
| benefit_time_gain_seconds | Gauge | 本任务预计节省秒数，负值表示预计变慢 |
| benefit_gpu_seconds_saved | Gauge | 本任务预计节省GPU秒，负值表示预计多耗卡时；除3600换算GPU小时 |
| benefit_valid | Gauge | 1表示当前阶段有完整有限数值结果，0表示结果不完整/不可用；不代表收益为正或约束满足 |
| benefit_observed_timestamp_seconds | Gauge | 该结果对应的训练窗口末端Unix时间；无可用时间为0 |
| exploration_windows | Gauge | journal中已接受的独立窗口数 |
| exploration_gpu_seconds | Gauge | journal中最近一次通过校验的累计探索GPU秒，不是全作业累计开销 |
| exploration_result_reported | Gauge | 适配服务确认终态入库后为1，未确认/尚未结束为0 |
| exploration_state{state} | Gauge | journal当前状态one-hot；状态值为小写 |
| benefit_evaluations_total{mode,result,reason} | Counter | checkpoint前收益判断次数；off不计数，观测轮询和控制请求不计入 |
| explorations_total{result} | Counter | 新探索终态次数，含切换阶段FAILED；同一探索只计一次 |

七个`benefit_*` Gauge使用相同标签：

- `stage=prediction`：调整前预测。候选吞吐来自历史、profile或trial证据。
- `stage=exploration`：切换后的实测吞吐外推结果，含已发生探索成本。
- `objective=time/gpu_hours/cluster_jct`；非法或未知配置归一化为unknown。
- `work_unit=steps/samples/tokens/unknown`。
- `data_source=mock/http/unknown`。它标识训练指标来源，不保证HTTP服务本身没有使用合成数据。

所有指标继续使用Master公共标签`cluster/namespace/job_uid/strategy`。不新增plan_id、request_id、generation等高基数标签，也没有task_id标签；task_id=123的测试应先找到对应ElasticJob UID，再按job_uid查询。独立测试作业可能共用task_id=123而拥有不同UID。

没有发生该阶段评估前，对应benefit Gauge序列可能尚不存在。未知数值为NaN，不以0冒充零收益；每次新评估会替换该阶段旧值，并移除旧的标签组合。`benefit_valid=0`时仍可能有已知基线吞吐，但不可把部分数值作为完整收益结果。

## 2. 计算口径与生命周期

普通预测沿用收益门禁公式；时间/GPU秒指标同时输出，gain_ratio按当前objective选择分母。cluster_jct的gain_ratio采用加权集群基线；time_gain_seconds和gpu_seconds_saved仍只表示当前任务，不是集群汇总。

探索设基线剩余时间T0、已耗时间E、候选剩余时间K、累计GPU秒S、原/候选总GPU数G0/G1：

```text
时间收益 = T0 - E - K
GPU秒收益 = G0*T0 - S - G1*K
时间目标gain_ratio = 时间收益/T0
卡时目标gain_ratio = GPU秒收益/(G0*T0)
```

RETAINED会输出有效负向/不足阈值的结果，不截断负数。INCONCLUSIVE若因有效测量违反延迟/deadline，仍可能valid=1；如果因窗口不足/超时而没有完整结果，则valid=0。判断是否采纳应结合exploration_state，不能仅依据valid或gain_ratio。

探索Gauge在journal保存后更新，不依赖HTTP回传成功；reported专门表示服务确认。终态后Gauge保留最后结果，不随着scrape重算收益；用observed_timestamp检查年龄。预测Gauge不持久化，重启后需等下一次评估；探索Gauge从journal恢复，旧版journal缺新数值字段时保留NaN/valid=0，不补造数据。

Counter是进程内值。每次新终态只计一次，重复保存/回传不重复计；读取已完成journal不增加计数。它不是持久化总账，极短生命周期/崩溃发生在两次scrape之间仍可能漏采。窗口数/累计GPU秒也只是最近已接受证据，超预算或非法的新样本不会替代已接受数值。

## 3. 在elastic-training测试命名空间核验

使用当前测试中的Master Pod变量`MASTER`，另开终端保持端口转发：

```bash
kubectl port-forward -n elastic-training pod/"$MASTER" 19090:9090
```

在另一终端抓取：

```bash
curl --fail http://127.0.0.1:19090/metrics > /tmp/benefit-metrics.prom
rg 'dlrover_master_vertical_(benefit_|exploration)' /tmp/benefit-metrics.prom
```

端点能抓取只证明Master暴露正常；还需检查Prometheus的目标发现、网络可达性和Targets页面。已有ServiceMonitor/PodMonitor/scrape配置可继续使用，不需要新建收益专用端点。

按手册E01/E02/E04执行时检查：

| 场景 | 预期 |
| --- | --- |
| E01正收益 | stage=exploration、valid=1、gain_ratio>0，state=accepted值1，accepted计数增加一次 |
| E02负收益且延迟约束放宽 | valid=1、gain_ratio<0，state=retained值1；候选2卡保持，无回退 |
| E02但仍使用原延迟约束 | state=inconclusive值1，若两轮有效则valid=1；负收益仍可查询 |
| E04缺窗口超时 | valid=0、收益NaN、state=inconclusive值1，不将其记成零收益 |
| checkpoint/切换失败 | failed计数增加一次、state=failed值1，无完整收益则valid=0 |
| 适配服务拒绝回传后恢复 | reported先为0；下次进入/重启重试确认后为1，不重新增加终态计数 |
| 重启并加载已完成journal | 恢复探索Gauge与原observed_timestamp，不增加新终态计数 |

## 4. PromQL示例

查询namespace下最新有效探索收益比例（百分比），保留负值。若只看某作业，在所有选择器增加相同`job_uid="实际UID"`：

```promql
100 * (
  dlrover_master_vertical_benefit_gain_ratio{namespace="elastic-training",stage="exploration"}
  and
  (dlrover_master_vertical_benefit_valid{namespace="elastic-training",stage="exploration"} == 1)
)
```

只查看最近10分钟内的训练证据，可在上述表达式再用`and`匹配以下条件（标签相同）：

```promql
(time() - dlrover_master_vertical_benefit_observed_timestamp_seconds{
  namespace="elastic-training",stage="exploration"
}) < 600
```

候选吞吐（注意不同work_unit不能相加比较）：

```promql
dlrover_master_vertical_benefit_candidate_throughput{
  namespace="elastic-training",stage="exploration",work_unit="steps",data_source="mock"
}
```

预计节省GPU小时：

```promql
dlrover_master_vertical_benefit_gpu_seconds_saved{
  namespace="elastic-training",stage="exploration"
} / 3600
```

最近一小时各探索结果次数：

```promql
sum by (result) (
  increase(dlrover_master_vertical_explorations_total{namespace="elastic-training"}[1h])
)
```

终态计数标签在首次出现时创建；新序列首个事件以及未被抓取的事件不一定能由increase完整还原，不能代替journal审计。收益Gauge不应用rate/increase；它们是最新预测值而非累计实际收益。
