import { useEffect, useState } from 'react';
import { Minus, TrendingDown, TrendingUp } from 'lucide-react';
import { z } from 'zod';
import { jobSchema, type Job } from './model';
import { loadJobs, type Mode } from './api';

const count = z.number().int().nonnegative();
const duration = z.number().finite().nonnegative().nullable();
const schema = z.object({ jobs: z.array(jobSchema.extend({ actual_seconds: duration })).optional(), cancelled_jobs: count.optional(), generated_at: z.number(), total_jobs: count, measured_jobs: count, running_jobs: count, failed_jobs: count, unknown_jobs: count, completed_jobs: count, unassigned_jobs: count, mean_seconds: duration, p50_seconds: duration, p95_seconds: duration });
type Stats = z.infer<typeof schema>;
const metrics = [['mean_seconds', '平均 JCT'], ['p50_seconds', 'JCT P50'], ['p95_seconds', 'JCT P95']] as const;
export function change(before: number | null, after: number | null) {
  return before !== null && before > 0 && after !== null ? (after - before) / before * 100 : null;
}
export function timestamp(value: string) { return Date.parse(value.includes('T') ? value + '+08:00' : value + 'T00:00:00+08:00') / 1000; }
export function localTime(seconds: number) { return new Date((seconds + 8*3600)*1000).toISOString().slice(0,16); }
function initialDays() {
  const end = Math.floor(Date.now()/60000)*60;
  return [end-6*3600,end-3*3600,end-3*3600,end].map(localTime);
}
export function adjustRange(values: string[], index: number, value: string, equal: boolean) {
  const next = values.map((v,k)=>k===index?value:v);
  const [a,b,c] = next.map(timestamp);
  if (equal && [a,b,c].every(Number.isFinite) && b>a) next[3]=localTime(c+b-a);
  return next;
}
async function load(mode: Mode, cluster: string, from: number, to: number, experiment: boolean, signal: AbortSignal): Promise<Stats> {
  if (mode === 'demo') return { generated_at: Date.now()/1000, total_jobs: 100, measured_jobs: experiment?72:80, running_jobs: experiment?23:15, failed_jobs: 5, unknown_jobs: 0, completed_jobs: experiment?72:80, unassigned_jobs: 0, mean_seconds: experiment?4800:6000, p50_seconds: experiment?4080:5100, p95_seconds: experiment?11700:10800 };
  const query = new URLSearchParams({ cluster, submitted_from: String(from), submitted_to: String(to), elasticity_enabled: String(experiment) });
  const response = await fetch(`/api/v1/elastic/jct-statistics?${query}`, { signal, credentials: 'same-origin' });
  if (!response.ok) throw new Error(`统计接口请求失败（${response.status}）`);
  return schema.parse(await response.json());
}
export default function Cluster({ mode }: { mode: Mode }) {
  const [clusters, setClusters] = useState<string[]>([]), [cluster, setCluster] = useState('');
  const [allJobs, setAllJobs] = useState<Job[]>([]);
  const [equal, setEqual] = useState(true);
  const [days, setDays] = useState(initialDays), [refresh, setRefresh] = useState(0);
  const [data, setData] = useState<[Stats, Stats] | null>(null), [error, setError] = useState(''), [loading, setLoading] = useState(false);
  useEffect(() => {
    const controller = new AbortController(); setError('');
    loadJobs(mode, controller.signal).then(jobs => { if (!controller.signal.aborted) { setAllJobs(jobs); const names = [...new Set(jobs.map(j=>j.cluster))]; setClusters(names); setCluster(previous=>names.includes(previous)?previous:names[0] ?? ''); } }).catch(e=>{ if (!controller.signal.aborted) setError(String(e)); });
    return ()=>controller.abort();
  }, [mode,refresh]);
  useEffect(() => {
    const controller = new AbortController(); setData(null); setError(''); setLoading(false);
    if (!cluster) return;
    const [a,b,c,d] = days.map(timestamp);
    if (![a,b,c,d].every(Number.isFinite) || a<0 || !(a<b && b<=c && c<d)) { setError('请选择有效日期：基线开始 < 基线结束 ≤ 实验开始 < 实验结束。'); return; }
    setLoading(true);
    Promise.all([load(mode,cluster,a,b,false,controller.signal),load(mode,cluster,c,d,true,controller.signal)]).then(result=>{if(!controller.signal.aborted)setData(result);}).catch(e=>{if(!controller.signal.aborted)setError(e instanceof Error?e.message:'统计加载失败');}).finally(()=>{if(!controller.signal.aborted)setLoading(false);});
    return ()=>controller.abort();
  }, [mode,cluster,days,refresh]);
  const maximum = Math.max(1,...(data ? data.flatMap(s=>metrics.map(([key])=>s[key]??0)) : []));
  const format = (v:number|null) => v===null?'—':(v/60).toLocaleString('zh-CN',{maximumFractionDigits:1});
  return <>
    <section className="panel cluster-filters"><label>测试时长<select defaultValue="3" onChange={e=>{const hours=Number(e.target.value); if(!hours)return; setDays(previous=>{const a=timestamp(previous[0]),c=timestamp(previous[2]); if(!Number.isFinite(a))return previous; const b=a+hours*3600; return [a,b,Math.max(b,c),Math.max(b,c)+hours*3600].map(localTime);});}}>{[1,3,6,12,24].map(h=><option key={h} value={h}>{h===24?'1天':h+'小时'}</option>)}<option value="0">自定义</option></select></label><label><input type="checkbox" checked={equal} onChange={e=>{setEqual(e.target.checked); if(e.target.checked)setDays(previous=>adjustRange(previous,2,previous[2],true));}}/>等长对比</label><label>集群 <select value={cluster} onChange={e=>setCluster(e.target.value)}><option value="" disabled>选择集群</option>{clusters.map(c=><option key={c}>{c}</option>)}</select></label>{['基线阶段 · 关闭弹性','实验阶段 · 开启弹性'].map((label,i)=><fieldset key={label}><legend>{label}</legend>{[0,1].map(j=><label key={j}>{j?'结束（不含）':'开始'}<input type="datetime-local" step="60" disabled={equal && i===1 && j===1} aria-label={`${label}${j?'结束':'开始'}`} value={days[i*2+j]} onChange={e=>setDays(previous=>adjustRange(previous,i*2+j,e.target.value,equal))}/></label>)}</fieldset>)}<button className="primary" onClick={()=>setRefresh(v=>v+1)} disabled={loading}>刷新</button></section>
    {error?<div className="panel error" role="alert">{error}；API 模式不会回退到示例数据。</div>:loading?<div className="panel" role="status">正在加载 JCT 统计…</div>:!data?<div className="panel empty">请选择已登记任务的集群</div>:<>
      <div className="cluster-cards">{metrics.map(([key, label]) => {
        const delta = change(data[0][key], data[1][key]);
        const Icon = delta === null || delta === 0 ? Minus : delta > 0 ? TrendingUp : TrendingDown;
        const direction = delta === null ? '暂无可比数据' : delta === 0 ? '持平' : delta > 0 ? '增加' : '下降';
        const tone = delta === null || delta === 0 ? 'neutral' : delta > 0 ? 'amber' : 'green';
        return <section className="panel" key={key}>
          <div className="cluster-card-values">
            <h2>{label}</h2>
            <small>基线 → 实验（已完成样本）</small>
            <strong>{format(data[0][key])} → {format(data[1][key])} <small>分钟</small></strong>
          </div>
          <div className={`cluster-trend ${tone}`}>
            <span><Icon size={26} aria-hidden="true"/>{direction}</span>
            {delta !== null && <strong>{Math.abs(delta).toFixed(1)}%</strong>}
          </div>
        </section>;
      })}</div>
      <section className="panel"><div className="section-heading"><h2>开启弹性前后 JCT 对比</h2><ul className="jct-chart-legend" aria-label="JCT 对比图例">
        <li><span className="baseline-bar" aria-hidden="true"/>关闭弹性</li>
        <li><span className="experiment-bar" aria-hidden="true"/>开启弹性</li>
      </ul></div><div className="cluster-chart" role="img" aria-label="平均 JCT、P50、P95前后对比，柱高使用相同尺度，单位分钟">{metrics.map(([key,label])=><div className="cluster-group" key={key}><div className="cluster-bars">{data.map((s,i)=><div key={i} className="cluster-bar-column"><span>{format(s[key])}</span><div className={i?'experiment-bar':'baseline-bar'} style={{height:s[key]===null?0:`${s[key]!/maximum*180}px`}}/></div>)}</div><b>{label}</b></div>)}</div><p className="muted">单位：分钟 · 无数据以 — 展示</p></section>
      <section className="panel"><b>有效完成样本：基线 {data[0].measured_jobs}/{data[0].total_jobs} · 实验 {data[1].measured_jobs}/{data[1].total_jobs}</b><p>{data.map((s,i)=>`${i?'实验':'基线'}：运行中 ${s.running_jobs}，失败 ${s.failed_jobs}，已取消 ${s.cancelled_jobs??0}，未知 ${s.unknown_jobs}，完成时间无效 ${s.completed_jobs-s.measured_jobs}`).join('；')}</p><p className="amber">{data.some(s=>s.measured_jobs===0 || s.unassigned_jobs>0 || s.running_jobs>0 || s.unknown_jobs>0 || s.completed_jobs>s.measured_jobs)||timestamp(days[3])>Math.min(...data.map(s=>s.generated_at))?'结果暂定：窗口未结束、样本不足或仍有未完成/无效记录。':'已完成样本对比。'} 前后变化不直接代表算法收益。</p>{data[0].unassigned_jobs>0&&<p>当前集群有 {data[0].unassigned_jobs} 个任务缺少提交时间，无法归组。</p>}<small>按提交时间归组 · Asia/Shanghai · JCT 包含排队 · 按任务弹性配置过滤，未知配置不计入两组 · {mode==='demo'?'示例数值固定，仅演示布局':`统计截止 ${new Date(Math.min(...data.map(s=>s.generated_at))*1000).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai'})}`}</small></section>
      <section className="panel"><h2>任务明细</h2><p className="muted">展示当前集群全部已登记任务；窗口外、配置未知及未完成任务不纳入本次 JCT。时间均为 Asia/Shanghai。</p><div style={{overflowX:'auto'}}><table><thead><tr>{['任务','任务类型','平台','弹性配置','状态','实际 JCT','统计说明'].map(v=><th key={v}>{v}</th>)}</tr></thead><tbody>{allJobs.filter(j=>j.cluster===cluster).map(j=>{
        const included = data.some(s=>s.jobs?.some(v=>v.uid===j.uid));
        const actual = j.status==='completed' && j.completed_at!=null && j.submitted_at!=null && j.completed_at>=j.submitted_at ? j.completed_at-j.submitted_at : null;
        return <tr key={j.uid}><td title={j.uid}>{j.name}<small> · {j.uid.slice(0,8)}</small></td><td>{j.job_type??'未知'}</td><td>{j.platform_type??'未知'}</td><td>{j.elasticity_enabled==null?'待确认':j.elasticity_enabled?'开启弹性':'关闭弹性'}</td><td>{{running:'运行中',completed:'已完成',failed:'失败',cancelled:'已取消',unknown:'未知'}[j.status]}</td><td>{format(actual)} 分钟</td><td>{j.elasticity_enabled==null?'配置未知':!included?'窗口外或缺少提交时间':actual===null?'未完成或时间无效':'已纳入'}</td></tr>;
      })}{!allJobs.some(j=>j.cluster===cluster)&&<tr><td colSpan={7}>该集群暂无已登记任务，请检查任务同步。</td></tr>}</tbody></table></div></section>
    </>}
  </>;
}
