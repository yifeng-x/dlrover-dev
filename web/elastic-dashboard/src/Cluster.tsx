import { useEffect, useState } from 'react';
import { Minus, TrendingDown, TrendingUp } from 'lucide-react';
import { z } from 'zod';
import { jobSchema, type Job } from './model';
import { loadJobs, type Mode } from './api';

const count = z.number().int().nonnegative();
const duration = z.number().finite().nonnegative().nullable();
const schema = z.object({ jobs: z.array(jobSchema.extend({ actual_seconds: duration })), cancelled_jobs: count.optional(), generated_at: z.number(), total_jobs: count, measured_jobs: count, running_jobs: count, failed_jobs: count, unknown_jobs: count, completed_jobs: count, unassigned_jobs: count, mean_seconds: duration, p50_seconds: duration, p95_seconds: duration });
type Stats = z.infer<typeof schema>;
const groups = ['关闭弹性', '开启弹性'];
const reasons = {running:'运行中', failed:'失败', cancelled:'已取消', completed:'完成时间无效', unknown:'状态未知'};
type Reason = keyof typeof reasons;
type Selection = { group: number; reason: Reason };

function excludedCounts(stats: Stats): Record<Reason, number> {
  return {running: stats.running_jobs, failed: stats.failed_jobs, cancelled: stats.cancelled_jobs ?? 0,
    completed: stats.completed_jobs - stats.measured_jobs, unknown: stats.unknown_jobs};
}

function SampleCoverage({data, days, demo, detailsOpen, onToggle, onSelect}: {
  data: [Stats, Stats]; days: string[]; demo: boolean; detailsOpen: boolean; onToggle: ()=>void; onSelect: (filter: Selection | null)=>void;
}) {
  const [expanded, setExpanded] = useState<number | null>(null);
  const pending = data.flatMap((s,i)=>s.running_jobs ? [`${groups[i]}组有 ${s.running_jobs} 个任务运行中`] : []);
  const missing = data.reduce((n,s)=>n+s.unassigned_jobs,0);
  const provisional = pending.length > 0 || missing > 0 || data.some((s,i)=>
    !s.measured_jobs || s.unknown_jobs || s.completed_jobs>s.measured_jobs || timestamp(days[i*2+1])>s.generated_at);
  return <section className="panel sample-coverage">
    <div className="section-heading"><h2>有效完成样本</h2>{demo && <small>示例数据</small>}</div>
    <p className="muted">覆盖率 = 有效完成任务数 / 该组时段内提交任务数</p>
    <div className="sample-scroll"><table><thead><tr><th>对比组</th><th>有效完成 / 提交</th><th>样本覆盖率</th><th>未纳入</th></tr></thead>
    <tbody>{data.map((stats,i)=>{
      const percent = stats.total_jobs ? stats.measured_jobs / stats.total_jobs * 100 : null;
      const excluded = stats.total_jobs - stats.measured_jobs;
      return <tr key={groups[i]} className={i?'sample-on':'sample-off'}>
        <th><i/>{groups[i]}<small>{days[i*2].replace('T',' ')} 至 {days[i*2+1].replace('T',' ')}</small></th>
        <td><strong>{stats.measured_jobs} / {stats.total_jobs}</strong></td>
        <td><div className="sample-meter"><span><b style={{width:`${percent??0}%`}}/></span>{percent===null?'—':percent.toLocaleString('zh-CN',{maximumFractionDigits:1})+'%'}</div></td>
        <td><button disabled={!excluded} aria-expanded={expanded===i} aria-label={groups[i]+'未纳入 '+excluded+' 个，查看原因'} onClick={()=>setExpanded(expanded===i?null:i)}>{excluded} 个 {excluded>0 && (expanded===i?'▴':'▾')}</button></td>
      </tr>;
    })}</tbody></table></div>
    {provisional && <p className="sample-notice">{pending.length?pending.join('，')+'。':''}当前 JCT 为暂定结果。</p>}
    {missing>0 && <p>另有 {missing} 个任务缺少提交时间，无法分配到对比时段。</p>}
    {expanded!==null && <div className="sample-reasons"><b>{groups[expanded]} · 未纳入原因</b>
      <div>{Object.entries(excludedCounts(data[expanded])).map(([key,n])=>
        <button key={key} disabled={!n || !data[expanded].jobs} onClick={()=>onSelect({group:expanded,reason:key as Reason})}>{reasons[key as Reason]} {n}</button>)}</div>
    </div>}
    <button className="sample-details-link" aria-expanded={detailsOpen} aria-controls="cluster-job-details" onClick={onToggle}>{detailsOpen ? '收起任务明细 ▴' : '查看任务明细 ▾'}</button>
  </section>;
}
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
  if (mode === 'demo') {
    const measured = experiment ? 12 : 18;
    const running = experiment ? 6 : 1;
    const seconds = experiment ? 4800 : 6000;
    const jobs = Array.from({length:20}, (_,i)=>{
      const status: Job['status'] = i<measured ? 'completed' : i<measured+running ? 'running' : i===measured+running ? 'failed' : 'cancelled';
      const submitted = from + i;
      const uid = `demo-${experiment?'on':'off'}-${i+1}`;
      return {uid, name:uid, namespace:'demo', cluster, job_type:'ElasticJob', platform_type:'kubernetes',
        elasticity_enabled:experiment, status, submitted_at:submitted, completed_at:status==='completed'?submitted+seconds:null,
        actual_seconds:status==='completed'?seconds:null};
    });
    return {jobs, generated_at:Date.now()/1000, total_jobs:jobs.length, measured_jobs:measured,
      running_jobs:running, failed_jobs:1, cancelled_jobs:experiment?1:0, unknown_jobs:0, completed_jobs:measured,
      unassigned_jobs:0, mean_seconds:seconds, p50_seconds:seconds, p95_seconds:seconds};
  }
  const query = new URLSearchParams({ cluster, submitted_from: String(from), submitted_to: String(to), elasticity_enabled: String(experiment) });
  const response = await fetch(`/api/v1/elastic/jct-statistics?${query}`, { signal, credentials: 'same-origin' });
  if (!response.ok) throw new Error(`统计接口请求失败（${response.status}）`);
  return schema.parse(await response.json());
}
export default function Cluster({ mode }: { mode: Mode }) {
  const [selection, setSelection] = useState<Selection | null>(null);
  const [detailsOpen, setDetailsOpen] = useState(false);
  useEffect(()=>{
    if (detailsOpen) document.getElementById('cluster-job-details')?.scrollIntoView({block:'start'});
  }, [detailsOpen, selection]);
  function showDetails(filter: Selection | null) {
    setSelection(filter);
    setDetailsOpen(true);
  }
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
    const controller = new AbortController(); setData(null); setDetailsOpen(false); setSelection(null); setError(''); setLoading(false);
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
      <SampleCoverage data={data} days={days} demo={mode==='demo'} detailsOpen={detailsOpen} onToggle={()=>detailsOpen ? setDetailsOpen(false) : showDetails(null)} onSelect={showDetails}/>
      {detailsOpen && <section className="panel" id="cluster-job-details"><h2>任务明细</h2>
      {selection ? <p>{groups[selection.group]} · {reasons[selection.reason]} <button onClick={()=>setSelection(null)}>清除筛选</button></p> :
        <p className="muted">当前集群全部已登记任务 · Asia/Shanghai</p>}
      <div style={{overflowX:'auto'}}><table><thead><tr>{['任务','任务类型','平台','弹性配置','状态','实际 JCT','统计说明'].map(v=><th key={v}>{v}</th>)}</tr></thead><tbody>{(selection ? (data[selection.group].jobs??[]).filter(j=>j.status===selection.reason && j.actual_seconds===null) : mode==='demo' ? data.flatMap(s=>s.jobs??[]) : allJobs.filter(j=>j.cluster===cluster)).map(j=>{
        const included = data.some(s=>s.jobs?.some(v=>v.uid===j.uid));
        const actual = j.actual_seconds ?? null;
        return <tr key={j.uid}><td title={j.uid}>{j.name}<small> · {j.uid.slice(0,8)}</small></td><td>{j.job_type??'未知'}</td><td>{j.platform_type??'未知'}</td><td>{j.elasticity_enabled==null?'待确认':j.elasticity_enabled?'开启弹性':'关闭弹性'}</td><td>{{running:'运行中',completed:'已完成',failed:'失败',cancelled:'已取消',unknown:'未知'}[j.status]}</td><td>{format(actual)} 分钟</td><td>{j.elasticity_enabled==null?'配置未知':!included?'窗口外或缺少提交时间':actual===null?'未完成或时间无效':'已纳入'}</td></tr>;
      })}{!allJobs.some(j=>j.cluster===cluster)&&<tr><td colSpan={7}>该集群暂无已登记任务，请检查任务同步。</td></tr>}</tbody></table></div></section>}
    </>}
  </>;
}
