import FormulaTip from './FormulaTip';
import { useEffect, useId, useRef, useState, type ReactNode } from 'react';
import { ChevronDown, Home, ClipboardList, CalendarDays, CircleX, CircleMinus, ArrowDownToLine, TrendingUp, BarChart3, Check, CircleCheck, Clock3, Info, Layers, RefreshCw, Search, Timer, TriangleAlert } from 'lucide-react';
import { loadDashboard, loadJobs, type Mode } from './api';
import { type Dashboard, type ElasticEvent, type Job, clock, filteredEvents, finite, fmt, gainValue, jctValues, elapsedJct, isEffectiveEvent, dateTime, objectiveLabel } from './model';
import Trend from './Trend';
import BaselineEstimate from './BaselineEstimate';
import Baselines from './Baselines';

const execution = { pending: '待执行', running: '执行中', completed: '执行完成', failed: '失败', skipped: '已跳过', unknown: '未知' };
const conclusion = { prediction_passed: '调整前评估通过', accepted: '已采纳', retained: '收益不足 · 保留配置', inconclusive: '无法确定', failed: '无有效评估', rejected: '门禁拒绝', unknown: '待评估' };
const recovery: Record<string,string> = { resources_restored: '资源已恢复 · 训练进度待确认', awaiting_progress: '控制已完成 · 训练进度待确认', workers_restarted: '训练进程已恢复', unverified: '训练进度未确认', timeout: '恢复确认超时', recovered: '已恢复', verified_by_exploration: '探索验证恢复', pending: '待恢复', not_started: '未执行', unknown: '未知' };
const typeLabel = { expand: '扩容', shrink: '缩容', other: '调整' };
function Badge({ children, tone = 'neutral' }: { children: ReactNode; tone?: string }) { return <span className={`badge ${tone}`}>{children}</span>; }
function Metric({ title, titleTip, value, hint, icon, action, tone = 'blue' }: { title: string; titleTip?: ReactNode; value: string; hint?: ReactNode; action?: ReactNode; icon: ReactNode; tone?: string }) { return <div className="metric"><span className={`metric-icon ${title.includes("时间") && !title.includes("收益率") || title.includes("基线 JCT") ? "blue" : tone}`}>{icon}</span><div><div className="metric-title">{title} {titleTip}</div><div className={`metric-value ${tone}`}>{value}</div>{hint && <div className="muted metric-hint">{hint}</div>}{action}</div></div>; }
function gainTone(v: number | null) { return finite(v) ? v < 0 ? 'amber' : v > 0 ? 'green' : 'neutral' : 'neutral'; }
function minutes(v: number | null) { return finite(v) ? v/60 : null; }
function hours(v: number | null) { return finite(v) ? v/3600 : null; }

export default function App() {
  const [tab, setTab] = useState('cluster');
  const [mode,setMode] = useState<Mode>(() => new URLSearchParams(window.location.search).get('mode') === 'demo' ? 'demo' : 'api');
  const [jobs,setJobs] = useState<Job[]>([]), [chosenUid,setUid] = useState('');
  const [taskCluster,setTaskCluster]=useState(''), [taskPool,setTaskPool]=useState('');
  const clusters=[...new Set(jobs.map(j=>j.cluster).filter(Boolean))];
  const cluster=clusters.includes(taskCluster)?taskCluster:'';
  const scopedJobs=cluster?jobs.filter(j=>j.cluster===cluster):jobs;
  const pools=[...new Set(scopedJobs.flatMap(j=>j.resource_pool?[j.resource_pool]:[]))];
  const pool=pools.includes(taskPool)?taskPool:'';
  const visibleJobs=pool?scopedJobs.filter(j=>j.resource_pool===pool):scopedJobs;
  const uid=visibleJobs.find(j=>j.uid===chosenUid)?.uid??visibleJobs[0]?.uid??'';
  const [data,setData] = useState<Dashboard|null>(null), [jobsFailed,setJobsFailed] = useState(false), [taskFailed,setTaskFailed] = useState(false), [loading,setLoading] = useState(true);
  const [refresh,setRefresh] = useState(0), [selected,setSelected] = useState(''), [filter,setFilter] = useState('all'), [query,setQuery] = useState(''), [range,setRange] = useState('all');
  useEffect(() => {
    const controller = new AbortController(); setJobs([]); setData(null); setLoading(true); setTaskFailed(false);
    setJobsFailed(false);
    loadJobs(mode,controller.signal).then(items => { if(controller.signal.aborted) return; setJobs([...items]); setUid(previous => items.some(j => j.uid===previous) ? previous : items[0]?.uid ?? ''); if(!items.length) setLoading(false); }).catch(() => { if(!controller.signal.aborted) { setJobsFailed(true); setLoading(false); } });
    return () => controller.abort();
  },[mode,refresh]);
  useEffect(() => {
    if(tab !== 'task' || !uid || !jobs.some(j=>j.uid===uid)) return;
    const controller = new AbortController(); setLoading(true); setData(null); setTaskFailed(false);
    loadDashboard(mode,uid,controller.signal).then(result => { if(controller.signal.aborted) return; setData(result); setSelected(result.events[0]?.id ?? ''); }).catch(() => { if(!controller.signal.aborted) setTaskFailed(true); }).finally(()=>{ if(!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  },[mode,uid,jobs,tab]);
  const end = data?.jct.completed_at ?? data?.generated_at ?? 0;
  const from = range==='all' ? 0 : end-Number(range)*60;
  const events = data ? filteredEvents(data.events,filter,query,from) : [];
  const active = events.find(e => e.id===selected) ?? events[0];
  return <div className="shell">
    <aside className="sidebar"><a className="brand" href="#top">DLRover</a><nav><a href="#jct"><Home size={19}/>总览</a><a href="#top"><ClipboardList size={19}/>训练任务</a><a className="active" href="#analysis"><BarChart3 size={19}/>弹性分析</a><a href="#events"><CalendarDays size={19}/>事件记录</a></nav><div className="sidebar-footer"><span className="live-dot"/>弹性训练观测台<small>DLRover / Elastic Insights</small></div></aside>
    <main id="top" className={tab==='cluster'?'cluster-page':'task-page'}><header><div><h1>弹性收益与事件 {tab==='task' && data && <Badge tone={data.job.status==='completed'?'green':'blue'}>{data.job.status==='completed'?<><Check size={12}/> 已完成</>:data.job.status==='running'?'运行中':data.job.status==='failed'?'失败':'生命周期未知'}</Badge>}</h1><p>{tab==='cluster'?'集群收益总览':data?.job.name ?? '选择训练作业'} <span className="muted">/ {data?.job.namespace ?? 'Elastic training observability'}</span></p></div><div className="header-right"><Badge tone={mode==='demo'?'blue':'neutral'}>{mode==='demo'?'示例数据 · 非真实采集':'API 数据'}</Badge><label className="mode-label">数据模式<select aria-label="数据模式" value={mode} onChange={e=>{setJobs([]);setUid('');setTaskCluster('');setTaskPool('');setMode(e.target.value as Mode);}}><option value="demo">演示模式</option><option value="api">真实 API</option></select></label></div></header>
    <div className="tabs page-tabs" role="tablist" aria-label="分析范围">{[['cluster','集群总览'],['task','任务分析']].map(([key,label])=><button role="tab" aria-selected={tab===key} className={tab===key?'selected':''} key={key} onClick={()=>setTab(key)}>{label}</button>)}</div>
    {tab==='cluster'?<Baselines key={mode} mode={mode} jobs={jobs} loadingJobs={loading && !jobs.length && !jobsFailed} jobsFailed={jobsFailed} onRefresh={()=>setRefresh(v=>v+1)} onJob={id=>{const job=jobs.find(j=>j.uid===id);setTaskCluster(job?.cluster??'');setTaskPool(job?.resource_pool??'');setUid(id);setRange('all');setFilter('all');setQuery('');setTab('task');}}/>:<>
    <div className="toolbar task-filters">
      <label>集群<select value={cluster} disabled={!clusters.length} onChange={e=>{setTaskCluster(e.target.value);setTaskPool('');setUid(uid);}}>
        <option value="">全部集群</option>{clusters.map(c=><option key={c}>{c}</option>)}
      </select></label>
      <label>资源池<select value={pool} disabled={!pools.length} onChange={e=>{setTaskPool(e.target.value);setUid(uid);}}>
        <option value="">全部资源池</option>{pools.map(p=><option key={p}>{p}</option>)}
      </select></label>
      <label>作业<select aria-label="作业" value={uid} onChange={e=>setUid(e.target.value)} disabled={!visibleJobs.length}>{!visibleJobs.length && <option value="">暂无作业</option>}{visibleJobs.map(j=><option key={j.uid} value={j.uid}>{j.name}</option>)}</select></label><label>事件记录范围<select value={range} onChange={e=>setRange(e.target.value)}><option value="all">全生命周期</option><option value="60">结束前 / 最近 1 小时</option><option value="10">结束前 / 最近 10 分钟</option></select></label><span className="toolbar-note">时区 Asia/Shanghai</span><button className="primary" onClick={()=>setRefresh(v=>v+1)} disabled={loading}><RefreshCw size={15} className={loading?'spin':''}/>刷新</button></div>
    {loading ? <div className="panel loading" role="status"><RefreshCw className="spin"/>正在加载作业与收益数据…</div> : jobsFailed || taskFailed ? <div className="panel error" role="alert"><TriangleAlert/><h2>{jobsFailed?'作业列表暂不可用':'任务分析暂不可用'}</h2><button onClick={()=>setRefresh(v=>v+1)}>重试</button></div> : !data ? <div className="panel empty">暂无可展示的作业</div> : <>
      <TaskOverview data={data}/>
      <div className="analysis-grid" id="analysis"><section className="panel trend-panel"><div className="section-heading"><h2>弹性事件与训练趋势</h2><div className="legend"><span className="purple-line"/>运行 Worker GPU 数 <span className="blue-line"/>吞吐 <Badge tone="blue">扩容</Badge><Badge tone="teal">缩容</Badge><Badge tone="red">失败</Badge><Badge>跳过</Badge></div></div><Trend data={data} events={data.events} selected={active?.id ?? ''} onSelect={id=>{
        if (!events.some(e=>e.id===id)) {setFilter('all'); setQuery(''); setRange('all');}
        setSelected(id);
      }}/></section><Details key={active?.id} event={active} completedAt={data.jct.completed_at}/></div>
      <section className="panel events-panel" id="events"><div className="section-heading event-heading"><h2>弹性事件记录 <span className="count">{events.length}</span></h2><div className="tabs" role="group" aria-label="事件类型筛选">{[['all','全部'],['expand','扩容'],['shrink','缩容'],['failed','失败'],['skipped','已跳过']].map(([key,label])=><button key={key} aria-pressed={filter===key} className={filter===key?'selected':''} onClick={()=>setFilter(key)}>{label}</button>)}</div><label className="search"><Search size={15}/><input aria-label="搜索事件" placeholder="搜索事件、计划或原因" value={query} onChange={e=>setQuery(e.target.value)}/></label></div>
      <div className="table-scroll"><table><thead><tr>{['时间','类型','GPU 变化（计划）','执行状态','中断耗时','预计时间收益','预计卡时收益','评估结论'].map(t=><th key={t}>{t}</th>)}</tr></thead><tbody>{events.map(e=><tr key={e.id} className={active?.id===e.id?'highlight':''}><td><button className="row-link" onClick={()=>setSelected(e.id)} aria-label={`查看事件 ${e.id}`}>{clock(e.occurred_at)}</button><small>{e.id}</small></td><td><Badge tone={e.type==='shrink'?'teal':'blue'}>{typeLabel[e.type]}</Badge></td><td>{fmt(e.before_gpus)} → {fmt(e.target_gpus)}</td><td><Badge tone={e.execution_status==='completed'?'green':e.execution_status==='failed'?'red':'neutral'}>{execution[e.execution_status]}</Badge></td><td>{fmt(e.interruption_seconds,'s')}{e.interruption_basis && <small>{{control_estimate:'控制阶段估算',worker_processes:'至进程启动',training_progress:'至进度确认'}[e.interruption_basis]}</small>}</td><td className={gainTone(gainValue(e.evaluation,'time_gain_seconds'))}>{fmt(minutes(gainValue(e.evaluation,'time_gain_seconds')),'min',true)}</td><td className={gainTone(gainValue(e.evaluation,'gpu_seconds_saved'))}>{fmt(hours(gainValue(e.evaluation,'gpu_seconds_saved')),'GPU·h',true)}</td><td><Badge tone={(e.conclusion==='accepted'||e.conclusion==='prediction_passed')?'green':'neutral'}>{conclusion[e.conclusion]}</Badge></td></tr>)}</tbody></table>{!events.length && <div className="empty">没有符合筛选条件的事件</div>}</div><div className="footnote"><Info size={14}/>执行完成不等于训练恢复或收益达标；GPU 变化为计划值，实际资源见详情。</div></section>
      {data.missing_data.length>0 && <div className="missing"><Info size={16}/><div><strong>数据完整性提示</strong><p>{data.missing_data.join(' · ')}</p></div></div>}
      <footer><span>DLRover · Elastic Insights</span><span>快照 {new Date(data.generated_at*1000).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai'})} · 收益非累计总账</span></footer>
    </>}
    </>}
    </main></div>;
}

function TaskOverview({data}:{data:Dashboard}) {
  const {actual,baseline,ratio}=jctValues(data);
  const running=data.job.status==='running';
  const {jct,summary}=data;
  const effective=data.events.filter(isEffectiveEvent).length;
  const effectiveValue=summary.complete?fmt(effective,'次'):effective?`≥ ${fmt(effective,'次')}`:'—';
  const source=jct.baseline_source;
  return <section className="panel" id="jct">
    <div className="section-heading">
      <h2>任务运行概览</h2>
      <span className="subtle">任务级收益 · 含排队时间</span>
    </div>
    <div className={'metrics-grid task-overview-grid'+(running?' running':'')}>
      <Metric icon={<Clock3/>} title="基线 JCT" value={fmt(minutes(baseline),'分钟')} tone="neutral"
        action={<div className="baseline-meta">
          <span>{source==='control_run'?'相对对照运行':source==='estimate'?'相对估算基线':'基线待补充'}</span>
          {source==='estimate' && jct.baseline_basis && <BaselineEstimate jct={jct}/>}
        </div>}/>
      {running&&<Metric icon={<Timer/>} title="运行耗时" value={fmt(minutes(elapsedJct(data)),'分钟')} hint="提交至本次刷新"/>}
      <Metric icon={<Clock3/>} title="实测 JCT"
        titleTip={<FormulaTip label="查看 JCT 含义和计算公式">
          <span>JCT（Job Completion Time，作业完成时长）指作业从提交到完成的总耗时，包含排队时间。</span>
          <span>实测 JCT = 实际完成时间 − 提交时间</span>
        </FormulaTip>}
        value={fmt(minutes(actual),'分钟')} hint={running?'待完成':undefined}
        action={<dl className="task-jct-times">
          <dt>提交时间</dt><dd>{dateTime(jct.submitted_at)}</dd>
          <dt>实际完成时间</dt><dd>{dateTime(data.job.status==='completed'?jct.completed_at:null)}</dd>
        </dl>}/>
      <div className="metric">
        <span className={'metric-icon '+gainTone(ratio)}><TrendingUp/></span>
        <div className="metric-title">JCT 改善率 <FormulaTip label="查看 JCT 改善率计算公式">JCT 改善率 =（基线 JCT − 实测 JCT）÷ 基线 JCT × 100%</FormulaTip></div>
        <div className={'metric-value '+gainTone(ratio)}>{fmt(finite(ratio)?ratio*100:null,'%',true)}</div>
      </div>
      <Metric icon={<Layers/>} title="有效弹性次数" value={effectiveValue}
        hint={summary.complete?'执行完成且收益评估已采纳':'仅计已确认有效的调整 · 记录不完整'}/>
    </div>
    <div className="footnote"><Info size={14}/>基线冻结于 {clock(jct.baseline_frozen_at)} · 来源：{jct.baseline_description}</div>
  </section>;
}
function Details({event:e,completedAt}:{event?:ElasticEvent;completedAt:number|null}) {

  return <aside className="panel details"><div className="section-heading"><h2>事件详情 {e&&<Badge tone="blue">{typeLabel[e.type]}</Badge>}</h2><span className="subtle">{e?.id}</span></div>{!e?<div className="empty">选择一条弹性事件查看详情</div>:<><dl className="event-facts"><dt>触发原因</dt><dd>{e.trigger}</dd><dt>计划 GPU</dt><dd>{fmt(e.before_gpus)} → {fmt(e.target_gpus)}</dd><dt>实际 GPU</dt><dd>{fmt(e.actual_gpus)}</dd><dt>执行结果</dt><dd><Badge tone={e.execution_status==='completed'?'green':e.execution_status==='failed'?'red':'neutral'}>{execution[e.execution_status]}</Badge></dd><dt>恢复状态</dt><dd>{recovery[e.recovery_status]??e.recovery_status}</dd><dt>评估结论</dt><dd className="conclusion-actions"><Badge tone={(e.conclusion==='accepted'||e.conclusion==='prediction_passed')?'green':'neutral'}>{conclusion[e.conclusion]}</Badge>
      <EvidencePopover event={e}/>
    </dd></dl>
    <ol className="stage-list">{e.stages.map((s,i)=><li key={`${s.at}-${i}`}>{/失败/.test(s.name)?<CircleX size={16} className="stage-failed"/>:/拒绝|跳过/.test(s.name)?<CircleMinus size={16} className="stage-skipped"/>:<CircleCheck size={16}/>}<time>{clock(s.at)}</time><span>{s.name}</span></li>)}</ol><div className="finish-grid"><div>原预计完成<b>{clock(e.before_finish_at)}</b></div><div>新预计完成<b>{clock(e.after_finish_at)}</b></div><div>实际完成<b>{clock(completedAt)}</b></div></div><div className="detail-reason"><ArrowDownToLine size={13}/>{e.reason}</div>
</>}</aside>;
}

function EvidencePopover({event}:{event:ElasticEvent}) {
  const id=useId();
  const trigger=useRef<HTMLButtonElement>(null);
  const panel=useRef<HTMLDivElement>(null);
  const [open,setOpen]=useState(false);

  function place() {
    if(!trigger.current || !panel.current) return;
    const anchor=trigger.current.getBoundingClientRect();
    const viewport=document.documentElement;
    const below=viewport.clientHeight-anchor.bottom-16;
    const above=anchor.top-16;
    const useAbove=below<panel.current.scrollHeight && above>below;
    const style=panel.current.style;
    style.maxHeight=Math.max(40,useAbove?above:below)+'px';
    style.left=Math.max(8,Math.min(anchor.right-panel.current.offsetWidth,viewport.clientWidth-panel.current.offsetWidth-8))+'px';
    style.top=(useAbove?anchor.top-6-panel.current.offsetHeight:anchor.bottom+6)+'px';
  }

  useEffect(()=>{
    if(!open) return;
    window.addEventListener('resize',place);
    window.addEventListener('scroll',place,true);
    return ()=>{
      window.removeEventListener('resize',place);
      window.removeEventListener('scroll',place,true);
    };
  },[open]);

  return <>
    <button type="button" ref={trigger} className="evidence-trigger" popoverTarget={id}
      aria-controls={id} aria-expanded={open}>评估依据 <ChevronDown size={12} aria-hidden="true"/></button>
    <div id={id} ref={panel} popover="auto" className="evidence-popover" role="region" aria-label="弹性收益评估证据"
      onToggle={e=>{
        const visible=e.currentTarget.matches(':popover-open');
        setOpen(visible);
        if(visible) place();
      }}>
      <EvaluationEvidence event={event}/>
    </div>
  </>;
}

function gainFormula(e:NonNullable<ElasticEvent['evaluation']>) {
  const trial=e.stage==='exploration';
  switch(e.objective){
    case 'time':
      return trial
        ? '（基线剩余时间 − 探索已耗时间 − 预计后续运行时间）÷ 基线剩余时间 × 100%'
        : '（基线剩余时间 − 调整后预计剩余时间）÷ 基线剩余时间 × 100%。调整后耗时含切换与重算。';
    case 'gpu_hours':
      return trial
        ? '（基线剩余卡时 − 探索已用卡时 − 预计后续卡时）÷ 基线剩余卡时 × 100%'
        : '（基线剩余卡时 − 候选方案预计卡时）÷ 基线剩余卡时 × 100%。候选卡时含调整开销。';
    case 'cluster_jct':
      return 'Σ[任务权重 ×（基线剩余时间 − 候选剩余时间）] ÷ Σ[任务权重 × 基线剩余时间] × 100%。包含当前任务和受影响任务。';
    case 'unknown':
      return '本次评估未记录判定目标，无法确定收益率公式。';
  }
}

function EvaluationEvidence({event}:{event:ElasticEvent}) {
  const e=event.evaluation;
  const ratio=gainValue(e,'gain_ratio');
  const threshold=e?.min_gain_ratio;
  const comparable=e?.objective!=='unknown' && finite(ratio) && finite(threshold);
  const meetsThreshold=comparable && ratio>0 && ratio>=threshold;
  const unit=e && e.work_unit!=='unknown'?e.work_unit+'/s':'';
  return <section className="event-evaluation">
    <h3>弹性收益评估证据</h3>
    {e ? <dl>
      <dt>评估阶段</dt><dd>{e.stage==='prediction'?'调整前预测':'探索后估算'}</dd>
      <dt>判定指标</dt><dd>{objectiveLabel[e.objective]}</dd>
      <dt className="rate-label">本次收益率 <FormulaTip label="查看本次收益率计算公式">{gainFormula(e)}</FormulaTip></dt>
      <dd className="evidence-rate">{fmt(finite(ratio)?ratio*100:null,'%',true)}</dd>
      <dt>最低收益率阈值</dt><dd>{fmt(finite(threshold)?threshold*100:null,'%')}</dd>
      <dt>收益门槛</dt><dd>{comparable
        ? <Badge tone={meetsThreshold?'green':'amber'}>{meetsThreshold?'已达到':'未达到'}</Badge>
        : '证据不足'}</dd>
      <dt>原配置吞吐</dt><dd>{fmt(e.baseline_throughput,unit)}</dd>
      <dt>候选配置吞吐</dt><dd>{fmt(e.candidate_throughput,unit)}</dd>
      <dt>观测窗口</dt><dd>{fmt(e.windows,'个')}</dd>
      <dt>数据来源</dt><dd>{{mock:'模拟数据',http:'HTTP 采集',unknown:'未知'}[e.data_source]}</dd>
    </dl> : <p className="muted">该事件未记录收益评估证据</p>}
  </section>;
}
