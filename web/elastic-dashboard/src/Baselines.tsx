import {useEffect, useRef, useState, type ReactNode} from 'react';
import FormulaTip from './FormulaTip';
import {Clock3, Cpu, Timer, Layers, TrendingUp, TrendingDown, Minus, X, RefreshCw, Info, type LucideIcon} from 'lucide-react';
import {loadDashboard, requestJson, RequestError, type Mode} from './api';
import {demoJobs, demoDashboard} from './demo';
import {change} from './Cluster';
import './overview.css';
import {dateTime, isEffectiveEvent, type Job} from './model';

type QueueSnapshot = {jobs:number; waitMinutes:number; gpus:number};
type Metrics = {gpu_coverage?:number|null;queue?:QueueSnapshot;gpu_utilization?:number|null; queue_p50_seconds?:number|null; effective_events?:number|null; total_jobs:number; measured_jobs:number; mean_seconds:number|null; p50_seconds:number|null; p95_seconds:number|null};
type Snapshot = {id:string; name:string; kind:string; resource_pool:string; strategy?:string; start:number; end:number; cutoff:number; metrics:Metrics};
const names:Record<string,string> = {disabled:'非弹性基线', enabled:'弹性基线'};
const local = (s:number)=>new Date((s+28800)*1000).toISOString().slice(0,16);
const epoch = (v:string)=>Date.parse(v+'+08:00')/1000;
const minuteValue = (v:number)=> (v/60).toLocaleString('zh-CN',{maximumFractionDigits:1});
const minutes = (v:number|null|undefined)=>v==null?'—':minuteValue(v)+' 分钟';
const endpoint='/api/v1/elastic/baselines';

async function post(path:string, body:object):Promise<Snapshot>{
  return requestJson(path?'预览基线':'保存基线', endpoint+path, {
    method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)
  }, json=>json as Snapshot);
}

function BaselineKindTip({comparison=false}:{comparison?:boolean}){
  return <FormulaTip label={comparison?'查看对比基线说明':'查看基线类型说明'}>
    {comparison && <span>先选类型，再选择同一集群和资源池中保存的基线。基线周期可以早于当前分析周期。</span>}
    <span><b>非弹性基线</b>　取全池关闭弹性期间的指标，用于比较开启弹性前后的表现。</span>
    <span><b>弹性基线</b>　取全池采用指定弹性策略期间的指标，用于比较策略版本或运行周期。</span>
  </FormulaTip>;
}

function CreateBaseline({demo,cluster,pool,start,end,initialKind,onClose,onSave}:{initialKind:string;demo:boolean;cluster:string;pool:string;start:number;end:number;onClose:()=>void;onSave:(s:Snapshot, cluster:string)=>void}){
  const dialog=useRef<HTMLDialogElement>(null);
  const [manualCluster,setManualCluster]=useState('');
  const targetCluster=cluster || manualCluster.trim();
  useEffect(()=>{
    const node=dialog.current!;
    node.showModal();
    return ()=>node.close();
  },[]);
  const [name,setName]=useState(''),[kind,setKind]=useState(initialKind),[strategy,setStrategy]=useState('');
  const example=demo ? exampleBaselines(pool).find(b=>b.kind===kind) : undefined;
  const [from,setFrom]=useState(example?local(example.start):Number.isFinite(start)?local(start):'');
  const [to,setTo]=useState(example?local(example.end):Number.isFinite(end)?local(end):'');
  const [confirmed,setConfirmed]=useState(false);
  const [preview,setPreview]=useState<Snapshot|null>(null),[error,setError]=useState(''),[busy,setBusy]=useState(false);

  const examplePeriod=example && example.start===epoch(from) && example.end===epoch(to);
  function changeKind(value:string){
    if(examplePeriod){
      const next=exampleBaselines(pool).find(b=>b.kind===value);
      if(next){setFrom(local(next.start));setTo(local(next.end));}
    }
    setKind(value);setConfirmed(false);
  }

  async function submit(save:boolean){
    setBusy(true);setError('');
    try{
      const form={cluster:targetCluster,resource_pool:pool,name:name.trim(),kind,strategy:strategy.trim(),confirmed,start:epoch(from),end:epoch(to)};
      if(!targetCluster){setError('请输入集群名称');return;}
      if(!(form.start<form.end)){setError('周期结束时间须晚于开始时间');return;}
      if(!form.name){setError('请输入基线名称');return;}
      if(kind==='enabled'&&!form.strategy){setError('请输入策略版本');return;}
      if(save&&!preview) return;
      let result:Snapshot;
      if(demo){
        result=save?preview!:demoPreview(form);
      }else{
        result=await post(save?'':'/preview',{...form,preview_id:save?preview?.id:undefined,preview_cutoff:save?preview?.cutoff:undefined});
      }
      if(save) onSave(result,targetCluster); else setPreview(result);
    }catch(e){
      const hint=e instanceof RequestError?e.formHint:undefined;
      setError(hint || (demo && e instanceof Error && !(e instanceof DOMException)?e.message:save?'基线未保存，请重试':'预览暂不可用，请重试'));
      if(!save||hint) setPreview(null);
    }finally{setBusy(false);}
  }
  return <dialog ref={dialog} className="baseline-drawer create-baseline" aria-labelledby="baseline-heading" onCancel={e=>{e.preventDefault();if(!busy)onClose();}}>
    <div className="baseline-drawer-heading">
      <h2 id="baseline-heading">创建对比基线 {demo&&<span className="badge blue">示例数据</span>}</h2>
      <button type="button" className="drawer-close" disabled={busy} onClick={onClose} aria-label="关闭创建基线抽屉">
        <X size={18} strokeWidth={1.7} aria-hidden="true"/>
      </button>
    </div>
    <p>{cluster?cluster+' / ':''}{pool||'全部资源池'}{demo?' · 演示基线仅保存在当前浏览器':''}</p>
    <form onChange={()=>{setPreview(null);setError('');}} onSubmit={e=>{e.preventDefault();void submit(false);}}>
    <fieldset disabled={busy}><legend>基线设置</legend>
    {!cluster&&<label>集群名称<input required maxLength={512} value={manualCluster} onChange={e=>setManualCluster(e.target.value)} placeholder="输入实际集群名称"/></label>}
    <label>基线名称<input required maxLength={512} value={name} onChange={e=>setName(e.target.value)} placeholder="例如 A100 关闭弹性对照测试"/></label>
    <div className="drawer-kind"><div className="kind-caption"><label htmlFor="drawer-kind">基线类型</label><BaselineKindTip/></div><select id="drawer-kind" value={kind} onChange={e=>changeKind(e.target.value)}>{Object.entries(names).map(([k,v])=><option key={k} value={k}>{v}</option>)}</select></div>
    {kind==='enabled'&&<label>策略版本<input required value={strategy} onChange={e=>setStrategy(e.target.value)}/></label>}
    <label>周期开始（北京时间）<input required type="datetime-local" value={from} onChange={e=>setFrom(e.target.value)}/></label>
    <label>周期结束（不含）<input required type="datetime-local" value={to} onChange={e=>setTo(e.target.value)}/></label>
    {kind!=='history'&&<label><input type="checkbox" required checked={confirmed} onChange={e=>setConfirmed(e.target.checked)}/>人工确认该周期内全池{kind==='disabled'?'关闭弹性':'使用所填弹性策略'}</label>}
    </fieldset>
    <section className="snapshot-preview" aria-labelledby="snapshot-heading">
      <div className="snapshot-heading">
        <h3 id="snapshot-heading">指标快照预览</h3>
        {preview&&<small title="北京时间">统计截止 {local(preview.cutoff).slice(5).replace('T',' ')}</small>}
        <button type="submit" disabled={busy}><RefreshCw size={14}/>{preview?'重新计算':'计算预览'}</button>
      </div>
      {preview&&<>
        <div className="snapshot-grid">{[
          ['平均 JCT',preview.metrics.mean_seconds==null?null:minuteValue(preview.metrics.mean_seconds),'分钟'],
          ['GPU 平均使用率',preview.metrics.gpu_utilization,'%'],
          ['排队时长 P50',preview.metrics.queue_p50_seconds==null?null:minuteValue(preview.metrics.queue_p50_seconds),'分钟'],
          ['有效弹性总次数',preview.metrics.effective_events,'次'],
        ].map(([label,value,unit])=><div key={label}><span>{label}</span><strong>{value??'—'} {value!=null&&<em>{unit}</em>}</strong></div>)}</div>
        <dl className="snapshot-summary">
          <div><dt>有效完成任务</dt><dd>{preview.metrics.measured_jobs} / {preview.metrics.total_jobs}</dd></div>
          <div><dt>GPU 监控覆盖率</dt><dd>{preview.metrics.gpu_coverage==null?'—':preview.metrics.gpu_coverage+'%'}</dd></div>
          <div><dt>JCT P50 / P95</dt><dd>{preview.metrics.p50_seconds==null?'—':minuteValue(preview.metrics.p50_seconds)} / {minutes(preview.metrics.p95_seconds)}</dd></div>
        </dl>
        <div className="snapshot-note"><Info size={15}/>保存后固定本次指标与样本量，可在同一资源池复用。</div>
      </>}
    </section></form>
    {error&&<p role="alert" className="error">{error}</p>}
    <footer><button disabled={busy} onClick={onClose}>取消</button><button className="primary" disabled={!preview||busy} onClick={()=>submit(true)}>保存并设为当前对比基线</button></footer>
  </dialog>;
}

const demoEnd = demoJobs[0].completed_at! + 20*60;
const day = 86400;
// Observed queue snapshots at the ends of the four demo periods.
const demoQueues:QueueSnapshot[]=[
  {jobs:9, waitMinutes:72, gpus:20},
  {jobs:15, waitMinutes:180, gpus:32},
  {jobs:11, waitMinutes:110, gpus:24},
  {jobs:13, waitMinutes:143, gpus:28},
];
function demoQueueAt(end:number){
  const daysAgo=(demoEnd-end)/day;
  return Number.isInteger(daysAgo)?demoQueues[daysAgo]:undefined;
}
function exampleBaselines(pool:string):Snapshot[]{
  return Object.entries(names).map(([kind,name],i)=>({
    id:'demo-'+kind,name,kind,resource_pool:pool,start:demoEnd-(i+2)*day,end:demoEnd-(i+1)*day,cutoff:demoEnd,
    metrics:{queue:demoQueueAt(demoEnd-(i+1)*day),total_jobs:20,measured_jobs:18,mean_seconds:[7200,6000,6600][i],p50_seconds:5700,p95_seconds:9000,
      gpu_utilization:[56,68][i],gpu_coverage:[98,99][i],queue_p50_seconds:[720,600][i],effective_events:[0,6][i]}
  }));
}

type DemoForm = Pick<Snapshot,'name'|'kind'|'resource_pool'|'start'|'end'> & {cluster:string; strategy:string; confirmed:boolean};

function demoPreview(form:DemoForm):Snapshot{
  const {name,kind,start,end,resource_pool,strategy,cluster}=form;
  if(!(start>=0 && start<end && end<=demoEnd)) throw new Error('请选择演示统计截止时间前已结束的周期');
  if(kind!=='history'&&!form.confirmed) throw new Error('请确认该周期的弹性配置');
  const scoped=demoJobs.filter(j=>j.cluster===cluster && (!resource_pool || j.resource_pool===resource_pool));
  const jobs=scoped.filter(j=>j.submitted_at>=start && j.submitted_at<end);
  const preset=exampleBaselines(resource_pool).find(b=>b.start===start && b.end===end);
  if(!jobs.length && !preset) throw new Error('该周期没有演示样本，请选择当前演示周期或已有基线的完整周期');
  const adjustments=scoped.flatMap(j=>demoDashboard(j.uid).events).filter(e=>e.execution_status==='completed' && e.occurred_at>=start && e.occurred_at<end);
  if(kind==='disabled' && (adjustments.length || (preset && preset.kind!=='disabled'))) throw new Error('该演示周期无法确认为全池关闭弹性，请选择全池关闭弹性的样本周期');
  const times=jobs.flatMap(j=>j.actual_seconds==null?[]:[j.actual_seconds]).sort((a,b)=>a-b);
  const metrics=preset?.metrics??{
    queue:demoQueueAt(end),total_jobs:jobs.length,measured_jobs:times.length,
    mean_seconds:times.length?times.reduce((a,b)=>a+b,0)/times.length:null,
    p50_seconds:times[Math.ceil(times.length*.5)-1]??null,p95_seconds:times[Math.ceil(times.length*.95)-1]??null,
    gpu_utilization:72,gpu_coverage:98,queue_p50_seconds:480,
    effective_events:jobs.every(j=>demoDashboard(j.uid).summary.complete)?adjustments.filter(isEffectiveEvent).length:null
  };
  return {id:'manual-demo-'+Array.from(crypto.getRandomValues(new Uint32Array(4))).join('-'),name,kind,strategy,resource_pool,start,end,cutoff:demoEnd,metrics};
}

type Activity = {expanded:number; shrunk:number; adjusted:number; effective:number; complete:boolean};
function useActivity(mode:Mode, ids:string[],start:number,end:number){
  const [result,setResult]=useState<Record<string,Activity|null>>({});
  const key=JSON.stringify(ids);
  useEffect(()=>{
    const c=new AbortController();
    setResult({});
    const queue:string[]=JSON.parse(key);
    async function read(){
      while(queue.length && !c.signal.aborted){
        const uid=queue.shift()!;
        try {
          const data=await loadDashboard(mode,uid,c.signal);
          const events=data.events.filter(e=>e.occurred_at>=start && e.occurred_at<end && e.execution_status==='completed');
          const activity={expanded:events.filter(e=>e.type==='expand').length,shrunk:events.filter(e=>e.type==='shrink').length,
            adjusted:events.length,effective:events.filter(isEffectiveEvent).length,complete:data.summary.complete};
          if(!c.signal.aborted)setResult(old=>({...old,[uid]:activity}));
        } catch {
          if(!c.signal.aborted)setResult(old=>({...old,[uid]:null}));
        }
      }
    }
    void Promise.all(Array.from({length:Math.min(3,queue.length)},read));
    return ()=>c.abort();
  },[key,start,end,mode]);
  return result;
}

function GpuTrend({current,baseline,start,end}:{current:number|null;baseline:number|null;start:number;end:number}){
  const duration=end-start;
  const panelRef=useRef<HTMLElement>(null);
  const [chartWidth,setChartWidth]=useState(675);
  useEffect(()=>{
    const panel=panelRef.current;
    if (!panel) return;
    const observer=new ResizeObserver(([entry])=>setChartWidth(entry.contentRect.width));
    observer.observe(panel);
    return ()=>observer.disconnect();
  },[]);
  const [hover,setHover]=useState<{index:number;x:number;y:number}|null>(null);
  // Both curves and the hover readout use the same demo samples.
  const points=Array.from({length:25},(_,i)=>{
    const offset=4*Math.sin(i*Math.PI/6);
    return {x:50+i*25,current:current===null?null:current+offset,baseline:baseline===null?null:baseline+offset};
  });
  const y=(value:number)=>132-value*1.12;
  const line=(key:'current'|'baseline')=>points.flatMap(p=>p[key]===null?[]:[p.x+','+y(p[key])]).join(' ');
  const active=hover?points[hover.index]:null;
  const days = duration / day;
  const divisions = days >= 2 ? Math.min(7, Math.floor(days)) : 4;
  const showYear=days>=2 && local(start).slice(0,4)!==local(end).slice(0,4);
  const plotWidth=Math.min(chartWidth,675)*600/675;
  const labelWidth=showYear ? 112 : duration<240 ? 94 : 78;
  const angled=plotWidth/divisions<labelWidth+16;
  const spacing=angled ? labelWidth*0.87+12 : labelWidth+16;
  const tickIntervals=Math.max(1,Math.min(divisions,Math.floor(plotWidth/spacing)));
  const labelY=angled ? 147+labelWidth*0.25 : 151;
  const tipWidth=172;
  return <section ref={panelRef} className="panel"><h2>GPU 使用率趋势</h2>{current===null?<p className="trend-empty">当前周期暂无 GPU 使用率采样</p>:
    <svg viewBox={`0 0 675 ${angled ? Math.ceil(labelY+labelWidth*0.25+12) : 162}`} role="img" aria-label="GPU 使用率，按周期内经过时间对齐">
      {[0,25,50,75,100].map(n=><g key={n}><text x="42" y={136-n*1.12} textAnchor="end">{n}%</text><path d={`M50 ${132-n*1.12}h600`} stroke="#e3e8ee"/></g>)}
      {baseline!==null&&<polyline points={line('baseline')} stroke="#9ca3af" fill="none" strokeWidth="2"/>}
      <polyline points={line('current')} stroke="#176afa" fill="none" strokeWidth="3"/>
      {Array.from({length:tickIntervals+1}, (_,i)=>{
        const fraction=i/tickIntervals;
        const at=start+duration*fraction;
        const stamp=local(at);
        const date=stamp.slice(showYear?0:5,10);
        const time=duration<240 ? dateTime(at).split(' ').pop() : stamp.slice(11);
        const tickX=50+600*fraction;
        const labelX=Math.max(labelWidth/2,Math.min(675-labelWidth/2,tickX));
        return <g key={i}>
          <line x1={tickX} x2={tickX} y1="132" y2="138" stroke="#8796aa" vectorEffect="non-scaling-stroke"/>
          <text x={labelX} y={labelY} textAnchor="middle"
            transform={angled ? `rotate(-30 ${labelX} ${labelY})` : undefined}>
            {date+' '+time}
          </text>
        </g>;
      })}
      <rect x="50" y="20" width="600" height="112" fill="transparent" style={{cursor:'crosshair'}}
        onPointerMove={event=>{
          const bounds=event.currentTarget.getBoundingClientRect();
          const fraction=Math.max(0,Math.min(1,(event.clientX-bounds.left)/bounds.width));
          setHover({index:Math.round(fraction*(points.length-1)),x:50+600*fraction,
            y:20+112*(event.clientY-bounds.top)/bounds.height});
        }} onPointerLeave={()=>setHover(null)} onPointerCancel={()=>setHover(null)}/>
      {active&&hover&&<g pointerEvents="none">
        <line x1={active.x} x2={active.x} y1="20" y2="132" stroke="#71849d" strokeDasharray="4 3"/>
        {(['baseline','current'] as const).map(key=>active[key]!==null&&<circle key={key} cx={active.x} cy={y(active[key])} r="3.5" fill={key==='current'?'#176afa':'#9ca3af'} stroke="white"/>)}
        <g transform={`translate(${hover.x+tipWidth+10<=675?hover.x+10:hover.x-tipWidth-10},${Math.max(2,Math.min(90,hover.y-22))})`}>
          <rect width={tipWidth} height="66" rx="6" fill="white" stroke="#ccd8e7"/>
          <text x="10" y="17" fontWeight="600">{dateTime(start+duration*hover.index/(points.length-1))}</text>
          <text x="10" y="38" fill="#176afa">当前周期　{active.current?.toFixed(1)}%</text>
          <text x="10" y="55">对比基线　{active.baseline===null?'—':active.baseline.toFixed(1)+'%'}</text>
        </g>
      </g>}
    </svg>}</section>;
}

function comparisonTone(delta:number|null, lower?:boolean){
  if(delta===null || delta===0 || lower===undefined) return 'steady';
  return (delta<0)===lower?'better':'worse';
}

function OverviewMetric({title,tip,icon:Icon,before,current,delta=null,deltaUnit="%",lower,children}:{
  title:string; tip:{meaning:string; formula:string; purpose:string}; icon:LucideIcon; before?:string; current:string; deltaUnit?:string;
  delta?:number|null; lower?:boolean; children?:ReactNode;
}){
  const Trend=delta===null||delta===0?Minus:delta>0?TrendingUp:TrendingDown;
  const tone=comparisonTone(delta,lower);
  return <section className="panel overview-metric">
    <h2>{title}<FormulaTip label={'查看'+title+'说明'}>
      <span><b>指标含义</b>{tip.meaning}</span>
      <span><b>计算公式</b>{tip.formula}</span>
      <span><b>作用</b>{tip.purpose}</span>
    </FormulaTip>
      <span className="overview-metric-icon"><Icon size={18} aria-hidden="true"/></span>
    </h2>
    <div className="overview-metric-body">
      <div><strong>{current}</strong>{before!==undefined&&<p>基线 {before}</p>}</div>
      {delta!==null&&<div className={'overview-change '+tone}>
        <span><Trend size={16} aria-hidden="true"/>{delta===0?'与基线持平':delta<0?'较基线下降':'较基线上升'}</span>
        <b>{Number(Math.abs(delta).toFixed(1))}{deltaUnit==='%'?'%':<small>{deltaUnit.trim()}</small>}</b>
      </div>}
    </div>
    {children}
  </section>;
}

type Props={mode:Mode;jobs:Job[];loadingJobs:boolean;jobsFailed:boolean;onRefresh:()=>void;onJob:(uid:string)=>void};
export default function Baselines({mode,jobs,loadingJobs,jobsFailed,onRefresh,onJob}:Props){
  const demo=mode==='demo';
  const [clusterChoice,setCluster]=useState(''),[pool,setPool]=useState('');
  const cluster=clusterChoice||jobs[0]?.cluster||'';
  const [now,setNow]=useState(()=>demo?demoEnd:Math.floor(Date.now()/1000));
  const [range,setRange]=useState('24'),[from,setFrom]=useState(local(now-day)),[to,setTo]=useState(local(now));
  const [items,setItems]=useState<Snapshot[]>([]),[selected,setSelected]=useState(''),[open,setOpen]=useState(false);
  const [error,setError]=useState(''),[detail,setDetail]=useState(true),[filter,setFilter]=useState('all');
  const key=mode+':baseline:'+JSON.stringify([cluster,pool]);
  const [kindChoice,setKindChoice]=useState<{scope:string;kind:string}|null>(null);
  const kind=kindChoice?.scope===key ? kindChoice.kind : localStorage.getItem(key+':kind')==='enabled'?'enabled':'disabled';
  function chooseKind(value:string,scope=key){
    localStorage.setItem(scope+':kind',value);
    setKindChoice({scope,kind:value});
  }
  const [refreshList,setRefreshList]=useState(0);
  const [loadingList,setLoadingList]=useState(false);
  const selectionKey=key+':'+kind;
  useEffect(()=>{
    const c=new AbortController();
    setItems([]);setSelected('');setError('');setLoadingList(!!cluster);
    function apply(rows:Snapshot[]){
      if(c.signal.aborted)return;
      const sorted=rows.filter(b=>b.kind===kind && b.resource_pool===pool)
        .sort((a,b)=>b.end-a.end || b.cutoff-a.cutoff || a.id.localeCompare(b.id));
      setItems(sorted);
      const saved=localStorage.getItem(selectionKey)??localStorage.getItem(key)??(!demo?localStorage.getItem(key.slice(4)):null);
      setSelected(sorted.find(b=>b.id===saved)?.id??sorted[0]?.id??'');
      setLoadingList(false);
    }
    function failed(){
      if(!c.signal.aborted){setError('基线列表暂不可用');setLoadingList(false);}
    }
    if(cluster && demo){
      try { apply([...JSON.parse(localStorage.getItem(key+':items')||'[]'),...exampleBaselines(pool)]); }
      catch { failed(); }
    }
    if(cluster && !demo){
      const query=new URLSearchParams({cluster,resource_pool:pool,kind});
      requestJson('基线列表',endpoint+'?'+query,{signal:c.signal},json=>json.items as Snapshot[])
        .then(apply).catch(failed);
    }
    return ()=>c.abort();
  },[cluster,pool,key,selectionKey,kind,now,demo,refreshList]);
  const start=range==='custom'?epoch(from):now-Number(range)*3600,end=range==='custom'?epoch(to):now;
  const valid=Number.isFinite(start)&&Number.isFinite(end)&&start<end;
  const scoped=jobs.filter(j=>j.cluster===cluster);
  const rows=scoped.filter(j=>(!pool||j.resource_pool===pool)&&j.submitted_at!=null&&j.submitted_at>=start&&j.submitted_at<end);
  const samples=rows.flatMap(j=>j.status==='completed'&&j.actual_seconds!=null?[j.actual_seconds]:[]);
  const mean=samples.length?samples.reduce((a,b)=>a+b,0)/samples.length:null;
  const baseline=items.find(b=>b.id===selected),base=baseline?.metrics;
  const activity=useActivity(mode,rows.map(j=>j.uid),start,end);
  const activityLoading=rows.some(j=>!(j.uid in activity));
  const effective=rows.reduce((n,j)=>n+(activity[j.uid]?.effective??0),0);
  const partial=rows.some(j=>!activity[j.uid]?.complete);
  const showDemo=demo&&rows.length>0;
  const gpu=showDemo?72:null, queue=showDemo?480:null;
  const currentQueue=demo&&cluster&&valid?demoQueueAt(end):undefined;
  const delta=change(base?.mean_seconds??null,mean);
  function choose(id:string){setSelected(id);localStorage.setItem(selectionKey,id);}
  const period=(a:number,b:number)=>local(a).slice(5).replace('T',' ')+' – '+local(b).slice(5).replace('T',' ');
  const visible=rows.filter(j=>filter==='all'||(filter==='adjusted'?activity[j.uid]?.adjusted:activity[j.uid]?.effective));
  return <div className="overview">
    <section className="cluster-filters"><label>集群<select value={cluster} onChange={e=>{setCluster(e.target.value);setPool('');}}>{[...new Set([cluster,...jobs.map(j=>j.cluster)].filter(Boolean))].map(c=><option key={c}>{c}</option>)}</select></label>
    <label>资源池<select value={pool} onChange={e=>setPool(e.target.value)}><option value="">全部资源池</option>{[...new Set(scoped.flatMap(j=>j.resource_pool?[j.resource_pool]:[]))].map(p=><option key={p}>{p}</option>)}</select></label>
    <div className="range-field" role="group" aria-label="时间范围"><span>时间范围</span><div className="tabs">{[['24','最近24小时'],['168','最近7天'],['custom','自定义范围']].map(([k,v])=><button key={k} className={range===k?'selected':''} aria-pressed={range===k} onClick={()=>setRange(k)}>{v}</button>)}</div></div>
    {range==='custom'&&<div className="range-dates"><label>从<input aria-label="开始时间" type="datetime-local" value={from} onChange={e=>setFrom(e.target.value)}/></label><label>至<input aria-label="结束时间" type="datetime-local" value={to} onChange={e=>setTo(e.target.value)}/></label></div>}
    <button className="primary" disabled={!valid} onClick={()=>{setNow(demo?demoEnd:Math.floor(Date.now()/1000));onRefresh();}}>查询</button></section>
    {!valid&&<p role="alert">结束时间须晚于开始时间</p>}
    <section className="comparison-strip">
    <div className="kind-caption"><label htmlFor="comparison-kind">对比基线类型</label><BaselineKindTip comparison/></div>
    <select id="comparison-kind" value={kind} onChange={e=>chooseKind(e.target.value)}>{Object.entries(names).map(([k,v])=><option key={k} value={k}>{v}</option>)}</select>
    {items.length>0 ? <label>可选基线<select value={selected} onChange={e=>choose(e.target.value)}>
      {items.map(b=><option key={b.id} value={b.id}>{b.name} · {period(b.start,b.end)}</option>)}
    </select></label> : <span role="status">{loadingList?'正在加载基线…':error||(!cluster?'请选择集群':'当前集群和资源池暂无此类型的基线，请创建基线。')}</span>}
    {error&&<button type="button" onClick={()=>setRefreshList(n=>n+1)}>重试</button>}
    <span>{pool||'全部资源池'}</span>{baseline&&<span>基线周期 {period(baseline.start,baseline.end)}</span>}{valid&&<span>当前周期 {period(start,end)}</span>}
    <button className="primary" aria-haspopup="dialog" onClick={()=>setOpen(true)}>创建基线</button></section>
    <div className="comparison-key"><i/>{baseline?names[baseline.kind]:'对比基线'} <i/>当前周期</div>
    <div className="overview-cards">
      <OverviewMetric title="平均 JCT" icon={Clock3} tip={{
        meaning:'所选周期内提交、已完成且有实测 JCT 的任务，从提交到完成的平均时长。',
        formula:'平均 JCT = 各有效样本的（完成时间 − 提交时间）之和 ÷ 有效样本数。较基线变化率 =（当前值 − 基线值）÷ 基线值 × 100%。',
        purpose:'判断任务平均完成得是否更快。比较时可结合卡片下方的有效样本数。',
      }}
        before={minutes(base?.mean_seconds)} current={minutes(mean)} delta={delta} lower>
        <small>有效样本　当前 {samples.length}/{rows.length} · 基线 {base?base.measured_jobs+'/'+base.total_jobs:'—'}</small>
      </OverviewMetric>
      <OverviewMetric title="GPU 平均使用率" icon={Cpu} tip={{
        meaning:'所选集群和资源池在统计周期内的 GPU 平均繁忙程度。',
        formula:'等间隔采样时，平均使用率 = 各 GPU 有效使用率样本之和 ÷ 有效样本数。当前值减去基线值，差值以百分点计。',
        purpose:'观察 GPU 空闲情况。结合平均 JCT，可以判断利用率提高时任务是否也更快完成。',
      }}
        before={base?.gpu_utilization==null?'—':base.gpu_utilization+'%'} current={gpu==null?'—':gpu+'%'} deltaUnit=" 个百分点" delta={gpu!=null&&base?.gpu_utilization!=null?gpu-base.gpu_utilization:null} lower={false}/>
      <OverviewMetric title="排队时长 P50" icon={Timer} tip={{
        meaning:'有有效排队记录的任务中，约一半任务的等待时间不超过该值。',
        formula:'单任务排队时长 = 开始运行时间 − 提交时间。P50 取有效时长的中位数，与基线的差值以分钟计。',
        purpose:'判断典型任务能否更快开始运行。结合当前排队任务数，可观察积压情况。',
      }}
        before={minutes(base?.queue_p50_seconds)} current={minutes(queue)} deltaUnit=" 分钟" delta={queue!=null&&base?.queue_p50_seconds!=null?(queue-base.queue_p50_seconds)/60:null} lower/>
      <OverviewMetric title="有效弹性总次数" icon={Layers} tip={{
        meaning:'所选周期内提交的任务中，统计同一周期内发生、执行完成且收益评估已采纳的弹性事件。',
        formula:'总次数 = 各任务的有效调整次数之和。一次调整的多个阶段合计 1 次。例如，同一任务先扩容、后缩容，两次调整均有效时计 2 次。记录不完整时只计已确认事件，无法确认时显示「—」。',
        purpose:'了解有效调整的频次，并通过「查看有效任务」定位发生调整的任务。',
      }}
        current={activityLoading||loadingJobs||jobsFailed||(partial&&effective===0)?'—':effective+' 次'}>
        <button className="row-link" onClick={()=>{setDetail(true);setFilter('effective');document.getElementById('overview-tasks')?.scrollIntoView({block:'nearest'});}}>查看有效任务 →</button>
      </OverviewMetric>
    </div>
    <div className="overview-trends"><GpuTrend current={gpu} baseline={base?.gpu_utilization??null} start={start} end={end}/><section className="panel"><h2>集群排队情况</h2>
    <small className="muted">周期结束时仍在排队的任务 · 基线 → 当前</small>
    {([['jobs','集群排队任务数','个'],['waitMinutes','排队总时长','分钟'],['gpus','排队等待总GPU数','张']] as const).map(([key,label,unit])=>{
      const previous=base?.queue?.[key], current=currentQueue?.[key];
      const tone=comparisonTone(previous!=null && current!=null?current-previous:null,true);
      return <p className="queue-line" key={key} title={key==='waitMinutes'?'各排队任务从进入队列到周期结束的等待时长之和':undefined}>
        {label}<span>{previous!=null?previous+' '+unit:'—'} → <b className={'queue-actual '+tone}>{current!=null?current+' '+unit:'—'}</b></span>
      </p>;
    })}</section></div>
    <section className="panel" id="overview-tasks"><div className="section-heading"><h2>任务明细</h2>{detail&&<div className="tabs" role="group" aria-label="任务明细筛选">{[['all','全部任务'],['adjusted','实际发生调整'],['effective','有效调整']].map(([k,v])=><button key={k} aria-pressed={filter===k} className={filter===k?'selected':''} onClick={()=>setFilter(k)}>{v}</button>)}</div>}<button className="row-link" aria-expanded={detail} onClick={()=>setDetail(!detail)}>{detail?'收起明细':'查看任务明细'}</button></div>
    {detail&&<div className="table-scroll"><table><thead><tr>{['任务名','弹性配置','实际调整','有效次数','实测 JCT','操作'].map(v=><th key={v}>{v}</th>)}</tr></thead><tbody>{visible.map(j=>{
      const a=activity[j.uid];
      return <tr key={j.uid}><td>{j.name}</td><td>{j.elasticity_enabled==null?'待确认':j.elasticity_enabled?'开启':'关闭'}</td><td>{a?.adjusted?[`扩容 ${a.expanded} 次`,`缩容 ${a.shrunk} 次`].join('，'):a?.complete?'未发生':j.uid in activity?'记录不完整':'读取中…'}</td><td>{a?.effective|| (a?.complete?0:'—')}</td><td>{minutes(j.actual_seconds)}</td><td><button className="row-link" onClick={()=>onJob(j.uid)}>查看任务</button></td></tr>;
    })}</tbody></table>{!visible.length&&<p className="empty">{loadingJobs||activityLoading?'正在加载任务…':jobsFailed?'作业列表暂不可用':'当前筛选下暂无任务'}</p>}</div>}</section>
    {open&&<CreateBaseline demo={demo} initialKind={kind} cluster={cluster} pool={pool} start={start} end={end} onClose={()=>setOpen(false)} onSave={(b,savedCluster)=>{
      const savedKey=mode+':baseline:'+JSON.stringify([savedCluster,pool]);
      if(demo){
        const previous:Snapshot[]=JSON.parse(localStorage.getItem(savedKey+':items')||'[]');
        localStorage.setItem(savedKey+':items',JSON.stringify([b,...previous.filter(i=>i.id!==b.id)]));
      }
      localStorage.setItem(savedKey+':'+b.kind,b.id);
      setCluster(savedCluster);chooseKind(b.kind,savedKey);setRefreshList(n=>n+1);
      setOpen(false);
    }}/>}
  </div>;
}
