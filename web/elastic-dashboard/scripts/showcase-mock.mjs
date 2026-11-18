// Local presentation fixture only. Never deploy as the production API.
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import ts from 'typescript';

const source = await readFile(new URL('../src/demo.ts', import.meta.url), 'utf8');
const code = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.ESNext } }).outputText;
const { demoDashboard } = await import('data:text/javascript;base64,' + Buffer.from(code).toString('base64'));
const at = value => Date.parse(`${value}+08:00`)/1000;
const now = at('2026-11-12T10:00:00');
const scenarios = ['complete', 'partial', 'negative', 'empty', 'outage', 'invalid'];
let scenario = 'complete';
const cluster = 'showcase-mock';
function records() {
  if (scenario === 'empty') return [];
  return ['baseline', 'experiment'].flatMap((group) => Array.from({length: 10}, (_,i) => {
    const experiment = group === 'experiment';
    const minutes = experiment ? (i===9?215:65) : (i===9?190:90);
    const submitted = at(`${experiment?'2026-11-03':'2026-10-27'}T13:04:00`);
    const running = scenario==='partial' && experiment && i===9;
    return {uid:`mock-${group}-${String(i+1).padStart(2,'0')}`, name:`MOCK-${group}-${String(i+1).padStart(2,'0')}`, cluster, namespace:'showcase', status:running?'running':'completed', submitted_at:submitted, completed_at:running?null:submitted+minutes*60};
  }));
}
function stats(url) {
  const allowed = new Set(['cluster','namespace','submitted_from','submitted_to']);
  for (const key of url.searchParams.keys()) if (!allowed.has(key) || url.searchParams.getAll(key).length!==1 || !url.searchParams.get(key)) throw Error('filter');
  const from = url.searchParams.get('submitted_from'), to = url.searchParams.get('submitted_to');
  if ((from===null)!==(to===null) || from!==null && (!Number.isFinite(+from)||!Number.isFinite(+to)||+from<0||+from>=+to)) throw Error('range');
  const jobs=records().filter(j=>(!url.searchParams.has('cluster')||j.cluster===url.searchParams.get('cluster')) && (!url.searchParams.has('namespace')||j.namespace===url.searchParams.get('namespace')) && (from===null||j.submitted_at>=+from&&j.submitted_at<+to));
  const values=jobs.filter(j=>j.status==='completed').map(j=>j.completed_at-j.submitted_at).sort((a,b)=>a-b);
  const percentile=p=>values.length?values[Math.ceil(values.length*p)-1]:null;
  return {generated_at:now, total_jobs:jobs.length, measured_jobs:values.length, excluded_jobs:jobs.length-values.length, completed_jobs:values.length, running_jobs:jobs.length-values.length, failed_jobs:0, unknown_jobs:0, unassigned_jobs:0, mean_seconds:values.length?values.reduce((a,b)=>a+b,0)/values.length:null, p50_seconds:percentile(.5),p95_seconds:percentile(.95),min_seconds:values[0]??null,max_seconds:values.at(-1)??null,percentile_method:'nearest_rank',time_basis:'submission_to_completion',submitted_from:from===null?null:+from,submitted_to:to===null?null:+to};
}
function dashboard(job) {
  const data=demoDashboard(job.status==='running'?'demo-train-002':'demo-train-001');
  const original=at('2026-11-06T13:04:00'), shift=job.submitted_at-original;
  const move=v=>v===null?null:v+shift;
  data.job=job; data.generated_at=now;
  data.jct.submitted_at=job.submitted_at; data.jct.completed_at=job.completed_at;
  data.jct.actual_seconds=job.completed_at===null?null:job.completed_at-job.submitted_at;
  data.jct.elapsed_seconds=job.completed_at===null?now-job.submitted_at:data.jct.actual_seconds;
  data.jct.measurement_status=job.status==='running'?'running':'completed';
  // Events end 96 minutes after submission: short jobs have no synthetic events.
  const detailed=job.uid==='mock-experiment-10' && job.status==='completed';
  data.jct.baseline_seconds=detailed?240*60:null;
  data.jct.baseline_frozen_at=detailed?move(data.jct.baseline_frozen_at):null;
  data.jct.baseline_source=detailed?'estimate':'unavailable';
  data.jct.baseline_description=detailed?'MOCK 页面展示基线，非后端账本':'MOCK 未提供基线';
  data.events=detailed?data.events.map(e=>({...e,occurred_at:move(e.occurred_at),interruption_start:move(e.interruption_start),interruption_end:move(e.interruption_end),before_finish_at:move(e.before_finish_at),after_finish_at:move(e.after_finish_at),stages:e.stages.map(s=>({...s,at:move(s.at)})),evaluation:e.evaluation?{...e.evaluation,observed_at:move(e.evaluation.observed_at)}:null})):[];
  data.series.points=detailed?data.series.points.map(p=>({...p,at:move(p.at)})):[];
  if(detailed) {
    data.latest_evaluation.observed_at=move(data.latest_evaluation.observed_at);
    if(scenario==='negative') {
      Object.assign(data.latest_evaluation,{gain_ratio:-.12,time_gain_seconds:-300,gpu_seconds_saved:-600,state:'retained'});
      data.events[0].evaluation={...data.latest_evaluation}; data.events[0].conclusion='retained';
    }
  } else { data.latest_evaluation=null; data.summary={attempted:0,succeeded:0,failed:0,complete:true}; }
  data.missing_data=['MOCK HTTP showcase：任务基线、吞吐及停训区间为合成展示数据，不证明生产采集能力。'];
  return data;
}
const server=http.createServer((req,res)=>{
  const url=new URL(req.url,'http://localhost');
  const send=(code,value)=>{res.writeHead(code,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});res.end(JSON.stringify(value));};
  if(url.pathname==='/__scenario'&&req.method==='POST') {
    const next=url.searchParams.get('name');
    if(!scenarios.includes(next))return send(422,{scenarios});
    scenario=next; return send(200,{scenario});
  }
  if(url.pathname==='/healthz')return send(200,{scenario,mock:true});
  if(scenario==='outage')return send(503,{error:'MOCK outage'});
  if(url.pathname==='/api/v1/elastic/jobs')return send(200,{items:records()});
  if(url.pathname==='/api/v1/elastic/jct-statistics') {
    if(scenario==='invalid')return send(200,{mean_seconds:'invalid'});
    try{return send(200,stats(url));}catch{return send(422,{code:'INVALID_FILTER'});}
  }
  const match=url.pathname.match(/^\/api\/v1\/elastic\/jobs\/([^/]+)\/dashboard$/);
  if(match){const job=records().find(j=>j.uid===decodeURIComponent(match[1]));return send(job?200:404,job?dashboard(job):{error:'not found'});}
  send(404,{error:'MOCK route not found'});
});
server.listen(18080,'127.0.0.1',()=>console.log('MOCK ONLY http://127.0.0.1:18080 — complete/partial/negative/empty/outage/invalid'));
function close(){server.close();}
process.on('SIGINT',close);process.on('SIGTERM',close);
