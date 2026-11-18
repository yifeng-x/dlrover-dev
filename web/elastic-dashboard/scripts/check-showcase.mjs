import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import ts from 'typescript';
const base='http://127.0.0.1:18080';
const scenario=async name=>assert.equal((await fetch(base+'/__scenario?name='+name,{method:'POST'})).status,200);
const stats=async()=>await(await fetch(base+'/api/v1/elastic/jct-statistics?cluster=showcase-mock&submitted_from=1793548800&submitted_to=1794153600')).json();
try {
 const source=await readFile(new URL('../src/model.ts',import.meta.url),'utf8');
 const code=ts.transpileModule(source,{compilerOptions:{module:ts.ModuleKind.ESNext}}).outputText.replace(/from ['"]zod['"]/g, 'from '+JSON.stringify(import.meta.resolve('zod')));
 const {dashboardSchema}=await import('data:text/javascript;base64,'+Buffer.from(code).toString('base64'));
 await scenario('complete');assert.equal((await stats()).mean_seconds,4800);assert.equal((await stats()).p95_seconds,12900);
 for(const name of ['complete','partial','negative']) {
  await scenario(name);
  const jobs=await(await fetch(base+'/api/v1/elastic/jobs')).json();
  for(const job of jobs.items){const d=dashboardSchema.parse(await(await fetch(base+'/api/v1/elastic/jobs/'+job.uid+'/dashboard')).json());assert.equal(d.job.uid,job.uid);}
  if(name==='partial')assert.equal((await stats()).mean_seconds,3900);
 }
 await scenario('outage');assert.equal((await fetch(base+'/api/v1/elastic/jobs')).status,503);
 await scenario('empty');assert.equal((await(await fetch(base+'/api/v1/elastic/jobs')).json()).items.length,0);
 await scenario('invalid');assert.equal((await stats()).mean_seconds,'invalid');
 console.log('PASS: 6 scenarios, 60 task response contracts, JCT expected values.');
} finally { await scenario('complete'); }
