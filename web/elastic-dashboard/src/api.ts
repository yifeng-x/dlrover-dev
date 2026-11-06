import { z } from 'zod';
import { dashboardSchema, jobSchema } from './model';
import { demoDashboard, demoJobs } from './demo';

export type Mode = 'demo' | 'api';
async function get(path: string, signal: AbortSignal) {
  const response = await fetch(path, { signal, credentials: 'same-origin', headers: { Accept: 'application/json' } });
  if (!response.ok) throw new Error(`接口请求失败（HTTP ${response.status}）`);
  try { return await response.json(); } catch { throw new Error('接口未返回 JSON，请检查 /api 代理配置。'); }
}
export async function loadJobs(mode: Mode, signal: AbortSignal) {
  if (mode === 'demo') return demoJobs;
  return z.object({ items: z.array(jobSchema) }).parse(await get('/api/v1/elastic/jobs', signal)).items;
}
export async function loadDashboard(mode: Mode, uid: string, signal: AbortSignal) {
  if (mode === 'demo') return dashboardSchema.parse(demoDashboard(uid));
  const result = dashboardSchema.parse(await get(`/api/v1/elastic/jobs/${encodeURIComponent(uid)}/dashboard`, signal));
  if (result.job.uid !== uid) throw new Error('接口返回的作业 UID 不匹配，已拒绝显示。');
  return result;
}
