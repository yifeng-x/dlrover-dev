import { z } from 'zod';
import { dashboardSchema, jobSchema } from './model';
import { demoDashboard, demoJobs } from './demo';

export type Mode = 'demo' | 'api';
export class RequestError extends Error {
  constructor(message: string, readonly formHint?: string) { super(message); }
}

export async function requestJson<T>(
  operation: string, path: string, options: RequestInit, decode: (body: any) => T,
): Promise<T> {
  let response: Response | undefined;
  try {
    response = await fetch(path, { credentials: 'same-origin', ...options });
    if (!response.ok) {
      const body = await response.json().catch(() => null);
      const validation = response.status === 422 && body?.code === 'INVALID_BASELINE';
      const conflict = response.status === 409 && path === '/api/v1/elastic/baselines';
      const hint = (validation || conflict) && typeof body?.message === 'string' ? body.message : undefined;
      throw new RequestError(`HTTP ${response.status}`, hint);
    }
    return decode(await response.json());
  } catch (error) {
    if (!options.signal?.aborted) {
      console.error('[弹性看板]', operation, options.method ?? 'GET', path, response?.status, error);
    }
    throw error;
  }
}

export async function loadJobs(mode: Mode, signal: AbortSignal) {
  if (mode === 'demo') return demoJobs;
  return requestJson('作业列表', '/api/v1/elastic/jobs', {signal}, body =>
    z.object({ items: z.array(jobSchema) }).parse(body).items);
}
export async function loadDashboard(mode: Mode, uid: string, signal: AbortSignal) {
  if (mode === 'demo') return dashboardSchema.parse(demoDashboard(uid));
  return requestJson('任务分析', `/api/v1/elastic/jobs/${encodeURIComponent(uid)}/dashboard`, {signal}, body => {
    const result = dashboardSchema.parse(body);
    if (result.job.uid !== uid) throw new Error('接口返回的作业 UID 不匹配，已拒绝显示。');
    return result;
  });
}
