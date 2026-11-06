import { afterEach, expect, it, vi } from 'vitest';
import { loadDashboard, loadJobs } from './api';
import { demoDashboard } from './demo';
afterEach(() => vi.unstubAllGlobals());
it('真实接口失败不降级为示例数据', async () => {
  vi.stubGlobal('fetch',vi.fn().mockResolvedValue(new Response('{}',{status:503})));
  await expect(loadJobs('api',new AbortController().signal)).rejects.toThrow('503');
});
it('拒绝其他作业响应，防止串任务', async () => {
  vi.stubGlobal('fetch',vi.fn().mockResolvedValue(new Response(JSON.stringify(demoDashboard('demo-train-001')))));
  await expect(loadDashboard('api','different-job',new AbortController().signal)).rejects.toThrow('UID 不匹配');
});
it('透传取消信号并编码 UID', async () => {
  const data=demoDashboard('demo-train-001'); data.job.uid='uid/with space';
  const fetchMock=vi.fn().mockResolvedValue(new Response(JSON.stringify(data))); vi.stubGlobal('fetch',fetchMock);
  const signal=new AbortController().signal;
  await loadDashboard('api',data.job.uid,signal);
  expect(fetchMock).toHaveBeenCalledWith('/api/v1/elastic/jobs/uid%2Fwith%20space/dashboard',expect.objectContaining({signal}));
});
