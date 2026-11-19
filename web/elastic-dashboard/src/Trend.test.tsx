import { renderToStaticMarkup } from 'react-dom/server';
import { expect, it } from 'vitest';
import { demoDashboard } from './demo';
import Trend, { TrendPlot } from './Trend';

it('同秒事件分行，独立的 mock 观测窗口在无 GPU 历史时可见', () => {
  const data = demoDashboard('demo-train-001');
  const event = data.events[0];
  const at = event.occurred_at;
  event.stages = [{ at, name: '调整前收益评估' }, { at, name: '确认训练恢复' }];
  data.series.points = [];
  data.series.windows = [{start:at-60, end:at-5, evaluated_at:at,
    throughput:1, candidate:2, source:'mock'}];
  const html = renderToStaticMarkup(<TrendPlot data={data} events={[event]}
    selected={event.id} onSelect={() => {}} start={at-60} end={at+1}/>);
  expect(html).toContain('SwanLab 观测窗口（mock）');
  expect(html).toContain('评估时候选吞吐（预测）');
  const rows = [...html.matchAll(/<g class="event-marker".*?<rect x="[^"]*" y="([^"]*)"/gs)];
  expect(rows.length).toBe(3);
  expect(new Set(rows.map(row => row[1])).size).toBe(3);
});

it('默认横轴固定为任务完成前五分钟，早期事件不会拉长范围', () => {
  const data = demoDashboard('demo-train-001');
  const html = renderToStaticMarkup(<Trend data={data} events={data.events} selected="" onSelect={() => {}}/>);
  expect(html).toContain('时间范围 14:35:00 至 14:40:00');
  expect(html).not.toContain('14:32:00 扩容');
});
