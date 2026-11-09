import { describe, expect, it } from 'vitest';
import { demoDashboard } from './demo';
import { dashboardSchema, filteredEvents, fmt, gainValue, jctValues, elapsedJct } from './model';

describe('收益口径与空值', () => {
  it('任务级 JCT 按提交到完成计算，不能使用剩余时间收益率', () => {
    const data=demoDashboard('demo-train-001');
    expect(jctValues(data)).toEqual({actual:5760,baseline:7200,gain:1440,ratio:.2});
    expect(data.latest_evaluation?.gain_ratio).toBe(.75);
  });
  it('运行中、缺基线或时间倒置不生成实际收益', () => {
    expect(jctValues(demoDashboard('demo-train-002')).gain).toBeNull();
    const data=demoDashboard('demo-train-001'); data.jct.baseline_frozen_at=null;
    expect(jctValues(data).gain).toBeNull();
    data.jct.completed_at=data.jct.submitted_at!-1;
    expect(jctValues(data).actual).toBeNull();
  });
  it('负收益保留；无效结果不会被当作零收益', () => {
    const data=demoDashboard('demo-train-001'); const e=data.events[1].evaluation!;
    expect(gainValue(e,'time_gain_seconds')).toBe(-300);
    expect(gainValue({...e,valid:false},'time_gain_seconds')).toBeNull();
    expect(fmt(null)).toBe('—'); expect(fmt(0)).toBe('0');
  });
  it('拒绝非有限数值与缺失协议字段', () => {
    const data=demoDashboard('demo-train-001'); data.latest_evaluation!.gain_ratio=NaN;
    expect(dashboardSchema.safeParse(data).success).toBe(false);
    expect(dashboardSchema.safeParse({}).success).toBe(false);
  });
  it('筛选不会把门禁跳过当作扩容成功，且支持时间与关键词', () => {
    const data=demoDashboard('demo-train-001');
    expect(filteredEvents(data.events,'skipped','',0).map(e=>e.id)).toEqual(['evt-002']);
    expect(filteredEvents(data.events,'expand','plan-003',0).map(e=>e.id)).toEqual(['evt-003']);
    expect(filteredEvents(data.events,'all','',data.events[0].occurred_at).length).toBe(1);
  });
});

it('服务端 JCT 字段及运行耗时，失败不展示成功 JCT', () => {
  const running = demoDashboard('demo-train-002');
  expect(elapsedJct(running)).toBe(running.generated_at-running.jct.submitted_at!);
  expect(jctValues(running).actual).toBeNull();
  const done = demoDashboard('demo-train-001');
  done.jct.actual_seconds = null;
  expect(jctValues(done).actual).toBeNull();
  done.jct.actual_seconds = 5760;
  expect(jctValues(done).actual).toBe(5760);
  done.job = {...done.job, status: 'failed'};
  expect(jctValues(done).actual).toBeNull();
  expect(elapsedJct(done)).toBeNull();
});
