import { describe, expect, it } from 'vitest';
import { adjustRange, change, timestamp } from './Cluster';
describe('cluster JCT comparison', () => {
  it('keeps regressions and does not invent missing or zero-baseline gains', () => {
    expect(change(100, 80)).toBe(-20);
    expect(change(100, 120)).toBe(20);
    expect(change(100, 0)).toBe(-100);
    expect(change(0, 10)).toBeNull();
    expect(change(null, 10)).toBeNull();
    expect(change(10, null)).toBeNull();
  });
  it('uses Shanghai midnight independent of browser timezone', () => {
    expect(timestamp('2026-11-02')).toBe(Date.parse('2026-11-01T16:00:00Z')/1000);
    expect(Number.isNaN(timestamp(''))).toBe(true);
  });
});

it('supports minute precision and equal comparison duration', () => {
  expect(timestamp('2026-11-19T09:30')).toBe(Date.parse('2026-11-19T01:30:00Z')/1000);
  const values=['2026-11-19T09:00','2026-11-19T12:00','2026-11-19T13:00','2026-11-19T16:00'];
  expect(adjustRange(values,1,'2026-11-19T10:30',true)[3]).toBe('2026-11-19T14:30');
  expect(adjustRange(values,1,'2026-11-19T10:30',false)[3]).toBe(values[3]);
});
