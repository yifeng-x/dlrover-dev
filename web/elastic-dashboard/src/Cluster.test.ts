import { describe, expect, it } from 'vitest';
import { change, timestamp } from './Cluster';
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
