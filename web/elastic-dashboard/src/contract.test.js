import { execFileSync } from 'node:child_process';
import { expect, it } from 'vitest';
import { dashboardSchema } from './model';

it('Python 聚合结果符合前端协议，未知生命周期和缺失时效不补造', () => {
  const script = `
import json, tempfile
from pathlib import Path
from dlrover.dashboard.elastic.store import EventStore
from dlrover.dashboard.elastic.aggregate import DashboardService
with tempfile.TemporaryDirectory() as directory:
    store = EventStore(Path(directory) / 'events.db')
    store.append(dict(schema_version=1, event_id='contract', occurred_at=100,
        event_type='evaluated', plan_id='plan', job=dict(job_uid='contract-job'),
        benefit=dict(mode='enforce', objective='time', evaluation_status='negative',
            estimates=dict(current_throughput=2, candidate_throughput=1,
                time_gain_seconds=-20, gpu_hours_saved=-1, gain_ratio=-.2))))
    print(json.dumps(DashboardService(store).dashboard('contract-job')))
`;
  const result = JSON.parse(execFileSync('python3', ['-c', script], {
    cwd: new URL('../../../', import.meta.url), encoding: 'utf-8',
  }));
  const data = dashboardSchema.parse(result);
  expect(data.job.status).toBe('unknown');
  expect(data.latest_evaluation?.max_age_seconds).toBeNull();
  expect(data.latest_evaluation?.gpu_seconds_saved).toBe(-3600);
  expect(data.jct.baseline_seconds).toBeNull();
  expect(data.jct.actual_seconds).toBeNull();
  expect(data.jct.measurement_status).toBe("unavailable");
  expect(data.series.points).toEqual([]);
});
