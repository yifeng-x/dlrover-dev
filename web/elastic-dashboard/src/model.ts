import { z } from 'zod';

const number = z.number().finite();
const nullable = number.nullable();
const timestamp = number.nonnegative();
export const jobSchema = z.object({ uid: z.string(), name: z.string(), cluster: z.string(), namespace: z.string(), status: z.enum(['running', 'completed', 'failed', 'unknown']) });
export const evaluationSchema = z.object({
  stage: z.enum(['prediction', 'exploration']), objective: z.enum(['time', 'gpu_hours', 'cluster_jct', 'unknown']),
  work_unit: z.enum(['steps', 'samples', 'tokens', 'unknown']), data_source: z.enum(['mock', 'http', 'unknown']),
  valid: z.boolean(), observed_at: timestamp.nullable(), max_age_seconds: number.nonnegative().nullable(),
  time_gain_seconds: nullable, gpu_seconds_saved: nullable, gain_ratio: nullable, min_gain_ratio: nullable,
  baseline_throughput: nullable, candidate_throughput: nullable, windows: number.int().nonnegative().nullable(),
  state: z.string().nullable(), reported: z.boolean().nullable(),
});
export const eventSchema = z.object({
  id: z.string(), plan_id: z.string().nullable(), occurred_at: timestamp,
  type: z.enum(['expand', 'shrink', 'other']), trigger: z.string(),
  before_gpus: nullable, target_gpus: nullable, actual_gpus: nullable,
  execution_status: z.enum(['pending', 'running', 'completed', 'failed', 'skipped', 'unknown']),
  recovery_status: z.string(), conclusion: z.enum(['accepted', 'retained', 'inconclusive', 'failed', 'rejected', 'unknown']),
  reason: z.string(), interruption_seconds: nullable, interruption_start: timestamp.nullable(), interruption_end: timestamp.nullable(),
  evaluation: evaluationSchema.nullable(),
  stages: z.array(z.object({ name: z.string(), at: timestamp })),
  before_finish_at: timestamp.nullable(), after_finish_at: timestamp.nullable(),
});
export const dashboardSchema = z.object({
  schema_version: z.literal(1), generated_at: timestamp, job: jobSchema,
  jct: z.object({ actual_seconds: number.nonnegative().nullable().optional(), elapsed_seconds: number.nonnegative().nullable().optional(), measurement_status: z.enum(['completed', 'running', 'unavailable']).optional(), submitted_at: timestamp.nullable(), completed_at: timestamp.nullable(), baseline_seconds: number.positive().nullable(), baseline_frozen_at: timestamp.nullable(), baseline_source: z.enum(['estimate', 'control_run', 'unavailable']), baseline_description: z.string() }),
  latest_evaluation: evaluationSchema.nullable(),
  summary: z.object({ attempted: number.int().nonnegative().nullable(), succeeded: number.int().nonnegative().nullable(), failed: number.int().nonnegative().nullable(), complete: z.boolean() }),
  series: z.object({ work_unit: z.enum(['steps', 'samples', 'tokens', 'unknown']), points: z.array(z.object({ at: timestamp, gpus: number.nonnegative().nullable(), throughput: number.nonnegative().nullable() })) }),
  events: z.array(eventSchema), missing_data: z.array(z.string()),
});
export type Dashboard = z.infer<typeof dashboardSchema>;
export type ElasticEvent = z.infer<typeof eventSchema>;
export type Evaluation = z.infer<typeof evaluationSchema>;
export type Job = z.infer<typeof jobSchema>;
export const objectiveLabel = { time: '剩余时间收益率', gpu_hours: '卡时收益率', cluster_jct: '集群加权 JCT 收益率', unknown: '收益率（目标未知）' };
export function finite(value: number | null | undefined): value is number { return typeof value === 'number' && Number.isFinite(value); }
export function fmt(value: number | null | undefined, unit = '', signed = false) { return finite(value) ? `${signed && value > 0 ? '+' : ''}${Number(value.toFixed(2)).toLocaleString('zh-CN')}${unit ? ` ${unit}` : ''}` : '—'; }
export function clock(at: number | null | undefined) { return finite(at) ? new Date(at * 1000).toLocaleTimeString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false }) : '—'; }
export function jctValues(data: Dashboard) {
  const j = data.jct;
  const computed = data.job.status === 'completed' && finite(j.submitted_at) && finite(j.completed_at) && j.completed_at >= j.submitted_at && j.completed_at <= data.generated_at ? j.completed_at - j.submitted_at : null;
  const actual = j.actual_seconds !== undefined ? (finite(computed) ? j.actual_seconds : null) : computed;
  const baseline = j.baseline_source !== 'unavailable' && finite(j.baseline_frozen_at) ? j.baseline_seconds : null;
  const gain = finite(baseline) && finite(actual) ? baseline - actual : null;
  return { actual, baseline, gain, ratio: finite(gain) && finite(baseline) && baseline > 0 ? gain / baseline : null };
}
export function elapsedJct(data: Dashboard) {
  if (data.job.status !== 'running') return null;
  const j = data.jct;
  if (!finite(j.submitted_at) || j.submitted_at > data.generated_at || j.completed_at !== null) return null;
  return j.elapsed_seconds !== undefined ? j.elapsed_seconds : data.generated_at - j.submitted_at;
}
export function dateTime(at: number | null | undefined) {
  return finite(at) ? new Date(at * 1000).toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai', hour12: false }) : '—';
}
export function gainValue(e: Evaluation | null, key: 'time_gain_seconds' | 'gpu_seconds_saved' | 'gain_ratio') { return e?.valid && finite(e[key]) ? e[key] : null; }
export function filteredEvents(events: ElasticEvent[], filter: string, query: string, from: number) {
  return events.filter(e => e.occurred_at >= from && (filter === 'all' || e.type === filter || e.execution_status === filter) && `${e.id} ${e.plan_id ?? ''} ${e.trigger} ${e.reason}`.toLowerCase().includes(query.toLowerCase())).sort((a,b) => b.occurred_at-a.occurred_at);
}
