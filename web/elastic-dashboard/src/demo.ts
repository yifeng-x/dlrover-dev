import { Dashboard, Evaluation } from './model';
const at = (time: string) => Date.parse(`2026-11-06T${time}+08:00`) / 1000;
const evaluation: Evaluation = { stage: 'exploration', objective: 'time', work_unit: 'steps', data_source: 'mock', valid: true, observed_at: at('14:36:00'), max_age_seconds: 60, time_gain_seconds: 1440, gpu_seconds_saved: 1320, gain_ratio: .75, min_gain_ratio: .05, baseline_throughput: 2, candidate_throughput: 9.85, windows: 2, state: 'accepted', reported: true };
export const demoJobs = [
  { uid: 'demo-train-001', name: 'train-001', cluster: 'demo-cluster', namespace: 'elastic-training', status: 'completed' as const },
  { uid: 'demo-train-002', name: 'train-002', cluster: 'demo-cluster', namespace: 'elastic-training', status: 'running' as const },
];
export function demoDashboard(uid: string): Dashboard {
  const job = demoJobs.find(j => j.uid === uid) ?? demoJobs[0];
  const data: Dashboard = {
    schema_version: 1, generated_at: at('14:40:00'), job,
    jct: { submitted_at: at('13:04:00'), completed_at: at('14:40:00'), baseline_seconds: 7200, baseline_frozen_at: at('14:32:00'), baseline_source: 'estimate', baseline_description: '原配置吞吐估算' },
    latest_evaluation: evaluation, summary: { attempted: 2, succeeded: 1, failed: 1, complete: true },
    series: { work_unit: 'steps', points: Array.from({ length: 41 }, (_, i) => ({ at: at('14:30:00') + i * 15, gpus: i < 12 ? 2 : 4, throughput: i < 8 ? 2 + Math.sin(i) * .06 : i < 14 ? 0 : 9.85 + Math.sin(i) * .1 })) },
    events: [
      { id: 'evt-003', plan_id: 'plan-003', occurred_at: at('14:32:00'), type: 'expand', trigger: '吞吐优化', before_gpus: 2, target_gpus: 4, actual_gpus: 4, execution_status: 'completed', recovery_status: 'verified_by_exploration', conclusion: 'accepted', reason: 'positive_observed_gain', interruption_seconds: 85, interruption_start: at('14:32:00'), interruption_end: at('14:33:25'), evaluation, stages: [ { name: '开始调整', at: at('14:32:00') }, { name: 'checkpoint 完成', at: at('14:32:15') }, { name: '资源替换完成', at: at('14:32:50') }, { name: '恢复训练', at: at('14:33:25') }, { name: '收益评估完成', at: at('14:36:00') } ], before_finish_at: at('15:04:00'), after_finish_at: at('14:40:00') },
      { id: 'evt-002', plan_id: 'plan-002', occurred_at: at('14:10:00'), type: 'shrink', trigger: '资源回收', before_gpus: 2, target_gpus: 1, actual_gpus: 2, execution_status: 'skipped', recovery_status: 'not_started', conclusion: 'rejected', reason: 'insufficient_gain', interruption_seconds: null, interruption_start: null, interruption_end: null, evaluation: { ...evaluation, stage: 'prediction', observed_at: at('14:10:00'), gain_ratio: -.12, time_gain_seconds: -300, gpu_seconds_saved: 600, state: null }, stages: [{ name: '收益门禁拒绝', at: at('14:10:00') }], before_finish_at: null, after_finish_at: null },
      { id: 'evt-001', plan_id: 'plan-001', occurred_at: at('13:55:00'), type: 'expand', trigger: '人工调整', before_gpus: 2, target_gpus: 4, actual_gpus: 2, execution_status: 'failed', recovery_status: 'unknown', conclusion: 'failed', reason: 'replacement_failed', interruption_seconds: null, interruption_start: null, interruption_end: null, evaluation: null, stages: [{ name: '开始调整', at: at('13:55:00') }, { name: '资源替换失败', at: at('13:57:00') }], before_finish_at: null, after_finish_at: null },
    ], missing_data: [],
  };
  if (job.status === 'running') {
    data.jct = { ...data.jct, completed_at: null, baseline_seconds: null, baseline_frozen_at: null, baseline_source: 'unavailable', baseline_description: '尚未冻结基线' };
    data.latest_evaluation = { ...evaluation, valid: false, observed_at: null, time_gain_seconds: null, gpu_seconds_saved: null, gain_ratio: null, state: 'inconclusive', windows: 1, reported: false };
    data.events = []; data.series.points = []; data.summary = { attempted: null, succeeded: null, failed: null, complete: false };
    data.missing_data = ['尚无完整观测窗口', '尚无持久化事件记录', 'JCT 基线未冻结'];
  }
  return structuredClone(data);
}
