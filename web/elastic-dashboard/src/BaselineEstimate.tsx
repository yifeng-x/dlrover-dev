import { useEffect, useId, useRef, useState } from 'react';
import { ChevronDown, X } from 'lucide-react';
import { type Dashboard, fmt } from './model';

export default function BaselineEstimate({jct}: {jct: Dashboard['jct']}) {
  const [open, setOpen] = useState(false);
  const group = useRef<HTMLSpanElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  const id = useId();
  const basis = jct.baseline_basis;

  useEffect(() => {
    if (!open) return;
    function dismiss(event: PointerEvent) {
      if (!group.current?.contains(event.target as Node)) setOpen(false);
    }
    function escape(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        setOpen(false);
        trigger.current?.focus();
      }
    }
    document.addEventListener('pointerdown', dismiss);
    document.addEventListener('keydown', escape);
    return () => {
      document.removeEventListener('pointerdown', dismiss);
      document.removeEventListener('keydown', escape);
    };
  }, [open]);

  const speed = basis?.average_steps_per_second.toLocaleString('zh-CN', {
    minimumSignificantDigits: 3, maximumSignificantDigits: 6,
  });
  const trainingMinutes = basis ? basis.total_steps / basis.average_steps_per_second / 60 : null;
  return <span ref={group} className="estimate-anchor">
    <button ref={trigger} type="button" className="estimate-trigger" aria-expanded={open}
      aria-controls={open ? id : undefined} onClick={() => setOpen(!open)}>
      {open ? '收起估算依据' : '查看估算依据'}<ChevronDown size={14} aria-hidden="true"/>
    </button>
    {open && <span id={id} role="region" aria-label="不调整基线的估算依据" className="estimate-popover">
      <span className="estimate-heading">不调整基线的估算依据
        <button type="button" aria-label="关闭估算依据" onClick={() => {setOpen(false); trigger.current?.focus();}}><X size={16}/></button>
      </span>
      {basis ? <>
        <span className="estimate-row">总训练步数<b>{fmt(basis.total_steps, 'steps')}</b></span>
        <span className="estimate-row">调整前平均训练速度<b>{speed} step/s</b></span>
        <span className="estimate-row">排队时间<b>{basis.queue_seconds === null ? '未采集' : fmt(basis.queue_seconds / 60, '分钟')}</b></span>
        <span className="estimate-math">{fmt(basis.total_steps, 'steps')} ÷ {speed} step/s ≈ {fmt(basis.total_steps / basis.average_steps_per_second, '秒')}（{fmt(trainingMinutes, '分钟')}）<br/>
          {basis.queue_seconds !== null && jct.baseline_seconds !== null
            ? `训练 ${fmt(trainingMinutes)} + 排队 ${fmt(basis.queue_seconds / 60)} ≈ ${fmt(jct.baseline_seconds / 60, '分钟')}`
            : '缺少排队时间，完整 JCT 待补充'}
        </span>
        <small>速度取自调整前的观测窗口 · {basis.data_source}</small>
      </> : <span>该历史基线未记录估算明细。</span>}
    </span>}
  </span>;
}
