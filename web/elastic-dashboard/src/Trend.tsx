import { Dashboard, ElasticEvent, clock, finite, fmt } from './model';
import { useState } from 'react';

export default function Trend({ data, events, selected, onSelect, from }: { data: Dashboard; events: ElasticEvent[]; selected: string; onSelect: (id: string) => void; from: number }) {
  const [hover, setHover] = useState<number | null>(null);
  const points = data.series.points.filter(p => p.at >= from).sort((a,b) => a.at-b.at);
  if (!points.length) return <div className="empty chart-empty">暂无历史采样数据<br/><small>需要接入 GPU 数量与训练吞吐时序</small><div className="empty-event-track"><strong>事件轨迹</strong>{events.length ? events.map(e => <button key={e.id} onClick={() => onSelect(e.id)}>{clock(e.occurred_at)} · {e.type === "expand" ? "扩容" : e.type === "shrink" ? "缩容" : "调整"}</button>) : <span>暂无弹性事件</span>}</div><ol className="empty-track-stages">{(events.find(e=>e.id===selected)??events[0])?.stages.map((s,i)=><li key={`${s.at}-${i}`}><time>{clock(s.at)}</time> {s.name}</li>)}</ol></div>;
  const start = points[0].at, end = Math.max(start + 1, points.at(-1)!.at);
  const x = (t: number) => 90 + (t - start) / (end - start) * 688;
  const maxGpu = Math.max(1, ...points.map(p => p.gpus ?? 0));
  const maxSpeed = Math.ceil(Math.max(1, ...points.map(p => p.throughput ?? 0)) / 2) * 2;
  const y = (v: number, gpu: boolean) => gpu ? 112 - v / maxGpu * 65 : 244 - v / maxSpeed * 90;
  function path(key: 'gpus' | 'throughput') {
    let d = '', active = false;
    for (const p of points) {
      const v = p[key];
      if (!finite(v)) { active = false; continue; }
      d += !active ? `M${x(p.at)},${y(v, key === 'gpus')}` : key === 'gpus' ? `H${x(p.at)}V${y(v, true)}` : `L${x(p.at)},${y(v,false)}`;
      active = true;
    }
    return d;
  }
  // Close each observed segment independently so missing samples stay gaps.
  function area(key: 'gpus' | 'throughput') {
    const floor = key === 'gpus' ? 112 : 244;
    let d = '', segment = '', first = 0, last = 0;
    const close = () => { if (segment) d += `${segment}L${last},${floor}L${first},${floor}Z`; segment = ''; };
    for (const p of points) {
      if (!finite(p[key])) { close(); continue; }
      const px = x(p.at), py = y(p[key], key === 'gpus');
      if (!segment) { first = px; segment = `M${px},${py}`; }
      else segment += key === 'gpus' ? `H${px}V${py}` : `L${px},${py}`;
      last = px;
    }
    close(); return d;
  }
  function marker(at: number, label: string, color: string, key: string, id?: string) {
    const width = Math.max(78, label.length * 11 + 12);
    const left = Math.max(90, Math.min(778 - width, x(at) - width / 2));
    return <g key={key} className="event-marker" role={id ? 'button' : undefined} tabIndex={id ? 0 : undefined} aria-label={id ? `选择事件 ${id}` : undefined} onClick={id ? () => onSelect(id) : undefined} onKeyDown={id ? k => { if(k.key==='Enter'||k.key===' ') { k.preventDefault(); onSelect(id); } } : undefined}>
      <circle cx={x(at)} cy="305" r={id===selected?6:5} fill="white" stroke={color} strokeWidth="2"/>
      <rect x={left} y="317" width={width} height="23" rx="4" fill={color} fillOpacity=".09" stroke={color} strokeOpacity=".3"/>
      <text x={left+width/2} y="333" textAnchor="middle" style={{fill:color,fontSize:11,fontWeight:600}}>{label}</text>
      <title>{clock(at)} {label}</title>
    </g>;
  }
  const current = points[hover ?? -1];
  const activeEvent = events.find(e => e.id === selected);
  const milestones = (activeEvent?.stages ?? []).filter(s => /恢复|评估/.test(s.name));
  if (finite(data.jct.completed_at)) milestones.push({ name: '任务完成', at: data.jct.completed_at });
  return <>
    <div className="chart-scroll"><svg viewBox="0 0 810 355" className="trend-svg" role="img" aria-label={`GPU 数量、${data.series.work_unit}/s 吞吐与弹性事件共用时间轴，时间范围 ${clock(start)} 至 ${clock(end)}`}>
      <defs><clipPath id="plot"><rect x="90" y="20" width="688" height="226"/></clipPath></defs>
      {[0, .5, 1].map(f => <g key={f}><line x1="90" x2="778" y1={112-f*65} y2={112-f*65} className="grid-line"/><text x="77" y={116-f*65} textAnchor="end">{fmt(maxGpu*f)}</text><line x1="90" x2="778" y1={244-f*90} y2={244-f*90} className="grid-line"/><text x="77" y={248-f*90} textAnchor="end">{fmt(maxSpeed*f)}</text></g>)}
      <g className="chart-axes" aria-label="纵坐标">
        <path d="M90 47V112H778 M90 154V244H778" fill="none"/>
        {[0, .5, 1].map(f => <g key={f}><line x1="85" x2="90" y1={112-f*65} y2={112-f*65}/><line x1="85" x2="90" y1={244-f*90} y2={244-f*90}/></g>)}
      </g>
      <text transform="translate(24 80) rotate(-90)" textAnchor="middle" className="axis-title">GPU 数</text>
      <text transform="translate(24 199) rotate(-90)" textAnchor="middle" className="axis-title">吞吐（{data.series.work_unit}/s）</text>
      <g clipPath="url(#plot)">{events.filter(e => finite(e.interruption_start) && finite(e.interruption_end)).map(e => <g key={e.id}><rect x={x(e.interruption_start!)} y="30" width={Math.max(0,x(e.interruption_end!)-x(e.interruption_start!))} height="216" fill="#fff2d8" opacity=".85"/><text x={x(e.interruption_start!)+4} y="42" fill="#ad6900">中断 {fmt(e.interruption_seconds,'s')}</text></g>)}
      <path d={area('gpus')} fill="#8554ef" fillOpacity=".10"/><path d={area('throughput')} fill="#1769ff" fillOpacity=".10"/>
      <path d={path('gpus')} fill="none" stroke="#8554ef" strokeWidth="2.5"/><path d={path('throughput')} fill="none" stroke="#1769ff" strokeWidth="2.5"/></g>
      {Array.from({length:6},(_,i) => start+(end-start)*i/5).map(t => <text key={t} x={x(t)} y="265" textAnchor="middle">{clock(t).slice(0,5)}</text>)}
      <g className="event-track" aria-label="事件轨迹">
        <rect x="0" y="280" width="810" height="72" rx="6" fill="#f7f9fd"/>
        <text x="6" y="310" className="event-track-title">事件轨迹</text>
        <line x1="90" x2="778" y1="305" y2="305" stroke="#9caac0" strokeWidth="2"/>
      </g>
      {events.filter(e => e.occurred_at >= start && e.occurred_at <= end).map(e => marker(e.occurred_at, `${clock(e.occurred_at).slice(0,5)} ${e.type==='expand'?'扩容':e.type==='shrink'?'缩容':'调整'}`, e.execution_status==='failed'?'#d84c4c':e.execution_status==='skipped'?'#758296':e.type==='shrink'?'#138d81':'#1769ff', e.id, e.id))}
      {milestones.filter(s=>s.at>=start&&s.at<=end).map((s,i)=>marker(s.at,`${clock(s.at).slice(0,5)} ${/恢复/.test(s.name)?'恢复':/评估/.test(s.name)?'评估':'完成'}`,/恢复/.test(s.name)?'#21964d':'#1769ff',`stage-${i}`))}
      {current && <line x1={x(current.at)} x2={x(current.at)} y1="45" y2="246" stroke="#8190aa" strokeDasharray="4 4"/>}
      {points.map((p,i) => <rect key={p.at} x={Math.max(90,x(p.at)-344/points.length)} y="45" width={688/points.length} height="200" fill="transparent" onMouseEnter={()=>setHover(i)} onMouseLeave={()=>setHover(null)}><title>{clock(p.at)} · GPU {fmt(p.gpus)} · {fmt(p.throughput,`${data.series.work_unit}/s`)}</title></rect>)}
    </svg></div>
    <div className="chart-caption">{current ? `${clock(current.at)} · GPU ${fmt(current.gpus)} · 吞吐 ${fmt(current.throughput, `${data.series.work_unit}/s`)}` : '运行 Worker 分配 GPU 数 · 非 GPU 忙碌数量 · 缺失采样保留断点'}</div>
  </>;
}
