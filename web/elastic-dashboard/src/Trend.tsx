import { Dashboard, ElasticEvent, clock, finite, fmt } from './model';
import { useState } from 'react';

type TrendProps = { data: Dashboard; events: ElasticEvent[]; selected: string; onSelect: (id: string) => void };
type Bounds = { start: number; end: number };

function inputTime(seconds: number) {
  // datetime-local inputs show Shanghai wall time regardless of browser timezone.
  return new Date((seconds + 8 * 3600) * 1000).toISOString().slice(0, 19);
}

export default function Trend(props: TrendProps) {
  const latest = props.data.jct.completed_at ?? props.data.generated_at;
  const [minutes, setMinutes] = useState(5);
  const [custom, setCustom] = useState<Bounds | null>(null);
  const [editing, setEditing] = useState(false);
  const [draftStart, setDraftStart] = useState('');
  const [draftEnd, setDraftEnd] = useState('');
  const [error, setError] = useState('');
  const bounds = custom ?? { start: latest - minutes * 60, end: latest };

  return <>
    <div className="trend-range" role="group" aria-label="横坐标时间范围">
      <strong>时间范围</strong>
      {[1, 5, 10].map(value => <button key={value} type="button" aria-pressed={!custom && value === minutes}
        onClick={() => { setMinutes(value); setCustom(null); setEditing(false); }}>{value} 分钟</button>)}
      <button type="button" aria-expanded={editing} aria-pressed={!!custom} onClick={() => {
        setDraftStart(inputTime(bounds.start)); setDraftEnd(inputTime(bounds.end));
        setError(''); setEditing(!editing);
      }}>自定义范围</button>
    </div>
    {editing && <form className="trend-range trend-custom" onSubmit={event => {
      event.preventDefault();
      const start = Date.parse(draftStart + '+08:00') / 1000;
      const end = Date.parse(draftEnd + '+08:00') / 1000;
      if (!finite(start) || !finite(end) || start >= end) {
        setError('结束时间须晚于开始时间'); return;
      }
      setCustom({start, end}); setEditing(false);
    }}>
      <label>开始时间<input required type="datetime-local" step="1" value={draftStart} onChange={e => setDraftStart(e.target.value)}/></label>
      <label>结束时间<input required type="datetime-local" step="1" value={draftEnd} onChange={e => setDraftEnd(e.target.value)}/></label>
      <span>北京时间</span><button type="submit">应用</button>
      {error && <span role="alert">{error}</span>}
    </form>}
    <TrendPlot key={bounds.start + '/' + bounds.end} {...props} {...bounds}/>
  </>;
}

export function TrendPlot({ data, events, selected, onSelect, start, end }: TrendProps & Bounds) {
  const [hover, setHover] = useState<{ at: number; x: number; y: number } | null>(null);
  const visible = (t: number) => t >= start && t <= end;
  const overlaps = (left: number, right: number) => left <= end && right >= start;
  const points = data.series.points.filter(p => visible(p.at)).sort((a,b) => a.at-b.at);
  const windows = (data.series.windows ?? []).filter(w => overlaps(w.start, w.end) || (finite(w.candidate) && visible(w.evaluated_at)));
  const x = (t: number) => 90 + (t - start) / (end - start) * 688;
  const maxGpu = Math.max(1, ...points.map(p => p.gpus ?? 0));
  const maxSpeed = Math.ceil(Math.max(1, ...points.map(p => p.throughput ?? 0), ...windows.flatMap(w => [overlaps(w.start,w.end) ? w.throughput : 0, visible(w.evaluated_at) ? w.candidate ?? 0 : 0])) / 2) * 2;
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
  const laneRights: number[] = [];
  function marker(at: number, label: string, color: string, key: string, id?: string) {
    const width = Math.max(80, label.length * 12 + 12);
    const left = Math.max(90, Math.min(778 - width, x(at) - width / 2));
    let lane = laneRights.findIndex(right => right + 6 <= left);
    if (lane < 0) lane = laneRights.length;
    laneRights[lane] = left + width;
    const row = 317 + lane * 30;
    return <g key={key} className="event-marker" role={id ? 'button' : undefined} tabIndex={id ? 0 : undefined} aria-label={id ? `选择事件 ${id}` : undefined} onClick={id ? () => onSelect(id) : undefined} onKeyDown={id ? k => { if(k.key==='Enter'||k.key===' ') { k.preventDefault(); onSelect(id); } } : undefined}>
      <line x1={x(at)} x2={x(at)} y1={305} y2={row} stroke={color} opacity=".3"/>
      <circle cx={x(at)} cy={row} r={3} fill={color}/>
      <rect x={left} y={row} width={width} height="23" rx="4" fill="white" stroke={color}/>
      <text x={left+width/2} y={row+16} textAnchor="middle" style={{fill:color,fontSize:11,fontWeight:600}}>{label}</text>
      <title>{label}</title>
    </g>;
  }
  const current = hover && points.reduce<(typeof points)[number] | undefined>((nearest, point) =>
    !nearest || Math.abs(point.at - hover.at) < Math.abs(nearest.at - hover.at) ? point : nearest, undefined);
  const tipWidth = 178;
  const activeEvent = events.find(e => e.id === selected);
  const milestones = (activeEvent?.stages ?? []).filter(s => /恢复|评估|进程已启动/.test(s.name));
  if (finite(data.jct.completed_at)) milestones.push({ name: '任务完成', at: data.jct.completed_at });
  const entries = [
    ...events.map(e => ({at:e.occurred_at, label:{expand:'扩容', shrink:'缩容', other:'调整'}[e.type],
      color:e.execution_status==='failed'?'#d84c4c':e.execution_status==='skipped'?'#758296':e.type==='shrink'?'#138d81':'#1769ff', key:e.id, id:e.id})),
    ...milestones.map((s,i) => ({at:s.at,label:s.name,color:'#1769ff',key:`stage-${i}`,id:undefined})),
  ];
  const markers = entries.filter(m => m.at >= start && m.at <= end).sort((a,b) => a.at-b.at)
    .map(m => marker(m.at, `${clock(m.at)} ${m.label}`, m.color, m.key, m.id));
  const height = 355 + Math.max(0, laneRights.length - 1) * 30;
  return <>
    <div className="chart-scroll"><svg viewBox={`0 0 810 ${height}`} className="trend-svg" role="img" aria-label={`GPU 数量、${data.series.work_unit}/s 吞吐与弹性事件共用时间轴，时间范围 ${clock(start)} 至 ${clock(end)}`}>
      <defs><clipPath id="plot"><rect x="90" y="20" width="688" height="226"/></clipPath></defs>
      {[0, .5, 1].map(f => <g key={f}><line x1="90" x2="778" y1={112-f*65} y2={112-f*65} className="grid-line"/><text x="77" y={116-f*65} textAnchor="end">{fmt(maxGpu*f)}</text><line x1="90" x2="778" y1={244-f*90} y2={244-f*90} className="grid-line"/><text x="77" y={248-f*90} textAnchor="end">{fmt(maxSpeed*f)}</text></g>)}
      <g className="chart-axes" aria-label="纵坐标">
        <path d="M90 47V112H778 M90 154V244H778" fill="none"/>
        {[0, .5, 1].map(f => <g key={f}><line x1="85" x2="90" y1={112-f*65} y2={112-f*65}/><line x1="85" x2="90" y1={244-f*90} y2={244-f*90}/></g>)}
      </g>
      <text transform="translate(24 80) rotate(-90)" textAnchor="middle" className="axis-title">GPU 数</text>
      <text transform="translate(24 199) rotate(-90)" textAnchor="middle" className="axis-title">吞吐（{data.series.work_unit}/s）</text>
      <g clipPath="url(#plot)">{events.filter(e => finite(e.interruption_start) && finite(e.interruption_end) && overlaps(e.interruption_start, e.interruption_end)).map(e => <g key={e.id}><rect x={x(Math.max(start,e.interruption_start!))} y="30" width={Math.max(0,x(Math.min(end,e.interruption_end!))-x(Math.max(start,e.interruption_start!)))} height="216" fill="#fff2d8" opacity=".85"/><text x={x(Math.max(start,e.interruption_start!))+4} y="42" fill="#ad6900">{e.interruption_basis==='control_estimate'?'控制阶段估算':'中断'} {fmt(e.interruption_seconds,'s')}</text></g>)}
      <path d={area('gpus')} fill="#8554ef" fillOpacity=".10"/><path d={area('throughput')} fill="#1769ff" fillOpacity=".10"/>
      <path d={path('gpus')} fill="none" stroke="#8554ef" strokeWidth="2.5"/><path d={path('throughput')} fill="none" stroke="#1769ff" strokeWidth="2.5"/>
      <g style={{cursor:'crosshair'}} onPointerMove={event => {
        const svg = event.currentTarget.ownerSVGElement;
        const matrix = svg?.getScreenCTM();
        if (!matrix) return;
        const cursor = new DOMPoint(event.clientX, event.clientY).matrixTransform(matrix.inverse());
        setHover({at: start + (cursor.x - 90) / 688 * (end - start), x: cursor.x, y: cursor.y});
      }} onPointerLeave={() => setHover(null)} onPointerCancel={() => setHover(null)}>
      <rect x="90" y="30" width="688" height="216" fill="transparent"/>
      {windows.map((w,i) => <g key={i}>
        {overlaps(w.start,w.end) && <line x1={x(Math.max(start,w.start))} x2={x(Math.min(end,w.end))} y1={y(w.throughput,false)} y2={y(w.throughput,false)} stroke="#1769ff" strokeWidth="4"><title>{`${w.source} · SwanLab 窗口吞吐 ${fmt(w.throughput)} · ${clock(w.start)}–${clock(w.end)}`}</title></line>}
        {finite(w.candidate) && visible(w.evaluated_at) && <circle cx={x(w.evaluated_at)} cy={y(w.candidate,false)} r="4" stroke="#ad6900" fill="white"><title>{`评估时候选吞吐（预测） ${fmt(w.candidate)}`}</title></circle>}
      </g>)}</g></g>
      {Array.from({length:6},(_,i) => start+(end-start)*i/5).map(t => <text key={t} x={x(t)} y="265" textAnchor="middle">{end-start >= 86400 ? new Date(t*1000).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',month:'numeric',day:'numeric',hour:'2-digit',minute:'2-digit'}) : clock(t)}</text>)}
      <g className="event-track" aria-label="事件轨迹">
        <rect x="0" y="280" width="810" height={height-283} rx="6" fill="#f7f9fd"/>
        <text x="6" y="310" className="event-track-title">事件轨迹</text>
        <line x1="90" x2="778" y1="305" y2="305" stroke="#9caac0" strokeWidth="2"/>
      </g>
      {markers}
      {!points.some(p => finite(p.gpus) || finite(p.throughput)) && !windows.length && <text x="434" y="143" textAnchor="middle">所选时段暂无采样数据</text>}
      {hover && current && <g pointerEvents="none">
        <line x1={x(current.at)} x2={x(current.at)} y1="30" y2="246" stroke="#71849d" strokeDasharray="4 3"/>
        {finite(current.gpus) && <circle cx={x(current.at)} cy={y(current.gpus,true)} r="3.5" fill="#8554ef" stroke="white"/>}
        {finite(current.throughput) && <circle cx={x(current.at)} cy={y(current.throughput,false)} r="3.5" fill="#1769ff" stroke="white"/>}
        <g transform={`translate(${hover.x + tipWidth + 10 <= 810 ? hover.x + 10 : hover.x - tipWidth - 10},${Math.max(2, Math.min(178, hover.y - 22))})`}>
          <rect width={tipWidth} height="66" rx="6" fill="white" stroke="#ccd8e7"/>
          <text x="10" y="17" fontWeight="600">{clock(current.at)}</text>
          <text x="10" y="38" style={{fill:'#8554ef'}}>运行 Worker GPU 数　{fmt(current.gpus)}</text>
          <text x="10" y="55" style={{fill:'#1769ff'}}>吞吐　{fmt(current.throughput, `${data.series.work_unit}/s`)}</text>
        </g>
      </g>}
    </svg></div>
    <div className="chart-caption">{windows.length ? `蓝线：SwanLab 观测窗口（${[...new Set(windows.map(w=>w.source))].join(' / ')}） · 空心点：候选吞吐预测 · 单位 ${data.series.work_unit}/s` : '运行 Worker 分配 GPU 数 · 非 GPU 忙碌数量 · 缺失采样保留断点'}</div>
  </>;
}
