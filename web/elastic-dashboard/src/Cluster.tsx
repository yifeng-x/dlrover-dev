export function change(before: number | null, after: number | null) {
  return before !== null && before > 0 && after !== null ? (after - before) / before * 100 : null;
}
export function timestamp(value: string) { return Date.parse(value.includes('T') ? value + '+08:00' : value + 'T00:00:00+08:00') / 1000; }
export function localTime(seconds: number) { return new Date((seconds + 8*3600)*1000).toISOString().slice(0,16); }
export function adjustRange(values: string[], index: number, value: string, equal: boolean) {
  const next = values.map((v,k)=>k===index?value:v);
  const [a,b,c] = next.map(timestamp);
  if (equal && [a,b,c].every(Number.isFinite) && b>a) next[3]=localTime(c+b-a);
  return next;
}
