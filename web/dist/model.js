export function simulate(price, change, elasticity) {
  if (![price,change,elasticity].every(Number.isFinite) || price <= 0 || change < -10 || change > 15 || elasticity < -3 || elasticity > 0) throw new RangeError('Unsupported scenario inputs');
  const multiplier = 1 + change / 100;
  const demand = 100 * Math.max(0, 1 + elasticity * change / 100);
  return {price: Number((price * multiplier).toPrecision(14)), demand, revenue: Number((multiplier * demand).toPrecision(14))};
}
export const curve = (price, elasticity) => Array.from({length:26},(_,i)=>({change:i-10,...simulate(price,i-10,elasticity)}));
export function parseState(search) {
  const q=new URLSearchParams(search);
  const bounded=(key,min,max,fallback,integer=false)=>{const v=q.has(key)?Number(q.get(key)):NaN;return Number.isFinite(v)&&v>=min&&v<=max&&(!integer||Number.isInteger(v))?v:fallback;};
  return {zone:bounded('zone',1,265,161,true),hour:bounded('hour',0,23,18,true),change:bounded('change',-10,15,10),elasticity:bounded('elasticity',-3,0,-1),mode:q.get('mode')==='history'?'history':'scenario'};
}
