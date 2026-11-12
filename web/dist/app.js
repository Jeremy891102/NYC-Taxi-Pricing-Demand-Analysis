import {simulate,curve,parseState} from './model.js';
const $=id=>document.getElementById(id);
const esc=v=>String(v).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const num=(v,d=0)=>Number.isFinite(v)?v.toLocaleString('en-US',{maximumFractionDigits:d,minimumFractionDigits:d}):'—';
const signed=(v,d=0)=>`${v>0?'+':''}${num(v,d)}%`;
const clock=h=>`${String(h).padStart(2,'0')}:00`;
let state=parseState(location.search),data,zones,rows,index,zoneIndex,saved=null;

async function start(){
 try{
  const fetchJSON=async url=>{const r=await fetch(url);if(!r.ok)throw Error(`Unable to load ${url}`);return r.json();};
  [data,zones]=await Promise.all([fetchJSON('data/observations.json'),fetchJSON('data/zones.json')]);
  if(!data.rows?.length||!Array.isArray(zones))throw Error('Archived data is empty');
  rows=data.rows.map(r=>Object.fromEntries(data.columns.map((k,i)=>[k,r[i]])));
  index=new Map(rows.map(r=>[`${r.PULocationID}:${r.pickup_hour}`,r]));zoneIndex=new Map(zones.map(z=>[z.id,z]));
  if(!zoneIndex.has(state.zone))state.zone=161;
  $('zone').innerHTML=[...zones].sort((a,b)=>`${a.borough} ${a.name}`.localeCompare(`${b.borough} ${b.name}`)).map(z=>`<option value="${z.id}">${esc(z.borough)} · ${esc(z.name)}</option>`).join('');
  $('map').innerHTML=zones.filter(z=>z.path).map(z=>`<path class="zone-shape" data-zone="${z.id}" d="${z.path}" fill-rule="evenodd"><title>${esc(z.name)}</title></path>`).join('');
  $('map').addEventListener('click',e=>{const id=e.target.dataset.zone;if(id){state.zone=Number(id);render();}});
  $('map').addEventListener('pointerover',e=>{const id=Number(e.target.dataset.zone);if(zoneIndex.has(id))$('map-hover').textContent=`${zoneIndex.get(id).name} · ${zoneIndex.get(id).borough}`;});
  $('zone').addEventListener('change',e=>{state.zone=Number(e.target.value);render();});
  for(const id of ['hour','change','elasticity'])$(id).addEventListener('input',e=>{state[id]=Number(e.target.value);render();});
  for(const mode of ['scenario','history'])$(`${mode}-tab`).addEventListener('click',()=>{state.mode=mode;render();});
  document.querySelectorAll('.presets button').forEach(b=>b.addEventListener('click',()=>{state.zone=Number(b.dataset.zone);state.hour=Number(b.dataset.hour);render();}));
  $('reset').addEventListener('click',()=>{state=parseState('');saved=null;render();$('action-status').textContent='Workspace reset.';});
  $('pin').addEventListener('click',()=>{saved={...state};render();$('action-status').textContent='Comparison A saved. Adjust the controls to create scenario B.';});
  $('clear-comparison').addEventListener('click',()=>{saved=null;render();});
  $('share').addEventListener('click',share);
  $('download').addEventListener('click',download);
  addEventListener('popstate',()=>{state=parseState(location.search);render();});
  $('provenance').textContent=`${num(data.meta.rows)} observations; ${num(data.meta.missingHighElasticity)} have no high-price association coefficient. All are preserved. The source is the repo’s committed master table; SHA-256 ${data.meta.sha256.slice(0,12)}…. Zone names and simplified boundaries come from official TLC files.`;
  $('load-state').hidden=true;$('workspace').hidden=false;render();
 }catch(e){
  console.error(e);$('load-state').textContent='The archived data could not be loaded. Please reload to try again.';
  const b=document.createElement('button');b.textContent='Reload';b.addEventListener('click',()=>location.reload());$('load-state').append(document.createElement('br'),b);
 }
}
function rowFor(s=state){return index.get(`${s.zone}:${s.hour}`);}
function render(){
 const row=rowFor(),zone=zoneIndex.get(state.zone),scenario=state.mode==='scenario';
 for(const id of ['zone','hour','change','elasticity'])$(id).value=state[id];
 $('hour-value').textContent=clock(state.hour);$('change-value').textContent=signed(state.change);$('elasticity-value').textContent=num(state.elasticity,1);
 document.querySelectorAll('.scenario-only').forEach(e=>e.classList.toggle('hidden',!scenario));
 $('scenario-tab').setAttribute('aria-pressed',String(scenario));$('history-tab').setAttribute('aria-pressed',String(!scenario));
 $('mode-note').textContent=scenario?'Your assumptions. Instant feedback.':'Observed patterns. No simulated effects.';
 $('view-badge').textContent=scenario?'CONDITIONAL SCENARIO':'HISTORICAL DATA';
 $('borough').textContent=`${zone.borough} / ZONE ${zone.id}`;
 $('selection-title').textContent=`${zone.name} · ${clock(state.hour)}`;
 $('context-tags').textContent=row?`${row.demand_level} relative volume · ${row.journey_type} trip context`:'No observation for this zone–hour';
 $('data-warning').hidden=Boolean(row)&&row.avg_price<=100;
 $('data-warning').textContent=!row?'No archived observation for this zone and hour. Select another context; no values have been filled in.':'Source-quality flag: this archived fare exceeds $100/mile. Treat this observation as unusual; it has not been corrected or removed.';
 $('pin').disabled=!row;$('download').disabled=!row;
 $('download').textContent=scenario?'Export scenario CSV ↓':'Export observations CSV ↓';
 $('price-label').textContent=scenario?'Scenario fare / mile':'Observed fare / mile';
 $('volume-label').textContent=scenario?'Trip volume index':'Archived trip volume';
 $('revenue-label').textContent=scenario?'Revenue index':'Mean trip distance';
 let result=row&&simulate(row.avg_price,state.change,state.elasticity);
 $('price-metric').textContent=row?`$${num(scenario?result.price:row.avg_price,2)}`:'—';
 $('price-detail').textContent=row?(scenario?`Baseline $${num(row.avg_price,2)} / mile · ${signed(state.change)}`:'Mean base passenger fare per mile'):'';
 $('volume-metric').textContent=row?num(scenario?result.demand:row.trip_count,scenario?1:0):'—';
 $('volume-detail').textContent=row?(scenario?`${signed(result.demand-100,1)} vs baseline 100`:'2024 weekday-category mean, not trips/hour'):'';
 $('revenue-metric').textContent=row?num(scenario?result.revenue:row.trip_miles,1):'—';
 $('revenue-detail').textContent=row?(scenario?`${signed(result.revenue-100,1)} vs baseline 100`:'Miles · archived average'):'';
 $('association').textContent=row&&Number.isFinite(row.elasticity_high)?num(row.elasticity_high,2):'Not available';
 $('association-note').textContent=row&&row.elasticity_high>0?'Positive association can reflect surge pricing during high demand. It is not a causal elasticity.':'Descriptive context only. Never used as the scenario elasticity.';
 $('map-hour').textContent=clock(state.hour);$('map-hover').textContent=`${zone.name} · ${zone.borough}`;
 const colors=['#e0e8e8','#b7cbcc','#7dabad','#397c80','#174d53'];
 $('map').querySelectorAll('path').forEach(p=>{const r=index.get(`${p.dataset.zone}:${state.hour}`);p.classList.toggle('selected',Number(p.dataset.zone)===state.zone);p.classList.toggle('no-data',!r);p.setAttribute('fill',r?colors[r.avg_price<4?0:r.avg_price<6?1:r.avg_price<8?2:r.avg_price<10?3:4]:'#ddd');});
 $('result-note').textContent=scenario?'Indices are relative to this zone–hour’s baseline (100). Conditional on your elasticity; not a real-world forecast.':'Archived volume is an aggregate across the source window. It does not measure unmet demand or hourly arrivals.';
 renderChart(row,scenario);renderComparison(scenario);
}
function drawChart(series,xMin,xMax,active,xLabel){
 const left=52,right=478,top=22,bottom=265;
 const vals=series.flatMap(s=>s.points.filter(p=>Number.isFinite(p.y)).map(p=>p.y));
 const lo=Math.floor(Math.min(100,...vals)/20)*20,hi=Math.ceil(Math.max(100,...vals)/20)*20+ (Math.max(100,...vals)===100?20:0);
 const x=v=>left+(v-xMin)/(xMax-xMin)*(right-left),y=v=>bottom-(v-lo)/(hi-lo)*(bottom-top);
 let html='';
 for(let i=0;i<=4;i++){const v=lo+(hi-lo)*i/4;html+=`<line x1="${left}" y1="${y(v)}" x2="${right}" y2="${y(v)}" stroke="#e3e7e0"/><text x="42" y="${y(v)+4}" text-anchor="end" fill="#626b6a" font-size="11">${num(v)}</text>`;}
 html+=`<line x1="${left}" y1="${y(100)}" x2="${right}" y2="${y(100)}" stroke="#7b8984" stroke-dasharray="4 4"/>`;
 for(const v of (xMax===23?[0,6,12,18,23]:[-10,-5,0,5,10,15])){html+=`<text x="${x(v)}" y="288" text-anchor="middle" font-size="11" fill="#626b6a">${num(v)}</text>`;}
 for(const s of series){let path='',gap=true;for(const p of s.points){if(!Number.isFinite(p.y)){gap=true;continue;}path+=`${gap?'M':'L'}${x(p.x).toFixed(2)},${y(p.y).toFixed(2)}`;gap=false;}html+=`<path d="${path}" fill="none" stroke="${s.color}" stroke-width="3" stroke-linecap="round"/>`;const point=s.points.find(p=>p.x===active);if(point&&Number.isFinite(point.y))html+=`<circle cx="${x(active)}" cy="${y(point.y)}" r="5" fill="${s.color}" stroke="white" stroke-width="2"/>`;}
 html+=`<line x1="${x(active)}" y1="${top}" x2="${x(active)}" y2="${bottom}" stroke="#8a9085" stroke-dasharray="3 4"/><text x="265" y="323" text-anchor="middle" font-size="11" fill="#626b6a">${xLabel}</text>`;
 $('chart').innerHTML=html;
}
function renderChart(row,scenario){
 $('chart-title').textContent=scenario?'The pricing trade-off':'Within-zone daily profile';
 $('chart').setAttribute('aria-label',scenario?'Trip volume and revenue indices under assumed fare changes':'Historical fare and volume profiles, normalized by zone means');
 $('chart-badge').textContent=scenario?'BASELINE = 100':'ZONE MEAN = 100';
 $('chart-key').innerHTML=scenario?'<span>Trip volume</span><span>Revenue</span>':'<span>Volume index</span><span>Fare index</span>';
 if(!row){$('chart').innerHTML='<text x="250" y="160" text-anchor="middle" fill="#626b6a" font-size="14">No observation for this context</text>';$('chart-caption').textContent='Choose another zone or hour to explore.';return;}
 if(scenario){
  const c=curve(row.avg_price,state.elasticity);
  drawChart([{color:'#146c73',points:c.map(p=>({x:p.change,y:p.demand}))},{color:'#a86b00',points:c.map(p=>({x:p.change,y:p.revenue}))}],-10,15,state.change,'Fare adjustment (%)');
  $('chart-caption').textContent=`At ${signed(state.change)}, assumed elasticity ${num(state.elasticity,1)} gives ${signed(simulate(row.avg_price,state.change,state.elasticity).demand-100,1)} trip volume. Distance and trip mix stay fixed.`;
 }else{
  const day=Array.from({length:24},(_,h)=>index.get(`${state.zone}:${h}`));
  const valid=day.filter(Boolean);const avg=k=>valid.reduce((a,r)=>a+r[k],0)/valid.length;
  const vol=avg('trip_count'),price=avg('avg_price');
  drawChart([{color:'#146c73',points:day.map((r,h)=>({x:h,y:r?r.trip_count/vol*100:null}))},{color:'#a86b00',points:day.map((r,h)=>({x:h,y:r?r.avg_price/price*100:null}))}],0,23,state.hour,'Pickup hour (0–23)');
  $('chart-caption').textContent='Each series is normalized by its available-hour mean for this zone. Gaps remain missing; this is a historical profile, not a pricing response.';
 }
}
function renderComparison(scenario){
 $('comparison').hidden=!saved||!scenario;if(!saved||!scenario)return;
 const display=(s,label)=>{const r=rowFor(s),v=simulate(r.avg_price,s.change,s.elasticity);return `<div class="comparison-row"><div><strong>${label} · ${esc(zoneIndex.get(s.zone).name)}</strong><small>${clock(s.hour)} · fare ${signed(s.change)} · ε ${num(s.elasticity,1)}</small></div><div>$${num(v.price,2)}</div><div>${num(v.demand,1)}</div><div>${num(v.revenue,1)}</div></div>`;};
 $('comparison-content').innerHTML='<div class="comparison-row"><span>Context & assumptions</span><span>Fare / mile</span><span>Trip index</span><span>Revenue index</span></div>'+display(saved,'A')+(rowFor()?display(state,'B'):'<p class="chart-caption">B has no archived observation.</p>');
}
async function share(){
 const url=new URL(location.href);url.search=new URLSearchParams(state).toString();url.hash='';
 try{await navigator.clipboard.writeText(url.href);history.replaceState(null,'',url);$('action-status').textContent='Scenario link copied. It opens with these controls; comparison A is local to this session.';}
 catch{$('action-status').textContent=`Copy this link: ${url.href}`;}
}
function download(){
 const r=rowFor();if(!r)return;
 const scenario=state.mode==='scenario';
 const fields=scenario?['zone_id','zone','hour','assumed_elasticity','fare_change_pct','fare_per_mile','trip_index','revenue_index','source_sha256']:['zone_id','zone','hour','observed_fare_per_mile','archived_volume','mean_trip_miles','observed_high_price_association','source_sha256'];
 const records=scenario?curve(r.avg_price,state.elasticity).map(p=>[state.zone,zoneIndex.get(state.zone).name,state.hour,state.elasticity,p.change,p.price,p.demand,p.revenue,data.meta.sha256]):rows.filter(p=>p.PULocationID===state.zone).map(p=>[state.zone,zoneIndex.get(state.zone).name,p.pickup_hour,p.avg_price,p.trip_count,p.trip_miles,p.elasticity_high??'',data.meta.sha256]);
 const quote=v=>`"${String(v).replace(/"/g,'""')}"`;
 const text=[fields.join(','),...records.map(r=>r.map(quote).join(','))].join('\r\n');
 const href=URL.createObjectURL(new Blob([text],{type:'text/csv;charset=utf-8'}));const a=document.createElement('a');a.href=href;a.download=`taxi-${state.mode}-zone-${state.zone}-hour-${state.hour}.csv`;a.click();setTimeout(()=>URL.revokeObjectURL(href),1000);$('action-status').textContent=scenario?'Exported all 26 fare scenarios with assumptions and source fingerprint.':'Exported available historical hours for this zone with source fingerprint.';
}
start();
