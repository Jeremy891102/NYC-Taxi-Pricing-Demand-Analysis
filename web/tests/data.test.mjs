import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const data=JSON.parse(readFileSync(new URL('../dist/data/observations.json',import.meta.url)));
const zones=JSON.parse(readFileSync(new URL('../dist/data/zones.json',import.meta.url)));
const rows=data.rows.map(r=>Object.fromEntries(data.columns.map((key,i)=>[key,r[i]])));
test('bundled export has one row per observed zone-hour and preserves missing coefficients',()=>{
 assert.equal(rows.length,6259); assert.equal(new Set(rows.map(r=>`${r.PULocationID}:${r.pickup_hour}`)).size,6259);
 assert.equal(rows.filter(r=>r.elasticity_high===null).length,4491);
 assert.equal(rows.filter(r=>r.avg_price>100).length,4);
 for(const r of rows){assert.ok(r.pickup_hour>=0&&r.pickup_hour<24);assert.ok(r.avg_price>0);assert.ok(zones.some(z=>z.id===r.PULocationID));}
});
test('zone geometry and source fingerprint are present without external runtime dependencies',()=>{
 assert.match(data.meta.sha256,/^[a-f0-9]{64}$/); assert.equal(zones.length,265);
 assert.equal(zones.filter(z=>z.path).length,263);assert.equal(zones.find(z=>z.id===132).name,'JFK Airport');
 for(const z of zones)assert.match(z.path,/^[MLZ0-9.,-]*$/);
});
