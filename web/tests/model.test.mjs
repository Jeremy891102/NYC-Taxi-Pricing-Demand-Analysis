import { test } from 'node:test';
import assert from 'node:assert/strict';
import { simulate, curve, parseState } from '../dist/model.js';

test('zero price change preserves baseline indices', () => {
 assert.deepEqual(simulate(6, 0, -1), {price:6, demand:100, revenue:100});
});
test('10% price increase with unit elasticity reduces demand by 10% and revenue by 1%', () => {
 const r=simulate(6,10,-1); assert.equal(r.price,6.6); assert.equal(r.demand,90); assert.ok(Math.abs(r.revenue-99)<1e-10);
});
test('zero elasticity leaves demand unchanged', () => {assert.equal(simulate(6,15,0).revenue,115);});
test('rejects invalid inputs instead of fabricating scenarios', () => {
 for(const args of [[NaN,0,-1],[0,0,-1],[6,16,-1],[6,-11,-1],[6,10,1],[6,10,-4]]) assert.throws(()=>simulate(...args));
});
test('curve includes baseline and both supported boundaries', () => {
 const c=curve(6,-2);assert.equal(c.length,26);assert.equal(c[0].change,-10);assert.equal(c.at(-1).change,15);assert.equal(c[10].revenue,100);
});
test('untrusted URL parameters are bounded and missing values use defaults',()=>{
 assert.deepEqual(parseState(''),{zone:161,hour:18,change:10,elasticity:-1,mode:'scenario'});
 assert.deepEqual(parseState('?zone=-1&hour=99&change=Infinity&elasticity=abc&mode=x'),parseState(''));
 assert.equal(parseState('?zone=132&hour=7&change=-10&elasticity=-2&mode=history').zone,132);
});
