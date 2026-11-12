import {describe,it,expect} from 'vitest';
import {sum,revenue,formatMoney} from './model';
describe('dashboard monetary presentation',()=>{it('sums only the selected currency in minor units',()=>{const w:{count:number;revenue:Record<string,number>}[]=[{count:2,revenue:{USD:123,EUR:999}},{count:3,revenue:{USD:456}}];expect(sum(w,'count')).toBe(5);expect(revenue(w)).toBe(579n);expect(revenue(w,'EUR')).toBe(999n)});it('handles an empty live stream',()=>{expect(revenue([])).toBe(0n);expect(sum([],'count')).toBe(0)})});

it('preserves monetary totals beyond JavaScript safe integers',()=>{const windows=[{revenue:{USD:0},revenue_minor_exact:{USD:'9007199254740993'}},{revenue:{USD:0},revenue_minor_exact:{USD:'7'}}];expect(revenue(windows)).toBe(9007199254741000n);expect(formatMoney(revenue(windows))).toBe('$90,071,992,547,410.00')});
