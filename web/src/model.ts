export type WindowMetric={start:number;end:number;count:number;success:number;failed:number;errors:number;revenue:Record<string,number>;revenue_minor_exact?:Record<string,string>;types:Record<string,number>;active_users:number;eps:number;failure_rate:number;avg_latency_ms:number;p95_latency_ms:number;avg_processing_lag_ms:number;p95_processing_lag_ms:number;revision:number;shards:number;final:boolean;conversion:number};
export type Alert={id:string;rule_id:string;severity:string;observed:number;threshold:number;triggered_at:string;status:string;window_start:number;evidence:unknown};
export type Rule={id:string;kind:string;enabled:boolean;threshold:number;min_samples:number;cooldown_seconds:number;severity:string};
export type Partition={partition_id:number;last_offset:number;owner:string;high_watermark:number;lag:number;watermark:number;created_at:string;checksum:string;config_hash:string};
export type Pipeline={group:string;outbox_depth:number;partition_count:number;partitions:Partition[]};
export type EventRecord={event_id:string;event_type:string;event_time:number;partition_id:number;offset_id:number;late:number;data:string};
export const compact=(n:number)=>new Intl.NumberFormat('en',{notation:n>=10000?'compact':'standard',maximumFractionDigits:1}).format(n);
export function sum<K extends 'count'|'success'|'failed'|'errors'>(w:Record<K,number>[],key:K){return w.reduce((s,x)=>s+x[key],0)}
export type MoneyWindow={revenue:Record<string,number>;revenue_minor_exact?:Record<string,string>};
export function minor(w:MoneyWindow,currency='USD'):bigint{const exact=w.revenue_minor_exact?.[currency];if(exact!==undefined)return BigInt(exact);const n=w.revenue[currency]||0;if(!Number.isSafeInteger(n))throw new Error('Exact monetary data is required');return BigInt(n)}
export function revenue(w:MoneyWindow[],currency='USD'):bigint{return w.reduce((s,x)=>s+minor(x,currency),0n)}
export function formatMoney(n:bigint):string{return '$'+new Intl.NumberFormat('en').format(n/100n)+'.'+(n%100n).toString().padStart(2,'0')}
