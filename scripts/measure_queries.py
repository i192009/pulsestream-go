"""Measure actual bounded ClickHouse queries; not an insert-throughput benchmark."""
import base64,datetime,json,time,urllib.request
from pathlib import Path
queries={
 'event_count':"SELECT count() FROM events FINAL WHERE pipeline='pulsestream-v1' AND tenant='tenant-demo'",
 'window_count':"SELECT count() FROM windows FINAL WHERE pipeline='pulsestream-v1' AND tenant='tenant-demo' AND size=60000 AND step=60000",
 'storage':"SELECT table,sum(rows),sum(bytes_on_disk) FROM system.parts WHERE active AND database='default' GROUP BY table FORMAT JSON",
}
def query(sql):
    req=urllib.request.Request('http://127.0.0.1:8123',data=sql.encode(),headers={'Authorization':'Basic '+base64.b64encode(b'pulse:pulse-dev').decode()})
    with urllib.request.urlopen(req,timeout=20) as r:return r.read().decode().strip()
results={}
for name,sql in queries.items():
    samples=[];value=None
    for _ in range(20):
        began=time.perf_counter();value=query(sql);samples.append((time.perf_counter()-began)*1000)
    ordered=sorted(samples);results[name]={'samples_ms':samples,'p50_ms':ordered[9],'p95_ms':ordered[18],'result':value}
root=Path(__file__).resolve().parents[1];(root/'docs/evidence/queries.json').write_text(json.dumps({'executed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'queries':results},indent=2))
print(json.dumps(results,indent=2))
