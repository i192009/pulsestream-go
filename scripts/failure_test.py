"""Executable assertions against the running Compose stack. Uses synthetic data only."""
import base64, datetime as dt, json, subprocess, time, urllib.request, urllib.error, uuid
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]

def compose(*args): subprocess.run(['docker','compose',*args],cwd=ROOT,check=True,stdout=subprocess.DEVNULL)
def http(method,path,body=None,port=18081,key='demo-key'):
    r=urllib.request.Request(f'http://127.0.0.1:{port}{path}',data=None if body is None else json.dumps(body).encode(),method=method,headers={'Authorization':'Bearer '+key,'Content-Type':'application/json'})
    with urllib.request.urlopen(r,timeout=45) as response: return json.load(response)
def sql(query):
    r=urllib.request.Request('http://127.0.0.1:8123',data=query.encode(),headers={'Authorization':'Basic '+base64.b64encode(b'pulse:pulse-dev').decode()})
    with urllib.request.urlopen(r,timeout=20) as response: return response.read().decode().strip()
def wait(predicate,message,timeout=180):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        try:
            if predicate(): return
        except (OSError,urllib.error.HTTPError,ValueError): pass
        time.sleep(1)
    raise AssertionError(message)
def event(i):
    return {'event_id':str(uuid.uuid4()),'tenant_id':'tenant-demo','event_type':'PaymentFailed','schema_version':1,'partition_key':f'fault-account-{i%8}','producer_id':'fault-test','event_timestamp':dt.datetime.now(dt.timezone.utc).isoformat(),'payload':{'order_id':f'order-{i}','failure_reason':'synthetic_failure','user_id':f'fault-user-{i}'}}
def publish(events):
    result=http('POST','/v1/events',{'events':events},18080)
    assert result['acknowledged']==len(events),result
def seen(events):
    ids=','.join("'"+e['event_id']+"'" for e in events)
    return int(sql(f"SELECT count() FROM events FINAL WHERE pipeline='pulsestream-v1' AND tenant='tenant-demo' AND event_id IN ({ids})"))==len(events)
def drained():return http('GET','/v1/pipeline')['outbox_depth']==0

def main():
    evidence=[]
    try:
        initial=[event(i) for i in range(100)]
        compose('stop','stream-processor');publish(initial)
        assert not seen(initial),'processor stopped but test records already visible'
        began=time.monotonic();compose('start','stream-processor');wait(lambda:seen(initial),'crash recovery lost accepted events');wait(drained,'outbox did not drain')
        evidence.append({'scenario':'processor stop while ingestion continues, restart and recover','seconds':time.monotonic()-began,'events':len(initial)})
        before=sql("SELECT sum(JSONExtractInt(data,'count')) FROM windows FINAL WHERE pipeline='pulsestream-v1' AND tenant='tenant-demo' AND size=60000 AND step=60000")
        publish(initial);time.sleep(3);wait(drained,'duplicate backlog');after=sql("SELECT sum(JSONExtractInt(data,'count')) FROM windows FINAL WHERE pipeline='pulsestream-v1' AND tenant='tenant-demo' AND size=60000 AND step=60000")
        assert before==after,(before,after);evidence.append({'scenario':'stable IDs at new offsets do not inflate aggregates','before':before,'after':after})
        compose('stop','clickhouse');during=[event(200+i) for i in range(50)];publish(during)
        wait(lambda:http('GET','/v1/pipeline')['outbox_depth']>0,'database outage did not accumulate durable retry work')
        compose('start','clickhouse');wait(lambda:seen(during),'ClickHouse retry lost outputs');wait(drained,'ClickHouse retry did not drain');evidence.append({'scenario':'ClickHouse outage and convergent retry','events':len(during)})
        compose('stop','redis')
        try: publish([event(400)]);raise AssertionError('quota outage admitted traffic')
        except urllib.error.HTTPError as e: assert e.code==503,e.code
        compose('start','redis');wait(lambda:http('GET','/readyz',port=18080)['status']=='ready','Redis readiness');evidence.append({'scenario':'Redis outage fails closed for ingestion; durable state remains intact'})
        compose('up','-d','--no-deps','--scale','stream-processor=2','stream-processor');rebalance=[event(500+i) for i in range(100)];publish(rebalance);wait(lambda:seen(rebalance),'rebalance lost events');evidence.append({'scenario':'consumer scale-out and rebalance','events':len(rebalance)})
        # Invalid broker records bypass external validation to exercise processor DLQ.
        poison=event(700);poison['schema_version']=99
        subprocess.run(['docker','compose','exec','-T','redpanda','rpk','topic','produce','pulsestream.events.raw.v1','-k','tenant-demo/'+poison['partition_key']],cwd=ROOT,input=json.dumps(poison)+'\n',text=True,check=True,stdout=subprocess.DEVNULL)
        found=None
        for p in range(8):
            request={'topic':'pulsestream.events.dlq.v1','partition':p,'start_offset':0,'end_offset':10000,'limit':1000,'dry_run':True,'category':'unsupported_version'}
            result=http('POST','/v1/dlq',request)
            for record in result.get('records',[]):
                original=json.loads(base64.b64decode(record['original_base64']))
                if original.get('event_id')==poison['event_id']:found=(p,request)
            if found:break
        assert found,'poison record not found in acknowledged DLQ'
        request=found[1];request.update(dry_run=False,confirm=True,set_schema_version=1)
        result=http('POST','/v1/replays',request);assert result['published']>=1,result
        wait(lambda:seen([poison]),'corrected replay did not persist');evidence.append({'scenario':'invalid version DLQ, filtered inspection, corrected audited replay','published':result['published']})
        compose('stop','redpanda')
        try: publish([event(900)]);raise AssertionError('broker outage acknowledged traffic')
        except urllib.error.HTTPError as e: assert e.code==503,e.code
        compose('start','redpanda');wait(lambda:http('GET','/readyz',port=18080)['status']=='ready','broker did not recover');evidence.append({'scenario':'single broker outage rejects acknowledgements (availability is not claimed)'})
    finally:
        compose('start','redpanda','redis','clickhouse','stream-processor')
        compose('up','-d','--no-deps','--scale','stream-processor=1','stream-processor')
    target=ROOT/'docs/evidence/failures.json';target.parent.mkdir(parents=True,exist_ok=True);target.write_text(json.dumps({'executed_at':dt.datetime.now(dt.timezone.utc).isoformat(),'scenarios':evidence},indent=2));print(target.read_text())
if __name__=='__main__':main()
