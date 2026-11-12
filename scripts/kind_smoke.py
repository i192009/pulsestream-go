"""Assert a fresh Kind deployment through forwarded gateway/API/dashboard ports."""
import datetime,json,time
from pathlib import Path
from failure_test import event,http,wait
from playwright.sync_api import sync_playwright,expect

events=[event(i) for i in range(100)]
for i,e in enumerate(events):
    if i%2==0:
        e['event_type']='PaymentSucceeded';e['payload'].pop('failure_reason');e['payload'].update(amount_minor=1299,currency='USD')
result=http('POST','/v1/events',{'events':events},18080)
assert result['acknowledged']==100,result
def correct():
    windows=http('GET','/v1/metrics')['windows']
    return sum(w['count'] for w in windows)==100 and sum(w.get('revenue',{}).get('USD',0) for w in windows)==64950 and sum(w['failed'] for w in windows)==50
wait(correct,'Kind count/revenue/failure reference mismatch')
wait(lambda:len(http('GET','/v1/alerts'))>0,'Kind failure spike did not trigger')
alert=next(a for a in http('GET','/v1/alerts') if a['rule_id']=='payment-failures')
assert http('POST',f"/v1/alerts/{alert['id']}/acknowledge",{})['status']=='acknowledged'
assert http('POST',f"/v1/alerts/{alert['id']}/resolve",{})['status']=='resolved'
group=http('GET','/v1/groups')['groups']['pulsestream-v1'];assert group['State']=='Stable',group
with sync_playwright() as p:
    browser=p.chromium.launch();page=browser.new_page(viewport={'width':1512,'height':1100})
    page.goto('http://localhost:13000');page.get_by_label('Tenant API key').fill('demo-key');page.get_by_role('button',name='Open workspace').click();expect(page.get_by_text('Live',exact=True)).to_be_visible(timeout=30000)
    out=Path(__file__).resolve().parents[1]/'docs/screenshots/kind.png';page.screenshot(path=str(out),full_page=True);browser.close()
evidence={'executed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'environment':'Kind v0.30.0, Kubernetes v1.34.0, Helm v3.19.0, single node','acknowledged':100,'verified_count':100,'verified_revenue_usd_minor':64950,'verified_failed_payments':50,'group_state':group['State'],'alert_lifecycle':'open -> acknowledged -> resolved','websocket':'Live in actual browser','scope':'fresh development cluster; no KEDA or multi-node failover claim'}
out=Path(__file__).resolve().parents[1]/'docs/evidence/kind.json';out.write_text(json.dumps(evidence,indent=2));print(out.read_text())
