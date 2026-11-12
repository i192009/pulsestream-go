"""Publish one current synthetic payment through the public API."""
import datetime,json,urllib.request,uuid
event={'event_id':str(uuid.uuid4()),'tenant_id':'tenant-demo','event_type':'PaymentSucceeded','schema_version':1,'partition_key':'account-example','producer_id':'example-script','event_timestamp':datetime.datetime.now(datetime.timezone.utc).isoformat(),'correlation_id':'example-payment','payload':{'order_id':'synthetic-order-1','amount_minor':1299,'currency':'USD','user_id':'synthetic-user-1'}}
request=urllib.request.Request('http://localhost:18080/v1/events',data=json.dumps({'events':[event]}).encode(),headers={'Authorization':'Bearer demo-key','Content-Type':'application/json'})
with urllib.request.urlopen(request,timeout=40) as response:print(response.read().decode())
print('Stable event ID for retry:',event['event_id'])
