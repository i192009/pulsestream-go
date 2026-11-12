"""Record actual Docker resource observations during a separately initiated workload."""
import datetime,json,subprocess,time
from pathlib import Path
root=Path(__file__).resolve().parents[1]
ids=subprocess.check_output(['docker','compose','ps','-q'],cwd=root,text=True).split()
samples=[]
for _ in range(20):
    result=subprocess.check_output(['docker','stats','--no-stream','--format','{{json .}}',*ids],text=True)
    samples.append({'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'containers':[json.loads(x) for x in result.splitlines()]})
    time.sleep(1)
out=root/'docs/evidence/resources.json';out.parent.mkdir(parents=True,exist_ok=True);out.write_text(json.dumps(samples,indent=2));print(f'{len(samples)} real resource samples saved to {out}')
