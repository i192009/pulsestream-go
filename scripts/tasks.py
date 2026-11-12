"""Cross-platform task runner. Requires Python 3 and Docker; native toolchains optional."""
from pathlib import Path
import argparse, os, subprocess, sys

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

def run(args, **kw):
    print('+ ' + ' '.join(map(str,args)), flush=True)
    return subprocess.run(list(map(str,args)),check=True,**kw)

def go(command, network=False, env=None):
    args=['docker','run','--rm','-v',f'{ROOT}:/src','-v','pulse-gomod:/go/pkg/mod','-v','pulse-gocache:/root/.cache/go-build','-w','/src']
    if network: args+=['--network','pulsestream_default']
    for k,v in (env or {}).items(): args+=['-e',f'{k}={v}']
    return run(args+['golang:1.26']+command)

ENV={'PULSE_KEYS':'demo-key:tenant-demo:admin,reader-key:tenant-demo:reader','PULSE_DATABASE_URL':'postgres://pulse:pulse-dev@postgres:5432/pulse?sslmode=disable','PULSE_BROKERS':'redpanda:9092','PULSE_CLICKHOUSE':'http://clickhouse:8123','PULSE_REDIS':'redis:6379'}

def main():
    p=argparse.ArgumentParser();p.add_argument('task');p.add_argument('--count',type=int,default=100000);p.add_argument('--rate',type=int,default=1000);a=p.parse_args();task=a.task
    if task=='build': go(['go','build','./cmd/...'])
    elif task=='test': go(['go','test','-count=1','./...'])
    elif task=='test-race': go(['go','test','-race','-count=1','./...'])
    elif task=='lint': go(['go','vet','./...']);go(['sh','-c','test -z "$(gofmt -l cmd internal tests)"'])
    elif task=='up': run(['docker','compose','up','--build','-d','--wait','--wait-timeout','180'])
    elif task=='down': run(['docker','compose','down'])
    elif task in ('seed','generate-events','load-test'):
        run(['docker','compose','--profile','simulation','run','--rm','event-generator','-url','http://ingestion-gateway:8080/v1/events','-count',1000 if task=='seed' else a.count,'-rate',a.rate,'-seed',str(__import__('time').time_ns())])
    elif task=='test-integration':
        run(['docker','compose','up','-d','--wait','redpanda','postgres','clickhouse','redis'])
        go(['go','test','-race','-v','-count=1','-timeout','5m','./tests/integration'],True,{**ENV,'PULSE_INTEGRATION':'1'})
    elif task in ('e2e','demo'):
        go(['go','test','-v','-count=1','-timeout','20m','./tests/e2e'],True,{**ENV,'PULSE_E2E':'1','PULSE_E2E_COUNT':str(a.count),'PULSE_E2E_RATE':str(a.rate)})
    elif task in ('failure-test','replay-test'): run([sys.executable,'scripts/failure_test.py'])
    elif task=='benchmark':
        (ROOT/'artifacts').mkdir(exist_ok=True)
        with open(ROOT/'artifacts/benchmark.txt','w') as out:
            run(['docker','run','--rm','-v',f'{ROOT}:/src','-v','pulse-gomod:/go/pkg/mod','-v','pulse-gocache:/root/.cache/go-build','-w','/src','golang:1.26','go','test','./internal/windows','-run','^$','-bench','.','-benchmem','-count=3'],stdout=out)
        print((ROOT/'artifacts/benchmark.txt').read_text())
    elif task=='frontend': run(['docker','run','--rm','-v',f'{ROOT}/web:/app','-v','pulse-node:/app/node_modules','-w','/app','node:24-alpine','sh','-c','npm ci && npm run build && npm test && npm audit --audit-level=high'])
    elif task=='helm-lint': run(['docker','run','--rm','-v',f'{ROOT}:/charts','alpine/helm:3.19.0','lint','/charts/deployments/helm/pulsestream'])
    elif task=='generate': run(['docker','run','--rm','-v',f'{ROOT}:/workspace','-w','/workspace','bufbuild/buf:1.75.0','generate'])
    elif task=='kind-deploy': run([sys.executable,'scripts/kind_deploy.py'])
    elif task=='clean':
        # Only explicitly named build artifacts within the repository.
        import shutil
        for name in ['bin','web/dist']:
            target=(ROOT/name).resolve();target.relative_to(ROOT);shutil.rmtree(target,ignore_errors=True)
    else: p.error('unknown task '+task)

if __name__=='__main__': main()
