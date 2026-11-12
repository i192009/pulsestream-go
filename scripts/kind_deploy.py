"""Requires kind, kubectl and helm on PATH. Creates an isolated local development cluster."""
from pathlib import Path
import subprocess
ROOT=Path(__file__).resolve().parents[1]
def run(*args):subprocess.run(args,cwd=ROOT,check=True)
run('kind','create','cluster','--name','pulsestream','--config','deployments/kubernetes/kind.yaml')
run('docker','compose','build','ingestion-gateway','dashboard')
run('kind','load','docker-image','--name','pulsestream','pulsestream-go:dev','pulsestream-dashboard:latest')
run('kubectl','create','namespace','pulsestream')
run('kubectl','-n','pulsestream','apply','-f','deployments/kubernetes/dependencies.yaml')
for name in ['postgres','redis','clickhouse','redpanda']:run('kubectl','-n','pulsestream','rollout','status',f'deployment/{name}','--timeout=180s')
run('kubectl','-n','pulsestream','create','secret','generic','pulsestream-secrets','--from-literal=PULSE_KEYS=demo-key:tenant-demo:admin,reader-key:tenant-demo:reader','--from-literal=PULSE_DATABASE_URL=postgres://pulse:pulse-dev@postgres:5432/pulse?sslmode=disable','--from-literal=PULSE_CLICKHOUSE_PASSWORD=pulse-dev')
run('helm','upgrade','--install','pulse','deployments/helm/pulsestream','-n','pulsestream','--wait','--timeout','5m')
print('Dashboard: kubectl -n pulsestream port-forward svc/pulse-dashboard 13000:8080')
