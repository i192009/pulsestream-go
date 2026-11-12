# Verification ledger

Executed locally on 2026-11-11. Tests marked opt-in are deliberately skipped by plain `go test` when their enabling environment variable is absent; their actual executions are listed separately. Remote CI runs are not claimed.

| Check | Executed command / mechanism | Result |
|---|---|---|
| Go compilation | `go build ./cmd/...`; backend Docker multi-stage build | Passed for runtime, generator and replay CLI |
| Unit/property checks | `go test ./...` | Passed; schema, windows, reference aggregation, restoration, late correction, finalization, quantiles, money (including cross-shard overflow and browser BigInt precision), quotas/admission, auth, rule and generator behavior |
| Data races | `go test -race -count=1 ./...` in Linux ARM64 Go container | Passed |
| Real-service integration | PULSE_INTEGRATION=1 with Compose network/database/broker/ClickHouse, `go test -race -v -count=1 ./...` | Passed state recovery, retries, duplicates, concurrent partitions, corruption/gap rejection and unacknowledged-DLQ offset safety |
| Goroutine lifecycle | goleak verification around cancelled/closed franz-go client | Passed in race-enabled test |
| Testcontainers | PULSE_TESTCONTAINERS=1, Docker socket, fresh Redis 8 container | Passed atomic counter increments and TTL; container terminated |
| Public E2E | `python scripts/tasks.py e2e --count 100000 --rate 1000` | Passed 100,000-event persistence, independent type/window count and integer-revenue references, auth, gRPC, WS and anomalies; [initial transcript](evidence/e2e.txt), [final run](evidence/e2e-final.txt) |
| Failure/replay suite | `python scripts/failure_test.py` | Seven scenarios passed, including actual process/container interruptions and scale-out; [evidence](evidence/failures.json) |
| k6 smoke | Containerized constant-arrival test, 100/s for 5s | 501 acknowledgement checks passed, 0 HTTP failures; [summary](evidence/k6.json) |
| Million-event CPU workload | PULSE_MILLION=1 `go test -v ... -run TestMillionSyntheticWindowBatch` | Passed independent count and integer revenue; [scope/evidence](evidence/million.txt) |
| Frontend | `npm run build`, `npm test`, `npm audit --audit-level=high` | Production build and three tests passed; 0 npm vulnerabilities at execution |
| Browser | `python scripts/screenshots.py` with real Chromium/Playwright | All seven pages rendered, WS Live, real event payload opened, mobile view captured, no page exceptions; [screenshots](screenshots) |
| Grafana | Provisioned dashboard opened in real browser | Charts rendered actual Prometheus samples; screenshot retained |
| Buf | `buf lint`, generated clients from pinned remote plugins | Passed lint/generation; generated sources checked in |
| Go static checks | `gofmt`, `go vet ./...` | Passed |
| Go vulnerability scan | `go run golang.org/x/vuln/cmd/govulncheck@latest ./...` | 0 reachable vulnerabilities; scan noted 2 module advisories not called by application code |
| Compose | `docker compose config --quiet`; `up --no-build --wait` after image builds | All required services healthy; init completed |
| Actual Kubernetes deployment | Kind v0.30.0 / Kubernetes 1.34.0 / Helm 3.19.0 | All six backends plus dashboard and dependencies ran; 100-event count/revenue/failure references, stable group, alert lifecycle and real browser WS verified; [evidence](evidence/kind.json) |
| Helm/Kubernetes schema | `helm lint`, template, strict kubeconform | 30 resources valid initially; final default plus optional Ingress/PDB rendering: 50 valid, 0 invalid/errors/skipped |

Measured acknowledgements, durations, resource samples, query data and CPU profiling are described in [performance.md](performance.md). Higher workload targets are not substituted for measured results.

Failures discovered and corrected during implementation included mismatched library option names, frontend type errors, occupied host ports from another project, stale Nginx upstream addresses after container recreation, and franz-go delivery-timeout rejection when event time was used as Kafka publication timestamp. Corrected behavior was rerun. A later E2E fixture used a millionth synthetic index for a gRPC event after event-time progression was made linear; its timestamp was correctly rejected by the gateway as too far in the future. The fixture was corrected to use current time; the final full 100,000-event run passed in 105.29s (102.900s ingestion through durable visibility). The failed fixture transcript is retained separately. A final negative-schema fixture also needed its tenant field populated so it tested schema rejection (400), rather than correctly receiving tenant rejection (403). A query-measurement attempt overlapped a deliberate stack shutdown for Kind verification and was interrupted; it is not a successful benchmark result. A broker-outage rerun also exposed a synchronous producer wait that could outlive the HTTP deadline; the publish path was changed to a bounded callback wait with explicit ambiguous-outcome semantics, then tested again. As the retained demo history grew, the Go WebSocket test client also required an explicit receive limit above its library default of 32KiB; it now matches the server?s 256KiB snapshot cap. No unexecuted check is counted as passed.

Not established by these tests: production power-loss durability, broker-quorum fault tolerance, multi-zone HA, long-duration maximum-cardinality memory behavior, every cloud/CNI/KEDA configuration, individual end-to-end latency percentiles, or a sustained 50k/100k events-per-second capacity.
