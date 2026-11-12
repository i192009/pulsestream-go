# Measured performance

Measurements executed on 2026-11-11. These are local development observations, not capacity or availability guarantees. No production throughput claim is inferred from them.

## Environment

- Windows 11 Home, Qualcomm Snapdragon X X126100 / Oryon, 8 CPU cores; 16,758,587,392 bytes physical RAM (~15.6GiB).
- Docker Desktop 4.99.0; Linux ARM64 engine 29.9.3; Docker VM reports 8 CPUs and 8,106,524,672 bytes memory (~7.55GiB).
- Go `go1.26.11 linux/arm64`; Node 24 container; versioned Go and npm dependencies in go.mod/go.sum/package-lock.json.
- One Redpanda broker, 8 raw partitions, one processor except the scale-out fault test. Processor limit: 2 CPUs / 768MiB. Dispatcher: 1 CPU / 512MiB. Gateway/analytics/alerts/WebSocket: 1 CPU / 256MiB each. PostgreSQL: 512MiB, ClickHouse: 1536MiB, Redpanda: 1GiB. See Compose for all limits.
- An unrelated existing container project remained running. Compilation, resource sampling and some CPU workloads overlapped the first demonstration. Results are not isolated server benchmarks.
- Synthetic e-commerce events only: deterministic IDs/random seeds, 13 event types, 1,000 uniform account keys, integer minor-unit USD payments, configurable failure/latency anomaly mix and late arrivals. HTTP batches contain 100 events; four generator workers by default. Payload padding was zero for published measurements.

## Executed measurements

| Workload | Target | Observed | Scope |
|---|---:|---:|---|
| Full public API demonstration, 100,000 events | paced 1,000 events/s | 127.902s ingestion through durable visibility; 781.8 events/s including pacing/drain | Every event-type total and window count/integer revenue independently verified; gRPC, tenancy, WS and alerts also passed |
| Final public API demonstration, 100,000 events | paced 1,000 events/s | 102.900s through durable visibility; 971.8 events/s including pacing/drain | Corrected fixture; both gRPC services, independent counts/revenue, authorization, WS and alerts passed |
| HTTP generator, 10,000 events | 1,000 events/s | 10,000 acknowledged, 0 failed; 9.90656s; 1,009.43 acknowledgements/s | Ingestion acknowledgement only; initial/final batch timing can slightly exceed the pacing target |
| HTTP generator, 10,000 events | 10,000 events/s | 10,000 acknowledged, 0 failed; 0.99413s; 10,059.05 acknowledgements/s | Short burst, not sustained processing or durable sink throughput |
| Overload, 30,000 attempted events | 100,000 events/s | 20,000 acknowledged; 10,000 explicit HTTP 429 rejections; 0.61743s | Admission/quota behavior; exit status 1 correctly reports rejected load |
| CPU-only million-event batch | correctness workload | 1,000,000 validated/aggregated in 13.54898s | Eight Go states; count and integer revenue match independent reference; no broker/database I/O |
| Final million-event CPU batch | correctness workload | 1,000,000 validated/aggregated in 11.43167s | Eight states; exact reference passed; [final transcript](evidence/million-final.txt) |
| Initial processor restart under continuing ingestion | 100 accepted events | 43.27494s until visible, including group recovery | Local observed recovery, not a failover SLA |

| Generator run | HTTP batch p50 | p95 | p99 |
|---|---:|---:|---:|
| Target 1,000/s | 8ms | 31ms | 45ms |
| Target 10,000/s | 6ms | 25ms | 39ms |
| Overload target 100,000/s | 6ms | 21ms | 33ms |

Overload latency includes fast rejected requests and must not be interpreted as successful-publication latency alone. Quotas use UTC fixed one-second counters; a burst crossing a second boundary can use two quota buckets. The 20,000 accepted in a short interval therefore does not imply a higher sustained tenant allowance. Broker acknowledgement uses all in-sync replicas with idempotent production; local topology has one broker and write caching, so this does not demonstrate quorum or power-loss durability.

The final fault rerun observed 2.74535s recovery for its 100-event warm restart; the earlier 43.27494s observation included a different consumer-group recovery interval. These are separate local observations, not an isolated optimization or an SLA. The latest full failure evidence is in [failures.json](evidence/failures.json).

## Resource observations

Twenty actual `docker stats --no-stream` samples during the 100k demonstration are retained in [resources.json](evidence/resources.json). CPU percentages use Docker's convention, where 100% is one CPU core. These are sampled maxima, not continuous peaks:

| Service | Maximum observed CPU | Last sampled memory / limit |
|---|---:|---|
| Stream processor | 56.14% | 66.46MiB / 768MiB |
| Ingestion gateway | 8.12% | 15.03MiB / 256MiB |
| Sink dispatcher | 14.95% | 17.74MiB / 512MiB |
| ClickHouse | 146.77% | 690.7MiB / 1.5GiB |
| PostgreSQL | 86.76% | 157.2MiB / 512MiB |

These samples do not establish long-duration bounded-memory behavior at maximum cardinality. Exact state has explicit capacity limits, and current memory usage is workload-dependent.

## CPU benchmark and profiling

The initial three `BenchmarkWindowEngine` observations under shared-machine activity were 25,651 / 19,897 / 20,561 ns/op, approximately 1,864–1,870 B/op and 60 allocations/op. A later 2s CPU-profile run observed 5,492 ns/op, 1,856 B/op and 60 allocations/op. This difference is **not claimed as an optimization**: shared host load and benchmark duration differed. The microbenchmark reuses 1,000 generated events and advances event time; it does not include validation, Kafka, PostgreSQL, JSON checkpoint serialization or ClickHouse.

The CPU profile attributed notable flat samples to integer formatting, map assignment and map iteration; `windows.State.Apply` accounted for 85.87% cumulative sampled time in that run. Profiles are generated to ignored `artifacts/cpu.prof` and `artifacts/mem.prof`. A change to tuple window keys and typed snapshots is a candidate for measured optimization; it has not been implemented or credited with fabricated gains.

A separate 501-request k6 smoke run at a target of 100 requests/s passed every acknowledgement check with 0 HTTP failures; median request duration was 2.02ms and p95 11.74ms. This is a short smoke test, not capacity evidence. The summary is retained in [k6.json](evidence/k6.json).

ClickHouse FINAL count-query samples on 140,353 tenant events observed p50 37.27ms / p95 83.86ms; the window shard-count query observed p50 11.06ms / p95 34.64ms. Twenty samples per query include HTTP/loopback overhead. Physical events occupied 25,053,689 bytes at that observation, including other test namespaces and not an isolated storage-efficiency experiment.

ClickHouse query samples and physical table footprints are recorded in [queries.json](evidence/queries.json). They include HTTP overhead on this machine. No standalone ClickHouse insert-saturation benchmark, distributed storage-efficiency comparison or per-event end-to-end p50/p95/p99 measurement is claimed.

## Reproduction

```bash
python scripts/tasks.py up
python scripts/tasks.py e2e --count 100000 --rate 1000
python scripts/tasks.py benchmark
python scripts/tasks.py load-test --count 10000 --rate 1000
python scripts/tasks.py failure-test
python scripts/measure_queries.py
python scripts/profile.py
# Explicit higher targets; inspect the report and exit status.
python scripts/tasks.py load-test --count 100000 --rate 10000
python scripts/tasks.py load-test --count 100000 --rate 50000
python scripts/tasks.py load-test --count 100000 --rate 100000
```

50k/s is supported as a configurable generator target but has not been measured as a sustained workload. 100k/s was deliberately attempted only as an overload burst, with observed rejection. The million-event executed workload is CPU-only; a distributed million-event run remains unverified. Retain reports, resource limits, broker partitions and achieved rate whenever comparing changes. Do not report generator configured rate as achieved durable-processing throughput.
