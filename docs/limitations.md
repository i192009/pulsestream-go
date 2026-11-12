# Scope and known limitations

Implemented functionality includes real HTTP/gRPC ingestion, strict schema and tenancy checks, Redis quotas/cache, Redpanda publishing/consumer groups, bounded partition processing, exact Go windows, late classification, PostgreSQL checkpoints/dedup/outbox, ClickHouse convergent queries, six anomaly rule families/lifecycle, bounded audited replay, authenticated WebSockets, seven React pages, observability, Compose and Kubernetes/Helm packaging. Read `docs/verification.md` for executed evidence; source code and packaged configuration are not automatically deployment proof.

Operational limitations:

- Development dependencies are single replicas. No broker quorum, highly available PostgreSQL, ClickHouse replication or multi-zone fault survival is claimed. Broker acknowledgements under local write caching do not demonstrate power-loss durability.
- PostgreSQL is an extra correctness dependency and a throughput bottleneck. Deduplication is durable and unexpired; disk capacity, backups and lifecycle retention require administration. Outbox limits are row-based and approximate under concurrent transactions.
- Global watermark finality deliberately stalls on idle/unseen partitions. Exact sets/histograms have capacity limits. Exceeding capacity pauses a partition; there is no spill-to-disk aggregate state implementation.
- ClickHouse/table/outbox commits do not share one transaction. Visibility is eventual and per-table snapshots can differ during delivery. Queries use FINAL; physical retry duplicates can remain until merges.
- Live ingestion timestamp replaces external ingestion timestamps. Event age is the implemented lag diagnostic; individual ingestion-to-visible latency percentiles are not instrumented. Acknowledgement HTTP batch percentiles are different measurements.
- Replay is bounded and synchronous. No durable background job scheduler, arbitrary payload correction, abandoned-job auto-repair or event-time-to-offset index is included. Dry-run inspection returns at most 100 sample records. Unknown-tenant invalid JSON needs trusted broker inspection.
- Same event IDs are immutable/first-writer-wins. Selectively replaying already late IDs cannot backfill current windows. Changed window/lateness configuration requires a fresh pipeline namespace and source replay; retained source availability bounds recovery.
- Anomaly baselines compare adjacent minute windows; USD revenue drop and all-event account bursts are the implemented variants. No ML anomaly model, arbitrary rule expression language, automatic alert retraction, notification email or external paging integration is included.
- gRPC uses a common Struct payload with strict runtime validation. PublishEventBatch and GetWindowMetrics are implemented; separate RPC methods for every illustrative API name are not. HTTP routes provide the other operations. GetPublishStatus is not implemented.
- WebSocket snapshots refresh every two seconds; sequences are connection-local. No durable subscription cursor or guaranteed delivery of every intermediate metric revision is claimed. Operational partition data uses HTTP.
- Dashboard history is capped to 24h per query and alert history pages to the last 200 rows; hourly summary covers the selected range. Long-term historical storage beyond configured TTL is not included. Per-day totals across more than 24h require multiple queries.
- Kafka TLS/SASL, JWT/OIDC and service mTLS are absent. API key rotation requires rollout. Production ingress, secret rotation, infrastructure hardening and backup/failover drills require additional deployment work.
- Docker/Go/Node images use stable version tags rather than a complete digest-pinned supply-chain policy. Dependency scanners are present and were executed; that is not a complete security audit.
- 50k/100k events/sec and one-million-event distributed processing are workload targets, not demonstrated capacities. The million-event executed test is a CPU/window correctness workload. See measured results without extrapolation in performance.md.
- Helm/Kubernetes schema checks do not prove every cloud/CNI/KEDA installation works. KEDA is opt-in and requires separately installed CRDs, a metrics environment and authenticated broker connectivity.

Future work should prioritize durable-state throughput, explicit Kafka transport security, typed ClickHouse aggregate columns, end-to-end event latency instrumentation, a durable replay job runner, configurable per-rule currency/event selectors and multi-node failure tests.
