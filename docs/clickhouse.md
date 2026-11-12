# Analytical tables and bounded delivery

| Table | Logical identity / engine | Retention and purpose |
|---|---|---|
| events | ReplacingMergeTree(version); pipeline, tenant, event_id | Monthly event-time partitions; 30d TTL; normalized envelopes and broker coordinates |
| windows | ReplacingMergeTree(version); pipeline, tenant, size, step, start, partition | Monthly window-start partitions; 30d TTL; latest cumulative shard snapshots |
| metric_revisions | Same snapshot dimensions plus version | 7d TTL; historical correction/revision evidence |
| alerts | ReplacingMergeTree(version); tenant, id | Lifecycle output; no automated TTL |
| processing_diagnostics | ReplacingMergeTree(version); pipeline, partition | Latest checkpoint offsets, watermarks, active-window count |

The dispatcher takes at most 500 PostgreSQL outbox rows per transaction and groups JSONEachRow inserts by fixed table. Only one dispatch selection is outstanding per process, limiting database write concurrency. Multiple replicas use SKIP LOCKED; output arrival order can differ, so every shard carries a deterministic version. Derived Kafka publication follows analytical inserts, and outbox rows are deleted only after all selected effects acknowledge. Kafka derived topics can contain retry duplicates; consumers must use stable event/alert IDs or versions. Normalized topic partition affinity is not a promised processing contract; the raw input is the authoritative ordered log.

Every dashboard read of events/windows uses `FINAL`; background merges alone are insufficient. Aggregates are calculated in Go and persisted as snapshots, not recomputed entirely through ClickHouse materialized views. Queries merge latest per-partition snapshots, union users, combine histograms and calculate exact required metrics. Event search and range predicates are parameterized. Results are bounded to 24h / 32MiB; event pages are capped at 500 rows.

The JSON snapshot strategy favors clear recovery semantics and exact user unions over optimal columnar query CPU. At larger cardinalities, typed aggregate columns and bounded query-time state would improve efficiency. `FINAL` has a real query cost. Deduplication keys are stable within an event-time month for immutable events; mutating event time while reusing IDs is unsupported. Local table definitions, memory settings and query patterns have been executed against ClickHouse 25.8; no multi-node ClickHouse replication has been demonstrated.
