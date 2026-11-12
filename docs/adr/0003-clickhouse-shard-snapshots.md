# ADR 0003: Versioned ClickHouse shard snapshots

Status: accepted.

Retries can duplicate physical ClickHouse rows. Additive aggregate output would inflate counts/revenue. Emit cumulative Go window snapshots keyed by pipeline, tenant, specification, start and input partition, versioned by that partition's offset. Query ReplacingMergeTree with FINAL, then combine the latest shards.

Exact distinct-user unions and exact validated latency histograms travel in snapshots. This is transparent and replay-convergent, at the cost of larger JSON rows and query CPU. A later typed-column optimization must preserve logical identities, versions, exact-user semantics and correction behavior, with the same reference tests. No background-merge timing is part of the correctness contract.
