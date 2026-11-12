# Event-time window semantics

Go maintains four specifications: 1m tumbling, 5m tumbling, 5m hopping at a 1m step, and 15m hopping at a 5m step. Windows are UTC, epoch-aligned, start-inclusive and end-exclusive. An event belongs to every aligned start `s` satisfying `s <= event_time < s + size`.

Per-partition watermark is `maximum observed valid unique event time - out_of_order` (10s by default). Lateness is evaluated against the **previous** watermark. Watermarks never move backwards. A window becomes closed at `watermark >= end`; an event within allowed lateness (2m) can correct a closed window and increase its offset-based version. At `watermark >= end + lateness`, the final snapshot is emitted and local window state is evicted. Events with `event_time < watermark - lateness` are retained as late normalized rows and published to a late topic; they do not update aggregates. Equality is accepted.

Empty partitions do not invent progress. Pipeline diagnostics identify a partition as idle after 30 seconds without a committed batch. Idle partitions remain in the global minimum; an unseen partition forces the global watermark to zero. APIs set `final` only when the minimum over every configured raw partition passes `end + lateness`. This conservative idle policy sacrifices finality progress for delayed-source safety. No processing-time advancement or finalization is injected, so replay of the same per-partition order is deterministic. Quiet streams can retain unfinished windows indefinitely in event time, within hard capacity limits. Send a controlled newer event to every partition to progress a synthetic test; no infinite simulator is used.

Per-partition memory limits: 4,096 live windows, 100,000 distinct users and 100,000 accounts per window, and a 32MiB serialized checkpoint. A capacity error rolls back and retries with backpressure rather than dropping accepted events or approximating exact metrics. Increase useful partition parallelism or reduce tenant fanout when a capacity limit is reached. The gateway caps each JSON batch at 4MiB and 500 events. State limits are enforced before commit; a batch may transiently allocate more memory before its serialization-size check. Process memory/container limits remain a final guard.

| Metric | Definition |
|---|---|
| Event rate | Unique, non-late events / window duration in seconds; a partial minute is divided by 60 |
| Active users | Exact union of nonempty payload user IDs |
| Payment failure rate | PaymentFailed / (PaymentSucceeded + PaymentFailed); attempts do not add to denominator |
| Revenue | Sum of PaymentSucceeded amount_minor, separately per currency; int64 |
| Average order value | Revenue / successful payment count for that currency; event-based rather than order-cohort based |
| Checkout conversion | OrderCompleted count / CheckoutStarted count in the same window; can exceed 1 across cohorts |
| Mean / p95 latency | ServiceLatencyRecorded only; exact integer millisecond histogram, nearest-rank p95 |
| Service error percentage | ApplicationErrorRecorded / (ServiceLatencyRecorded + ApplicationErrorRecorded) for each service |
| Overall event error rate | ApplicationErrorRecorded / all unique non-late events |
| Processing-lag diagnostic | Ingestion time minus event time, clamped to nonnegative; histogram capped at 1h |

The processing-lag diagnostic measures event age on admission, **not** ingestion-to-ClickHouse latency. Service p95 is exact within the validated 0–60,000ms domain. Lag p95 is capped and labeled diagnostic. Full end-to-end per-event latency distributions have not been implemented. Monetary totals never use floating-point arithmetic. The API supplies revenue_minor_exact as decimal strings; the UI sums and formats those with BigInt, including values beyond JavaScript safe integers. Numeric revenue fields remain for chart coordinates/backward compatibility; charts, averages and rates are presentation numbers. Cross-shard int64 revenue overflow fails the query explicitly instead of wrapping.

Shard snapshots hold exact user sets and integer histograms; APIs union/sum these before producing metrics. Dashboard charts contain actual committed values. No synthetic idle samples are inserted into empty chart intervals. Query responses are capped to 24 hours and 32MiB; high-cardinality workloads should narrow time ranges.
