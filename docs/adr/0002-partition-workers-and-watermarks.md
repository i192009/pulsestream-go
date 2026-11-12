# ADR 0002: Partition-affine batches and conservative watermarks

Status: accepted.

Uncoordinated per-record workers can complete out of order and commit beyond unfinished required effects. Use one serial goroutine per polled partition, bounded by a semaphore and joined before allowing rebalance. Row-locked durable state protects restarts and overlapping ownership. Polling remains bounded.

Maintain event-time watermarks per partition. Use their conservative minimum for global finality; idle partitions remain included and unseen partitions prevent finality. Consequences: delayed sources cannot be silently excluded, but quiet streams can delay finalization. Local final state is evicted only after allowed lateness, with hard cardinality and byte limits. Do not advance watermarks from processing time merely to make a dashboard look current.
