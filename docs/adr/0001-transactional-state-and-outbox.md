# ADR 0001: PostgreSQL transactional state and output outbox

Status: accepted.

Kafka commits, local window state and ClickHouse inserts have no shared transaction. Directly committing an offset after a partial sink write either risks missing state after a crash or requires output deduplication anyway. Redis is explicitly not authoritative storage.

Use PostgreSQL to atomically commit checkpoint, exact Go state, durable event-ID deduplication and output work. A separate dispatcher retries ClickHouse/Kafka destinations. Commit Kafka only after this state transaction. Consequences: durable effects are staged before offset acknowledgement; analytical visibility is eventual; PostgreSQL capacity and write latency are critical; there is no global exactly-once claim. Integration and failure tests exercise the crash boundaries.
