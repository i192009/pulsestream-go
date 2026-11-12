# Checkpoints

The primary key is `(pipeline, topic, partition_id)`. Each checkpoint stores the last durable offset, JSON window state, maximum event time, watermark, creation time, a versioned configuration fingerprint, and SHA-256 checksum over canonical JSON. PostgreSQL defaults provide WAL-backed synchronous local transaction commits. The application does not weaken `synchronous_commit`; PostgreSQL replication and backups remain infrastructure responsibilities.

On every batch, the worker locks the row and verifies both fingerprint and checksum before reading or advancing it. Corruption and configuration mismatch stop the partition with visible retries. No automated reset substitutes an empty state for a damaged checkpoint. Restore a known consistent backup or perform an explicit rebuild into a fresh namespace.

The checkpoint transaction also holds durable deduplication and output work. This permits recovery without coordinating a distributed transaction with ClickHouse. See the [failure matrix](consistency-model.md). The local Compose named volume survives container replacement; deleting that volume destroys authoritative state. Keep PostgreSQL, broker and ClickHouse backup/retention policies aligned.
