# Event model and compatibility

Schema version 1 is supported. Unknown envelope/payload fields, unsupported event types, unsupported versions, malformed UUIDs, invalid identifiers and out-of-domain numeric values are rejected. External JSON allows a common explicitly defined payload field set; each event type has required fields, but known optional fields from another event type remain accepted. A version bump is required for incompatible changes. Producers must keep event IDs stable across retries. A duplicated ID with different content is first-writer-wins, not an update protocol.

| Types | Required payload fields |
|---|---|
| UserRegistered, UserLoggedIn | user_id |
| PageViewed | session_id, page |
| ProductViewed, CartUpdated | session_id, product_id |
| CheckoutStarted, OrderCreated, OrderCompleted | order_id |
| PaymentAttempted, PaymentSucceeded | order_id, uppercase three-letter currency |
| PaymentFailed | order_id, failure_reason |
| ServiceLatencyRecorded | service; latency_ms is an integer in 0–60000 |
| ApplicationErrorRecorded | service, error_code |

Amounts are integers from zero through 1,000,000,000,000 minor units. Identifiers used in routing are 1–128 alphanumeric, dot, underscore or hyphen characters. Metadata allows at most 16 entries with bounded keys/values. Optional synthetic padding is at most 16KiB. The gateway rejects event times more than five minutes in the future. Historical ingestion is permitted within the event-time validation domain (2020–2100), but ClickHouse's 30-day event-time TTL can immediately expire older input; do not use the live tables for archival backfill. A separate retention configuration/table namespace is required for that workload.

```json
{
  "event_id":"a22be73e-3f5d-4b1b-90ca-e54d60487b63",
  "tenant_id":"tenant-demo",
  "event_type":"PaymentSucceeded",
  "schema_version":1,
  "partition_key":"account-1024",
  "producer_id":"synthetic-generator",
  "event_timestamp":"2026-11-11T12:00:00Z",
  "correlation_id":"synthetic-order-9001",
  "payload":{"order_id":"order-9001","amount_minor":12500,"currency":"USD","user_id":"user-1024"}
}
```

The gateway replaces ingestion timestamp and trace ID. Event timestamp stays in the versioned envelope. **Kafka record timestamp is ingestion/publication time** because franz-go delivery timeout uses it; setting it to an old event time would reject legitimate late data. Event-time replay therefore filters envelopes within explicit offset bounds rather than assuming Kafka timestamp indexes match event time.

Protobuf uses `google.protobuf.Struct` for the versioned payload; the same Go schema validation runs after protobuf JSON conversion. Monetary integers up to the supported 1e12 limit are exactly representable in Struct's JSON numeric domain. The generated contract is checked in and reproducible through Buf. [OpenAPI JSON](../api/openapi/pulse.json) describes external event variants; runtime validation is authoritative.
