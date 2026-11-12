# API reference

Defaults bind to localhost only. Ingestion HTTP: `18080`; analytics/admin HTTP: `18081`; WebSocket: `18082`; ingestion gRPC: `19090`; analytics gRPC: `19091`. The dashboard on `13000` proxies tenant reads and WebSockets. Every `/v1` route requires `Authorization: Bearer <key>`. The WebSocket authenticates its first frame instead of placing secrets in URLs.

Development keys are `demo-key` (tenant-demo admin) and `reader-key` (tenant-demo reader). Replace `PULSE_KEYS` with comma-separated `key:tenant:admin|reader|writer`. Every key authorizes one tenant. There is no unauthenticated tenant selection override; payload tenant IDs must match the authenticated identity. Define another tenant by provisioning another key and running the controlled `init` role to seed its rules. Tenants have separate rate counters, deduplication keys, aggregate filters and alerts. `PULSE_TENANT_RATES=tenant-demo:10000,tenant-other:500` overrides the shared default rate.

| Method / route | Behavior |
|---|---|
| POST ingestion `/v1/events` | `{"events":[<event>]}`; 1–500 events, <=4MiB request; 202 with acknowledged count |
| GET `/v1/overview` | Tenant-scoped persisted total, UTC-today processed count and authorized active-tenant count |
| GET analytics `/v1/metrics` | `from`, `to` RFC3339; <=24h; `size_ms`, `step_ms`; defaults 1m tumbling |
| GET analytics `/v1/events` | `from`, `to`, `type`, `q` substring search, `limit` 1–500, `offset` 0–100000 |
| GET `/v1/alerts` | 200 most recent tenant alerts, persisted lifecycle state |
| GET `/v1/alerts/summary` | Hourly counts by severity/status in bounded `from`/`to` range |
| POST `/v1/alerts/{id}/acknowledge` | Admin; idempotent acknowledgement; resolved alerts stay resolved |
| POST `/v1/alerts/{id}/resolve` | Admin; idempotent resolution |
| GET `/v1/rules` | Six configurable rule families |
| PUT `/v1/rules/{id}` | Admin; complete rule object with id, kind, enabled, threshold, min_samples, cooldown_seconds, severity |
| GET `/v1/pipeline` | Admin; PostgreSQL checkpoint/owner observations and outbox depth; cross-tenant operational metadata |
| GET `/v1/groups` | Admin; live Redpanda consumer membership/lag through kadm |
| POST `/v1/dlq` | Admin; bounded inspection body below; forces DLQ topic and dry_run |
| POST `/v1/replays` | Admin; bounded, synchronous, audited replay |
| GET `/v1/replays` | Last 100 caller-tenant replay audit entries |
| GET `/healthz`, `/readyz`, `/metrics` | Infrastructure-only health/readiness/Prometheus; restrict network access |

Admin keys are trusted operators. They can inspect cross-tenant partition ownership and broker membership but cannot query another tenant's event payloads or replay them. For stronger isolation use separate operational credentials and network segments; the demo uses one admin identity per tenant.

```bash
curl -H 'Authorization: Bearer demo-key' http://localhost:18081/v1/metrics
curl -H 'Authorization: Bearer demo-key' http://localhost:18081/v1/groups
curl -X POST http://localhost:18081/v1/replays \
  -H 'Authorization: Bearer demo-key' -H 'Content-Type: application/json' \
  -d '{"topic":"pulsestream.events.dlq.v1","partition":0,"start_offset":0,"end_offset":100,"limit":100,"category":"unsupported_version","dry_run":true}'
```

To publish a corrected replay, submit the same bounded request with `dry_run:false`, `confirm:true`, `set_schema_version:1`. Every selected event must validate. The CLI accepts a JSON request file: `go run ./cmd/replay-cli -request replay.json` or run the image with `--entrypoint /replay-cli` and a mounted request file. The frontend offers the same explicit dry-run and confirmation controls. Confirmation is not inferred from elapsed time.

WebSocket example:

```javascript
const ws = new WebSocket('ws://localhost:13000/ws');
ws.onopen = () => ws.send(JSON.stringify({token:'demo-key', subscriptions:['metrics','alerts']}));
ws.onmessage = event => console.log(JSON.parse(event.data));
```

Browser origin must equal `PULSE_ORIGIN`. Frames include a connection-local increasing sequence and snapshot time; clients must resubscribe and reset their sequence on reconnect. Snapshots include window offset revisions. There is no durable per-client notification cursor; reconnect fetches current committed snapshots. Ping heartbeats run every 15s. Outbound snapshot frames are capped at 256KiB; oversized snapshots return an explicit error frame and require a narrower HTTP query. Non-browser WebSocket clients must configure their receive limit to 256KiB (the Go client default is only 32KiB). Only `metrics` and `alerts` currently have frame payloads; partition operational data uses the HTTP API.

`pulse.v1.IngestionService/PublishEventBatch` and `pulse.v1.AnalyticsService/GetWindowMetrics` use generated protobuf clients in `api/gen`. Send the same Bearer key through `authorization` gRPC metadata. Server deadlines bound requests even when clients omit them. gRPC returns InvalidArgument, PermissionDenied, ResourceExhausted, Unavailable and Unauthenticated appropriately. Batch gRPC errors can have partial/ambiguous delivery; retry stable IDs. `GetPublishStatus` is not implemented; use ingestion acknowledgement and tenant event search to distinguish accepted from persisted data.

HTTP ingestion returns 400 for schema/body errors, 403 for tenant/role errors, 429 for quota/admission pressure (with Retry-After), and 503 for dependency/publish failure. A 503 response includes the known acknowledgement count, but cannot establish that every failed delivery is absent from the broker. The caller waits at most 30s for bounded delivery callbacks. Idempotent in-flight requests can finish later; a deadline does not turn an ambiguous delivery into a definite rejection. Retry identical stable IDs. There is no arbitrary SQL endpoint. ClickHouse filters use typed named parameters; the only interpolated SQL identifiers are fixed allowlisted tables/actions.

Window responses include revenue_minor_exact, a currency-to-decimal-string map. Use this for authoritative monetary totals; the browser uses BigInt. Numeric revenue fields support plotting/legacy clients and must not be summed as floating-point financial totals.
