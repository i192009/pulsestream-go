CREATE TABLE IF NOT EXISTS checkpoints (
 pipeline text NOT NULL, topic text NOT NULL, partition_id integer NOT NULL,
 last_offset bigint NOT NULL DEFAULT -1, state jsonb NOT NULL DEFAULT '{"windows":{}}',
 checksum text NOT NULL DEFAULT '', config_hash text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(pipeline, topic, partition_id)
);
CREATE TABLE IF NOT EXISTS dedup (
 pipeline text NOT NULL, tenant text NOT NULL, event_id text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(pipeline, tenant, event_id)
);
CREATE INDEX IF NOT EXISTS dedup_age ON dedup(created_at);
CREATE TABLE IF NOT EXISTS outbox (
 id bigserial PRIMARY KEY, kind text NOT NULL, payload jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS traceparent text NOT NULL DEFAULT '';
CREATE TABLE IF NOT EXISTS rules (
 tenant text NOT NULL, id text NOT NULL, kind text NOT NULL, enabled boolean NOT NULL DEFAULT true,
 threshold double precision NOT NULL, min_samples bigint NOT NULL DEFAULT 20,
 cooldown_seconds integer NOT NULL DEFAULT 300, severity text NOT NULL DEFAULT 'warning',
 PRIMARY KEY(tenant,id)
);
CREATE TABLE IF NOT EXISTS alerts (
 id text PRIMARY KEY, tenant text NOT NULL, rule_id text NOT NULL, severity text NOT NULL,
 window_start bigint NOT NULL, observed double precision NOT NULL, threshold double precision NOT NULL,
 evidence jsonb NOT NULL, triggered_at timestamptz NOT NULL DEFAULT now(),
 acknowledged_at timestamptz, resolved_at timestamptz, status text NOT NULL DEFAULT 'open'
);
CREATE INDEX IF NOT EXISTS alerts_tenant_time ON alerts(tenant,triggered_at DESC);
CREATE TABLE IF NOT EXISTS replay_audit (
 id bigserial PRIMARY KEY, tenant text NOT NULL, request jsonb NOT NULL,
 status text NOT NULL, published bigint NOT NULL DEFAULT 0, error text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL DEFAULT now(), completed_at timestamptz
);
CREATE TABLE IF NOT EXISTS diagnostics (
 pipeline text NOT NULL, partition_id integer NOT NULL, owner text NOT NULL,
 high_watermark bigint NOT NULL, processed_offset bigint NOT NULL, updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(pipeline,partition_id)
);
