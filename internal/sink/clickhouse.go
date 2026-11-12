package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pulsestream/pulsestream-go/internal/broker"
	"github.com/pulsestream/pulsestream-go/internal/config"
	"github.com/pulsestream/pulsestream-go/internal/observe"
	"github.com/pulsestream/pulsestream-go/internal/processing"
	"github.com/pulsestream/pulsestream-go/internal/store"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type ClickHouse struct {
	Config config.Config
	Client *http.Client
}

func New(c config.Config) *ClickHouse {
	return &ClickHouse{c, &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{MaxIdleConns: 16, MaxIdleConnsPerHost: 8}}}
}
func (c *ClickHouse) Do(ctx context.Context, sql string, params map[string]string, body []byte) ([]byte, error) {
	ctx, span := otel.Tracer("pulse").Start(ctx, "clickhouse.query")
	defer span.End()
	start := time.Now()
	defer func() { observe.Duration.WithLabelValues("clickhouse").Observe(time.Since(start).Seconds()) }()
	u, err := url.Parse(c.Config.ClickHouse)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("query", sql)
	q.Set("database", "default")
	for k, v := range params {
		q.Set("param_"+k, v)
	}
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth("pulse", c.Config.ClickHousePassword)
	r, err := c.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	if r.StatusCode != 200 {
		return nil, fmt.Errorf("ClickHouse status %d: %.300s", r.StatusCode, b)
	}
	return b, nil
}
func (c *ClickHouse) Init(ctx context.Context) error {
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS events (pipeline LowCardinality(String),tenant String,event_id String,event_type LowCardinality(String),event_time Int64,ingested_time Int64,partition_id Int32,offset_id Int64,version UInt64,data String,late UInt8) ENGINE=ReplacingMergeTree(version) PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(event_time)) ORDER BY (pipeline,tenant,event_id) TTL fromUnixTimestamp64Milli(event_time)+INTERVAL 30 DAY`,
		`CREATE TABLE IF NOT EXISTS windows (pipeline LowCardinality(String),tenant String,partition_id Int32,start Int64,end Int64,size Int64,step Int64,version UInt64,data String) ENGINE=ReplacingMergeTree(version) PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(start)) ORDER BY (pipeline,tenant,size,step,start,partition_id) TTL fromUnixTimestamp64Milli(start)+INTERVAL 30 DAY`,
		`CREATE TABLE IF NOT EXISTS metric_revisions (pipeline String,tenant String,partition_id Int32,start Int64,end Int64,size Int64,step Int64,version UInt64,data String) ENGINE=ReplacingMergeTree(version) PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(start)) ORDER BY (pipeline,tenant,size,step,start,partition_id,version) TTL fromUnixTimestamp64Milli(start)+INTERVAL 7 DAY`,
		`CREATE TABLE IF NOT EXISTS alerts (id String,tenant String,version UInt64,data String) ENGINE=ReplacingMergeTree(version) ORDER BY (tenant,id)`,
		`CREATE TABLE IF NOT EXISTS processing_diagnostics (pipeline String,partition_id Int32,version UInt64,data String) ENGINE=ReplacingMergeTree(version) ORDER BY (pipeline,partition_id)`,
	} {
		if _, err := c.Do(ctx, q, nil, nil); err != nil {
			return err
		}
	}
	return nil
}
func Run(ctx context.Context, s *store.Store, c config.Config) error {
	ch := New(c)
	pub, err := broker.New(c)
	if err != nil {
		return err
	}
	defer pub.Close()
	for ctx.Err() == nil {
		if err = Dispatch(ctx, s, ch, pub, c.MaxBatch); err != nil {
			slog.Error("outbox retry", "error", err)
			observe.Events.WithLabelValues("outbox", "retry").Inc()
			if !processing.Wait(ctx, time.Second) {
				break
			}
		} else if !processing.Wait(ctx, 50*time.Millisecond) {
			break
		}
	}
	return ctx.Err()
}

// Dispatch holds row locks until every destination acknowledges. A crash can repeat
// physical rows; stable keys and FINAL reads converge to the same logical results.
func Dispatch(ctx context.Context, s *store.Store, ch *ClickHouse, pub *kgo.Client, limit int) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id,kind,payload,traceparent FROM outbox ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return err
	}
	var ids []int64
	groups := map[string]*bytes.Buffer{}
	var records []*kgo.Record
	links := []trace.Link{}
	for rows.Next() {
		var id int64
		var kind string
		var payload []byte
		var traceparent string
		if err = rows.Scan(&id, &kind, &payload, &traceparent); err != nil {
			rows.Close()
			return err
		}
		if traceparent != "" && len(links) < 32 {
			parent := otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
			sc := trace.SpanContextFromContext(parent)
			if sc.IsValid() {
				links = append(links, trace.Link{SpanContext: sc})
			}
		}
		ids = append(ids, id)
		if strings.HasPrefix(kind, "kafka:") {
			records = append(records, &kgo.Record{Topic: strings.TrimPrefix(kind, "kafka:"), Value: payload})
			continue
		}
		if kind != "events" && kind != "windows" && kind != "alerts" && kind != "processing_diagnostics" {
			rows.Close()
			return fmt.Errorf("unknown outbox kind")
		}
		if groups[kind] == nil {
			groups[kind] = new(bytes.Buffer)
		}
		groups[kind].Write(payload)
		groups[kind].WriteByte('\n')
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	ctx, span := otel.Tracer("pulse").Start(ctx, "outbox.deliver", trace.WithLinks(links...))
	defer span.End()
	for _, r := range records {
		broker.Inject(ctx, r)
	}
	for kind, b := range groups {
		if _, err = ch.Do(ctx, "INSERT INTO "+kind+" FORMAT JSONEachRow", nil, b.Bytes()); err != nil {
			return err
		}
		if kind == "windows" {
			if _, err = ch.Do(ctx, "INSERT INTO metric_revisions FORMAT JSONEachRow", nil, b.Bytes()); err != nil {
				return err
			}
		}
	}
	if len(records) > 0 {
		if _, err = broker.ProduceBatch(ctx, pub, records); err != nil {
			return err
		}
	}
	if groups["windows"] != nil {
		b, _ := json.Marshal(map[string]any{"sequence": ids[len(ids)-1], "committed_at": time.Now().UTC()})
		if _, err = broker.ProduceBatch(ctx, pub, []*kgo.Record{{Topic: broker.Updates, Value: b}}); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, `DELETE FROM outbox WHERE id=ANY($1)`, ids); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	observe.Events.WithLabelValues("outbox", "delivered").Add(float64(len(ids)))
	return nil
}
