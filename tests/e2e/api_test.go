package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/coder/websocket"
	"github.com/google/uuid"
	pulsev1 "github.com/pulsestream/pulsestream-go/api/gen/pulse/v1"
	analytical "github.com/pulsestream/pulsestream-go/internal/analytics"
	"github.com/pulsestream/pulsestream-go/internal/config"
	"github.com/pulsestream/pulsestream-go/internal/event"
	"github.com/pulsestream/pulsestream-go/internal/sink"
	"github.com/pulsestream/pulsestream-go/internal/synthetic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"io"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestPublicPipeline(t *testing.T) {
	if os.Getenv("PULSE_E2E") != "1" {
		t.Skip("requires running full Compose stack and PULSE_E2E=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	gateway := env("E2E_GATEWAY", "http://ingestion-gateway:8080")
	analytics := env("E2E_ANALYTICS", "http://analytics-api:8080")
	seed := time.Now().UnixNano()
	base := time.Now().Add(-time.Minute).Truncate(time.Minute)
	g := synthetic.New(seed, base)
	g.Anomaly = true
	count := 1000
	if v := os.Getenv("PULSE_E2E_COUNT"); v != "" {
		n, e := strconv.Atoi(v)
		if e != nil || n < 100 {
			t.Fatal("invalid count")
		}
		count = n
	}
	c := config.Load("test")
	ch := sink.New(c)
	began := time.Now()
	rate := 1000
	if v := os.Getenv("PULSE_E2E_RATE"); v != "" {
		rate, _ = strconv.Atoi(v)
	}
	if rate < 1 {
		t.Fatal("invalid rate")
	}
	expected := map[string]int64{}
	events := make([]event.Event, 0, 100)
	for i := 0; i < count; i++ {
		e := g.Event(int64(i), "tenant-demo")
		expected[e.Type]++
		events = append(events, e)
		if len(events) == 100 || i == count-1 {
			b, _ := json.Marshal(map[string]any{"events": events})
			var ack struct{ Acknowledged int }
			request(t, ctx, "POST", gateway+"/v1/events", b, "demo-key", 202, &ack)
			if ack.Acknowledged != len(events) {
				t.Fatal("not all events acknowledged")
			}
			events = events[:0]
			target := began.Add(time.Duration(float64(i+1) / float64(rate) * float64(time.Second)))
			if delay := time.Until(target); delay > 0 {
				time.Sleep(delay)
			}
		}
	}

	// Verify every event against the independently generated event-type reference.
	params := map[string]string{"pipeline": c.Group, "tenant": "tenant-demo", "correlation": "seed-" + strconv.FormatInt(seed, 10)}
	deadline := time.Now().Add(8 * time.Minute)
	verified := false
	for time.Now().Before(deadline) {
		data, err := ch.Do(ctx, `SELECT event_type,toString(count()) AS total FROM events FINAL WHERE pipeline={pipeline:String} AND tenant={tenant:String} AND JSONExtractString(data,'correlation_id')={correlation:String} GROUP BY event_type FORMAT JSON`, params, nil)
		if err != nil {
			t.Fatal(err)
		}
		var result struct {
			Data []struct {
				Type  string `json:"event_type"`
				Total string `json:"total"`
			} `json:"data"`
		}
		if err = json.Unmarshal(data, &result); err != nil {
			t.Fatal(err)
		}
		total := int64(0)
		matches := true
		for _, row := range result.Data {
			n, _ := strconv.ParseInt(row.Total, 10, 64)
			total += n
			if n != expected[row.Type] {
				matches = false
			}
		}
		if total == int64(count) && matches {
			verified = true
			break
		}
		time.Sleep(time.Second)
	}
	if !verified {
		t.Fatal("accepted dataset disagrees with independent event-type totals")
	}
	for {
		var pipeline struct {
			Depth int `json:"outbox_depth"`
		}
		request(t, ctx, "GET", analytics+"/v1/pipeline", nil, "demo-key", 200, &pipeline)
		if pipeline.Depth == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	ingestionAndDrain := time.Since(began).Seconds()
	// Compare Go window counts and integer revenue with an independent SQL scan
	// of normalized events. No ingestion is running during this reference check.
	from := base.UnixMilli()
	to := base.Add(2 * time.Minute).UnixMilli()
	params["from"] = strconv.FormatInt(from, 10)
	params["to"] = strconv.FormatInt(to, 10)
	data, err := ch.Do(ctx, `SELECT toString(intDiv(event_time,60000)*60000) AS start,toString(count()) AS total,toString(sumIf(JSONExtractInt(data,'payload','amount_minor'),event_type='PaymentSucceeded' AND JSONExtractString(data,'payload','currency')='USD')) AS revenue FROM events FINAL WHERE pipeline={pipeline:String} AND tenant={tenant:String} AND late=0 AND event_time>={from:Int64} AND event_time<{to:Int64} GROUP BY start FORMAT JSON`, params, nil)
	if err != nil {
		t.Fatal(err)
	}
	var reference struct {
		Data []struct {
			Start   string `json:"start"`
			Total   string `json:"total"`
			Revenue string `json:"revenue"`
		} `json:"data"`
	}
	if err = json.Unmarshal(data, &reference); err != nil {
		t.Fatal(err)
	}
	var metrics struct {
		Windows []analytical.Window `json:"windows"`
	}
	request(t, ctx, "GET", analytics+"/v1/metrics?from="+base.Format(time.RFC3339)+"&to="+base.Add(2*time.Minute).Format(time.RFC3339), nil, "demo-key", 200, &metrics)
	actual := map[int64]analytical.Window{}
	for _, w := range metrics.Windows {
		actual[w.Start] = w
	}
	for _, row := range reference.Data {
		start, _ := strconv.ParseInt(row.Start, 10, 64)
		n, _ := strconv.ParseInt(row.Total, 10, 64)
		revenue, _ := strconv.ParseInt(row.Revenue, 10, 64)
		w, ok := actual[start]
		if !ok || w.Count != n || w.Revenue["USD"] != revenue {
			t.Fatalf("independent window reference mismatch at %d: count %d/%d revenue %d/%d", start, w.Count, n, w.Revenue["USD"], revenue)
		}
	}
	t.Logf("%d events acknowledged and durably visible in %.3fs (%.1f events/sec including pacing); independent counts and revenue verified", count, ingestionAndDrain, float64(count)/ingestionAndDrain)
	// Check real tenant isolation and writer permissions through public HTTP routes.
	request(t, ctx, "GET", analytics+"/v1/metrics", nil, "invalid", 401, nil)
	bad := g.Event(999999, "other-tenant")
	b, _ := json.Marshal(map[string]any{"events": []event.Event{bad}})
	request(t, ctx, "POST", gateway+"/v1/events", b, "demo-key", 403, nil)
	bad.Tenant = "tenant-demo"
	b, _ = json.Marshal(map[string]any{"events": []event.Event{bad}})
	request(t, ctx, "POST", gateway+"/v1/events", b, "reader-key", 403, nil)
	invalid := g.Event(999998, "tenant-demo")
	invalid.Time = time.Now().UTC()
	invalid.Version = 99
	b, _ = json.Marshal(map[string]any{"events": []event.Event{invalid}})
	request(t, ctx, "POST", gateway+"/v1/events", b, "demo-key", 400, nil)
	request(t, ctx, "POST", gateway+"/v1/events", []byte(`{"events":[{"tenant_id":"tenant-demo","event_id":"bad"}]}`), "demo-key", 400, nil)
	// Generated protobuf client exercises the real gRPC server and authentication.
	conn, err := grpc.NewClient(env("E2E_GRPC", "ingestion-gateway:9090"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	e := g.Event(1000001, "tenant-demo")
	e.ID = uuid.NewString()
	e.Time = time.Now().UTC()
	b, _ = json.Marshal(e)
	var p pulsev1.Event
	if err = protojson.Unmarshal(b, &p); err != nil {
		t.Fatal(err)
	}
	auth := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer demo-key")
	res, err := pulsev1.NewIngestionServiceClient(conn).PublishEventBatch(auth, &pulsev1.PublishRequest{Events: []*pulsev1.Event{&p}})
	if err != nil || res.Acknowledged != 1 {
		t.Fatal("grpc", res, err)
	}
	analyticsConn, err := grpc.NewClient(env("E2E_ANALYTICS_GRPC", "analytics-api:9090"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer analyticsConn.Close()
	queryResult, err := pulsev1.NewAnalyticsServiceClient(analyticsConn).GetWindowMetrics(auth, &pulsev1.QueryRequest{From: base.Format(time.RFC3339), To: base.Add(2 * time.Minute).Format(time.RFC3339), Limit: 10})
	if err != nil || !json.Valid(queryResult.GetJson()) {
		t.Fatal("analytics grpc", err)
	}
	// WebSocket updates must contain committed windows and a monotonically increasing sequence.
	wsURL := env("E2E_WS", "ws://websocket-gateway:8080/ws")
	connWS, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{"http://localhost:13000"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer connWS.CloseNow()
	connWS.SetReadLimit(256 << 10)
	authJSON := []byte(`{"token":"demo-key","subscriptions":["metrics","alerts"]}`)
	if err = connWS.Write(ctx, websocket.MessageText, authJSON); err != nil {
		t.Fatal(err)
	}
	var last uint64
	for i := 0; i < 2; i++ {
		_, b, err = connWS.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var frame struct {
			Sequence uint64            `json:"sequence"`
			Source   string            `json:"committed_source"`
			Windows  []json.RawMessage `json:"windows"`
		}
		if json.Unmarshal(b, &frame) != nil || frame.Sequence <= last || frame.Source != "clickhouse" || len(frame.Windows) == 0 {
			t.Fatal("invalid committed update", string(b))
		}
		last = frame.Sequence
	}
	var alerts []map[string]any
	request(t, ctx, "GET", analytics+"/v1/alerts", nil, "demo-key", 200, &alerts)
	if len(alerts) == 0 {
		t.Fatal("failure spike did not produce alerts")
	}

	t.Logf("%d events acknowledged; persisted records, gRPC, auth, WebSocket and anomaly alerts verified", count)
}
func env(k, f string) string {
	if s := os.Getenv(k); s != "" {
		return s
	}
	return f
}
func request(t *testing.T, ctx context.Context, method, url string, b []byte, key string, want int, result any) {
	t.Helper()
	r, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+key)
	r.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != want {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		t.Fatalf("%s %s status %d want %d: %s", method, url, response.StatusCode, want, detail)
	}
	if result != nil {
		if err = json.NewDecoder(response.Body).Decode(result); err != nil {
			t.Fatal(err)
		}
	}
}
