package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	pulsev1 "github.com/pulsestream/pulsestream-go/api/gen/pulse/v1"
	"github.com/pulsestream/pulsestream-go/internal/analytics"
	"github.com/pulsestream/pulsestream-go/internal/anomaly"
	"github.com/pulsestream/pulsestream-go/internal/broker"
	"github.com/pulsestream/pulsestream-go/internal/cache"
	"github.com/pulsestream/pulsestream-go/internal/config"
	"github.com/pulsestream/pulsestream-go/internal/event"
	"github.com/pulsestream/pulsestream-go/internal/observe"
	"github.com/pulsestream/pulsestream-go/internal/replay"
	"github.com/pulsestream/pulsestream-go/internal/sink"
	"github.com/pulsestream/pulsestream-go/internal/store"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"golang.org/x/net/netutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Server struct {
	pulsev1.UnimplementedIngestionServiceServer
	pulsev1.UnimplementedAnalyticsServiceServer
	C             config.Config
	Store         *store.Store
	CH            *sink.ClickHouse
	Cache         *cache.Cache
	Publisher     *kgo.Client
	Pending       chan struct{}
	HTTPAdmission chan struct{}
	ConsumerReady atomic.Bool
}

func New(c config.Config, s *store.Store) *Server {
	return &Server{C: c, Store: s, CH: sink.New(c), Cache: cache.New(c.Redis), Pending: make(chan struct{}, 64), HTTPAdmission: make(chan struct{}, 16)}
}
func (s *Server) Close() {
	s.Cache.Client.Close()
	if s.Publisher != nil {
		s.Publisher.Close()
	}
}
func (s *Server) Authenticate(token string) (config.Identity, bool) {
	token = strings.TrimPrefix(token, "Bearer ")
	id, ok := s.C.Keys[token]
	return id, ok
}
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, code string) {
	JSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": http.StatusText(status)}})
}
func (s *Server) Publish(ctx context.Context, id config.Identity, events []event.Event) (int, error) {
	if id.Role == "reader" {
		return 0, status.Error(codes.PermissionDenied, "writer role required")
	}
	if len(events) == 0 || len(events) > s.C.MaxBatch {
		return 0, status.Error(codes.InvalidArgument, "invalid batch size")
	}
	for _, e := range events {
		if e.Tenant != id.Tenant {
			return 0, status.Error(codes.PermissionDenied, "tenant mismatch")
		}
		if err := e.Validate(); err != nil {
			return 0, status.Error(codes.InvalidArgument, err.Error())
		}
		if e.Time.After(time.Now().Add(5 * time.Minute)) {
			return 0, status.Error(codes.InvalidArgument, "event timestamp too far in future")
		}
	}
	select {
	case s.Pending <- struct{}{}:
		defer func() { <-s.Pending }()
	default:
		observe.Events.WithLabelValues("ingest", "backpressure").Inc()
		return 0, status.Error(codes.ResourceExhausted, "publisher admission full")
	}
	observe.Gauge.WithLabelValues("pending_publish").Set(float64(len(s.Pending)))
	defer func() { observe.Gauge.WithLabelValues("pending_publish").Set(float64(len(s.Pending))) }()
	rate := s.C.Rate
	if configured := s.C.TenantRates[id.Tenant]; configured > 0 {
		rate = configured
	}
	allowed, err := s.Cache.Allow(ctx, id.Tenant, len(events), rate)
	if err != nil {
		return 0, status.Error(codes.Unavailable, "quota store unavailable")
	}
	if !allowed {
		observe.Events.WithLabelValues("ingest", "quota").Add(float64(len(events)))
		return 0, status.Error(codes.ResourceExhausted, "tenant rate exceeded")
	}
	ctx, span := otel.Tracer("pulse").Start(ctx, "ingest.batch")
	defer span.End()
	records := make([]*kgo.Record, 0, len(events))
	for i := range events {
		events[i].Ingested = time.Now().UTC()
		events[i].Trace = span.SpanContext().TraceID().String()
		b, _ := json.Marshal(events[i])
		r := &kgo.Record{Topic: broker.Raw, Key: []byte(events[i].PartitionKey()), Value: b, Timestamp: events[i].Ingested}
		broker.Inject(ctx, r)
		records = append(records, r)
	}
	start := time.Now()
	acked, publishErr := broker.ProduceBatch(ctx, s.Publisher, records)
	observe.Duration.WithLabelValues("publish").Observe(time.Since(start).Seconds())
	observe.Events.WithLabelValues("ingest", "acked").Add(float64(acked))
	if err = publishErr; err != nil {
		slog.Warn("publish failed", "error", err, "acknowledged", acked)
		return acked, status.Error(codes.Unavailable, "publish outcome may be ambiguous; retry same event IDs")
	}
	return acked, nil
}
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(observe.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { JSON(w, 200, map[string]string{"status": "alive"}) })
	mux.HandleFunc("GET /readyz", s.ready)
	mux.HandleFunc("/v1/", func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			if origin != s.C.Origin {
				fail(w, 403, "origin_denied")
				return
			}
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization,Content-Type")
			w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,OPTIONS")
		}
		if r.Method == "OPTIONS" {
			w.WriteHeader(204)
			return
		}
		id, ok := s.Authenticate(r.Header.Get("Authorization"))
		if !ok {
			fail(w, 401, "unauthorized")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 35*time.Second)
		defer cancel()
		r = r.WithContext(ctx)
		if s.C.Role == "ingestion-gateway" {
			if r.URL.Path == "/v1/events" && r.Method == "POST" {
				s.publishHTTP(w, r, id)
				return
			}
			fail(w, 404, "not_found")
			return
		}
		s.route(w, r, id)
	})
	if s.C.Role == "websocket-gateway" {
		mux.HandleFunc("GET /ws", s.websocket)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/") {
			select {
			case s.HTTPAdmission <- struct{}{}:
				defer func() { <-s.HTTPAdmission }()
			default:
				observe.Events.WithLabelValues("http", "backpressure").Inc()
				w.Header().Set("Retry-After", "1")
				fail(w, 429, "request_admission_full")
				return
			}
		}
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), headerCarrier(r.Header))
		ctx, span := otel.Tracer("pulse").Start(ctx, "http "+r.Method+" "+routeName(r.URL.Path))
		defer span.End()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

type headerCarrier http.Header

func (h headerCarrier) Get(k string) string { return http.Header(h).Get(k) }
func (h headerCarrier) Set(k, v string)     { http.Header(h).Set(k, v) }
func (h headerCarrier) Keys() []string {
	out := []string{}
	for k := range h {
		out = append(out, k)
	}
	return out
}
func routeName(p string) string {
	if strings.HasPrefix(p, "/v1/alerts/") {
		return "/v1/alerts/:id"
	}
	if strings.HasPrefix(p, "/v1/rules/") {
		return "/v1/rules/:id"
	}
	return p
}
func decode(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 4<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}
func (s *Server) publishHTTP(w http.ResponseWriter, r *http.Request, id config.Identity) {
	var body struct {
		Events []event.Event `json:"events"`
	}
	if err := decode(w, r, &body); err != nil {
		fail(w, 400, "invalid_json")
		return
	}
	n, err := s.Publish(r.Context(), id, body.Events)
	if err != nil {
		code := 503
		switch status.Code(err) {
		case codes.InvalidArgument:
			code = 400
		case codes.PermissionDenied:
			code = 403
		case codes.ResourceExhausted:
			code = 429
			w.Header().Set("Retry-After", "1")
		}
		JSON(w, code, map[string]any{"error": status.Convert(err).Message(), "acknowledged": n, "retry": "reuse event IDs"})
		return
	}
	JSON(w, 202, map[string]any{"acknowledged": n, "durability": "all in-sync replicas; local topology has one replica"})
}
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Store.Pool.Ping(ctx); err != nil {
		fail(w, 503, "database_unavailable")
		return
	}
	if s.C.Role == "stream-processor" && !s.ConsumerReady.Load() {
		fail(w, 503, "consumer_not_assigned")
		return
	}
	if s.C.Role == "ingestion-gateway" {
		if err := s.Cache.Client.Ping(ctx).Err(); err != nil {
			fail(w, 503, "quota_unavailable")
			return
		}
		if err := s.Publisher.Ping(ctx); err != nil {
			fail(w, 503, "broker_unavailable")
			return
		}
	}
	if s.C.Role == "analytics-api" || s.C.Role == "websocket-gateway" || s.C.Role == "sink-dispatcher" || s.C.Role == "alert-service" {
		if _, err := s.CH.Do(ctx, "SELECT 1", nil, nil); err != nil {
			fail(w, 503, "analytics_unavailable")
			return
		}
	}
	JSON(w, 200, map[string]string{"status": "ready"})
}
func (s *Server) grpcIdentity(ctx context.Context) (config.Identity, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	v := md.Get("authorization")
	if len(v) != 1 {
		return config.Identity{}, status.Error(codes.Unauthenticated, "API key required")
	}
	id, ok := s.Authenticate(v[0])
	if !ok {
		return id, status.Error(codes.Unauthenticated, "invalid API key")
	}
	return id, nil
}
func (s *Server) PublishEventBatch(ctx context.Context, req *pulsev1.PublishRequest) (*pulsev1.PublishResponse, error) {
	id, err := s.grpcIdentity(ctx)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	events := make([]event.Event, 0, len(req.Events))
	ids := []string{}
	for _, p := range req.Events {
		b, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(p)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid protobuf event")
		}
		e, err := event.Decode(b)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		events = append(events, e)
		ids = append(ids, e.ID)
	}
	n, err := s.Publish(ctx, id, events)
	if err != nil {
		return nil, err
	}
	return &pulsev1.PublishResponse{Acknowledged: uint32(n), EventIds: ids}, nil
}
func (s *Server) GetWindowMetrics(ctx context.Context, req *pulsev1.QueryRequest) (*pulsev1.JSONResponse, error) {
	id, err := s.grpcIdentity(ctx)
	if err != nil {
		return nil, err
	}
	from, to := analytics.Range(1)
	if req.From != "" {
		t, e := time.Parse(time.RFC3339, req.From)
		if e != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid from")
		}
		from = t.UnixMilli()
	}
	if req.To != "" {
		t, e := time.Parse(time.RFC3339, req.To)
		if e != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid to")
		}
		to = t.UnixMilli()
	}
	if to <= from || to-from > 86400000 {
		return nil, status.Error(codes.InvalidArgument, "range must be <=24h")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ws, err := analytics.Query(ctx, s.CH, id.Tenant, s.C.Group, from, to, 60000, 60000)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "analytics unavailable")
	}
	if req.Limit > 1440 {
		return nil, status.Error(codes.InvalidArgument, "limit must be <=1440")
	}
	if req.Limit > 0 && len(ws) > int(req.Limit) {
		ws = ws[len(ws)-int(req.Limit):]
	}
	if err = analytics.ApplyFinality(ctx, s.Store, ws); err != nil {
		return nil, status.Error(codes.Unavailable, "checkpoint metadata unavailable")
	}
	b, _ := json.Marshal(ws)
	return &pulsev1.JSONResponse{Json: b}, nil
}
func (s *Server) GRPC(ctx context.Context) error {
	l, err := net.Listen("tcp", s.C.GRPC)
	if err != nil {
		return err
	}
	g := grpc.NewServer(grpc.MaxRecvMsgSize(4<<20), grpc.MaxConcurrentStreams(2))
	if s.C.Role == "ingestion-gateway" {
		pulsev1.RegisterIngestionServiceServer(g, s)
	} else {
		pulsev1.RegisterAnalyticsServiceServer(g, s)
	}
	go func() {
		<-ctx.Done()
		done := make(chan struct{})
		go func() { g.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			g.Stop()
		}
	}()
	return g.Serve(netutil.LimitListener(l, 8))
}
func (s *Server) route(w http.ResponseWriter, r *http.Request, id config.Identity) {
	switch {
	case r.URL.Path == "/v1/overview" && r.Method == "GET":
		s.overview(w, r, id)
	case r.URL.Path == "/v1/groups" && r.Method == "GET":
		s.groups(w, r, id)
	case r.URL.Path == "/v1/alerts/summary" && r.Method == "GET":
		s.anomalyCounts(w, r, id)
	case r.URL.Path == "/v1/metrics" && r.Method == "GET":
		s.metrics(w, r, id)
	case r.URL.Path == "/v1/events" && r.Method == "GET":
		from, to, err := timeRange(r)
		if err != nil {
			fail(w, 400, "invalid_range")
			return
		}
		limit := number(r, "limit", 50, 1, 500)
		offset := number(r, "offset", 0, 0, 100000)
		b, err := analytics.Events(r.Context(), s.CH, id.Tenant, s.C.Group, r.URL.Query().Get("type"), r.URL.Query().Get("q"), from, to, limit, offset)
		if err != nil {
			fail(w, 503, "query_failed")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	case r.URL.Path == "/v1/alerts" && r.Method == "GET":
		s.queryJSON(w, r, `SELECT coalesce(json_agg(a),'[]') FROM (SELECT * FROM alerts WHERE tenant=$1 ORDER BY triggered_at DESC LIMIT 200) a`, id.Tenant)
	case strings.HasPrefix(r.URL.Path, "/v1/alerts/") && r.Method == "POST":
		if id.Role != "admin" {
			fail(w, 403, "admin_required")
			return
		}
		s.alertAction(w, r, id)
	case r.URL.Path == "/v1/rules" && r.Method == "GET":
		rules, err := anomaly.Rules(r.Context(), s.Store, id.Tenant)
		if err != nil {
			fail(w, 503, "query_failed")
			return
		}
		JSON(w, 200, rules)
	case strings.HasPrefix(r.URL.Path, "/v1/rules/") && r.Method == "PUT":
		if id.Role != "admin" {
			fail(w, 403, "admin_required")
			return
		}
		var rule anomaly.Rule
		if decode(w, r, &rule) != nil || rule.Validate() != nil || rule.ID != strings.TrimPrefix(r.URL.Path, "/v1/rules/") {
			fail(w, 400, "invalid_rule")
			return
		}
		_, err := s.Store.Pool.Exec(r.Context(), `INSERT INTO rules(tenant,id,kind,enabled,threshold,min_samples,cooldown_seconds,severity) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(tenant,id) DO UPDATE SET kind=$3,enabled=$4,threshold=$5,min_samples=$6,cooldown_seconds=$7,severity=$8`, id.Tenant, rule.ID, rule.Kind, rule.Enabled, rule.Threshold, rule.MinSamples, rule.Cooldown, rule.Severity)
		if err != nil {
			fail(w, 503, "write_failed")
			return
		}
		JSON(w, 200, rule)
	case r.URL.Path == "/v1/pipeline" && r.Method == "GET":
		if id.Role != "admin" {
			fail(w, 403, "admin_required")
			return
		}
		s.pipeline(w, r)
	case r.URL.Path == "/v1/replays" && r.Method == "GET":
		if id.Role != "admin" {
			fail(w, 403, "admin_required")
			return
		}
		s.queryJSON(w, r, `SELECT coalesce(json_agg(a),'[]') FROM (SELECT * FROM replay_audit WHERE tenant=$1 ORDER BY id DESC LIMIT 100) a`, id.Tenant)
	case (r.URL.Path == "/v1/replays" || r.URL.Path == "/v1/dlq") && r.Method == "POST":
		if id.Role != "admin" {
			fail(w, 403, "admin_required")
			return
		}
		var req replay.Request
		if decode(w, r, &req) != nil {
			fail(w, 400, "invalid_json")
			return
		}
		if r.URL.Path == "/v1/dlq" {
			req.Topic = broker.DLQ
			req.DryRun = true
		}
		if err := req.Validate(s.C); err != nil {
			JSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		res, err := replay.Run(r.Context(), s.C, s.Store, id.Tenant, req)
		if err != nil {
			JSON(w, 409, map[string]any{"error": err.Error(), "result": res})
			return
		}
		JSON(w, 200, res)
	default:
		fail(w, 404, "not_found")
	}
}
func (s *Server) metrics(w http.ResponseWriter, r *http.Request, id config.Identity) {
	from, to, err := timeRange(r)
	if err != nil {
		fail(w, 400, "invalid_range")
		return
	}
	size := number(r, "size_ms", 60000, 60000, 900000)
	step := number(r, "step_ms", 60000, 60000, 300000)
	valid := false
	for _, pair := range [][2]int{{60000, 60000}, {300000, 300000}, {300000, 60000}, {900000, 300000}} {
		if pair[0] == size && pair[1] == step {
			valid = true
		}
	}
	if !valid {
		fail(w, 400, "unsupported_window")
		return
	}
	key := fmt.Sprintf("metrics:%s:%s:%d:%d:%d:%d", id.Tenant, s.C.Group, from/2000, to/2000, size, step)
	if b := s.Cache.Get(r.Context(), key); b != nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
		return
	}
	ws, err := analytics.Query(r.Context(), s.CH, id.Tenant, s.C.Group, from, to, int64(size), int64(step))
	if err != nil {
		fail(w, 503, "query_failed")
		return
	}
	if err = analytics.ApplyFinality(r.Context(), s.Store, ws); err != nil {
		fail(w, 503, "checkpoint_metadata_unavailable")
		return
	}
	b, _ := json.Marshal(map[string]any{"windows": ws, "tenant": id.Tenant, "pipeline": s.C.Group, "as_of": time.Now().UTC(), "source": "clickhouse", "finality": "global minimum watermark includes idle and unseen partitions"})
	s.Cache.Put(r.Context(), key, b)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}
func (s *Server) queryJSON(w http.ResponseWriter, r *http.Request, q, tenant string) {
	var b []byte
	if err := s.Store.Pool.QueryRow(r.Context(), q, tenant).Scan(&b); err != nil {
		fail(w, 503, "query_failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}
func (s *Server) pipeline(w http.ResponseWriter, r *http.Request) {
	var b []byte
	err := s.Store.Pool.QueryRow(r.Context(), `SELECT json_build_object('group',$1::text,'outbox_depth',(SELECT count(*) FROM outbox),'partitions',(SELECT coalesce(json_agg(d),'[]') FROM (SELECT c.partition_id,c.last_offset,c.created_at,c.checksum,c.config_hash,c.state->'watermark' AS watermark,d.owner,d.high_watermark,greatest(d.high_watermark-c.last_offset-1,0) AS lag,d.updated_at,(c.created_at < now()-interval '30 seconds') AS idle FROM checkpoints c LEFT JOIN diagnostics d ON d.pipeline=c.pipeline AND d.partition_id=c.partition_id WHERE c.pipeline=$1 ORDER BY c.partition_id) d),'partition_count',$2::int)`, s.C.Group, s.C.Partitions).Scan(&b)
	if err != nil {
		fail(w, 503, "query_failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}
func (s *Server) alertAction(w http.ResponseWriter, r *http.Request, id config.Identity) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/alerts/"), "/")
	if len(parts) != 2 || (parts[1] != "acknowledge" && parts[1] != "resolve") {
		fail(w, 400, "invalid_action")
		return
	}
	field, next := "acknowledged_at", "acknowledged"
	if parts[1] == "resolve" {
		field, next = "resolved_at", "resolved"
	}
	tx, err := s.Store.Pool.Begin(r.Context())
	if err != nil {
		fail(w, 503, "write_failed")
		return
	}
	defer tx.Rollback(r.Context())
	var payload []byte
	err = tx.QueryRow(r.Context(), `UPDATE alerts SET `+field+`=coalesce(`+field+`,now()),status=$3 WHERE tenant=$1 AND id=$2 AND (status!='resolved' OR $3='resolved') RETURNING row_to_json(alerts)`, id.Tenant, parts[0], next).Scan(&payload)
	if err != nil {
		fail(w, 404, "alert_not_found")
		return
	}
	out := map[string]any{"id": parts[0], "tenant": id.Tenant, "version": uint64(time.Now().UnixMicro()), "data": string(payload)}
	if store.Enqueue(r.Context(), tx, "alerts", out) != nil || tx.Commit(r.Context()) != nil {
		fail(w, 503, "write_failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(payload)
}
func timeRange(r *http.Request) (int64, int64, error) {
	from, to := analytics.Range(1)
	var err error
	for k, dest := range map[string]*int64{"from": &from, "to": &to} {
		if raw := r.URL.Query().Get(k); raw != "" {
			var t time.Time
			t, err = time.Parse(time.RFC3339, raw)
			if err != nil {
				return 0, 0, err
			}
			*dest = t.UnixMilli()
		}
	}
	if to <= from || to-from > 86400000 {
		return 0, 0, errors.New("range must be 0..24 hours")
	}
	return from, to, nil
}
func number(r *http.Request, k string, fallback, lo, hi int) int {
	n, e := strconv.Atoi(r.URL.Query().Get(k))
	if e != nil {
		return fallback
	}
	return max(lo, min(hi, n))
}
