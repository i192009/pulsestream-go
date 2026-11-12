package api

import (
	"context"
	"encoding/json"
	"github.com/coder/websocket"
	"github.com/pulsestream/pulsestream-go/internal/analytics"
	"github.com/pulsestream/pulsestream-go/internal/observe"
	"net/http"
	"time"
)

func (s *Server) websocket(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Origin") != s.C.Origin {
		fail(w, 403, "origin_denied")
		return
	}
	select {
	case s.Pending <- struct{}{}:
		defer func() { <-s.Pending }()
	default:
		fail(w, 503, "connection_capacity")
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(4096)
	authCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	_, b, err := conn.Read(authCtx)
	cancel()
	if err != nil {
		return
	}
	var auth struct {
		Token         string   `json:"token"`
		Subscriptions []string `json:"subscriptions"`
	}
	if json.Unmarshal(b, &auth) != nil {
		return
	}
	id, ok := s.Authenticate(auth.Token)
	if !ok {
		_ = conn.Close(websocket.StatusPolicyViolation, "authentication required")
		return
	}
	subs := map[string]bool{}
	for _, v := range auth.Subscriptions {
		if v == "metrics" || v == "alerts" || v == "pipeline" && id.Role == "admin" {
			subs[v] = true
		}
	}
	ctx, stop := context.WithCancel(r.Context())
	defer stop()
	observe.Gauge.WithLabelValues("websocket_clients").Inc()
	defer observe.Gauge.WithLabelValues("websocket_clients").Dec()
	queue := make(chan []byte, 4)
	done := make(chan struct{})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		defer stop()
		for ctx.Err() == nil {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
		}
	}()
	go func() {
		defer close(done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		var sequence uint64
		for {
			if ctx.Err() != nil {
				return
			}
			sequence++
			payload := map[string]any{"sequence": sequence, "committed_source": "clickhouse", "as_of": time.Now().UTC()}
			qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			if subs["metrics"] {
				from, to := analytics.Range(1)
				ws, err := analytics.Query(qctx, s.CH, id.Tenant, s.C.Group, from, to, 60000, 60000)
				if err != nil {
					payload["error"] = "analytics temporarily unavailable"
				} else {
					if analytics.ApplyFinality(qctx, s.Store, ws) == nil {
						payload["windows"] = ws
					} else {
						payload["error"] = "checkpoint metadata unavailable"
					}
				}
			}
			if subs["alerts"] {
				var raw json.RawMessage
				if s.Store.Pool.QueryRow(qctx, `SELECT coalesce(json_agg(a),'[]') FROM (SELECT * FROM alerts WHERE tenant=$1 ORDER BY triggered_at DESC LIMIT 100) a`, id.Tenant).Scan(&raw) == nil {
					payload["alerts"] = raw
				}
			}
			cancel()
			b, _ := json.Marshal(payload)
			if len(b) > 256<<10 {
				observe.Events.WithLabelValues("websocket", "snapshot_size_limit").Inc()
				b, _ = json.Marshal(map[string]any{"sequence": sequence, "as_of": time.Now().UTC(), "error": "Snapshot exceeds 256KiB; use a narrower HTTP query"})
			}
			select {
			case queue <- b:
			case <-ctx.Done():
				return
			default:
				observe.Events.WithLabelValues("websocket", "slow_client").Inc()
				stop()
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	defer func() { stop(); conn.CloseNow(); <-done; <-readerDone }()
	for {
		select {
		case <-ctx.Done():
			return
		case b := <-queue:
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := conn.Write(writeCtx, websocket.MessageText, b)
			cancel()
			if err != nil {
				return
			}
		case <-heartbeat.C:
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}
