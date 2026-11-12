package api

import (
	"fmt"
	"github.com/pulsestream/pulsestream-go/internal/broker"
	"github.com/pulsestream/pulsestream-go/internal/config"
	"github.com/twmb/franz-go/pkg/kadm"
	"net/http"
	"time"
)

func (s *Server) overview(w http.ResponseWriter, r *http.Request, id config.Identity) {
	now := time.Now().UTC()
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).UnixMilli()
	params := map[string]string{"tenant": id.Tenant, "pipeline": s.C.Group, "start": fmt.Sprint(midnight), "end": fmt.Sprint(now.UnixMilli())}
	b, err := s.CH.Do(r.Context(), `SELECT toString(count()) AS stored_events,toString(countIf(event_time>={start:Int64} AND event_time<{end:Int64})) AS processed_today,toString(uniqExactIf(tenant,event_time>={start:Int64} AND event_time<{end:Int64})) AS active_tenants FROM events FINAL WHERE tenant={tenant:String} AND pipeline={pipeline:String} FORMAT JSON`, params, nil)
	if err != nil {
		fail(w, 503, "query_failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

func (s *Server) groups(w http.ResponseWriter, r *http.Request, id config.Identity) {
	if id.Role != "admin" {
		fail(w, 403, "admin_required")
		return
	}
	cl, err := broker.New(s.C)
	if err != nil {
		fail(w, 503, "broker_unavailable")
		return
	}
	defer cl.Close()
	a := kadm.NewClient(cl)
	lag, err := a.Lag(r.Context(), s.C.Group)
	if err != nil {
		fail(w, 503, "broker_unavailable")
		return
	}
	JSON(w, 200, map[string]any{"groups": lag})
}
func (s *Server) anomalyCounts(w http.ResponseWriter, r *http.Request, id config.Identity) {
	from, to, err := timeRange(r)
	if err != nil {
		fail(w, 400, "invalid_range")
		return
	}
	var b []byte
	err = s.Store.Pool.QueryRow(r.Context(), `SELECT coalesce(json_agg(a),'[]') FROM (SELECT date_trunc('hour',triggered_at) AS hour,severity,status,count(*) AS count FROM alerts WHERE tenant=$1 AND triggered_at>=to_timestamp($2::double precision/1000) AND triggered_at<to_timestamp($3::double precision/1000) GROUP BY hour,severity,status ORDER BY hour) a`, id.Tenant, from, to).Scan(&b)
	if err != nil {
		fail(w, 503, "query_failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}
