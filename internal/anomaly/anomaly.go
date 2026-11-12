package anomaly

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pulsestream/pulsestream-go/internal/analytics"
	"github.com/pulsestream/pulsestream-go/internal/broker"
	"github.com/pulsestream/pulsestream-go/internal/config"
	"github.com/pulsestream/pulsestream-go/internal/observe"
	"github.com/pulsestream/pulsestream-go/internal/processing"
	"github.com/pulsestream/pulsestream-go/internal/sink"
	"github.com/pulsestream/pulsestream-go/internal/store"
	"log/slog"
	"time"
)

type Rule struct {
	ID         string  `json:"id"`
	Kind       string  `json:"kind"`
	Enabled    bool    `json:"enabled"`
	Threshold  float64 `json:"threshold"`
	MinSamples int64   `json:"min_samples"`
	Cooldown   int     `json:"cooldown_seconds"`
	Severity   string  `json:"severity"`
}

func (r Rule) Validate() error {
	if r.ID == "" || len(r.ID) > 64 || r.Threshold < 0 || r.MinSamples < 1 || r.Cooldown < 0 || r.Cooldown > 86400 {
		return fmt.Errorf("invalid rule configuration")
	}
	if r.Severity != "warning" && r.Severity != "critical" && r.Severity != "info" {
		return fmt.Errorf("invalid severity")
	}
	switch r.Kind {
	case "payment_failure", "traffic_spike", "revenue_drop", "error_rate", "latency", "account_burst":
		return nil
	}
	return fmt.Errorf("unsupported rule kind")
}
func Seed(ctx context.Context, s *store.Store) error {
	tenants := map[string]bool{}
	for _, id := range s.Config.Keys {
		tenants[id.Tenant] = true
	}
	for tenant := range tenants {
		for _, r := range []Rule{{"payment-failures", "payment_failure", true, .3, 20, 300, "critical"}, {"traffic-spike", "traffic_spike", true, 3, 100, 300, "warning"}, {"revenue-drop", "revenue_drop", true, .5, 20, 300, "warning"}, {"application-errors", "error_rate", true, .1, 20, 300, "critical"}, {"service-latency", "latency", true, 1500, 20, 300, "warning"}, {"account-burst", "account_burst", true, 100, 20, 300, "warning"}} {
			if _, err := s.Pool.Exec(ctx, `INSERT INTO rules(tenant,id,kind,enabled,threshold,min_samples,cooldown_seconds,severity) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING`, tenant, r.ID, r.Kind, r.Enabled, r.Threshold, r.MinSamples, r.Cooldown, r.Severity); err != nil {
				return err
			}
		}
	}
	return nil
}
func Rules(ctx context.Context, s *store.Store, tenant string) ([]Rule, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id,kind,enabled,threshold,min_samples,cooldown_seconds,severity FROM rules WHERE tenant=$1 ORDER BY id`, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Rule{}
	for rows.Next() {
		var r Rule
		if err = rows.Scan(&r.ID, &r.Kind, &r.Enabled, &r.Threshold, &r.MinSamples, &r.Cooldown, &r.Severity); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func Evaluate(r Rule, w analytics.Window, previous *analytics.Window) (float64, bool) {
	if !r.Enabled || w.Count < r.MinSamples {
		return 0, false
	}
	switch r.Kind {
	case "payment_failure":
		return w.FailureRate, w.Success+w.Failed >= r.MinSamples && w.FailureRate > r.Threshold
	case "traffic_spike":
		if previous != nil && previous.Count >= r.MinSamples {
			v := float64(w.Count) / float64(previous.Count)
			return v, v > r.Threshold
		}
	case "revenue_drop":
		if previous != nil && previous.Success >= r.MinSamples {
			base := previous.Revenue["USD"]
			if base > 0 {
				v := 1 - float64(w.Revenue["USD"])/float64(base)
				return v, v > r.Threshold
			}
		}
	case "error_rate":
		var peak float64
		for _, v := range w.Services {
			if v[0]+v[1] >= r.MinSamples {
				rate := float64(v[1]) / float64(v[0]+v[1])
				if rate > peak {
					peak = rate
				}
			}
		}
		return peak, peak > r.Threshold
	case "latency":
		return float64(w.P95Latency), w.LatencyCount >= r.MinSamples && float64(w.P95Latency) > r.Threshold
	case "account_burst":
		return float64(w.MaxAccountCount), float64(w.MaxAccountCount) > r.Threshold
	}
	return 0, false
}
func Tick(ctx context.Context, s *store.Store, ch *sink.ClickHouse) error {
	tenants := map[string]bool{}
	for _, id := range s.Config.Keys {
		tenants[id.Tenant] = true
	}
	for tenant := range tenants {
		rules, err := Rules(ctx, s, tenant)
		if err != nil {
			return err
		}
		from, to := analytics.Range(1)
		ws, err := analytics.Query(ctx, ch, tenant, s.Config.Group, from, to, 60000, 60000)
		if err != nil {
			return err
		}
		if err = analytics.ApplyFinality(ctx, s, ws); err != nil {
			return err
		}
		for i, w := range ws {
			var prev *analytics.Window
			if i > 0 && ws[i-1].End == w.Start {
				prev = &ws[i-1]
			}
			for _, r := range rules {
				value, trigger := Evaluate(r, w, prev)
				if !trigger {
					continue
				}
				if err = triggerAlert(ctx, s, tenant, r, w, value); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
func triggerAlert(ctx context.Context, s *store.Store, tenant string, r Rule, w analytics.Window, value float64) error {
	id := store.Hash([]byte(fmt.Sprintf("%s/%s/%s/%d", s.Config.Group, tenant, r.ID, w.Start)))
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Serialize rule cooldown across replicas, independent of Kafka ownership.
	if _, err = tx.Exec(ctx, `SELECT id FROM rules WHERE tenant=$1 AND id=$2 FOR UPDATE`, tenant, r.ID); err != nil {
		return err
	}
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM alerts WHERE tenant=$1 AND rule_id=$2 AND (id=$3 OR triggered_at>now()-make_interval(secs=>$4)))`, tenant, r.ID, id, r.Cooldown).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return tx.Commit(ctx)
	}
	evidence, _ := json.Marshal(map[string]any{"count": w.Count, "failed": w.Failed, "success": w.Success, "shards": w.Shards, "provisional": !w.Final, "pipeline": s.Config.Group})
	_, err = tx.Exec(ctx, `INSERT INTO alerts(id,tenant,rule_id,severity,window_start,observed,threshold,evidence) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING`, id, tenant, r.ID, r.Severity, w.Start, value, r.Threshold, evidence)
	if err != nil {
		return err
	}
	output := map[string]any{"id": id, "tenant": tenant, "version": uint64(time.Now().UnixMicro()), "data": string(evidence)}
	if err = store.Enqueue(ctx, tx, "alerts", output); err != nil {
		return err
	}
	if err = store.Enqueue(ctx, tx, "kafka:"+broker.Alerts, output); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	observe.Events.WithLabelValues("alert", r.Kind).Inc()
	return nil
}
func Run(ctx context.Context, c config.Config, s *store.Store) error {
	ch := sink.New(c)
	for ctx.Err() == nil {
		if err := Tick(ctx, s, ch); err != nil {
			slog.Error("alert evaluation", "error", err)
		}
		if !processing.Wait(ctx, 2*time.Second) {
			break
		}
	}
	return ctx.Err()
}
