package app

import (
	"context"
	"github.com/pulsestream/pulsestream-go/internal/broker"
	"github.com/pulsestream/pulsestream-go/internal/config"
	"github.com/pulsestream/pulsestream-go/internal/observe"
	"github.com/pulsestream/pulsestream-go/internal/store"
	"github.com/twmb/franz-go/pkg/kadm"
	"time"
)

func pollMetrics(ctx context.Context, c config.Config, s *store.Store, done chan<- struct{}) {
	defer close(done)
	if c.Role != "stream-processor" && c.Role != "sink-dispatcher" {
		return
	}
	var admin *kadm.Client
	if c.Role == "stream-processor" {
		cl, err := broker.New(c)
		if err == nil {
			defer cl.Close()
			admin = kadm.NewClient(cl)
		}
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			query, cancel := context.WithTimeout(ctx, 3*time.Second)
			if admin != nil {
				if lag, err := admin.Lag(query, c.Group); err == nil {
					if group, ok := lag[c.Group]; ok && group.Error() == nil {
						observe.Gauge.WithLabelValues("consumer_lag").Set(float64(group.Lag.Total()))
					}
				}
			}
			if c.Role == "sink-dispatcher" {
				var n int64
				if s.Pool.QueryRow(query, `SELECT count(*) FROM outbox`).Scan(&n) == nil {
					observe.Gauge.WithLabelValues("outbox_depth").Set(float64(n))
					observe.Gauge.WithLabelValues("outbox_capacity").Set(500000)
				}
			}
			cancel()
		}
	}
}
