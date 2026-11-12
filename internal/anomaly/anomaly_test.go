package anomaly

import (
	"github.com/pulsestream/pulsestream-go/internal/analytics"
	"github.com/pulsestream/pulsestream-go/internal/windows"
	"testing"
)

func TestRules(t *testing.T) {
	w := analytics.Window{Aggregate: &windows.Aggregate{Count: 100, Success: 20, Failed: 80, LatencyCount: 30, Revenue: map[string]int64{"USD": 100}, Services: map[string][2]int64{"checkout": {10, 90}}}, FailureRate: .8, P95Latency: 2000, MaxAccountCount: 120}
	prev := analytics.Window{Aggregate: &windows.Aggregate{Count: 20, Success: 30, Revenue: map[string]int64{"USD": 1000}}}
	for _, r := range []Rule{{Kind: "payment_failure", Threshold: .3}, {Kind: "traffic_spike", Threshold: 3}, {Kind: "revenue_drop", Threshold: .5}, {Kind: "error_rate", Threshold: .1}, {Kind: "latency", Threshold: 1500}, {Kind: "account_burst", Threshold: 100}} {
		r.Enabled = true
		r.MinSamples = 20
		if _, ok := Evaluate(r, w, &prev); !ok {
			t.Fatal("rule did not trigger", r.Kind)
		}
		r.Enabled = false
		if _, ok := Evaluate(r, w, &prev); ok {
			t.Fatal("disabled rule fired")
		}
		r.Enabled = true
		r.MinSamples = 1000
		if _, ok := Evaluate(r, w, &prev); ok {
			t.Fatal("low sample fired")
		}
	}
}
