package integration

import (
	"github.com/pulsestream/pulsestream-go/internal/synthetic"
	"github.com/pulsestream/pulsestream-go/internal/windows"
	"os"
	"testing"
	"time"
)

func TestMillionSyntheticWindowBatch(t *testing.T) {
	if os.Getenv("PULSE_MILLION") != "1" {
		t.Skip("set PULSE_MILLION=1 for the million-event CPU workload")
	}
	base := time.Now().Truncate(time.Minute)
	g := synthetic.New(1000000, base)
	g.Step = 0
	states := make([]*windows.State, 8)
	for i := range states {
		states[i] = windows.New()
	}
	var expected int64
	start := time.Now()
	for i := int64(0); i < 1_000_000; i++ {
		e := g.Event(i, "tenant-million")
		e.Ingested = e.Time
		if err := e.Validate(); err != nil {
			t.Fatal(err)
		}
		if e.Type == "PaymentSucceeded" {
			expected += e.Payload.AmountMinor
		}
		if _, late, err := states[i%8].Apply(e, 2*time.Minute, 10*time.Second, i+1); err != nil || late {
			t.Fatal(err, late)
		}
	}
	var count, revenue int64
	for _, s := range states {
		for _, w := range s.Windows {
			if w.Size == 60000 {
				count += w.Count
				revenue += w.Revenue["USD"]
			}
		}
	}
	if count != 1_000_000 || revenue != expected {
		t.Fatal(count, revenue, expected)
	}
	t.Logf("1,000,000 synthetic events validated and aggregated across eight states in %s; count and integer revenue match offline reference. This is a CPU workload, not a distributed throughput measurement.", time.Since(start))
}
