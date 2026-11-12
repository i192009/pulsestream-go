// Package windows implements deterministic partition-owned event-time state.
package windows

import (
	"errors"
	"fmt"
	"github.com/pulsestream/pulsestream-go/internal/event"
	"math"
	"sort"
	"time"
)

type Spec struct{ Size, Step time.Duration }

var Specs = []Spec{{time.Minute, time.Minute}, {5 * time.Minute, 5 * time.Minute}, {5 * time.Minute, time.Minute}, {15 * time.Minute, 5 * time.Minute}}

type Aggregate struct {
	Tenant       string              `json:"tenant"`
	Start        int64               `json:"start"`
	End          int64               `json:"end"`
	Size         int64               `json:"size"`
	Step         int64               `json:"step"`
	Count        int64               `json:"count"`
	Success      int64               `json:"success"`
	Failed       int64               `json:"failed"`
	Checkouts    int64               `json:"checkouts"`
	Orders       int64               `json:"orders"`
	Errors       int64               `json:"errors"`
	Revenue      map[string]int64    `json:"revenue"`
	Payments     map[string]int64    `json:"payments"`
	Users        map[string]bool     `json:"users"`
	Accounts     map[string]int64    `json:"accounts"`
	Services     map[string][2]int64 `json:"services"`
	Types        map[string]int64    `json:"types"`
	Latencies    map[int]int64       `json:"latencies"`
	LatencySum   int64               `json:"latency_sum"`
	LatencyCount int64               `json:"latency_count"`
	LagSum       int64               `json:"lag_sum"`
	LagHistogram map[int]int64       `json:"lag_histogram"`
	Revision     int64               `json:"revision"`
	Closed       bool                `json:"closed"`
	Final        bool                `json:"final"`
}
type State struct {
	MaxTime   int64                 `json:"max_time"`
	Watermark int64                 `json:"watermark"`
	Windows   map[string]*Aggregate `json:"windows"`
}

func New() *State { return &State{Windows: map[string]*Aggregate{}} }
func Starts(t time.Time, s Spec) []int64 {
	n := t.UnixMilli()
	size, step := s.Size.Milliseconds(), s.Step.Milliseconds()
	last := n - n%step
	var out []int64
	for start := last; start > n-size; start -= step {
		out = append(out, start)
	}
	return out
}
func Key(tenant string, start int64, s Spec) string {
	return fmt.Sprintf("%s/%d/%d/%d", tenant, start, s.Size.Milliseconds(), s.Step.Milliseconds())
}
func (s *State) Apply(e event.Event, lateness, disorder time.Duration, revision int64) ([]*Aggregate, bool, error) {
	t := e.Time.UnixMilli()
	if t < s.Watermark-lateness.Milliseconds() {
		return nil, true, nil
	}
	if t > s.MaxTime {
		s.MaxTime = t
	}
	s.Watermark = s.MaxTime - disorder.Milliseconds()
	changed := map[string]*Aggregate{}
	for _, spec := range Specs {
		for _, start := range Starts(e.Time, spec) {
			k := Key(e.Tenant, start, spec)
			a := s.Windows[k]
			if a == nil {
				if len(s.Windows) >= 4096 {
					return nil, false, errors.New("window capacity exceeded: increase partitions or reduce tenant fanout")
				}
				a = &Aggregate{Tenant: e.Tenant, Start: start, End: start + spec.Size.Milliseconds(), Size: spec.Size.Milliseconds(), Step: spec.Step.Milliseconds(), Revenue: map[string]int64{}, Payments: map[string]int64{}, Users: map[string]bool{}, Accounts: map[string]int64{}, Services: map[string][2]int64{}, Types: map[string]int64{}, Latencies: map[int]int64{}, LagHistogram: map[int]int64{}}
				s.Windows[k] = a
			}
			if len(a.Users) >= 100000 && !a.Users[e.Payload.UserID] {
				return nil, false, errors.New("exact user capacity exceeded")
			}
			a.Count++
			a.Types[e.Type]++
			if e.Payload.UserID != "" {
				a.Users[e.Payload.UserID] = true
			}
			if len(a.Accounts) >= 100000 && a.Accounts[e.Key] == 0 {
				return nil, false, errors.New("account capacity exceeded")
			}
			a.Accounts[e.Key]++
			switch e.Type {
			case "PaymentSucceeded":
				a.Success++
				if a.Payments == nil {
					a.Payments = map[string]int64{}
				}
				a.Payments[e.Payload.Currency]++
				v := a.Revenue[e.Payload.Currency]
				if v > math.MaxInt64-e.Payload.AmountMinor {
					return nil, false, errors.New("revenue overflow")
				}
				a.Revenue[e.Payload.Currency] += e.Payload.AmountMinor
			case "PaymentFailed":
				a.Failed++
			case "CheckoutStarted":
				a.Checkouts++
			case "OrderCompleted":
				a.Orders++
			case "ApplicationErrorRecorded":
				a.Errors++
				x := a.Services[e.Payload.Service]
				x[1]++
				a.Services[e.Payload.Service] = x
			case "ServiceLatencyRecorded":
				a.LatencySum += int64(e.Payload.LatencyMS)
				a.LatencyCount++
				a.Latencies[e.Payload.LatencyMS]++
				x := a.Services[e.Payload.Service]
				x[0]++
				a.Services[e.Payload.Service] = x
			}
			lag := e.Ingested.Sub(e.Time).Milliseconds()
			if lag < 0 {
				lag = 0
			}
			a.LagSum += lag
			bucket := int(lag)
			if bucket > 3600000 {
				bucket = 3600000
			}
			a.LagHistogram[bucket]++
			a.Revision = revision
			changed[k] = a
		}
	}
	for k, a := range s.Windows {
		closed := s.Watermark >= a.End
		final := s.Watermark >= a.End+lateness.Milliseconds()
		if a.Closed != closed || a.Final != final {
			a.Closed = closed
			a.Final = final
			a.Revision = revision
			changed[k] = a
		}
		if final {
			delete(s.Windows, k)
		}
	}
	out := make([]*Aggregate, 0, len(changed))
	for _, a := range changed {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Start != out[j].Start {
			return out[i].Start < out[j].Start
		}
		return out[i].Tenant < out[j].Tenant
	})
	return out, false, nil
}
func Quantile(h map[int]int64, q float64) int {
	var n int64
	keys := make([]int, 0, len(h))
	for k, v := range h {
		n += v
		keys = append(keys, k)
	}
	if n == 0 {
		return 0
	}
	sort.Ints(keys)
	target := int64(math.Ceil(float64(n) * q))
	var total int64
	for _, k := range keys {
		total += h[k]
		if total >= target {
			return k
		}
	}
	return 0
}
