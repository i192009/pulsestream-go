package windows_test

import (
	"encoding/json"
	"github.com/pulsestream/pulsestream-go/internal/event"
	"github.com/pulsestream/pulsestream-go/internal/synthetic"
	"github.com/pulsestream/pulsestream-go/internal/windows"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestMembershipProperty(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	for i := 0; i < 10000; i++ {
		ts := time.UnixMilli(1700000000000 + r.Int63n(86400000))
		for _, s := range windows.Specs {
			starts := windows.Starts(ts, s)
			if len(starts) != int(s.Size/s.Step) {
				t.Fatal("wrong membership count")
			}
			for _, start := range starts {
				if start%s.Step.Milliseconds() != 0 || ts.UnixMilli() < start || ts.UnixMilli() >= start+s.Size.Milliseconds() {
					t.Fatal("invalid membership")
				}
			}
		}
	}
}
func TestReferenceAndRestore(t *testing.T) {
	g := synthetic.New(123, time.Unix(1800000000, 0))
	s := windows.New()
	expected := map[string]int64{}
	for i := int64(0); i < 3000; i++ {
		e := g.Event(i, "tenant-demo")
		e.Ingested = e.Time.Add(time.Second)
		_, late, err := s.Apply(e, 2*time.Minute, 10*time.Second, i+1)
		if err != nil || late {
			t.Fatal(err, late)
		}
		for _, spec := range windows.Specs {
			for _, start := range windows.Starts(e.Time, spec) {
				expected[windows.Key(e.Tenant, start, spec)]++
			}
		}
		if i == 1500 {
			b, _ := json.Marshal(s)
			restored := windows.New()
			if err = json.Unmarshal(b, restored); err != nil {
				t.Fatal(err)
			}
			s = restored
		}
	}
	for k, n := range expected {
		if s.Windows[k] == nil || s.Windows[k].Count != n {
			t.Fatalf("reference mismatch %s want %d", k, n)
		}
	}
	b, _ := json.Marshal(s)
	var copy windows.State
	_ = json.Unmarshal(b, &copy)
	if !reflect.DeepEqual(s, &copy) {
		t.Fatal("restore changed state")
	}
}
func TestLateCorrectionsAndFinalization(t *testing.T) {
	base := time.Unix(1800000000, 0).Truncate(time.Minute)
	g := synthetic.New(4, base)
	s := windows.New()
	apply := func(dt time.Duration, rev int64) ([]*windows.Aggregate, bool) {
		e := g.Event(rev, "tenant-demo")
		e.Time = base.Add(dt)
		a, l, err := s.Apply(e, 2*time.Minute, 10*time.Second, rev)
		if err != nil {
			t.Fatal(err)
		}
		return a, l
	}
	apply(0, 1)
	apply(90*time.Second, 2)
	key := windows.Key("tenant-demo", base.UnixMilli(), windows.Specs[0])
	if !s.Windows[key].Closed || s.Windows[key].Final {
		t.Fatal("window closing semantics")
	}
	apply(30*time.Second, 3)
	if s.Windows[key].Count != 2 || s.Windows[key].Revision != 3 {
		t.Fatal("late correction lost")
	}
	a, _ := apply(4*time.Minute, 4)
	found := false
	for _, w := range a {
		if w.Start == base.UnixMilli() && w.Size == 60000 && w.Final {
			found = true
		}
	}
	if !found || s.Windows[key] != nil {
		t.Fatal("final snapshot or eviction missing")
	}
	_, late := apply(20*time.Second, 5)
	if !late {
		t.Fatal("too-late event accepted")
	}
}
func TestMoneyAndQuantile(t *testing.T) {
	s := windows.New()
	base := time.Unix(1800000000, 0)
	for i := int64(1); i <= 100; i++ {
		e := event.Event{Tenant: "t", Type: "PaymentSucceeded", Time: base, Ingested: base, Payload: event.Payload{Currency: "USD", AmountMinor: 999, UserID: "u"}}
		if _, _, err := s.Apply(e, time.Minute, 0, i); err != nil {
			t.Fatal(err)
		}
	}
	for _, w := range s.Windows {
		if w.Revenue["USD"] != 99900 || len(w.Users) != 1 {
			t.Fatal("incorrect exact metric")
		}
	}
	if windows.Quantile(map[int]int64{1: 94, 100: 6}, .95) != 100 {
		t.Fatal("p95 wrong")
	}
}
func FuzzMembership(f *testing.F) {
	f.Add(int64(1800000000000))
	f.Fuzz(func(t *testing.T, ms int64) {
		if ms < 0 || ms > 4000000000000 {
			return
		}
		for _, spec := range windows.Specs {
			for _, s := range windows.Starts(time.UnixMilli(ms), spec) {
				if !(s <= ms && ms < s+spec.Size.Milliseconds()) {
					t.Fatal("membership")
				}
			}
		}
	})
}
func BenchmarkWindowEngine(b *testing.B) {
	g := synthetic.New(42, time.Unix(1800000000, 0))
	events := make([]event.Event, 1000)
	for i := range events {
		events[i] = g.Event(int64(i), "tenant-demo")
	}
	b.ReportAllocs()
	s := windows.New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := events[i%len(events)]
		e.Time = e.Time.Add(time.Duration(i/1000) * time.Minute)
		if _, _, err := s.Apply(e, 2*time.Minute, 10*time.Second, int64(i)); err != nil {
			b.Fatal(err)
		}
	}
}
