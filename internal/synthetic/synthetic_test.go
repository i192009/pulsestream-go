package synthetic

import (
	"reflect"
	"testing"
	"time"
)

func TestDeterministicWorkloadAndClock(t *testing.T) {
	base := time.Unix(1800000000, 0)
	a, b := New(42, base), New(42, base)
	a.Types = []string{"PaymentFailed"}
	b.Types = a.Types
	for i := int64(0); i < 100000; i += 1000 {
		x, y := a.Event(i, "tenant-demo"), b.Event(i, "tenant-demo")
		if !reflect.DeepEqual(x, y) {
			t.Fatal("seed drift")
		}
		if x.Type != "PaymentFailed" || !x.Time.Equal(base.Add(time.Duration(i)*time.Millisecond)) {
			t.Fatal("mix or event-time progression")
		}
	}
}
