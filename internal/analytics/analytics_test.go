package analytics

import (
	"github.com/pulsestream/pulsestream-go/internal/windows"
	"math"
	"testing"
)

func TestRevenueMergeRejectsOverflow(t *testing.T) {
	a := &windows.Aggregate{Revenue: map[string]int64{"USD": math.MaxInt64 - 1}}
	b := &windows.Aggregate{Revenue: map[string]int64{"USD": 2}}
	if Merge(a, b) == nil {
		t.Fatal("overflow would corrupt financial totals")
	}
	if a.Revenue["USD"] != math.MaxInt64-1 {
		t.Fatal("overflow check changed total")
	}
}
