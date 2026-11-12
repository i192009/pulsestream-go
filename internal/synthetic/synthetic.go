package synthetic

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/pulsestream/pulsestream-go/internal/event"
	"math/rand"
	"strings"
	"time"
)

type Generator struct {
	R           *rand.Rand
	Seed        int64
	Base        time.Time
	Padding     int
	Anomaly     bool
	LatePercent int
	Keys        int
	Step        time.Duration
	Types       []string
}

func New(seed int64, base time.Time) *Generator {
	return &Generator{R: rand.New(rand.NewSource(seed)), Seed: seed, Base: base, Keys: 1000, Step: time.Millisecond}
}
func (g *Generator) Event(i int64, tenant string) event.Event {
	r := g.R
	k := r.Intn(max(g.Keys, 1))
	user := fmt.Sprintf("user-%d", k)
	order := fmt.Sprintf("order-%d", i/4)
	types := []string{"PageViewed", "ProductViewed", "CartUpdated", "CheckoutStarted", "PaymentSucceeded", "PaymentFailed", "OrderCompleted", "ServiceLatencyRecorded", "ApplicationErrorRecorded", "UserLoggedIn", "UserRegistered", "PaymentAttempted", "OrderCreated"}
	if len(g.Types) > 0 {
		types = g.Types
	}
	kind := types[r.Intn(len(types))]
	if g.Anomaly && i%2 == 0 {
		kind = "PaymentFailed"
	}
	p := event.Payload{UserID: user}
	switch kind {
	case "PageViewed":
		p.SessionID = user
		p.Page = "/products"
	case "ProductViewed", "CartUpdated":
		p.SessionID = user
		p.ProductID = fmt.Sprintf("product-%d", r.Intn(100))
		p.Quantity = 1
	case "CheckoutStarted", "OrderCreated", "OrderCompleted", "PaymentAttempted", "PaymentSucceeded", "PaymentFailed":
		p.OrderID = order
		p.AmountMinor = int64(100 + r.Intn(20000))
		p.Currency = "USD"
		if kind == "PaymentFailed" {
			p.FailureReason = "synthetic_decline"
		}
	case "ServiceLatencyRecorded":
		p.Service = "checkout"
		p.LatencyMS = 10 + r.Intn(300)
		if g.Anomaly {
			p.LatencyMS += 2000
		}
	case "ApplicationErrorRecorded":
		p.Service = "checkout"
		p.ErrorCode = "SYNTHETIC_500"
	}
	if g.Padding > 0 {
		p.Padding = strings.Repeat("x", min(g.Padding, 16384))
	}
	t := g.Base.Add(time.Duration(i) * g.Step)
	if r.Intn(100) < g.LatePercent {
		t = t.Add(-10 * time.Minute)
	}
	id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("%d/%s/%d", g.Seed, tenant, i)))
	return event.Event{ID: id.String(), Tenant: tenant, Type: kind, Version: 1, Key: fmt.Sprintf("account-%d", k), Producer: "synthetic-generator", Time: t.UTC(), Correlation: fmt.Sprintf("seed-%d", g.Seed), Payload: p}
}
