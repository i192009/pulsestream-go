package api

import (
	"context"
	"github.com/pulsestream/pulsestream-go/internal/config"
	"github.com/pulsestream/pulsestream-go/internal/event"
	"github.com/pulsestream/pulsestream-go/internal/synthetic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAdmissionAndAuthorization(t *testing.T) {
	s := &Server{C: config.Config{MaxBatch: 100, Keys: map[string]config.Identity{"key": {Tenant: "tenant-demo", Role: "admin"}}}, Pending: make(chan struct{}, 1)}
	e := synthetic.New(1, time.Now()).Event(1, "tenant-demo")
	ctx := context.Background()
	s.Pending <- struct{}{}
	if _, err := s.Publish(ctx, config.Identity{Tenant: "tenant-demo", Role: "admin"}, []event.Event{e}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, config.Identity{Tenant: "other", Role: "admin"}, []event.Event{e}); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	if _, err := s.Publish(ctx, config.Identity{Tenant: "tenant-demo", Role: "reader"}, []event.Event{e}); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	if _, ok := s.Authenticate("Bearer wrong"); ok {
		t.Fatal("unauthorized")
	}
	if id, ok := s.Authenticate("Bearer key"); !ok || id.Tenant != "tenant-demo" {
		t.Fatal("valid key rejected")
	}
}
func TestRanges(t *testing.T) {
	for _, q := range []string{"?from=bad", "?from=2026-01-01T00:00:00Z&to=2026-01-03T00:00:00Z", "?from=2026-01-02T00:00:00Z&to=2026-01-01T00:00:00Z"} {
		r := httptest.NewRequest("GET", "/v1/metrics"+q, nil)
		if _, _, err := timeRange(r); err == nil {
			t.Fatal("invalid range accepted")
		}
	}
}
