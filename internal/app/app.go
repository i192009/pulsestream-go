package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/pulsestream/pulsestream-go/internal/anomaly"
	"github.com/pulsestream/pulsestream-go/internal/api"
	"github.com/pulsestream/pulsestream-go/internal/broker"
	"github.com/pulsestream/pulsestream-go/internal/config"
	"github.com/pulsestream/pulsestream-go/internal/observe"
	"github.com/pulsestream/pulsestream-go/internal/processing"
	"github.com/pulsestream/pulsestream-go/internal/sink"
	"github.com/pulsestream/pulsestream-go/internal/store"
	"golang.org/x/net/netutil"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func Run(role string) error {
	if role == "healthcheck" {
		cl := http.Client{Timeout: 2 * time.Second}
		r, err := cl.Get("http://127.0.0.1:8080/readyz")
		if err != nil {
			return err
		}
		defer r.Body.Close()
		if r.StatusCode != 200 {
			return fmt.Errorf("not ready: %d", r.StatusCode)
		}
		return nil
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	c := config.Load(role)
	shutdown := observe.Init(ctx, role)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(ctx)
	}()
	s, err := store.Open(ctx, c)
	if err != nil {
		return err
	}
	defer s.Pool.Close()
	if err = s.Migrate(ctx); err != nil {
		return err
	}
	if role == "init" {
		if err = broker.Init(ctx, c); err != nil {
			return err
		}
		if err = sink.New(c).Init(ctx); err != nil {
			return err
		}
		return anomaly.Seed(ctx, s)
	}
	metricsDone := make(chan struct{})
	go pollMetrics(ctx, c, s, metricsDone)
	defer func() { cancel(); <-metricsDone }()
	server := api.New(c, s)
	defer server.Close()
	if role == "ingestion-gateway" {
		server.Publisher, err = broker.New(c)
		if err != nil {
			return err
		}
	}
	h := &http.Server{Addr: c.HTTP, Handler: server.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 40 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	errs := make(chan error, 3)
	listener, err := net.Listen("tcp", c.HTTP)
	if err != nil {
		return err
	}
	go func() { errs <- h.Serve(netutil.LimitListener(listener, 128)) }()
	switch role {
	case "ingestion-gateway", "analytics-api":
		go func() { errs <- server.GRPC(ctx) }()
	case "stream-processor":
		go func() { errs <- processing.Run(ctx, c, s, server.ConsumerReady.Store) }()
	case "sink-dispatcher":
		go func() { errs <- sink.Run(ctx, s, c) }()
	case "alert-service":
		go func() { errs <- anomaly.Run(ctx, c, s) }()
	case "websocket-gateway":
	default:
		return fmt.Errorf("unknown role %q", role)
	}
	slog.Info("service started", "http", c.HTTP, "role", role)
	select {
	case err = <-errs:
		cancel()
	case <-ctx.Done():
		err = nil
	}
	stopCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	_ = h.Shutdown(stopCtx)
	// Wait for the worker to stop before closing its PostgreSQL pool.
	if role == "stream-processor" || role == "sink-dispatcher" || role == "alert-service" {
		select {
		case <-errs:
		case <-stopCtx.Done():
		}
	}
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
