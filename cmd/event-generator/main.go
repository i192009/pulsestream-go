package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/pulsestream/pulsestream-go/internal/event"
	"github.com/pulsestream/pulsestream-go/internal/synthetic"
	"github.com/pulsestream/pulsestream-go/internal/windows"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type batch struct {
	Events []event.Event `json:"events"`
}

func main() {
	url := flag.String("url", "http://localhost:18080/v1/events", "ingestion endpoint")
	key := flag.String("key", "demo-key", "development API key")
	tenant := flag.String("tenants", "tenant-demo", "comma separated tenants (keys must authorize each)")
	count := flag.Int64("count", 100000, "finite event count")
	rate := flag.Int("rate", 1000, "target events per second")
	workers := flag.Int("workers", 4, "bounded HTTP concurrency")
	seed := flag.Int64("seed", 42, "deterministic random seed and event IDs")
	padding := flag.Int("payload-bytes", 0, "extra payload bytes <=16384")
	late := flag.Int("late-percent", 2, "late arrival percentage")
	anomaly := flag.Bool("anomaly", false, "payment failures and latency spike")
	duration := flag.Duration("duration", 0, "optional deadline")
	output := flag.String("report", "", "write measured JSON report")
	base := flag.String("base-time", "", "optional RFC3339 deterministic event epoch")
	tenantKeyConfig := flag.String("tenant-keys", "", "tenant:key comma-separated credentials for multi-tenant workloads")
	partitionKeys := flag.Int("partition-keys", 1000, "uniform business key cardinality")
	typeConfig := flag.String("event-types", "", "comma-separated event mix; repeat entries to weight them")
	spikeAfter := flag.Duration("spike-after", 0, "start a traffic spike after this duration")
	spikeMultiplier := flag.Int("spike-multiplier", 1, "rate multiplier after spike-after")
	flag.Parse()
	if *count < 1 || *rate < 1 || *workers < 1 || *workers > 64 || *padding > 16384 {
		fmt.Fprintln(os.Stderr, "invalid workload bounds")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if *duration > 0 {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, *duration)
		defer stop()
	}
	epoch := time.Now().Add(-time.Minute)
	if *base != "" {
		var err error
		epoch, err = time.Parse(time.RFC3339, *base)
		if err != nil {
			panic(err)
		}
	}
	g := synthetic.New(*seed, epoch)
	g.Step = time.Second / time.Duration(*rate)
	g.Keys = *partitionKeys
	if *typeConfig != "" {
		g.Types = strings.Split(*typeConfig, ",")
		for _, kind := range g.Types {
			valid := false
			for _, supported := range event.Types {
				if kind == supported {
					valid = true
				}
			}
			if !valid {
				panic("unsupported event type mix")
			}
		}
	}
	g.Padding = *padding
	g.Anomaly = *anomaly
	g.LatePercent = *late
	tenants := strings.Split(*tenant, ",")
	tenantKeys := map[string]string{}
	for _, entry := range strings.Split(*tenantKeyConfig, ",") {
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, ":", 2)
		if len(parts) != 2 {
			panic("invalid tenant-keys")
		}
		tenantKeys[parts[0]] = parts[1]
	}
	for _, t := range tenants {
		if tenantKeys[t] == "" {
			if len(tenants) > 1 {
				panic("multi-tenant generation requires tenant-keys for every tenant")
			}
			tenantKeys[t] = *key
		}
	}
	queue := make(chan batch, *workers*2)
	var wg sync.WaitGroup
	var acked, failed, requests atomic.Int64
	hist := map[int]int64{}
	var mu sync.Mutex
	client := &http.Client{Timeout: 40 * time.Second, Transport: &http.Transport{MaxIdleConns: *workers, MaxIdleConnsPerHost: *workers}}
	start := time.Now()
	for n := 0; n < *workers; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range queue {
				body, _ := json.Marshal(b)
				begin := time.Now()
				req, err := http.NewRequestWithContext(ctx, "POST", *url, bytes.NewReader(body))
				if err == nil {
					req.Header.Set("Authorization", "Bearer "+tenantKeys[b.Events[0].Tenant])
					req.Header.Set("Content-Type", "application/json")
					var resp *http.Response
					resp, err = client.Do(req)
					if err == nil {
						var result struct {
							Acknowledged int `json:"acknowledged"`
						}
						_ = json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&result)
						resp.Body.Close()
						acked.Add(int64(result.Acknowledged))
						failed.Add(int64(len(b.Events) - result.Acknowledged))
						if resp.StatusCode != 202 {
							fmt.Fprintf(os.Stderr, "HTTP %d (%d acknowledged)\n", resp.StatusCode, result.Acknowledged)
						}
					}
				}
				if err != nil {
					failed.Add(int64(len(b.Events)))
				}
				requests.Add(1)
				ms := int(time.Since(begin).Milliseconds())
				if ms > 60000 {
					ms = 60000
				}
				mu.Lock()
				hist[ms]++
				mu.Unlock()
			}
		}()
	}
	nextPublish := start
	var generated int64
loop:
	for generated < *count {
		n := min(int64(100), *count-generated)
		events := make([]event.Event, n)
		for i := range events {
			events[i] = g.Event(generated+int64(i), tenants[(generated/100)%int64(len(tenants))])
		}
		target := nextPublish
		effectiveRate := *rate
		if *spikeAfter > 0 && time.Since(start) >= *spikeAfter {
			effectiveRate *= max(1, *spikeMultiplier)
		}
		nextPublish = nextPublish.Add(time.Duration(float64(n) / float64(effectiveRate) * float64(time.Second)))
		if delay := time.Until(target); delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				break loop
			case <-timer.C:
			}
		}
		select {
		case queue <- batch{events}:
			generated += n
		case <-ctx.Done():
			break loop
		}
	}
	close(queue)
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	report := map[string]any{"target_eps": *rate, "generated": generated, "acknowledged": acked.Load(), "failed": failed.Load(), "requests": requests.Load(), "seconds": elapsed, "acknowledged_eps": float64(acked.Load()) / elapsed, "http_batch_latency_p50_ms": windows.Quantile(hist, .5), "http_batch_latency_p95_ms": windows.Quantile(hist, .95), "http_batch_latency_p99_ms": windows.Quantile(hist, .99), "seed": *seed, "base_time": epoch.UTC().Format(time.RFC3339Nano), "scope": "ingestion acknowledgements only; not durable processing throughput"}
	b, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(b))
	if *output != "" {
		if err := os.WriteFile(*output, b, 0644); err != nil {
			panic(err)
		}
	}
	if failed.Load() > 0 || generated < *count {
		os.Exit(1)
	}
}
