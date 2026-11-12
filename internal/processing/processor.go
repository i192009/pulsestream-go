package processing

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/pulsestream/pulsestream-go/internal/broker"
	"github.com/pulsestream/pulsestream-go/internal/config"
	"github.com/pulsestream/pulsestream-go/internal/event"
	"github.com/pulsestream/pulsestream-go/internal/observe"
	"github.com/pulsestream/pulsestream-go/internal/store"
	"github.com/pulsestream/pulsestream-go/internal/windows"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

type EventRow struct {
	Pipeline  string `json:"pipeline"`
	Tenant    string `json:"tenant"`
	ID        string `json:"event_id"`
	Type      string `json:"event_type"`
	Time      int64  `json:"event_time"`
	Ingested  int64  `json:"ingested_time"`
	Partition int32  `json:"partition_id"`
	Offset    int64  `json:"offset_id"`
	Version   uint64 `json:"version"`
	Data      string `json:"data"`
	Late      uint8  `json:"late"`
}
type WindowRow struct {
	Pipeline  string `json:"pipeline"`
	Tenant    string `json:"tenant"`
	Partition int32  `json:"partition_id"`
	Start     int64  `json:"start"`
	End       int64  `json:"end"`
	Size      int64  `json:"size"`
	Step      int64  `json:"step"`
	Version   uint64 `json:"version"`
	Data      string `json:"data"`
}
type DeadLetter struct {
	ID        string    `json:"id"`
	Original  string    `json:"original_base64"`
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Category  string    `json:"category"`
	Details   string    `json:"details"`
	Stage     string    `json:"stage"`
	Attempts  int       `json:"attempts"`
	FailedAt  time.Time `json:"failed_at"`
	Tenant    string    `json:"tenant"`
}

func Process(ctx context.Context, s *store.Store, publisher *kgo.Client, records []*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}
	links := []trace.Link{}
	for _, r := range records[:min(len(records), 32)] {
		parent := broker.Extract(ctx, r)
		sc := trace.SpanContextFromContext(parent)
		if sc.IsValid() {
			links = append(links, trace.Link{SpanContext: sc})
		}
	}
	ctx, batchSpan := otel.Tracer("pulse").Start(ctx, "process.partition.batch", trace.WithLinks(links...))
	defer batchSpan.End()
	start := time.Now()
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	first := records[0]
	for i, r := range records {
		if r.Topic != first.Topic || r.Partition != first.Partition || (i > 0 && r.Offset <= records[i-1].Offset) {
			return fmt.Errorf("unordered or mixed partition batch")
		}
	}
	cp, err := s.Lock(ctx, tx, first.Topic, first.Partition)
	if err != nil {
		return err
	}
	changed := map[string]*windows.Aggregate{}
	for _, r := range records {
		if r.Offset <= cp.Offset {
			continue
		}
		if r.Partition != first.Partition || r.Topic != first.Topic {
			return fmt.Errorf("mixed partition batch")
		}
		// Kafka compacted logs may have offset gaps. Delivered records must be increasing.
		if r.Offset != cp.Offset+1 {
			return fmt.Errorf("input offset gap: checkpoint %d, next %d; restore retained source or rebuild explicitly", cp.Offset, r.Offset)
		}
		e, validation := event.Decode(r.Value)
		if validation == nil && string(r.Key) != e.PartitionKey() {
			validation = fmt.Errorf("partition key mismatch")
		}
		if validation != nil {
			category := "invalid_schema"
			if strings.Contains(validation.Error(), "unsupported schema_version") {
				category = "unsupported_version"
			}
			dlq := DeadLetter{ID: fmt.Sprintf("%s/%d/%d", r.Topic, r.Partition, r.Offset), Original: base64.StdEncoding.EncodeToString(r.Value), Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Category: category, Details: "event failed schema or routing validation", Stage: "normalize", Attempts: 1, FailedAt: time.Now().UTC(), Tenant: e.Tenant}
			b, _ := json.Marshal(dlq)
			// Acknowledgement precedes checkpoint advancement; duplicates after a crash share dlq.ID.
			if _, err = broker.ProduceBatch(ctx, publisher, []*kgo.Record{{Topic: broker.DLQ, Key: []byte(dlq.ID), Value: b}}); err != nil {
				return err
			}
			observe.Events.WithLabelValues("dlq", category).Inc()
			cp.Offset = r.Offset
			continue
		}
		tag, err := tx.Exec(ctx, `INSERT INTO dedup(pipeline,tenant,event_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, s.Config.Group, e.Tenant, e.ID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			observe.Events.WithLabelValues("process", "duplicate").Inc()
			cp.Offset = r.Offset
			continue
		}
		parent := broker.Extract(ctx, r)
		_, span := otel.Tracer("pulse").Start(ctx, "process.batch.record", trace.WithLinks(trace.Link{SpanContext: trace.SpanContextFromContext(parent)}))
		if e.Ingested.IsZero() {
			e.Ingested = r.Timestamp
		}
		a, late, err := cp.State.Apply(e, s.Config.Lateness, s.Config.OutOfOrder, r.Offset+1)
		span.End()
		if err != nil {
			return err
		}
		for _, v := range a {
			k := fmt.Sprintf("%s/%d/%d/%d", v.Tenant, v.Start, v.Size, v.Step)
			changed[k] = v
		}
		lateFlag := uint8(0)
		if late {
			lateFlag = 1
			if err = store.Enqueue(ctx, tx, "kafka:"+broker.Late, e); err != nil {
				return err
			}
		}
		b, _ := json.Marshal(e)
		row := EventRow{s.Config.Group, e.Tenant, e.ID, e.Type, e.Time.UnixMilli(), e.Ingested.UnixMilli(), r.Partition, r.Offset, uint64(r.Offset + 1), string(b), lateFlag}
		if err = store.Enqueue(ctx, tx, "events", row); err != nil {
			return err
		}
		if err = store.Enqueue(ctx, tx, "kafka:"+broker.Normalized, e); err != nil {
			return err
		}
		cp.Offset = r.Offset
	}
	for _, a := range changed {
		b, _ := json.Marshal(a)
		if err = store.Enqueue(ctx, tx, "windows", WindowRow{s.Config.Group, a.Tenant, first.Partition, a.Start, a.End, a.Size, a.Step, uint64(a.Revision), string(b)}); err != nil {
			return err
		}
	}
	diag, _ := json.Marshal(map[string]any{"offset": cp.Offset, "watermark": cp.State.Watermark, "windows": len(cp.State.Windows)})
	if err = store.Enqueue(ctx, tx, "processing_diagnostics", map[string]any{"pipeline": s.Config.Group, "partition_id": first.Partition, "version": uint64(cp.Offset + 1), "data": string(diag)}); err != nil {
		return err
	}
	if err = s.Save(ctx, tx, first.Topic, first.Partition, cp); err != nil {
		return err
	}
	// Bound the durable retry queue too. Backlog pressure rolls this transaction back.
	var queued int64
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&queued); err != nil {
		return err
	}
	if queued > 500000 {
		return fmt.Errorf("outbox backpressure: %d rows", queued)
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	observe.Duration.WithLabelValues("checkpoint").Observe(time.Since(start).Seconds())
	observe.Events.WithLabelValues("process", "committed").Add(float64(len(records)))
	return nil
}
func Run(ctx context.Context, c config.Config, s *store.Store, onReady func(bool)) error {
	defer onReady(false)
	pub, err := broker.New(c)
	if err != nil {
		return err
	}
	defer pub.Close()
	owner, _ := os.Hostname()
	opts := broker.Options(c)
	opts = append(opts, kgo.ConsumerGroup(c.Group), kgo.ConsumeTopics(broker.Raw), kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.BlockRebalanceOnPoll(), kgo.FetchMaxBytes(16<<20), kgo.FetchMaxPartitionBytes(2<<20), kgo.RebalanceTimeout(5*time.Minute), kgo.OnPartitionsLost(func(context.Context, *kgo.Client, map[string][]int32) {
		onReady(false)
		observe.Events.WithLabelValues("rebalance", "lost").Inc()
	}), kgo.OnPartitionsAssigned(func(ctx context.Context, _ *kgo.Client, p map[string][]int32) {
		onReady(true)
		observe.Events.WithLabelValues("rebalance", "assigned").Inc()
		slog.Info("partition assignment", "partitions", p)
	}), kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, p map[string][]int32) {
		onReady(false)
		observe.Events.WithLabelValues("rebalance", "revoked").Inc()
		slog.Info("partition revocation", "partitions", p)
	}))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer cl.CloseAllowingRebalance()
	observe.Gauge.WithLabelValues("queue_capacity").Set(float64(c.Poll))
	observe.Gauge.WithLabelValues("worker_capacity").Set(float64(c.Partitions))
	for ctx.Err() == nil {
		fetch := cl.PollRecords(ctx, c.Poll)
		if ctx.Err() != nil {
			break
		}
		if errs := fetch.Errors(); len(errs) > 0 {
			slog.Error("poll failed", "error", errs[0].Err)
			cl.AllowRebalance()
			if !Wait(ctx, time.Second) {
				break
			}
			continue
		}
		observe.Gauge.WithLabelValues("queue_depth").Set(float64(fetch.NumRecords()))
		var wg sync.WaitGroup
		sem := make(chan struct{}, c.Partitions)
		fetch.EachPartition(func(p kgo.FetchTopicPartition) {
			wg.Add(1)
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				wg.Done()
				return
			}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				observe.Gauge.WithLabelValues("workers_active").Inc()
				defer observe.Gauge.WithLabelValues("workers_active").Dec()
				var committed *kgo.Record
				for start := 0; start < len(p.Records); start += c.MaxBatch {
					end := min(start+c.MaxBatch, len(p.Records))
					batch := p.Records[start:end]
					for ctx.Err() == nil {
						attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
						e := Process(attempt, s, pub, batch)
						cancel()
						if e == nil {
							committed = batch[len(batch)-1]
							break
						}
						slog.Error("partition retry", "partition", p.Partition, "offset", batch[0].Offset, "error", e)
						observe.Events.WithLabelValues("process", "retry").Inc()
						t := time.Now()
						if !Wait(ctx, time.Second) {
							return
						}
						observe.Duration.WithLabelValues("backpressure").Observe(time.Since(t).Seconds())
					}
					if ctx.Err() != nil {
						return
					}
				}
				if committed != nil {
					if err := cl.CommitRecords(ctx, committed); err != nil {
						slog.Warn("offset commit failed; checkpoint remains authoritative", "error", err)
					}
					_, err := s.Pool.Exec(ctx, `INSERT INTO diagnostics(pipeline,partition_id,owner,high_watermark,processed_offset) VALUES($1,$2,$3,$4,$5) ON CONFLICT(pipeline,partition_id) DO UPDATE SET owner=EXCLUDED.owner,high_watermark=EXCLUDED.high_watermark,processed_offset=EXCLUDED.processed_offset,updated_at=now()`, c.Group, p.Partition, owner, p.HighWatermark, committed.Offset)
					if err != nil {
						slog.Warn("diagnostics write", "error", err)
					}
				}
			}()
		})
		wg.Wait()
		observe.Gauge.WithLabelValues("queue_depth").Set(0)
		cl.AllowRebalance()
	}
	return ctx.Err()
}
func Wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
