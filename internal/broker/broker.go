package broker

import (
	"context"
	"encoding/json"
	"github.com/pulsestream/pulsestream-go/internal/config"
	"github.com/pulsestream/pulsestream-go/internal/event"
	"github.com/pulsestream/pulsestream-go/internal/observe"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"time"
)

const Raw = "pulsestream.events.raw.v1"
const DLQ = "pulsestream.events.dlq.v1"
const Late = "pulsestream.events.late.v1"
const Normalized = "pulsestream.events.normalized.v1"
const Updates = "pulsestream.analytics.updates.v1"
const Alerts = "pulsestream.alerts.v1"

func Options(c config.Config) []kgo.Opt {
	return []kgo.Opt{kgo.SeedBrokers(c.Brokers...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerBatchCompression(kgo.ZstdCompression()), kgo.ProducerLinger(5 * time.Millisecond), kgo.RecordDeliveryTimeout(30 * time.Second), kgo.MaxBufferedRecords(c.MaxBatch * 8), kgo.MaxBufferedBytes(32 << 20), kgo.RecordRetries(10), kgo.RequestTimeoutOverhead(5 * time.Second)}
}
func New(c config.Config) (*kgo.Client, error) { return kgo.NewClient(Options(c)...) }

// ProduceBatch bounds the caller's wait even when an idempotent producer must
// retain an ambiguous in-flight request. Callbacks can finish later; the buffered
// result channel is bounded to this batch and cannot block after a timeout.
// A timeout means "unknown", never "definitely absent from Kafka".
func ProduceBatch(ctx context.Context, cl *kgo.Client, records []*kgo.Record) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	results := make(chan error, len(records))
	for _, r := range records {
		cl.Produce(ctx, r, func(_ *kgo.Record, err error) { results <- err })
	}
	acked := 0
	var first error
	for range records {
		select {
		case err := <-results:
			if err == nil {
				acked++
			} else if first == nil {
				first = err
			}
		case <-ctx.Done():
			return acked, ctx.Err()
		}
	}
	return acked, first
}
func Init(ctx context.Context, c config.Config) error {
	cl, err := New(c)
	if err != nil {
		return err
	}
	defer cl.Close()
	a := kadm.NewClient(cl)
	topics := []string{Raw, DLQ, Late, Normalized, Updates, Alerts}
	_, err = a.CreateTopics(ctx, int32(c.Partitions), 1, map[string]*string{}, topics...)
	if err != nil {
		return err
	}
	// Verify every topic exists with the configured partition count, including pre-provisioned topics.
	meta, err := a.ListTopics(ctx, topics...)
	if err != nil {
		return err
	}
	for _, t := range topics {
		d := meta[t]
		if d.Err != nil {
			return d.Err
		}
		if len(d.Partitions) != c.Partitions {
			return &topicError{t}
		}
	}
	return nil
}

type topicError struct{ topic string }

func (e *topicError) Error() string { return "unexpected partition count: " + e.topic }
func Publish(ctx context.Context, cl *kgo.Client, e event.Event) error {
	ctx, span := otel.Tracer("pulse").Start(ctx, "kafka.publish")
	defer span.End()
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	r := &kgo.Record{Topic: Raw, Key: []byte(e.PartitionKey()), Value: b, Timestamp: time.Now().UTC()}
	Inject(ctx, r)
	start := time.Now()
	_, err = ProduceBatch(ctx, cl, []*kgo.Record{r})
	observe.Duration.WithLabelValues("publish").Observe(time.Since(start).Seconds())
	if err != nil {
		observe.Events.WithLabelValues("publish", "failed").Inc()
	} else {
		observe.Events.WithLabelValues("publish", "acked").Inc()
	}
	return err
}
func Inject(ctx context.Context, r *kgo.Record) {
	c := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, c)
	for k, v := range c {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
}
func Extract(ctx context.Context, r *kgo.Record) context.Context {
	c := propagation.MapCarrier{}
	for _, h := range r.Headers {
		c[h.Key] = string(h.Value)
	}
	return otel.GetTextMapPropagator().Extract(ctx, c)
}
