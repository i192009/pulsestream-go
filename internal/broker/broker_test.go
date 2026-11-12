package broker

import (
	"context"
	"github.com/pulsestream/pulsestream-go/internal/config"
	"github.com/pulsestream/pulsestream-go/internal/synthetic"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"
	"net"
	"testing"
	"time"
)

func TestBatchDeadlineAndLateCallbackCleanup(t *testing.T) {
	defer goleak.VerifyNone(t)
	cl, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
	defer cancel()
	began := time.Now()
	acked, err := ProduceBatch(ctx, cl, []*kgo.Record{{Topic: Raw, Value: []byte("synthetic")}})
	if err == nil || acked != 0 || time.Since(began) > time.Second {
		t.Fatal("caller deadline violated", acked, err, time.Since(began))
	}
}

// A real broker test is in E2E. This hook observes the buffered Kafka timestamp
// before network delivery, guarding against delivery-timeout rejection of late events.
type recordHook struct{ timestamp chan time.Time }

func (h *recordHook) OnProduceRecordBuffered(r *kgo.Record) { h.timestamp <- r.Timestamp }
func TestLateEnvelopeUsesIngestionKafkaTimestamp(t *testing.T) {
	defer goleak.VerifyNone(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	h := &recordHook{make(chan time.Time, 1)}
	opts := Options(config.Config{Brokers: []string{address}, MaxBatch: 1})
	opts = append(opts, kgo.WithHooks(h))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	e := synthetic.New(1, time.Now().Add(-10*time.Minute)).Event(1, "tenant-demo")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = Publish(ctx, cl, e)
	select {
	case ts := <-h.timestamp:
		if time.Since(ts) > time.Second {
			t.Fatal("Kafka timestamp is stale event time", ts)
		}
	default:
		t.Fatal("record not buffered")
	}
}
