package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/pulsestream/pulsestream-go/internal/analytics"
	"github.com/pulsestream/pulsestream-go/internal/broker"
	"github.com/pulsestream/pulsestream-go/internal/config"
	"github.com/pulsestream/pulsestream-go/internal/processing"
	"github.com/pulsestream/pulsestream-go/internal/sink"
	"github.com/pulsestream/pulsestream-go/internal/store"
	"github.com/pulsestream/pulsestream-go/internal/synthetic"
	"github.com/twmb/franz-go/pkg/kgo"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func setup(t *testing.T) (context.Context, config.Config, *store.Store, *kgo.Client) {
	t.Helper()
	if os.Getenv("PULSE_INTEGRATION") != "1" {
		t.Skip("requires docker compose dependencies and PULSE_INTEGRATION=1")
	}
	t.Setenv("PULSE_KEYS", "test-key:tenant-test:admin")
	c := config.Load("test")
	c.Group = "test-" + uuid.NewString()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	s, err := store.Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Pool.Close)
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = broker.Init(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err = sink.New(c).Init(ctx); err != nil {
		t.Fatal(err)
	}
	pub, err := broker.New(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.Close)
	t.Cleanup(func() {
		clean, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, table := range []string{"checkpoints", "dedup", "diagnostics"} {
			_, _ = s.Pool.Exec(clean, "DELETE FROM "+table+" WHERE pipeline=$1", c.Group)
		}
	})
	return ctx, c, s, pub
}
func records(n int, partition int32) []*kgo.Record {
	g := synthetic.New(123, time.Now().Add(-time.Minute).Truncate(time.Minute))
	out := []*kgo.Record{}
	for i := 0; i < n; i++ {
		e := g.Event(int64(i), "tenant-test")
		e.Ingested = e.Time.Add(time.Second)
		b, _ := json.Marshal(e)
		out = append(out, &kgo.Record{Topic: broker.Raw, Partition: partition, Offset: int64(i), Key: []byte(e.PartitionKey()), Value: b, Timestamp: e.Time})
	}
	return out
}
func offset(t *testing.T, ctx context.Context, s *store.Store, p int32) int64 {
	t.Helper()
	var v int64
	if err := s.Pool.QueryRow(ctx, `SELECT last_offset FROM checkpoints WHERE pipeline=$1 AND partition_id=$2`, s.Config.Group, p).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}
func TestDurableRecoveryDuplicateAndSinkConvergence(t *testing.T) {
	ctx, c, s, pub := setup(t)
	rs := records(100, 0)
	if err := processing.Process(ctx, s, pub, rs[:50]); err != nil {
		t.Fatal(err)
	}
	if got := offset(t, ctx, s, 0); got != 49 {
		t.Fatal(got)
	}
	// Model a crash after PostgreSQL commit, before Kafka commit, followed by reassignment.
	reopened, err := store.Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Pool.Close()
	if err = processing.Process(ctx, reopened, pub, rs); err != nil {
		t.Fatal(err)
	}
	if offset(t, ctx, s, 0) != 99 {
		t.Fatal("recovery checkpoint")
	}
	// Same IDs at new Kafka offsets must not change business aggregates.
	duplicates := make([]*kgo.Record, len(rs))
	for i, r := range rs {
		copy := *r
		copy.Offset += 100
		duplicates[i] = &copy
	}
	if err = processing.Process(ctx, s, pub, duplicates); err != nil {
		t.Fatal(err)
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := s.Lock(ctx, tx, broker.Raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback(ctx)
	var total int64
	for _, w := range cp.State.Windows {
		if w.Size == 60000 {
			total += w.Count
		}
	}
	if total != 100 {
		t.Fatalf("dedup inflated totals: %d", total)
	}
	ch := sink.New(c)
	for i := 0; i < 100; i++ {
		if err = sink.Dispatch(ctx, s, ch, pub, 1000); err != nil {
			t.Fatal(err)
		}
		var n int
		if err = s.Pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}
	from, to := analytics.Range(1)
	ws, err := analytics.Query(ctx, ch, "tenant-test", c.Group, from, to, 60000, 60000)
	if err != nil {
		t.Fatal(err)
	}
	var count int64
	for _, w := range ws {
		count += w.Count
	}
	if count != 100 {
		t.Fatal("sink disagrees", count)
	}
	// Reinsert an acknowledged shard snapshot to model an ambiguous ClickHouse timeout.
	for _, a := range cp.State.Windows {
		if a.Size != 60000 {
			continue
		}
		data, _ := json.Marshal(a)
		row := processing.WindowRow{Pipeline: c.Group, Tenant: a.Tenant, Partition: 0, Start: a.Start, End: a.End, Size: a.Size, Step: a.Step, Version: uint64(a.Revision), Data: string(data)}
		b, _ := json.Marshal(row)
		for i := 0; i < 2; i++ {
			if _, err = ch.Do(ctx, "INSERT INTO windows FORMAT JSONEachRow", nil, b); err != nil {
				t.Fatal(err)
			}
		}
	}
	ws, err = analytics.Query(ctx, ch, "tenant-test", c.Group, from, to, 60000, 60000)
	if err != nil {
		t.Fatal(err)
	}
	count = 0
	for _, w := range ws {
		count += w.Count
	}
	if count != 100 {
		t.Fatal("physical duplicate inflated logical query", count)
	}
}
func TestUnacknowledgedDLQNeverAdvancesCheckpoint(t *testing.T) {
	ctx, c, s, _ := setup(t)
	rs := records(2, 0)
	if err := processing.Process(ctx, s, nil, rs[:1]); err != nil {
		t.Fatal(err)
	}
	rs[1].Value = []byte(`{"tenant_id":"tenant-test","schema_version":99}`)
	bad, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"), kgo.RecordDeliveryTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Close()
	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if err = processing.Process(short, s, bad, rs[1:]); err == nil {
		t.Fatal("unacknowledged poison accepted")
	}
	if offset(t, ctx, s, 0) != 0 {
		t.Fatal("offset advanced past unacknowledged DLQ")
	}
	_ = c
}
func TestInvalidCheckpointAndGapsFailClosed(t *testing.T) {
	ctx, _, s, pub := setup(t)
	rs := records(3, 0)
	if err := processing.Process(ctx, s, pub, rs[:1]); err != nil {
		t.Fatal(err)
	}
	if err := processing.Process(ctx, s, pub, rs[2:]); err == nil {
		t.Fatal("offset gap accepted")
	}
	if offset(t, ctx, s, 0) != 0 {
		t.Fatal("gap advanced checkpoint")
	}
	_, err := s.Pool.Exec(ctx, `UPDATE checkpoints SET checksum='corrupt' WHERE pipeline=$1`, s.Config.Group)
	if err != nil {
		t.Fatal(err)
	}
	if err = processing.Process(ctx, s, pub, rs[1:2]); err == nil {
		t.Fatal("corruption ignored")
	}
}
func TestConcurrentPartitionOwnership(t *testing.T) {
	ctx, _, s, pub := setup(t)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for partition := int32(0); partition < 8; partition++ {
		wg.Add(1)
		go func(p int32) {
			defer wg.Done()
			rs := records(50, p)
			for i, r := range rs {
				var e map[string]any
				_ = json.Unmarshal(r.Value, &e)
				e["event_id"] = uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("%d/%d", p, i))).String()
				r.Value, _ = json.Marshal(e)
			}
			errs <- processing.Process(ctx, s, pub, rs)
		}(partition)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for p := int32(0); p < 8; p++ {
		if offset(t, ctx, s, p) != 49 {
			t.Fatal("partition prefix")
		}
	}
}

func TestRandomizedDedupReference(t *testing.T) {
	ctx, _, s, pub := setup(t)
	source := records(100, 0)
	r := rand.New(rand.NewSource(987))
	batch := make([]*kgo.Record, 500)
	unique := map[string]bool{}
	for i := range batch {
		original := source[r.Intn(len(source))]
		copy := *original
		copy.Offset = int64(i)
		batch[i] = &copy
		var e struct {
			ID string `json:"event_id"`
		}
		if err := json.Unmarshal(copy.Value, &e); err != nil {
			t.Fatal(err)
		}
		unique[e.ID] = true
	}
	if err := processing.Process(ctx, s, pub, batch); err != nil {
		t.Fatal(err)
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	cp, err := s.Lock(ctx, tx, broker.Raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, w := range cp.State.Windows {
		if w.Size == 60000 {
			total += w.Count
		}
		if w.Count < 0 {
			t.Fatal("negative count")
		}
	}
	if total != int64(len(unique)) || cp.Offset != 499 {
		t.Fatal(total, len(unique), cp.Offset)
	}
}
