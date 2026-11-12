package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pulsestream/pulsestream-go/internal/config"
	"github.com/pulsestream/pulsestream-go/internal/windows"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"time"
)

//go:embed schema.sql
var schema string

type Store struct {
	Pool   *pgxpool.Pool
	Config config.Config
}

func Open(ctx context.Context, c config.Config) (*Store, error) {
	pc, err := pgxpool.ParseConfig(c.Database)
	if err != nil {
		return nil, err
	}
	pc.MaxConns = 16
	pc.MinConns = 1
	pc.ConnConfig.ConnectTimeout = 5 * time.Second
	p, e := pgxpool.NewWithConfig(ctx, pc)
	if e != nil {
		return nil, e
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}
	s := &Store{p, c}
	return s, nil
}
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(7391821)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, schema); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func Hash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func (s *Store) ConfigHash() string {
	return Hash([]byte(fmt.Sprintf("v1/%v/%s/%s", windows.Specs, s.Config.Lateness, s.Config.OutOfOrder)))
}

type Checkpoint struct {
	Offset int64
	State  *windows.State
}

func (s *Store) Lock(ctx context.Context, tx pgx.Tx, topic string, p int32) (Checkpoint, error) {
	h := s.ConfigHash()
	_, err := tx.Exec(ctx, `INSERT INTO checkpoints(pipeline,topic,partition_id,config_hash) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, s.Config.Group, topic, p, h)
	if err != nil {
		return Checkpoint{}, err
	}
	var offset int64
	var b []byte
	var checksum, ch string
	err = tx.QueryRow(ctx, `SELECT last_offset,state,checksum,config_hash FROM checkpoints WHERE pipeline=$1 AND topic=$2 AND partition_id=$3 FOR UPDATE`, s.Config.Group, topic, p).Scan(&offset, &b, &checksum, &ch)
	if err != nil {
		return Checkpoint{}, err
	}
	if ch != h {
		return Checkpoint{}, errors.New("checkpoint configuration changed: use a new pipeline group and rebuild namespace")
	}
	// Hash canonical JSON, since PostgreSQL jsonb normalizes whitespace and key order.
	canonical, err := Canonical(b)
	if err != nil {
		return Checkpoint{}, err
	}
	if checksum != "" && Hash(canonical) != checksum {
		return Checkpoint{}, errors.New("checkpoint checksum mismatch; refusing to skip records")
	}
	state := windows.New()
	if err = json.Unmarshal(b, state); err != nil {
		return Checkpoint{}, err
	}
	return Checkpoint{offset, state}, nil
}
func (s *Store) Save(ctx context.Context, tx pgx.Tx, topic string, p int32, c Checkpoint) error {
	b, err := json.Marshal(c.State)
	if err != nil {
		return err
	}
	if len(b) > s.Config.MaxStateBytes {
		return errors.New("checkpoint capacity exceeded")
	}
	canonical, err := Canonical(b)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE checkpoints SET last_offset=$4,state=$5,checksum=$6,created_at=now() WHERE pipeline=$1 AND topic=$2 AND partition_id=$3`, s.Config.Group, topic, p, c.Offset, b, Hash(canonical))
	return err
}
func Enqueue(ctx context.Context, tx pgx.Tx, kind string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	_, err = tx.Exec(ctx, `INSERT INTO outbox(kind,payload,traceparent) VALUES($1,$2,$3)`, kind, b, carrier.Get("traceparent"))
	return err
}

// Canonical preserves exact integer tokens while normalizing JSON map order.
func Canonical(b []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}
