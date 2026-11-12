package replay

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/pulsestream/pulsestream-go/internal/broker"
	"github.com/pulsestream/pulsestream-go/internal/config"
	"github.com/pulsestream/pulsestream-go/internal/event"
	"github.com/pulsestream/pulsestream-go/internal/observe"
	"github.com/pulsestream/pulsestream-go/internal/processing"
	"github.com/pulsestream/pulsestream-go/internal/store"
	"github.com/twmb/franz-go/pkg/kgo"
	"time"
)

type Request struct {
	Topic      string `json:"topic"`
	Partition  int32  `json:"partition"`
	Start      int64  `json:"start_offset"`
	End        int64  `json:"end_offset"`
	Limit      int    `json:"limit"`
	Category   string `json:"category"`
	DryRun     bool   `json:"dry_run"`
	Confirm    bool   `json:"confirm"`
	SetVersion int    `json:"set_schema_version,omitempty"`
	From       int64  `json:"from_ms,omitempty"`
	To         int64  `json:"to_ms,omitempty"`
}
type Result struct {
	ID        int64             `json:"id"`
	Scanned   int               `json:"scanned"`
	Matched   int               `json:"matched"`
	Published int               `json:"published"`
	Records   []json.RawMessage `json:"records,omitempty"`
	Status    string            `json:"status"`
}

func (r Request) Validate(c config.Config) error {
	if r.Topic != broker.Raw && r.Topic != broker.DLQ && r.Topic != broker.Late {
		return errors.New("unsupported replay topic")
	}
	if r.Partition < 0 || int(r.Partition) >= c.Partitions || r.Start < 0 || r.End <= r.Start || r.End-r.Start > 10000 || r.Limit < 1 || r.Limit > 1000 {
		return errors.New("explicit partition, nonempty offset range <=10000 and limit <=1000 required")
	}
	if !r.DryRun && !r.Confirm {
		return errors.New("confirm=true required for publication")
	}
	if r.SetVersion != 0 && r.SetVersion != 1 {
		return errors.New("only schema version 1 correction supported")
	}
	return nil
}
func Run(ctx context.Context, c config.Config, s *store.Store, tenant string, r Request) (result Result, err error) {
	if err = r.Validate(c); err != nil {
		return result, err
	}
	b, _ := json.Marshal(r)
	if err = s.Pool.QueryRow(ctx, `INSERT INTO replay_audit(tenant,request,status) VALUES($1,$2,'running') RETURNING id`, tenant, b).Scan(&result.ID); err != nil {
		return result, err
	}
	defer func() {
		status := "completed"
		detail := ""
		if err != nil {
			status = "failed"
			detail = "replay interrupted; inspect service logs and retry stable IDs"
		}
		result.Status = status
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = s.Pool.Exec(cleanup, `UPDATE replay_audit SET status=$2,published=$3,error=$4,completed_at=now() WHERE id=$1`, result.ID, status, result.Published, detail)
	}()
	opts := broker.Options(c)
	opts = append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{r.Topic: {r.Partition: kgo.NewOffset().At(r.Start)}}), kgo.FetchMaxBytes(2<<20))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return result, err
	}
	defer cl.Close()
	pub, err := broker.New(c)
	if err != nil {
		return result, err
	}
	defer pub.Close()
	done := false
	result.Records = []json.RawMessage{}
	for !done && result.Scanned < 10000 && result.Matched < r.Limit {
		pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		fetch := cl.PollRecords(pollCtx, 100)
		cancel()
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if fetch.NumRecords() == 0 {
			break
		}
		if es := fetch.Errors(); len(es) > 0 {
			return result, es[0].Err
		}
		for _, record := range fetch.Records() {
			if record.Offset >= r.End {
				done = true
				break
			}
			result.Scanned++
			original := record.Value
			category := ""
			if r.Topic == broker.DLQ {
				var dead processing.DeadLetter
				if err = json.Unmarshal(original, &dead); err != nil {
					continue
				}
				if dead.Tenant != tenant {
					continue
				}
				category = dead.Category
				original, err = base64.StdEncoding.DecodeString(dead.Original)
				if err != nil {
					continue
				}
			}
			if r.Category != "" && category != r.Category {
				continue
			}
			var e event.Event
			if err = json.Unmarshal(original, &e); err != nil {
				continue
			}
			if e.Tenant != tenant {
				continue
			}
			if r.From != 0 && e.Time.UnixMilli() < r.From {
				continue
			}
			if r.To != 0 && e.Time.UnixMilli() >= r.To {
				continue
			}
			if r.SetVersion != 0 {
				e.Version = r.SetVersion
			}
			result.Matched++
			if r.DryRun {
				if len(result.Records) < 100 {
					result.Records = append(result.Records, append([]byte(nil), record.Value...))
				}
			} else {
				if err = e.Validate(); err != nil {
					return result, fmt.Errorf("record %d remains invalid: %w", record.Offset, err)
				}
				if err = broker.Publish(ctx, pub, e); err != nil {
					return result, err
				}
				result.Published++
				observe.Events.WithLabelValues("replay", "published").Inc()
			}
			if result.Matched >= r.Limit {
				break
			}
		}
	}
	return result, nil
}
