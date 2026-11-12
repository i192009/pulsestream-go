package analytics

import (
	"context"
	"github.com/pulsestream/pulsestream-go/internal/store"
)

// ApplyFinality is conservative: idle and unseen partitions are never silently
// removed from the global watermark. A quiet partition can delay finality.
func ApplyFinality(ctx context.Context, s *store.Store, ws []Window) error {
	var watermark int64
	var partitions int
	err := s.Pool.QueryRow(ctx, `SELECT coalesce(min((state->>'watermark')::bigint),0),count(*) FROM checkpoints WHERE pipeline=$1 AND topic='pulsestream.events.raw.v1'`, s.Config.Group).Scan(&watermark, &partitions)
	if err != nil {
		return err
	}
	if partitions < s.Config.Partitions {
		watermark = 0
	}
	for i := range ws {
		ws[i].Final = watermark >= ws[i].End+s.Config.Lateness.Milliseconds()
	}
	return nil
}
