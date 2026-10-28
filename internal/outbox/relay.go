package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Publisher interface {
	Publish(ctx context.Context, messageID string, body []byte) error
}

type Relay struct {
	pool      *pgxpool.Pool
	pub       Publisher
	log       *zap.Logger
	interval  time.Duration
	retention time.Duration
	batch     int
}

func NewRelay(pool *pgxpool.Pool, pub Publisher, l *zap.Logger, retention time.Duration) *Relay {
	return &Relay{pool: pool, pub: pub, log: l, interval: time.Second, retention: retention, batch: 100}
}

type row struct {
	id        int64
	messageID string
	payload   string
}

func (r *Relay) Flush(ctx context.Context) (published int, err error) {
	var publishErr error
	txErr := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT id, message_id, payload FROM geofence_outbox
			 WHERE published_at IS NULL ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, r.batch)
		if err != nil {
			return fmt.Errorf("select outbox: %w", err)
		}
		pending, err := pgx.CollectRows(rows, func(rw pgx.CollectableRow) (row, error) {
			var x row
			return x, rw.Scan(&x.id, &x.messageID, &x.payload)
		})
		if err != nil {
			return fmt.Errorf("read outbox: %w", err)
		}
		for _, x := range pending {
			if publishErr = r.pub.Publish(ctx, x.messageID, []byte(x.payload)); publishErr != nil {
				return nil
			}
			if _, err := tx.Exec(ctx, `UPDATE geofence_outbox SET published_at = now() WHERE id = $1`, x.id); err != nil {
				return fmt.Errorf("mark published: %w", err)
			}
			published++
			r.log.Info("event published", zap.String("message_id", x.messageID))
		}
		return nil
	})
	if txErr != nil {
		return 0, txErr
	}
	return published, publishErr
}

func (r *Relay) Register(lc fx.Lifecycle) {
	stop := make(chan struct{})
	done := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go r.loop(stop, done)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			close(stop)
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
}

const (
	maxBackoff    = 30 * time.Second
	flushTimeout  = 10 * time.Second
	pruneInterval = 10 * time.Minute
)

func nextWait(current, base time.Duration, failed bool) time.Duration {
	if !failed {
		return base
	}
	return min(max(current, base)*2, maxBackoff)
}

func (r *Relay) loop(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	wait := r.interval
	var lastPrune time.Time
	for {
		select {
		case <-stop:
			return
		case <-time.After(wait):
		}

		failed := false

		for {
			ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
			n, err := r.Flush(ctx)
			cancel()
			if err != nil {
				failed = true
				next := nextWait(wait, r.interval, true)
				r.log.Warn("outbox flush failed, will retry", zap.Int("published", n), zap.Duration("retry_in", next), zap.Error(err))
				break
			}
			if n < r.batch {
				break
			}
		}
		wait = nextWait(wait, r.interval, failed)

		if !failed && time.Since(lastPrune) >= pruneInterval {
			lastPrune = time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
			if n, err := r.Prune(ctx); err != nil {
				r.log.Warn("outbox prune failed", zap.Error(err))
			} else if n > 0 {
				r.log.Info("outbox pruned", zap.Int64("rows", n), zap.Duration("older_than", r.retention))
			}
			cancel()
		}
	}
}

func (r *Relay) Prune(ctx context.Context) (int64, error) {
	tag, err := r.pool.Exec(ctx,
		`DELETE FROM geofence_outbox WHERE published_at < now() - make_interval(secs => $1)`, r.retention.Seconds())
	if err != nil {
		return 0, fmt.Errorf("prune outbox: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (r *Relay) Pending(ctx context.Context) (int, error) {
	var n int
	if err := r.pool.QueryRow(ctx, `SELECT count(*) FROM geofence_outbox WHERE published_at IS NULL`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count pending outbox rows: %w", err)
	}
	return n, nil
}
