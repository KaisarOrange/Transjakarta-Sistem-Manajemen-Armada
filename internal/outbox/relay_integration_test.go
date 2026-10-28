//go:build integration

package outbox

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/platform/postgres"
)

type sent struct{ messageID, body string }

type fakePublisher struct {
	sent   []sent
	calls  int
	failOn int
}

func (f *fakePublisher) Publish(_ context.Context, messageID string, body []byte) error {
	f.calls++
	if f.calls == f.failOn {
		return errors.New("broker unavailable")
	}
	f.sent = append(f.sent, sent{messageID, string(body)})
	return nil
}

func setup(t *testing.T, n int) *pgxpool.Pool {
	t.Helper()
	adminURL := os.Getenv("DATABASE_URL")
	if adminURL == "" {
		t.Skip("DATABASE_URL not set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("fleet_test_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(adminURL)
	u.Path = "/" + name
	if err := postgres.Migrate(u.String()); err != nil {
		t.Fatal(err)
	}
	pool, err := postgres.NewPool(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(ctx, "DROP DATABASE "+name+" WITH (FORCE)")
		admin.Close(ctx)
	})
	for i := 1; i <= n; i++ {
		if _, err := pool.Exec(ctx, `INSERT INTO geofence_outbox (message_id, payload) VALUES ($1, $2)`,
			fmt.Sprint(i), fmt.Sprintf(`{"n":%d}`, i)); err != nil {
			t.Fatal(err)
		}
	}
	return pool
}

func unpublished(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM geofence_outbox WHERE published_at IS NULL`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestFlushPublishesInOrderAndMarksPublished(t *testing.T) {
	pool := setup(t, 3)
	pub := &fakePublisher{}
	relay := NewRelay(pool, pub, zap.NewNop(), time.Hour)

	n, err := relay.Flush(context.Background())
	if err != nil || n != 3 {
		t.Fatalf("published %d err %v, want 3", n, err)
	}
	if len(pub.sent) != 3 || pub.sent[0] != (sent{"1", `{"n":1}`}) || pub.sent[2].messageID != "3" {
		t.Fatalf("sent = %+v, want rows 1..3 in order with payload and message id unchanged", pub.sent)
	}
	if left := unpublished(t, pool); left != 0 {
		t.Fatalf("%d rows still unpublished", left)
	}

	pub.sent = nil
	if n, err := relay.Flush(context.Background()); err != nil || n != 0 || len(pub.sent) != 0 {
		t.Fatalf("second flush published %d (err %v), want nothing: rows must not be sent twice", n, err)
	}
}

func TestFlushStopsAtFailureThenResumes(t *testing.T) {
	pool := setup(t, 3)
	pub := &fakePublisher{failOn: 2}
	relay := NewRelay(pool, pub, zap.NewNop(), time.Hour)

	n, err := relay.Flush(context.Background())
	if err == nil || n != 1 {
		t.Fatalf("published %d err %v, want 1 and an error", n, err)
	}
	if left := unpublished(t, pool); left != 2 {
		t.Fatalf("unpublished = %d, want 2 (the failed row and the one after it)", left)
	}

	pub.sent = nil
	n, err = relay.Flush(context.Background())
	if err != nil || n != 2 || pub.sent[0].messageID != "2" || pub.sent[1].messageID != "3" {
		t.Fatalf("resume published %d err %v sent %+v, want rows 2 and 3 in order", n, err, pub.sent)
	}
	if left := unpublished(t, pool); left != 0 {
		t.Fatalf("%d rows still unpublished", left)
	}
}

func TestFlushRespectsBatchSize(t *testing.T) {
	pool := setup(t, 5)
	relay := NewRelay(pool, &fakePublisher{}, zap.NewNop(), time.Hour)
	relay.batch = 2

	if n, err := relay.Flush(context.Background()); err != nil || n != 2 {
		t.Fatalf("published %d err %v, want one batch of 2", n, err)
	}
	if left := unpublished(t, pool); left != 3 {
		t.Fatalf("unpublished = %d, want 3", left)
	}
}

func TestPruneDeletesOnlyOldPublishedRows(t *testing.T) {
	pool := setup(t, 4)
	ctx := context.Background()

	mustExec(t, pool, `UPDATE geofence_outbox SET published_at = now() - interval '2 days' WHERE message_id IN ('1','4')`)
	mustExec(t, pool, `UPDATE geofence_outbox SET published_at = now() WHERE message_id = '2'`)
	mustExec(t, pool, `UPDATE geofence_outbox SET created_at = now() - interval '3 days' WHERE message_id = '3'`)

	relay := NewRelay(pool, &fakePublisher{}, zap.NewNop(), 24*time.Hour)
	n, err := relay.Prune(ctx)
	if err != nil || n != 2 {
		t.Fatalf("pruned %d err %v, want 2", n, err)
	}
	var remaining []string
	rows, _ := pool.Query(ctx, `SELECT message_id FROM geofence_outbox ORDER BY message_id`)
	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		remaining = append(remaining, id)
	}
	if fmt.Sprint(remaining) != "[2 3]" {
		t.Fatalf("remaining = %v, want [2 3]", remaining)
	}
}

func TestPendingCountsUnpublishedRows(t *testing.T) {
	pool := setup(t, 3)
	relay := NewRelay(pool, &fakePublisher{}, zap.NewNop(), time.Hour)
	if n, err := relay.Pending(context.Background()); err != nil || n != 3 {
		t.Fatalf("pending = %d err %v, want 3", n, err)
	}
	if _, err := relay.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n, _ := relay.Pending(context.Background()); n != 0 {
		t.Fatalf("pending after flush = %d, want 0", n)
	}
}

func mustExec(t *testing.T, pool *pgxpool.Pool, sql string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql); err != nil {
		t.Fatal(err)
	}
}
