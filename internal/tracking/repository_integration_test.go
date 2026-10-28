//go:build integration

package tracking_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/geofence"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/platform/postgres"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/tracking"
)

func testRepo(t *testing.T) (*tracking.Repository, *pgxpool.Pool, string) {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}
	if err := postgres.Migrate(url); err != nil {
		t.Fatal(err)
	}
	pool, err := postgres.NewPool(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}

	id := fmt.Sprintf("T%d", time.Now().UnixNano())
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx, `DELETE FROM vehicle_locations WHERE vehicle_id = $1`, id)
		_, _ = pool.Exec(ctx, `DELETE FROM vehicle_geofence_state WHERE vehicle_id = $1`, id)
		_, _ = pool.Exec(ctx, `DELETE FROM geofence_outbox WHERE message_id LIKE $1`, id+":%")
		pool.Close()
	})
	return tracking.NewRepository(pool), pool, id
}

func plain(ctx context.Context, repo *tracking.Repository, l tracking.Location) (bool, error) {
	return repo.Ingest(ctx, l, func(prev geofence.State) (geofence.State, *geofence.Event) { return prev, nil })
}

func TestRepositoryInsertIsIdempotent(t *testing.T) {
	repo, pool, id := testRepo(t)
	ctx := context.Background()
	l := tracking.Location{VehicleID: id, Latitude: -6.2088, Longitude: 106.8456, Timestamp: 1715003456}

	if inserted, err := plain(ctx, repo, l); err != nil || !inserted {
		t.Fatalf("first insert: inserted=%v err=%v", inserted, err)
	}
	if inserted, err := plain(ctx, repo, l); err != nil || inserted {
		t.Fatalf("duplicate insert: inserted=%v err=%v, want false/nil", inserted, err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM vehicle_locations WHERE vehicle_id = $1`, id).Scan(&n); err != nil || n != 1 {
		t.Fatalf("rows = %d err=%v, want 1", n, err)
	}

	if err := pool.QueryRow(ctx, `SELECT count(*) FROM vehicle_loctions WHERE vehicle_id = $1`, id).Scan(&n); err != nil || n != 1 {
		t.Fatalf("vehicle_loctions view rows = %d err=%v, want 1", n, err)
	}
}

func TestRepositoryLatestUsesGreatestTimestampNotArrivalOrder(t *testing.T) {
	repo, _, id := testRepo(t)
	ctx := context.Background()
	for _, ts := range []int64{300, 100, 200} {
		if _, err := plain(ctx, repo, tracking.Location{VehicleID: id, Latitude: -6.2, Longitude: 106.8, Timestamp: ts}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := repo.Latest(ctx, id)
	if err != nil || got.Timestamp != 300 {
		t.Fatalf("Latest = %+v err=%v, want timestamp 300", got, err)
	}
}

func TestRepositoryLatestNotFound(t *testing.T) {
	repo, _, id := testRepo(t)
	if _, err := repo.Latest(context.Background(), id); !errors.Is(err, tracking.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestRepositoryHistoryRangeOrderAndLimit(t *testing.T) {
	repo, _, id := testRepo(t)
	ctx := context.Background()
	for _, ts := range []int64{500, 100, 400, 200, 300} {
		if _, err := plain(ctx, repo, tracking.Location{VehicleID: id, Timestamp: ts}); err != nil {
			t.Fatal(err)
		}
	}

	got, err := repo.History(ctx, id, 200, 400, 100)
	if err != nil {
		t.Fatal(err)
	}
	var ts []int64
	for _, l := range got {
		ts = append(ts, l.Timestamp)
	}
	if fmt.Sprint(ts) != "[200 300 400]" {
		t.Fatalf("timestamps = %v, want [200 300 400]", ts)
	}

	if got, _ := repo.History(ctx, id, 0, 1000, 2); len(got) != 2 || got[0].Timestamp != 100 {
		t.Fatalf("limit 2 returned %+v, want the two oldest", got)
	}

	empty, err := repo.History(ctx, id, 600, 700, 10)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("empty range = %#v err=%v, want non-nil empty slice", empty, err)
	}
}
