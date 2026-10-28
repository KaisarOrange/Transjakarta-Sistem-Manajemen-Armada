//go:build integration

package tracking_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/geofence"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/tracking"
)

func realService(repo *tracking.Repository) *tracking.Service {
	return tracking.NewService(repo, testFence, 5*time.Minute, 24*time.Hour)
}

func record(t *testing.T, svc *tracking.Service, id string, ts int64, metres float64) bool {
	t.Helper()
	inserted, err := svc.Record(context.Background(),
		tracking.Location{VehicleID: id, Latitude: metresNorth(metres), Longitude: testFence.CenterLon, Timestamp: ts})
	if err != nil {
		t.Fatal(err)
	}
	return inserted
}

func countOutbox(t *testing.T, pool *pgxpool.Pool, id string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM geofence_outbox WHERE message_id LIKE $1`, id+":%").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestGeofenceTransitionsInPostgres(t *testing.T) {
	repo, pool, id := testRepo(t)
	svc := realService(repo)
	base := time.Now().Unix() - 1000

	for i, m := range []float64{300, 20, 5, 120, 30} {
		record(t, svc, id, base+int64(i+1), m)
	}

	rows, err := pool.Query(context.Background(),
		`SELECT message_id, payload FROM geofence_outbox WHERE message_id LIKE $1 ORDER BY id`, id+":%")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got [][2]string
	for rows.Next() {
		var msgID, payload string
		if err := rows.Scan(&msgID, &payload); err != nil {
			t.Fatal(err)
		}
		got = append(got, [2]string{msgID, payload})
	}
	if len(got) != 2 {
		t.Fatalf("outbox rows = %d, want 2 (%v)", len(got), got)
	}

	lat := metresNorth(20)
	wantBody := fmt.Sprintf(`{"vehicle_id":%q,"event":"geofence_entry","location":{"latitude":%s,"longitude":%s},"timestamp":%d}`,
		id, jsonFloat(lat), jsonFloat(testFence.CenterLon), base+2)
	if got[0][1] != wantBody {
		t.Fatalf("payload\n got  %s\n want %s", got[0][1], wantBody)
	}
	if want := fmt.Sprintf("%s:%d", id, base+2); got[0][0] != want {
		t.Fatalf("message_id = %q, want %q", got[0][0], want)
	}

	var inside bool
	var last int64
	if err := pool.QueryRow(context.Background(),
		`SELECT inside, last_timestamp FROM vehicle_geofence_state WHERE vehicle_id = $1`, id).Scan(&inside, &last); err != nil {
		t.Fatal(err)
	}
	if !inside || last != base+5 {
		t.Fatalf("state = inside:%v last:%d, want true/%d", inside, last, base+5)
	}
}

func TestGeofenceFirstReadingInsideIsEntry(t *testing.T) {
	repo, pool, id := testRepo(t)
	record(t, realService(repo), id, time.Now().Unix()-10, 0)
	if n := countOutbox(t, pool, id); n != 1 {
		t.Fatalf("outbox rows = %d, want 1", n)
	}
}

func TestGeofenceDuplicateDeliveryQueuesOneEvent(t *testing.T) {
	repo, pool, id := testRepo(t)
	svc := realService(repo)
	ts := time.Now().Unix() - 10
	if !record(t, svc, id, ts, 10) {
		t.Fatal("first delivery should insert")
	}
	if record(t, svc, id, ts, 10) {
		t.Fatal("redelivery should report duplicate")
	}
	if n := countOutbox(t, pool, id); n != 1 {
		t.Fatalf("outbox rows = %d, want 1", n)
	}
}

func TestGeofenceLateReadingIsStoredButIgnoredForState(t *testing.T) {
	repo, pool, id := testRepo(t)
	svc := realService(repo)
	base := time.Now().Unix() - 1000
	record(t, svc, id, base+10, 300)
	record(t, svc, id, base+5, 10)

	if n := countOutbox(t, pool, id); n != 0 {
		t.Fatalf("late reading produced %d events, want 0", n)
	}
	var rows int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM vehicle_locations WHERE vehicle_id = $1`, id).Scan(&rows); err != nil || rows != 2 {
		t.Fatalf("stored rows = %d err=%v, want 2", rows, err)
	}
	latest, _ := repo.Latest(context.Background(), id)
	if latest.Timestamp != base+10 {
		t.Fatalf("latest = %d, want %d", latest.Timestamp, base+10)
	}
}

func TestGeofenceConcurrentReadingsQueueExactlyOneEntry(t *testing.T) {
	repo, pool, id := testRepo(t)
	base := time.Now().Unix() - 1000

	record(t, realService(repo), id, base, 300)

	var wg sync.WaitGroup
	for i := 1; i <= 8; i++ {
		wg.Add(1)
		go func(ts int64) {
			defer wg.Done()
			l := tracking.Location{VehicleID: id, Latitude: metresNorth(10), Longitude: testFence.CenterLon, Timestamp: ts}
			_, err := repo.Ingest(context.Background(), l, func(prev geofence.State) (geofence.State, *geofence.Event) {
				time.Sleep(30 * time.Millisecond)
				next, entered := testFence.Evaluate(prev, l.Latitude, l.Longitude, l.Timestamp)
				if !entered {
					return next, nil
				}
				ev := geofence.NewEntryEvent(id, l.Latitude, l.Longitude, l.Timestamp)
				return next, &ev
			})
			if err != nil {
				t.Error(err)
			}
		}(base + int64(i))
	}
	wg.Wait()

	if n := countOutbox(t, pool, id); n != 1 {
		t.Fatalf("outbox rows = %d, want exactly 1", n)
	}
}

func jsonFloat(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }
