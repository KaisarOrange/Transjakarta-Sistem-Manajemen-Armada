package tracking_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/geofence"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/tracking"
)

type fakeStore struct {
	rows       []tracking.Location
	latestErr  error
	inserted   []tracking.Location
	state      map[string]geofence.State
	events     []geofence.Event
	gotStart   int64
	gotEnd     int64
	gotLimit   int
	historyHit bool
}

func (f *fakeStore) Ingest(_ context.Context, l tracking.Location, decide tracking.Decide) (bool, error) {
	for _, prev := range f.inserted {
		if prev.VehicleID == l.VehicleID && prev.Timestamp == l.Timestamp {
			return false, nil
		}
	}
	f.inserted = append(f.inserted, l)
	if f.state == nil {
		f.state = map[string]geofence.State{}
	}
	next, ev := decide(f.state[l.VehicleID])
	f.state[l.VehicleID] = next
	if ev != nil {
		f.events = append(f.events, *ev)
	}
	return true, nil
}

func (f *fakeStore) Latest(_ context.Context, id string) (tracking.Location, error) {
	if f.latestErr != nil {
		return tracking.Location{}, f.latestErr
	}
	for _, l := range f.rows {
		if l.VehicleID == id {
			return l, nil
		}
	}
	return tracking.Location{}, tracking.ErrNotFound
}

func (f *fakeStore) History(_ context.Context, _ string, start, end int64, limit int) ([]tracking.Location, error) {
	f.historyHit, f.gotStart, f.gotEnd, f.gotLimit = true, start, end, limit
	return f.rows, nil
}

var testFence = geofence.Fence{CenterLat: -6.2088, CenterLon: 106.8456, RadiusM: 50}

func newService(store tracking.Store) *tracking.Service {
	return tracking.NewService(store, testFence, 5*time.Minute, 24*time.Hour)
}

func metresNorth(m float64) float64 { return testFence.CenterLat + m/6371000*180/math.Pi }

func TestRecordGeofenceThroughService(t *testing.T) {
	const id = "B1234XYZ"
	base := time.Now().Unix() - 1000
	rec := func(svc *tracking.Service, offset int64, metres float64) bool {
		t.Helper()
		inserted, err := svc.Record(context.Background(),
			tracking.Location{VehicleID: id, Latitude: metresNorth(metres), Longitude: testFence.CenterLon, Timestamp: base + offset})
		if err != nil {
			t.Fatal(err)
		}
		return inserted
	}

	t.Run("route outside, inside, inside, outside, inside gives two entries", func(t *testing.T) {
		store := &fakeStore{}
		svc := newService(store)
		for i, m := range []float64{300, 20, 5, 120, 30} {
			rec(svc, int64(i+1), m)
		}
		if len(store.events) != 2 || store.events[0].Timestamp != base+2 || store.events[1].Timestamp != base+5 {
			t.Fatalf("events = %+v, want entries at base+2 and base+5", store.events)
		}
		if ev := store.events[0]; ev.VehicleID != id || ev.Event != "geofence_entry" || ev.Location.Longitude != testFence.CenterLon {
			t.Fatalf("event content wrong: %+v", ev)
		}
	})

	t.Run("first reading inside the fence is an entry", func(t *testing.T) {
		store := &fakeStore{}
		rec(newService(store), 1, 0)
		if len(store.events) != 1 {
			t.Fatalf("events = %d, want 1", len(store.events))
		}
	})

	t.Run("redelivered message adds no second event", func(t *testing.T) {
		store := &fakeStore{}
		svc := newService(store)
		if !rec(svc, 1, 10) || rec(svc, 1, 10) {
			t.Fatal("want first insert true, duplicate false")
		}
		if len(store.events) != 1 {
			t.Fatalf("events = %d, want 1", len(store.events))
		}
	})

	t.Run("late reading is stored but cannot change the geofence state", func(t *testing.T) {
		store := &fakeStore{}
		svc := newService(store)
		rec(svc, 10, 300)
		if !rec(svc, 5, 10) {
			t.Fatal("late reading should still be stored")
		}
		if len(store.events) != 0 || store.state[id].LastTimestamp != base+10 {
			t.Fatalf("events=%d state=%+v, want no events and state still at base+10", len(store.events), store.state[id])
		}
	})

	t.Run("invalid reading never reaches the store", func(t *testing.T) {
		store := &fakeStore{}
		_, err := newService(store).Record(context.Background(), tracking.Location{VehicleID: id, Latitude: 200, Timestamp: base})
		if err == nil || len(store.inserted) != 0 {
			t.Fatalf("err=%v inserted=%d", err, len(store.inserted))
		}
	})
}

func TestServiceRecord(t *testing.T) {
	store := &fakeStore{}
	svc := newService(store)
	now := time.Now().Unix()

	if _, err := svc.Record(context.Background(), tracking.Location{VehicleID: "B1", Latitude: 1, Longitude: 2, Timestamp: now}); err != nil {
		t.Fatalf("valid reading rejected: %v", err)
	}
	if len(store.inserted) != 1 {
		t.Fatalf("inserted %d rows, want 1", len(store.inserted))
	}

	bad := []tracking.Location{
		{VehicleID: "B1", Latitude: 91, Timestamp: now},
		{VehicleID: "B1", Timestamp: now + 3600},
		{VehicleID: "", Timestamp: now},
	}
	for _, l := range bad {
		if _, err := svc.Record(context.Background(), l); err == nil {
			t.Errorf("Record(%+v) accepted an invalid reading", l)
		}
	}
	if len(store.inserted) != 1 {
		t.Fatalf("invalid readings reached the store: %d rows", len(store.inserted))
	}
}

func TestServiceHistoryRules(t *testing.T) {
	tests := []struct {
		name       string
		start, end int64
		wantErr    bool
	}{
		{"valid", 1000, 2000, false},
		{"single instant", 1000, 1000, false},
		{"exactly max range", 0, 86400, false},
		{"one second over max range", 0, 86401, true},
		{"span that overflows time.Duration", 0, 10_000_000_000, true},
		{"largest possible span", 0, math.MaxInt64, true},
		{"start after end", 2000, 1000, true},
		{"negative start", -1, 1000, true},
		{"negative end", 0, -1, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{}
			_, _, err := newService(store).History(context.Background(), "B1", tt.start, tt.end)
			var verr *tracking.ValidationError
			if tt.wantErr {
				if !errors.As(err, &verr) {
					t.Fatalf("err = %v, want ValidationError", err)
				}
				if store.historyHit {
					t.Fatal("store queried despite invalid input")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if store.gotStart != tt.start || store.gotEnd != tt.end {
				t.Fatalf("store got [%d,%d], want [%d,%d]", store.gotStart, store.gotEnd, tt.start, tt.end)
			}
		})
	}
}

func TestServiceHistoryTruncation(t *testing.T) {
	rows := make([]tracking.Location, tracking.MaxHistoryRows+1)
	for i := range rows {
		rows[i] = tracking.Location{VehicleID: "B1", Timestamp: int64(i + 1)}
	}
	store := &fakeStore{rows: rows}

	locs, truncated, err := newService(store).History(context.Background(), "B1", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !truncated || len(locs) != tracking.MaxHistoryRows {
		t.Fatalf("truncated=%v len=%d, want true/%d", truncated, len(locs), tracking.MaxHistoryRows)
	}
	if store.gotLimit != tracking.MaxHistoryRows+1 {
		t.Fatalf("store limit = %d, want one extra row to detect truncation", store.gotLimit)
	}

	store.rows = rows[:tracking.MaxHistoryRows]
	locs, truncated, _ = newService(store).History(context.Background(), "B1", 0, 1000)
	if truncated || len(locs) != tracking.MaxHistoryRows {
		t.Fatalf("at the cap: truncated=%v len=%d, want false/%d", truncated, len(locs), tracking.MaxHistoryRows)
	}
}

func TestServiceRejectsBadVehicleID(t *testing.T) {
	svc := newService(&fakeStore{})
	var verr *tracking.ValidationError
	if _, err := svc.Latest(context.Background(), "bad id!"); !errors.As(err, &verr) {
		t.Errorf("Latest err = %v, want ValidationError", err)
	}
	if _, _, err := svc.History(context.Background(), "bad id!", 0, 1); !errors.As(err, &verr) {
		t.Errorf("History err = %v, want ValidationError", err)
	}
}
