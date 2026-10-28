package simulator

import (
	"testing"
	"time"

	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/geofence"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/tracking"
)

var fence = geofence.Fence{CenterLat: -6.2088, CenterLon: 106.8456, RadiusM: 50}

func vehicle(t *testing.T, id string) Vehicle {
	t.Helper()
	for _, v := range Vehicles() {
		if v.ID == id {
			return v
		}
	}
	t.Fatalf("vehicle %s not found", id)
	return Vehicle{}
}

func countEntries(t *testing.T, v Vehicle, ticks int) int {
	t.Helper()
	var state geofence.State
	entries := 0
	for tick := 0; tick < ticks; tick++ {
		msg, err := v.Next(fence, tick, time.Unix(int64(1000+tick), 0))
		if err != nil {
			t.Fatal(err)
		}
		loc, err := tracking.Decode(msg.Payload)
		if err != nil {
			t.Fatal(err)
		}
		var entered bool
		state, entered = fence.Evaluate(state, loc.Latitude, loc.Longitude, loc.Timestamp)
		if entered {
			entries++
		}
	}
	return entries
}

func TestReentryVehicleEntersTwicePerCycle(t *testing.T) {
	v := vehicle(t, "B2001TJ")
	cycle := len(v.Route)
	if got := countEntries(t, v, cycle); got != 2 {
		t.Fatalf("entries in one cycle = %d, want 2", got)
	}
	if got := countEntries(t, v, 3*cycle); got != 6 {
		t.Fatalf("entries in three cycles = %d, want 6 (the route repeats cleanly)", got)
	}
}

func TestReentryVehicleStartsOutside(t *testing.T) {
	v := vehicle(t, "B2001TJ")
	msg, _ := v.Next(fence, 0, time.Now())
	loc, _ := tracking.Decode(msg.Payload)
	if fence.Contains(loc.Latitude, loc.Longitude) {
		t.Fatal("first position is inside the fence; the demo should show an approach first")
	}
}

func TestOutsideVehicleNeverEnters(t *testing.T) {
	v := vehicle(t, "B2002TJ")
	if got := countEntries(t, v, 5*len(v.Route)); got != 0 {
		t.Fatalf("entries = %d, want 0", got)
	}
}

func TestRouteStepsStayClearOfBoundary(t *testing.T) {
	for _, v := range Vehicles() {
		for i, m := range v.Route {
			if m > 40 && m < 60 {
				t.Errorf("%s step %d is %.0f m: too close to the 50 m boundary", v.ID, i, m)
			}
		}
	}
}

func TestMessageMatchesSpec(t *testing.T) {
	msg, err := vehicle(t, "B2001TJ").Next(fence, 0, time.Unix(1715003456, 0))
	if err != nil {
		t.Fatal(err)
	}
	if msg.Topic != "/fleet/vehicle/B2001TJ/location" {
		t.Fatalf("topic = %q", msg.Topic)
	}
	loc, err := tracking.Decode(msg.Payload)
	if err != nil || loc.VehicleID != "B2001TJ" || loc.Timestamp != 1715003456 || loc.Longitude != 106.8456 {
		t.Fatalf("payload %s decoded to %+v err %v", msg.Payload, loc, err)
	}
	if err := loc.Validate(time.Unix(1715003456, 0), time.Minute); err != nil {
		t.Fatalf("simulated reading fails the ingest validation: %v", err)
	}
}
