package geofence

import (
	"encoding/json"
	"math"
	"testing"
)

var jakarta = Fence{CenterLat: -6.2088, CenterLon: 106.8456, RadiusM: 50}

func northOf(lat, metres float64) float64 { return lat + metres/earthRadiusM*180/math.Pi }

func TestDistanceM(t *testing.T) {
	tests := []struct {
		name                   string
		lat1, lon1, lat2, lon2 float64
		want, tol              float64
	}{
		{"same point", -6.2088, 106.8456, -6.2088, 106.8456, 0, 1e-9},
		{"0.001 degree of latitude", 0, 0, 0.001, 0, 111.195, 0.01},
		{"1 degree of longitude at the equator", 0, 0, 0, 1, 111194.9, 1},
		{"1 degree of longitude at 60N is about half", 60, 0, 60, 1, 55597.5, 1},
		{"pole to pole", 90, 0, -90, 0, math.Pi * earthRadiusM, 1},
		{"across the antimeridian", 0, 179.5, 0, -179.5, 111194.9, 1},
	}
	for _, tt := range tests {
		got := DistanceM(tt.lat1, tt.lon1, tt.lat2, tt.lon2)
		if math.Abs(got-tt.want) > tt.tol {
			t.Errorf("%s: got %.3f, want %.3f ±%v", tt.name, got, tt.want, tt.tol)
		}
		if back := DistanceM(tt.lat2, tt.lon2, tt.lat1, tt.lon1); math.Abs(back-got) > 1e-6 {
			t.Errorf("%s: not symmetric: %v vs %v", tt.name, got, back)
		}
	}
}

func TestDistanceMNearAntipodesIsNotNaN(t *testing.T) {
	if d := DistanceM(0, 0, 0, 180); math.IsNaN(d) {
		t.Fatal("distance between antipodes is NaN")
	}
}

func TestContainsBoundary(t *testing.T) {
	tests := []struct {
		metres float64
		want   bool
	}{
		{0, true},
		{49, true},
		{49.9, true},
		{50.1, false},
		{51, false},
		{500, false},
	}
	for _, tt := range tests {
		got := jakarta.Contains(northOf(jakarta.CenterLat, tt.metres), jakarta.CenterLon)
		if got != tt.want {
			t.Errorf("%.1f m north: Contains = %v, want %v", tt.metres, got, tt.want)
		}
	}
}

func TestEvaluateTransitions(t *testing.T) {
	inLat, outLat := northOf(jakarta.CenterLat, 10), northOf(jakarta.CenterLat, 200)
	lon := jakarta.CenterLon

	tests := []struct {
		name        string
		prev        State
		lat         float64
		ts          int64
		want        State
		wantEntered bool
	}{
		{"never seen, first reading inside: entry", State{}, inLat, 100, State{true, 100}, true},
		{"never seen, first reading outside: nothing", State{}, outLat, 100, State{false, 100}, false},
		{"outside to inside: entry", State{false, 100}, inLat, 101, State{true, 101}, true},
		{"inside to inside: no repeat", State{true, 100}, inLat, 101, State{true, 101}, false},
		{"inside to outside: exit is silent", State{true, 100}, outLat, 101, State{false, 101}, false},
		{"outside to outside: nothing", State{false, 100}, outLat, 101, State{false, 101}, false},
		{"re-entry after exit: entry again", State{false, 102}, inLat, 103, State{true, 103}, true},
		{"older reading inside is stale: state untouched", State{false, 100}, inLat, 99, State{false, 100}, false},
		{"older reading outside cannot fake an exit", State{true, 100}, outLat, 99, State{true, 100}, false},
		{"same timestamp is not newer", State{false, 100}, inLat, 100, State{false, 100}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, entered := jakarta.Evaluate(tt.prev, tt.lat, lon, tt.ts)
			if got != tt.want || entered != tt.wantEntered {
				t.Fatalf("got (%+v, %v), want (%+v, %v)", got, entered, tt.want, tt.wantEntered)
			}
		})
	}
}

func TestEvaluateWalkThroughRoute(t *testing.T) {
	metres := []float64{300, 20, 5, 120, 30}
	var state State
	entries := 0
	for i, m := range metres {
		var entered bool
		state, entered = jakarta.Evaluate(state, northOf(jakarta.CenterLat, m), jakarta.CenterLon, int64(i+1))
		if entered {
			entries++
		}
	}
	if entries != 2 {
		t.Fatalf("entries = %d, want 2", entries)
	}
}

func TestEventJSONMatchesSpecExactly(t *testing.T) {
	ev := NewEntryEvent("B1234XYZ", -6.2088, 106.8456, 1715003456)
	got, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"vehicle_id":"B1234XYZ","event":"geofence_entry","location":{"latitude":-6.2088,"longitude":106.8456},"timestamp":1715003456}`
	if string(got) != want {
		t.Fatalf("\n got  %s\n want %s", got, want)
	}
	if id := ev.MessageID(); id != "B1234XYZ:1715003456" {
		t.Fatalf("MessageID = %q", id)
	}
}
