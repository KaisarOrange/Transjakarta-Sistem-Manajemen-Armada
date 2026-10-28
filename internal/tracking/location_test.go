package tracking_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/tracking"
)

func TestDecode(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    tracking.Location
		wantErr string
	}{
		{"spec example", `{"vehicle_id":"B1234XYZ","latitude":-6.2088,"longitude":106.8456,"timestamp":1715003456}`,
			tracking.Location{VehicleID: "B1234XYZ", Latitude: -6.2088, Longitude: 106.8456, Timestamp: 1715003456}, ""},
		{"unknown extra field is ignored", `{"vehicle_id":"A","latitude":0,"longitude":0,"timestamp":1,"speed":9}`,
			tracking.Location{VehicleID: "A", Timestamp: 1}, ""},
		{"zero coordinates are valid, not missing", `{"vehicle_id":"A","latitude":0,"longitude":0,"timestamp":1}`,
			tracking.Location{VehicleID: "A", Timestamp: 1}, ""},
		{"not json", `hello`, tracking.Location{}, "not a valid location"},
		{"empty", ``, tracking.Location{}, "not a valid location"},
		{"json array", `[]`, tracking.Location{}, "not a valid location"},
		{"trailing garbage", `{"vehicle_id":"A","latitude":0,"longitude":0,"timestamp":1}x`, tracking.Location{}, "not a valid location"},
		{"missing vehicle_id", `{"latitude":0,"longitude":0,"timestamp":1}`, tracking.Location{}, "vehicle_id is required"},
		{"missing latitude", `{"vehicle_id":"A","longitude":0,"timestamp":1}`, tracking.Location{}, "latitude is required"},
		{"missing longitude", `{"vehicle_id":"A","latitude":0,"timestamp":1}`, tracking.Location{}, "longitude is required"},
		{"missing timestamp", `{"vehicle_id":"A","latitude":0,"longitude":0}`, tracking.Location{}, "timestamp is required"},
		{"null field counts as missing", `{"vehicle_id":"A","latitude":null,"longitude":0,"timestamp":1}`, tracking.Location{}, "latitude is required"},
		{"string latitude", `{"vehicle_id":"A","latitude":"1","longitude":0,"timestamp":1}`, tracking.Location{}, "not a valid location"},
		{"fractional timestamp", `{"vehicle_id":"A","latitude":0,"longitude":0,"timestamp":1.5}`, tracking.Location{}, "not a valid location"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tracking.Decode([]byte(tt.payload))
			if tt.wantErr != "" {
				var verr *tracking.ValidationError
				if !errors.As(err, &verr) || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want ValidationError containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	skew := 5 * time.Minute
	ok := tracking.Location{VehicleID: "B1234XYZ", Latitude: -6.2, Longitude: 106.8, Timestamp: now.Unix()}

	with := func(f func(*tracking.Location)) tracking.Location { l := ok; f(&l); return l }

	tests := []struct {
		name    string
		loc     tracking.Location
		wantErr bool
	}{
		{"valid", ok, false},
		{"lat at -90", with(func(l *tracking.Location) { l.Latitude = -90 }), false},
		{"lat at 90", with(func(l *tracking.Location) { l.Latitude = 90 }), false},
		{"lon at -180", with(func(l *tracking.Location) { l.Longitude = -180 }), false},
		{"lon at 180", with(func(l *tracking.Location) { l.Longitude = 180 }), false},
		{"lat too high", with(func(l *tracking.Location) { l.Latitude = 90.0001 }), true},
		{"lat too low", with(func(l *tracking.Location) { l.Latitude = -90.0001 }), true},
		{"lon too high", with(func(l *tracking.Location) { l.Longitude = 180.0001 }), true},
		{"lon too low", with(func(l *tracking.Location) { l.Longitude = -180.0001 }), true},
		{"old timestamp is accepted", with(func(l *tracking.Location) { l.Timestamp = 1 }), false},
		{"zero timestamp", with(func(l *tracking.Location) { l.Timestamp = 0 }), true},
		{"negative timestamp", with(func(l *tracking.Location) { l.Timestamp = -5 }), true},
		{"exactly at skew limit", with(func(l *tracking.Location) { l.Timestamp = now.Add(skew).Unix() }), false},
		{"one second past skew limit", with(func(l *tracking.Location) { l.Timestamp = now.Add(skew).Unix() + 1 }), true},
		{"empty vehicle id", with(func(l *tracking.Location) { l.VehicleID = "" }), true},
		{"vehicle id with slash", with(func(l *tracking.Location) { l.VehicleID = "a/b" }), true},
		{"vehicle id with wildcard", with(func(l *tracking.Location) { l.VehicleID = "a+" }), true},
		{"vehicle id 32 chars", with(func(l *tracking.Location) { l.VehicleID = strings.Repeat("a", 32) }), false},
		{"vehicle id 33 chars", with(func(l *tracking.Location) { l.VehicleID = strings.Repeat("a", 33) }), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.loc.Validate(now, skew)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				var verr *tracking.ValidationError
				if !errors.As(err, &verr) {
					t.Fatalf("err %T is not a *ValidationError", err)
				}
			}
		})
	}
}
