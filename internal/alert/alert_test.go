package alert

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestHandle(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{"spec example", `{"vehicle_id":"B1234XYZ","event":"geofence_entry","location":{"latitude":-6.2088,"longitude":106.8456},"timestamp":1715003456}`, false},
		{"not json", `hello`, true},
		{"empty body", ``, true},
		{"missing vehicle", `{"event":"geofence_entry","location":{"latitude":1,"longitude":2},"timestamp":5}`, true},
		{"unknown event", `{"vehicle_id":"A","event":"geofence_exit","location":{"latitude":1,"longitude":2},"timestamp":5}`, true},
		{"latitude out of range", `{"vehicle_id":"A","event":"geofence_entry","location":{"latitude":91,"longitude":2},"timestamp":5}`, true},
		{"longitude out of range", `{"vehicle_id":"A","event":"geofence_entry","location":{"latitude":1,"longitude":-181},"timestamp":5}`, true},
		{"zero timestamp", `{"vehicle_id":"A","event":"geofence_entry","location":{"latitude":1,"longitude":2},"timestamp":0}`, true},
		{"wrong field types", `{"vehicle_id":5,"event":"geofence_entry"}`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			core, logs := observer.New(zap.InfoLevel)
			err := NewHandler(zap.New(core)).Handle("m1", []byte(tt.body))
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && logs.Len() != 0 {
				t.Fatal("a rejected message must not be logged as an alert")
			}
			if !tt.wantErr {
				entry := logs.All()
				if len(entry) != 1 || entry[0].ContextMap()["vehicle_id"] != "B1234XYZ" || entry[0].ContextMap()["message_id"] != "m1" {
					t.Fatalf("alert log = %+v", entry)
				}
			}
		})
	}
}
