package geofence

import "fmt"

const EntryEventName = "geofence_entry"

type Point struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type Event struct {
	VehicleID string `json:"vehicle_id"`
	Event     string `json:"event"`
	Location  Point  `json:"location"`
	Timestamp int64  `json:"timestamp"`
}

func NewEntryEvent(vehicleID string, lat, lon float64, timestamp int64) Event {
	return Event{VehicleID: vehicleID, Event: EntryEventName, Location: Point{lat, lon}, Timestamp: timestamp}
}

func (e Event) MessageID() string { return fmt.Sprintf("%s:%d", e.VehicleID, e.Timestamp) }
