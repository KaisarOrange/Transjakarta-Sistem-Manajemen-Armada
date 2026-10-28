package simulator

import (
	"encoding/json"
	"math"
	"time"

	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/geofence"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/ingest"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/tracking"
)

type Vehicle struct {
	ID    string
	Route []float64
}

func Vehicles() []Vehicle {
	return []Vehicle{
		{
			ID: "B2001TJ",
			Route: []float64{
				300, 200, 120, 70,
				40, 15, 5, 15,
				60, 150, 250, 150,
				70, 30, 10, 30,
			},
		},
		{
			ID:    "B2002TJ",
			Route: []float64{400, 300, 200, 150, 100, 150, 200, 300},
		},
	}
}

type Message struct {
	Topic   string
	Payload []byte
}

func (v Vehicle) Next(f geofence.Fence, tick int, now time.Time) (Message, error) {
	metres := v.Route[tick%len(v.Route)]
	lat, lon := f.PointNorth(metres)
	payload, err := json.Marshal(tracking.Location{
		VehicleID: v.ID,
		Latitude:  round6(lat),
		Longitude: round6(lon),
		Timestamp: now.Unix(),
	})
	if err != nil {
		return Message{}, err
	}
	return Message{Topic: ingest.Topic(v.ID), Payload: payload}, nil
}

func round6(x float64) float64 { return math.Round(x*1e6) / 1e6 }
