package tracking

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"time"
)

type Location struct {
	VehicleID string  `json:"vehicle_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Timestamp int64   `json:"timestamp"`
}

var ErrNotFound = errors.New("not found")

type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

func invalid(format string, args ...any) error {
	return &ValidationError{Msg: fmt.Sprintf(format, args...)}
}

var vehicleIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

func ValidateVehicleID(id string) error {
	if !vehicleIDPattern.MatchString(id) {
		return invalid("vehicle_id must be 1-32 characters of letters, digits, '_' or '-'")
	}
	return nil
}

func Decode(data []byte) (Location, error) {
	var raw struct {
		VehicleID *string  `json:"vehicle_id"`
		Latitude  *float64 `json:"latitude"`
		Longitude *float64 `json:"longitude"`
		Timestamp *int64   `json:"timestamp"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return Location{}, invalid("payload is not a valid location object: %v", err)
	}
	switch {
	case raw.VehicleID == nil:
		return Location{}, invalid("vehicle_id is required")
	case raw.Latitude == nil:
		return Location{}, invalid("latitude is required")
	case raw.Longitude == nil:
		return Location{}, invalid("longitude is required")
	case raw.Timestamp == nil:
		return Location{}, invalid("timestamp is required")
	}
	return Location{*raw.VehicleID, *raw.Latitude, *raw.Longitude, *raw.Timestamp}, nil
}

func (l Location) Validate(now time.Time, maxFutureSkew time.Duration) error {
	if err := ValidateVehicleID(l.VehicleID); err != nil {
		return err
	}
	if math.IsNaN(l.Latitude) || l.Latitude < -90 || l.Latitude > 90 {
		return invalid("latitude must be within [-90, 90]")
	}
	if math.IsNaN(l.Longitude) || l.Longitude < -180 || l.Longitude > 180 {
		return invalid("longitude must be within [-180, 180]")
	}
	if l.Timestamp <= 0 {
		return invalid("timestamp must be a positive Unix time in seconds")
	}
	if l.Timestamp > now.Add(maxFutureSkew).Unix() {
		return invalid("timestamp is too far in the future")
	}
	return nil
}
