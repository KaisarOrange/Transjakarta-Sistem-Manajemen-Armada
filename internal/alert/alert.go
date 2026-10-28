package alert

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"go.uber.org/zap"

	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/geofence"
)

type Handler struct{ log *zap.Logger }

func NewHandler(l *zap.Logger) *Handler { return &Handler{log: l} }

func (h *Handler) Handle(messageID string, body []byte) error {
	var ev geofence.Event
	if err := json.Unmarshal(body, &ev); err != nil {
		return fmt.Errorf("not a valid event: %w", err)
	}
	if err := validate(ev); err != nil {
		return err
	}
	h.log.Info("geofence alert",
		zap.String("message_id", messageID),
		zap.String("vehicle_id", ev.VehicleID),
		zap.String("event", ev.Event),
		zap.Float64("latitude", ev.Location.Latitude),
		zap.Float64("longitude", ev.Location.Longitude),
		zap.Int64("timestamp", ev.Timestamp))
	return nil
}

func validate(ev geofence.Event) error {
	switch {
	case ev.VehicleID == "":
		return errors.New("vehicle_id is required")
	case ev.Event != geofence.EntryEventName:
		return fmt.Errorf("unsupported event %q", ev.Event)
	case math.Abs(ev.Location.Latitude) > 90 || math.Abs(ev.Location.Longitude) > 180:
		return errors.New("location is out of range")
	case ev.Timestamp <= 0:
		return errors.New("timestamp must be positive")
	}
	return nil
}
