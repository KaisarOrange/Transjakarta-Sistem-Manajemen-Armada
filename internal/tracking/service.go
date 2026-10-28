package tracking

import (
	"context"
	"time"

	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/geofence"
)

const MaxHistoryRows = 10000

type Decide func(prev geofence.State) (next geofence.State, event *geofence.Event)

type Store interface {
	Ingest(ctx context.Context, l Location, decide Decide) (inserted bool, err error)
	Latest(ctx context.Context, vehicleID string) (Location, error)
	History(ctx context.Context, vehicleID string, start, end int64, limit int) ([]Location, error)
}

type Service struct {
	store           Store
	fence           geofence.Fence
	maxFutureSkew   time.Duration
	maxHistoryRange time.Duration
}

func NewService(store Store, fence geofence.Fence, maxFutureSkew, maxHistoryRange time.Duration) *Service {
	return &Service{store: store, fence: fence, maxFutureSkew: maxFutureSkew, maxHistoryRange: maxHistoryRange}
}

func (s *Service) Record(ctx context.Context, l Location) (bool, error) {
	if err := l.Validate(time.Now(), s.maxFutureSkew); err != nil {
		return false, err
	}
	return s.store.Ingest(ctx, l, func(prev geofence.State) (geofence.State, *geofence.Event) {
		next, entered := s.fence.Evaluate(prev, l.Latitude, l.Longitude, l.Timestamp)
		if !entered {
			return next, nil
		}
		ev := geofence.NewEntryEvent(l.VehicleID, l.Latitude, l.Longitude, l.Timestamp)
		return next, &ev
	})
}

func (s *Service) Latest(ctx context.Context, vehicleID string) (Location, error) {
	if err := ValidateVehicleID(vehicleID); err != nil {
		return Location{}, err
	}
	return s.store.Latest(ctx, vehicleID)
}

func (s *Service) History(ctx context.Context, vehicleID string, start, end int64) (locs []Location, truncated bool, err error) {
	if err := ValidateVehicleID(vehicleID); err != nil {
		return nil, false, err
	}
	switch {
	case start < 0 || end < 0:
		return nil, false, invalid("start and end must not be negative")
	case start > end:
		return nil, false, invalid("start must not be after end")

	case end-start > int64(s.maxHistoryRange/time.Second):
		return nil, false, invalid("range too large: at most %s per request", s.maxHistoryRange)
	}
	locs, err = s.store.History(ctx, vehicleID, start, end, MaxHistoryRows+1)
	if err != nil {
		return nil, false, err
	}
	if len(locs) > MaxHistoryRows {
		return locs[:MaxHistoryRows], true, nil
	}
	return locs, false, nil
}
