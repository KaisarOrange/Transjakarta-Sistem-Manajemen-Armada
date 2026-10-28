package ingest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/tracking"
)

const TopicFilter = "/fleet/vehicle/+/location"

const MaxPayloadBytes = 4096

type Recorder interface {
	Record(ctx context.Context, l tracking.Location) (inserted bool, err error)
}

type Subscriber struct {
	rec Recorder
	log *zap.Logger
	ctx context.Context

	attempts int
	backoff  time.Duration
}

func New(rec Recorder, l *zap.Logger, lc fx.Lifecycle) *Subscriber {
	ctx, cancel := context.WithCancel(context.Background())

	lc.Append(fx.Hook{OnStop: func(context.Context) error { cancel(); return nil }})
	return &Subscriber{rec: rec, log: l, ctx: ctx, attempts: 5, backoff: 200 * time.Millisecond}
}

func (s *Subscriber) Handle(topic string, payload []byte) {
	var err error
	delay := s.backoff
	for attempt := 1; attempt <= s.attempts; attempt++ {
		var inserted bool
		inserted, err = s.Process(s.ctx, topic, payload)
		var verr *tracking.ValidationError
		switch {
		case err == nil:
			s.log.Debug("location stored", zap.String("topic", topic), zap.Bool("duplicate", !inserted))
			return
		case errors.As(err, &verr):
			s.log.Warn("message rejected", zap.String("topic", topic), zap.Int("bytes", len(payload)), zap.Error(err))
			return
		}
		if attempt == s.attempts {
			break
		}
		s.log.Warn("storing location failed, will retry", zap.String("topic", topic), zap.Int("attempt", attempt), zap.Error(err))
		select {
		case <-time.After(delay):
			delay *= 2
		case <-s.ctx.Done():
			s.log.Error("shutting down, location dropped", zap.String("topic", topic))
			return
		}
	}
	s.log.Error("location dropped after retries", zap.String("topic", topic), zap.Int("attempts", s.attempts), zap.Error(err))
}

func (s *Subscriber) Process(ctx context.Context, topic string, payload []byte) (inserted bool, err error) {
	if len(payload) > MaxPayloadBytes {
		return false, &tracking.ValidationError{Msg: fmt.Sprintf("payload of %d bytes exceeds the %d byte limit", len(payload), MaxPayloadBytes)}
	}
	topicID, err := vehicleIDFromTopic(topic)
	if err != nil {
		return false, err
	}
	loc, err := tracking.Decode(payload)
	if err != nil {
		return false, err
	}
	if loc.VehicleID != topicID {
		return false, &tracking.ValidationError{Msg: fmt.Sprintf("vehicle_id %q does not match topic vehicle %q", loc.VehicleID, topicID)}
	}
	return s.rec.Record(ctx, loc)
}

func vehicleIDFromTopic(topic string) (string, error) {
	parts := strings.Split(topic, "/")
	if len(parts) != 5 || parts[0] != "" || parts[1] != "fleet" || parts[2] != "vehicle" || parts[4] != "location" {
		return "", &tracking.ValidationError{Msg: "unexpected topic " + fmt.Sprintf("%q", topic)}
	}
	if err := tracking.ValidateVehicleID(parts[3]); err != nil {
		return "", err
	}
	return parts[3], nil
}

func Topic(vehicleID string) string { return "/fleet/vehicle/" + vehicleID + "/location" }
