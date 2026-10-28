package ingest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"

	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/tracking"
)

type fakeRecorder struct {
	errs   []error
	calls  int
	got    []tracking.Location
	called chan struct{}
}

func (f *fakeRecorder) Record(_ context.Context, l tracking.Location) (bool, error) {
	f.calls++
	if f.called != nil {
		f.called <- struct{}{}
	}
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		if err != nil {
			return false, err
		}
	}
	f.got = append(f.got, l)
	return true, nil
}

func newSubscriber(t *testing.T, rec Recorder) *Subscriber {
	t.Helper()
	lc := fxtest.NewLifecycle(t)
	s := New(rec, zap.NewNop(), lc)
	s.backoff = time.Millisecond
	return s
}

const spec = `{"vehicle_id":"B1234XYZ","latitude":-6.2088,"longitude":106.8456,"timestamp":1715003456}`
const specTopic = "/fleet/vehicle/B1234XYZ/location"

func TestVehicleIDFromTopic(t *testing.T) {
	tests := []struct {
		topic   string
		want    string
		wantErr bool
	}{
		{specTopic, "B1234XYZ", false},
		{"/fleet/vehicle/B-1_x/location", "B-1_x", false},
		{"fleet/vehicle/B1234XYZ/location", "", true},
		{"/fleet/vehicle/B1234XYZ/location/x", "", true},
		{"/fleet/vehicle/B1234XYZ", "", true},
		{"/fleet/vehicle//location", "", true},
		{"/fleet/vehicle/B1234XYZ/position", "", true},
		{"/fleet/truck/B1234XYZ/location", "", true},
		{"/fleet/vehicle/bad id/location", "", true},
		{"/fleet/vehicle/" + string(make([]byte, 40)) + "/location", "", true},
	}
	for _, tt := range tests {
		got, err := vehicleIDFromTopic(tt.topic)
		var verr *tracking.ValidationError
		if tt.wantErr {
			if !errors.As(err, &verr) {
				t.Errorf("%q: err = %v, want ValidationError", tt.topic, err)
			}
			continue
		}
		if err != nil || got != tt.want {
			t.Errorf("%q: got %q err %v, want %q", tt.topic, got, err, tt.want)
		}
	}
}

func TestProcess(t *testing.T) {
	tests := []struct {
		name    string
		topic   string
		payload string
		stored  bool
	}{
		{"spec example", specTopic, spec, true},
		{"topic and payload vehicle differ", "/fleet/vehicle/OTHER/location", spec, false},
		{"bad topic", "/fleet/vehicle/B1234XYZ/nope", spec, false},
		{"not json", specTopic, `{{`, false},
		{"missing field", specTopic, `{"vehicle_id":"B1234XYZ","latitude":1,"longitude":2}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &fakeRecorder{}
			_, err := newSubscriber(t, rec).Process(context.Background(), tt.topic, []byte(tt.payload))
			if tt.stored {
				if err != nil || len(rec.got) != 1 {
					t.Fatalf("err=%v stored=%d, want stored", err, len(rec.got))
				}
				want := tracking.Location{VehicleID: "B1234XYZ", Latitude: -6.2088, Longitude: 106.8456, Timestamp: 1715003456}
				if rec.got[0] != want {
					t.Fatalf("stored %+v, want %+v", rec.got[0], want)
				}
				return
			}
			var verr *tracking.ValidationError
			if !errors.As(err, &verr) || rec.calls != 0 {
				t.Fatalf("err=%v calls=%d, want ValidationError and no store call", err, rec.calls)
			}
		})
	}
}

func TestHandleDoesNotRetryInvalidInput(t *testing.T) {
	rec := &fakeRecorder{errs: []error{&tracking.ValidationError{Msg: "latitude must be within [-90, 90]"}}}
	newSubscriber(t, rec).Handle(specTopic, []byte(spec))
	if rec.calls != 1 {
		t.Fatalf("calls = %d, want 1 (validation failures are permanent)", rec.calls)
	}
}

func TestHandleRetriesTransientFailureThenSucceeds(t *testing.T) {
	boom := errors.New("connection refused")
	rec := &fakeRecorder{errs: []error{boom, boom}}
	newSubscriber(t, rec).Handle(specTopic, []byte(spec))
	if rec.calls != 3 || len(rec.got) != 1 {
		t.Fatalf("calls=%d stored=%d, want 3 calls and 1 stored", rec.calls, len(rec.got))
	}
}

func TestHandleGivesUpAfterAttempts(t *testing.T) {
	boom := errors.New("connection refused")
	rec := &fakeRecorder{errs: []error{boom, boom, boom, boom, boom, boom, boom}}
	s := newSubscriber(t, rec)
	s.Handle(specTopic, []byte(spec))
	if rec.calls != s.attempts || len(rec.got) != 0 {
		t.Fatalf("calls=%d stored=%d, want %d calls and nothing stored", rec.calls, len(rec.got), s.attempts)
	}
}

func TestHandleStopsRetryingOnShutdown(t *testing.T) {
	boom := errors.New("connection refused")
	rec := &fakeRecorder{errs: []error{boom, boom, boom, boom, boom}, called: make(chan struct{}, 10)}
	lc := &stopLifecycle{}
	s := New(rec, zap.NewNop(), lc)
	s.backoff = time.Hour
	done := make(chan struct{})
	go func() { s.Handle(specTopic, []byte(spec)); close(done) }()

	<-rec.called
	lc.stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Handle did not return after shutdown")
	}
}

type stopLifecycle struct{ hooks []fx.Hook }

func (l *stopLifecycle) Append(h fx.Hook) { l.hooks = append(l.hooks, h) }
func (l *stopLifecycle) stop() {
	for _, h := range l.hooks {
		_ = h.OnStop(context.Background())
	}
}

func TestProcessRejectsOversizedPayloadBeforeDecoding(t *testing.T) {
	rec := &fakeRecorder{}
	big := []byte(`{"vehicle_id":"B1234XYZ","pad":"` + strings.Repeat("x", MaxPayloadBytes) + `"}`)
	_, err := newSubscriber(t, rec).Process(context.Background(), specTopic, big)
	var verr *tracking.ValidationError
	if !errors.As(err, &verr) || !strings.Contains(err.Error(), "exceeds") || rec.calls != 0 {
		t.Fatalf("err=%v calls=%d, want an oversize ValidationError and no store call", err, rec.calls)
	}
}
