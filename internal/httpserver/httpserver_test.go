package httpserver

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"
)

type pinger struct{ err error }

func (p pinger) Ping(context.Context) error { return p.err }

func health(t *testing.T, h Health) (int, string) {
	t.Helper()
	app := New(zap.NewNop(), h)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func TestHealthz(t *testing.T) {
	tests := []struct {
		name     string
		h        Health
		wantCode int
		wantBody string
	}{
		{"everything fine", Health{
			Database:      pinger{},
			MQTTConnected: func() bool { return true },
			OutboxPending: func(context.Context) (int, error) { return 3, nil },
		}, 200, `{"checks":{"database":"ok","mqtt":"connected","outbox_pending":3},"status":"ok"}`},
		{"mqtt down is degraded but still 200: REST keeps working", Health{
			Database:      pinger{},
			MQTTConnected: func() bool { return false },
		}, 200, `{"checks":{"database":"ok","mqtt":"disconnected"},"status":"degraded"}`},
		{"outbox query failing is degraded", Health{
			Database:      pinger{},
			OutboxPending: func(context.Context) (int, error) { return 0, errors.New("boom") },
		}, 200, `{"checks":{"database":"ok","outbox_pending":"unknown"},"status":"degraded"}`},
		{"database down is 503 and hides the other checks", Health{
			Database:      pinger{err: errors.New("connection refused")},
			MQTTConnected: func() bool { return true },
		}, 503, `{"checks":{"database":"down"},"status":"unavailable"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, body := health(t, tt.h)
			if code != tt.wantCode || body != tt.wantBody {
				t.Fatalf("got %d %s\nwant %d %s", code, body, tt.wantCode, tt.wantBody)
			}
		})
	}
}
