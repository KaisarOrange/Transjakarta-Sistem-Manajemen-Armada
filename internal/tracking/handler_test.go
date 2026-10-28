package tracking_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"

	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/httpserver"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/tracking"
)

type okPinger struct{}

func (okPinger) Ping(context.Context) error { return nil }

func get(t *testing.T, store *fakeStore, url string) (*http.Response, string) {
	t.Helper()
	app := httpserver.New(zap.NewNop(), httpserver.Health{Database: okPinger{}}, tracking.NewHandler(newService(store)).Register)
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, url, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp, string(body)
}

func TestLatestHandler(t *testing.T) {
	store := &fakeStore{rows: []tracking.Location{{VehicleID: "B1234XYZ", Latitude: -6.2088, Longitude: 106.8456, Timestamp: 1715003456}}}

	resp, body := get(t, store, "/vehicles/B1234XYZ/location")
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, body %s", resp.StatusCode, body)
	}

	want := `{"vehicle_id":"B1234XYZ","latitude":-6.2088,"longitude":106.8456,"timestamp":1715003456}`
	if body != want {
		t.Fatalf("body\n got  %s\n want %s", body, want)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type %q", ct)
	}
}

func TestLatestHandlerErrors(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		store    *fakeStore
		status   int
		wantCode string
	}{
		{"unknown vehicle", "/vehicles/NOPE/location", &fakeStore{}, 404, "not_found"},
		{"malformed vehicle id", "/vehicles/bad%20id/location", &fakeStore{}, 400, "invalid_request"},
		{"unknown route", "/nothing", &fakeStore{}, 404, "not_found"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, body := get(t, tt.store, tt.url)
			assertError(t, resp, body, tt.status, tt.wantCode)
		})
	}
}

func TestHistoryHandler(t *testing.T) {
	store := &fakeStore{rows: []tracking.Location{
		{VehicleID: "B1", Latitude: 1, Longitude: 2, Timestamp: 1000},
		{VehicleID: "B1", Latitude: 3, Longitude: 4, Timestamp: 1002},
	}}
	resp, body := get(t, store, "/vehicles/B1/history?start=1000&end=2000")
	if resp.StatusCode != 200 {
		t.Fatalf("status %d, body %s", resp.StatusCode, body)
	}
	want := `[{"vehicle_id":"B1","latitude":1,"longitude":2,"timestamp":1000},{"vehicle_id":"B1","latitude":3,"longitude":4,"timestamp":1002}]`
	if body != want {
		t.Fatalf("body\n got  %s\n want %s", body, want)
	}
	if store.gotStart != 1000 || store.gotEnd != 2000 {
		t.Fatalf("store queried [%d,%d]", store.gotStart, store.gotEnd)
	}
	if h := resp.Header.Get("X-Result-Truncated"); h != "" {
		t.Fatalf("unexpected truncation header %q", h)
	}
}

func TestHistoryHandlerEmptyIsArrayNot404(t *testing.T) {
	resp, body := get(t, &fakeStore{rows: []tracking.Location{}}, "/vehicles/B1/history?start=1&end=2")
	if resp.StatusCode != 200 || body != "[]" {
		t.Fatalf("status %d body %q, want 200 []", resp.StatusCode, body)
	}
}

func TestHistoryHandlerTruncationHeader(t *testing.T) {
	rows := make([]tracking.Location, tracking.MaxHistoryRows+1)
	resp, _ := get(t, &fakeStore{rows: rows}, "/vehicles/B1/history?start=1&end=2")
	if resp.StatusCode != 200 || resp.Header.Get("X-Result-Truncated") != "true" {
		t.Fatalf("status %d, truncated header %q", resp.StatusCode, resp.Header.Get("X-Result-Truncated"))
	}
}

func TestHistoryHandlerBadInput(t *testing.T) {
	urls := []string{
		"/vehicles/B1/history",
		"/vehicles/B1/history?start=1",
		"/vehicles/B1/history?end=2",
		"/vehicles/B1/history?start=abc&end=2",
		"/vehicles/B1/history?start=1&end=2.5",
		"/vehicles/B1/history?start=-1&end=2",
		"/vehicles/B1/history?start=5&end=2",
		"/vehicles/B1/history?start=0&end=99999999",
	}
	for _, url := range urls {
		t.Run(url, func(t *testing.T) {
			store := &fakeStore{}
			resp, body := get(t, store, url)
			assertError(t, resp, body, 400, "invalid_request")
			if store.historyHit {
				t.Fatal("store queried despite invalid input")
			}
		})
	}
}

func assertError(t *testing.T, resp *http.Response, body string, status int, code string) {
	t.Helper()
	if resp.StatusCode != status {
		t.Fatalf("status %d, want %d; body %s", resp.StatusCode, status, body)
	}
	var e struct {
		Error struct{ Code, Message string } `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &e); err != nil {
		t.Fatalf("error body is not JSON: %q", body)
	}
	if e.Error.Code != code || e.Error.Message == "" {
		t.Fatalf("error = %+v, want code %q and a message", e.Error, code)
	}
}

func TestHealthz(t *testing.T) {
	resp, body := get(t, &fakeStore{}, "/healthz")
	if resp.StatusCode != 200 || body != `{"checks":{"database":"ok"},"status":"ok"}` {
		t.Fatalf("status %d body %s", resp.StatusCode, body)
	}
}
