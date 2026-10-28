package outbox

import (
	"testing"
	"time"
)

func TestNextWait(t *testing.T) {
	const base = time.Second
	tests := []struct {
		name    string
		current time.Duration
		failed  bool
		want    time.Duration
	}{
		{"success returns to the base interval", 16 * time.Second, false, base},
		{"first failure doubles the base", base, true, 2 * time.Second},
		{"repeated failures keep doubling", 8 * time.Second, true, 16 * time.Second},
		{"capped at the maximum", 20 * time.Second, true, maxBackoff},
		{"stays at the cap", maxBackoff, true, maxBackoff},
	}
	for _, tt := range tests {
		if got := nextWait(tt.current, base, tt.failed); got != tt.want {
			t.Errorf("%s: nextWait(%v, failed=%v) = %v, want %v", tt.name, tt.current, tt.failed, got, tt.want)
		}
	}
}
