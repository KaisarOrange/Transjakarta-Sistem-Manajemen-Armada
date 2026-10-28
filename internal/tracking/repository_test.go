package tracking

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestPermanentAsInvalid(t *testing.T) {
	pg := func(code string) error {
		return fmt.Errorf("insert location: %w", &pgconn.PgError{Code: code, Message: "secret row data"})
	}
	tests := []struct {
		name        string
		err         error
		wantInvalid bool
	}{
		{"check violation", pg("23514"), true},
		{"not-null violation", pg("23502"), true},
		{"numeric out of range", pg("22003"), true},
		{"connection failure stays retryable", pg("08006"), false},
		{"serialization failure stays retryable", pg("40001"), false},
		{"shutdown in progress stays retryable", pg("57P01"), false},
		{"plain network error stays retryable", errors.New("connection refused"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := permanentAsInvalid(tt.err)
			var verr *ValidationError
			if gotInvalid := errors.As(got, &verr); gotInvalid != tt.wantInvalid {
				t.Fatalf("invalid = %v, want %v (err %v)", gotInvalid, tt.wantInvalid, got)
			}
			if tt.wantInvalid && strings.Contains(verr.Msg, "secret row data") {
				t.Fatalf("message leaks driver text: %q", verr.Msg)
			}
			if !tt.wantInvalid && got != tt.err {
				t.Fatalf("retryable error was altered: %v", got)
			}
		})
	}
}
