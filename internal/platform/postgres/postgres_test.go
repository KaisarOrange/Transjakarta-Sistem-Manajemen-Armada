package postgres

import "testing"

func TestMigrateURL(t *testing.T) {
	tests := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"postgres://fleet:fleet@localhost:5432/fleet?sslmode=disable", "pgx5://fleet:fleet@localhost:5432/fleet?sslmode=disable", false},
		{"postgresql://u:p@db/fleet", "pgx5://u:p@db/fleet", false},
		{"", "", true},
		{"short", "", true},
		{"mysql://u:p@db/x", "", true},
		{"postgres:/missing-slash", "", true},
	}
	for _, tt := range tests {
		got, err := migrateURL(tt.in)
		if (err != nil) != tt.wantErr || got != tt.want {
			t.Errorf("migrateURL(%q) = %q, %v; want %q (err %v)", tt.in, got, err, tt.want, tt.wantErr)
		}
	}
}
