package mqttclient

import (
	"strings"
	"testing"
)

func TestRedactURLNeverLeaksCredentials(t *testing.T) {
	tests := []struct{ in, want string }{
		{"tcp://mosquitto:1883", "tcp://mosquitto:1883"},
		{"tcp://user:s3cret@mosquitto:1883", "tcp://mosquitto:1883"},
		{"ssl://admin:p%40ss@broker.example.com:8883/path?x=1", "ssl://broker.example.com:8883"},
		{"not a url", "<invalid url>"},
		{"", "<invalid url>"},
	}
	for _, tt := range tests {
		got := RedactURL(tt.in)
		if got != tt.want {
			t.Errorf("RedactURL(%q) = %q, want %q", tt.in, got, tt.want)
		}
		if strings.Contains(got, "s3cret") || strings.Contains(got, "p%40ss") {
			t.Errorf("RedactURL(%q) leaked a password: %q", tt.in, got)
		}
	}
}
