package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.Geofence.RadiusM != 50 {
		t.Errorf("radius = %v, want 50", c.Geofence.RadiusM)
	}
	if c.Geofence.CenterLat != -6.2088 || c.Geofence.CenterLon != 106.8456 {
		t.Errorf("unexpected default center %v", c.Geofence)
	}
	if c.OutboxRetention != 24*time.Hour {
		t.Errorf("outbox retention = %v, want 24h", c.OutboxRetention)
	}
}

func TestLoadRejectsBadValues(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantMsg string
	}{
		{"latitude out of range", map[string]string{"GEOFENCE_CENTER_LAT": "95"}, "GEOFENCE_CENTER_LAT"},
		{"longitude out of range", map[string]string{"GEOFENCE_CENTER_LON": "-181"}, "GEOFENCE_CENTER_LON"},
		{"non-numeric radius", map[string]string{"GEOFENCE_RADIUS_M": "abc"}, "GEOFENCE_RADIUS_M"},
		{"zero radius", map[string]string{"GEOFENCE_RADIUS_M": "0"}, "GEOFENCE_RADIUS_M"},
		{"NaN latitude passes ParseFloat", map[string]string{"GEOFENCE_CENTER_LAT": "NaN"}, "GEOFENCE_CENTER_LAT"},
		{"infinite radius", map[string]string{"GEOFENCE_RADIUS_M": "Inf"}, "GEOFENCE_RADIUS_M"},
		{"negative skew", map[string]string{"MAX_FUTURE_SKEW": "-5m"}, "MAX_FUTURE_SKEW"},
		{"zero history range", map[string]string{"MAX_HISTORY_RANGE": "0s"}, "MAX_HISTORY_RANGE"},
		{"bad duration", map[string]string{"OUTBOX_RETENTION": "soon"}, "OUTBOX_RETENTION"},
		{"unsupported broker scheme", map[string]string{"MQTT_BROKER_URL": "foo://broker:1883"}, "MQTT_BROKER_URL"},
		{"broker without scheme", map[string]string{"MQTT_BROKER_URL": "localhost:1883"}, "MQTT_BROKER_URL"},
		{"broker without host", map[string]string{"MQTT_BROKER_URL": "tcp://"}, "MQTT_BROKER_URL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("err = %v, want it to name %s", err, tt.wantMsg)
			}
		})
	}
}

func TestLoadReportsEverythingWrongAtOnce(t *testing.T) {
	t.Setenv("GEOFENCE_CENTER_LAT", "95")
	t.Setenv("MAX_FUTURE_SKEW", "-1s")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "GEOFENCE_CENTER_LAT") || !strings.Contains(err.Error(), "MAX_FUTURE_SKEW") {
		t.Fatalf("err = %v, want both problems listed", err)
	}
}

func TestLoadAcceptsBrokerSchemes(t *testing.T) {
	for _, u := range []string{"tcp://mosquitto:1883", "ssl://broker:8883", "ws://broker:9001/mqtt"} {
		t.Setenv("MQTT_BROKER_URL", u)
		if _, err := Load(); err != nil {
			t.Errorf("%s rejected: %v", u, err)
		}
	}
}

func TestRequireListsAllMissingInStableOrder(t *testing.T) {
	c := Config{}
	err := c.Require(map[string]string{"RABBITMQ_URL": "", "DATABASE_URL": "", "OK": "set"})
	if err == nil || err.Error() != "required setting(s) not set: DATABASE_URL, RABBITMQ_URL" {
		t.Fatalf("err = %v", err)
	}
	if err := c.Require(map[string]string{"A": "x"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
