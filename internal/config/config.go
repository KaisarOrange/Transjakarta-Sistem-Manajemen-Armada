package config

import (
	"fmt"
	"math"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	LogLevel string

	HTTPAddr string

	DatabaseURL string

	MQTTBrokerURL string
	MQTTClientID  string

	MQTTUsername string
	MQTTPassword string

	RabbitURL string

	Geofence Geofence

	MaxFutureSkew time.Duration

	MaxHistoryRange time.Duration

	OutboxRetention time.Duration
}

type Geofence struct {
	CenterLat float64
	CenterLon float64
	RadiusM   float64
}

var mqttSchemes = []string{"tcp", "mqtt", "ssl", "tls", "mqtts", "ws", "wss"}

func Load() (Config, error) {
	var errs []string
	f := func(key string, def float64) float64 {
		v := os.Getenv(key)
		if v == "" {
			return def
		}
		n, err := strconv.ParseFloat(v, 64)

		if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
			errs = append(errs, fmt.Sprintf("%s: not a finite number", key))
			return def
		}
		return n
	}

	d := func(key string, def time.Duration) time.Duration {
		v := os.Getenv(key)
		if v == "" {
			return def
		}
		n, err := time.ParseDuration(v)
		if err != nil || n <= 0 {
			errs = append(errs, fmt.Sprintf("%s: not a positive duration (e.g. 5m)", key))
			return def
		}
		return n
	}

	c := Config{
		LogLevel:      get("LOG_LEVEL", "info"),
		HTTPAddr:      get("HTTP_ADDR", ":8080"),
		DatabaseURL:   os.Getenv("DATABASE_URL"),
		MQTTBrokerURL: get("MQTT_BROKER_URL", "tcp://localhost:1883"),
		MQTTClientID:  get("MQTT_CLIENT_ID", "fleet-api"),
		MQTTUsername:  os.Getenv("MQTT_USERNAME"),
		MQTTPassword:  os.Getenv("MQTT_PASSWORD"),
		RabbitURL:     os.Getenv("RABBITMQ_URL"),
		Geofence: Geofence{
			CenterLat: f("GEOFENCE_CENTER_LAT", -6.2088),
			CenterLon: f("GEOFENCE_CENTER_LON", 106.8456),
			RadiusM:   f("GEOFENCE_RADIUS_M", 50),
		},
		MaxFutureSkew:   d("MAX_FUTURE_SKEW", 5*time.Minute),
		MaxHistoryRange: d("MAX_HISTORY_RANGE", 31*24*time.Hour),
		OutboxRetention: d("OUTBOX_RETENTION", 24*time.Hour),
	}

	g := c.Geofence
	if g.CenterLat < -90 || g.CenterLat > 90 {
		errs = append(errs, "GEOFENCE_CENTER_LAT must be within [-90, 90]")
	}
	if g.CenterLon < -180 || g.CenterLon > 180 {
		errs = append(errs, "GEOFENCE_CENTER_LON must be within [-180, 180]")
	}
	if g.RadiusM <= 0 {
		errs = append(errs, "GEOFENCE_RADIUS_M must be positive")
	}
	if err := validateMQTTURL(c.MQTTBrokerURL); err != nil {
		errs = append(errs, "MQTT_BROKER_URL: "+err.Error())
	}
	if len(errs) > 0 {
		return Config{}, fmt.Errorf("invalid configuration: %s", strings.Join(errs, "; "))
	}
	return c, nil
}

func validateMQTTURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("not a valid URL")
	}
	if !slices.Contains(mqttSchemes, u.Scheme) {
		return fmt.Errorf("scheme must be one of %s (e.g. tcp://host:1883)", strings.Join(mqttSchemes, ", "))
	}
	if u.Host == "" {
		return fmt.Errorf("missing host")
	}
	return nil
}

func (c Config) Require(names map[string]string) error {
	var missing []string
	for name, val := range names {
		if val == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	slices.Sort(missing)
	return fmt.Errorf("required setting(s) not set: %s", strings.Join(missing, ", "))
}

func get(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
