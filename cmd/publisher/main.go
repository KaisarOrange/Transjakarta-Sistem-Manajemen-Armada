package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"

	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/config"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/geofence"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/logger"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/platform/mqttclient"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/simulator"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "publisher:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	l, err := logger.New(cfg.LogLevel)
	if err != nil {
		return err
	}
	defer l.Sync()

	interval := 2 * time.Second
	if v := os.Getenv("PUBLISH_INTERVAL"); v != "" {
		if interval, err = time.ParseDuration(v); err != nil || interval <= 0 {
			return fmt.Errorf("PUBLISH_INTERVAL must be a positive duration such as 2s")
		}
	}
	clientID := os.Getenv("PUBLISHER_CLIENT_ID")
	if clientID == "" {
		clientID = "fleet-publisher"
	}
	fence := geofence.Fence{CenterLat: cfg.Geofence.CenterLat, CenterLon: cfg.Geofence.CenterLon, RadiusM: cfg.Geofence.RadiusM}

	opts := paho.NewClientOptions().
		AddBroker(cfg.MQTTBrokerURL).
		SetClientID(clientID).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(2 * time.Second).
		SetConnectionLostHandler(func(_ paho.Client, err error) { l.Warn("mqtt connection lost", zap.Error(err)) })
	if cfg.MQTTUsername != "" {
		opts.SetUsername(cfg.MQTTUsername).SetPassword(cfg.MQTTPassword)
	}
	client := paho.NewClient(opts)

	l.Info("connecting", zap.String("broker", mqttclient.RedactURL(cfg.MQTTBrokerURL)))
	if tok := client.Connect(); tok.Wait() && tok.Error() != nil {
		return fmt.Errorf("connect: %w", tok.Error())
	}
	defer client.Disconnect(500)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	vehicles := simulator.Vehicles()
	l.Info("publishing", zap.Duration("interval", interval), zap.Int("vehicles", len(vehicles)),
		zap.Float64("center_lat", fence.CenterLat), zap.Float64("center_lon", fence.CenterLon), zap.Float64("radius_m", fence.RadiusM))

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for tick := 0; ; tick++ {
		for _, v := range vehicles {
			msg, err := v.Next(fence, tick, time.Now())
			if err != nil {
				return err
			}
			tok := client.Publish(msg.Topic, 1, false, msg.Payload)
			if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
				l.Warn("publish failed", zap.String("topic", msg.Topic), zap.Error(tok.Error()))
				continue
			}
			l.Info("published", zap.String("topic", msg.Topic), zap.ByteString("payload", msg.Payload))
		}
		select {
		case <-ctx.Done():
			l.Info("shutting down")
			return nil
		case <-ticker.C:
		}
	}
}
