package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/config"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/geofence"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/httpserver"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/ingest"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/logger"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/outbox"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/platform/mqttclient"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/platform/postgres"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/platform/rabbitmq"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/tracking"
)

func main() {
	cfg, l, err := bootstrap()
	if err != nil {
		fmt.Fprintln(os.Stderr, "api:", err)
		os.Exit(1)
	}
	defer l.Sync()

	fx.New(
		fx.Supply(cfg, l),
		fx.WithLogger(logger.FxEvents),

		fx.StopTimeout(30*time.Second),
		fx.Provide(
			newPool,
			tracking.NewRepository,
			newTrackingService,
			tracking.NewHandler,
			newSubscriber,
			newMQTT,
			newPublisher,
			newRelay,
			newHTTPApp,
		),
		fx.Invoke(func(app *fiber.App, c config.Config, l *zap.Logger, lc fx.Lifecycle) {
			httpserver.Serve(lc, app, c.HTTPAddr, l)
		}),
		fx.Invoke(func(r *outbox.Relay, lc fx.Lifecycle) { r.Register(lc) }),
	).Run()
}

func newTrackingService(r *tracking.Repository, c config.Config) *tracking.Service {
	fence := geofence.Fence{CenterLat: c.Geofence.CenterLat, CenterLon: c.Geofence.CenterLon, RadiusM: c.Geofence.RadiusM}
	return tracking.NewService(r, fence, c.MaxFutureSkew, c.MaxHistoryRange)
}

func newSubscriber(svc *tracking.Service, l *zap.Logger, lc fx.Lifecycle) *ingest.Subscriber {
	return ingest.New(svc, l, lc)
}

func newMQTT(c config.Config, l *zap.Logger, lc fx.Lifecycle, sub *ingest.Subscriber) *mqttclient.Client {
	return mqttclient.Start(lc, mqttclient.Config{
		BrokerURL: c.MQTTBrokerURL,
		ClientID:  c.MQTTClientID,
		Username:  c.MQTTUsername,
		Password:  c.MQTTPassword,
	}, l, ingest.TopicFilter, sub.Handle)
}

func newHTTPApp(l *zap.Logger, pool *pgxpool.Pool, h *tracking.Handler, mqtt *mqttclient.Client, relay *outbox.Relay) *fiber.App {
	return httpserver.New(l, httpserver.Health{
		Database:      pool,
		MQTTConnected: mqtt.IsConnected,
		OutboxPending: relay.Pending,
	}, h.Register)
}

func bootstrap() (config.Config, *zap.Logger, error) {
	c, err := config.Load()
	if err != nil {
		return c, nil, err
	}
	if err := c.Require(map[string]string{"DATABASE_URL": c.DatabaseURL, "RABBITMQ_URL": c.RabbitURL}); err != nil {
		return c, nil, err
	}
	if err := rabbitmq.ValidateURL(c.RabbitURL); err != nil {
		return c, nil, err
	}
	l, err := logger.New(c.LogLevel)
	return c, l, err
}

func newPool(c config.Config, l *zap.Logger, lc fx.Lifecycle) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := postgres.Migrate(c.DatabaseURL); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	pool, err := postgres.NewPool(ctx, c.DatabaseURL)
	if err != nil {
		return nil, err
	}
	l.Info("database ready")
	lc.Append(fx.Hook{OnStop: func(context.Context) error { pool.Close(); return nil }})
	return pool, nil
}

func newPublisher(c config.Config, l *zap.Logger, lc fx.Lifecycle) (*rabbitmq.Publisher, error) {
	return rabbitmq.NewPublisher(c.RabbitURL, l, lc)
}

func newRelay(c config.Config, pool *pgxpool.Pool, pub *rabbitmq.Publisher, l *zap.Logger) *outbox.Relay {
	return outbox.NewRelay(pool, pub, l, c.OutboxRetention)
}
