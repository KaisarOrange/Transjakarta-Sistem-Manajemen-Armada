package main

import (
	"fmt"
	"os"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/alert"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/config"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/logger"
	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/platform/rabbitmq"
)

func main() {
	cfg, l, err := bootstrap()
	if err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
	defer l.Sync()

	fx.New(
		fx.Supply(cfg, l),
		fx.WithLogger(logger.FxEvents),
		fx.Provide(alert.NewHandler),
		fx.Invoke(func(c config.Config, l *zap.Logger, h *alert.Handler, lc fx.Lifecycle) error {
			return rabbitmq.StartConsumer(lc, c.RabbitURL, l, h.Handle)
		}),
	).Run()
}

func bootstrap() (config.Config, *zap.Logger, error) {
	c, err := config.Load()
	if err != nil {
		return c, nil, err
	}
	if err := c.Require(map[string]string{"RABBITMQ_URL": c.RabbitURL}); err != nil {
		return c, nil, err
	}
	if err := rabbitmq.ValidateURL(c.RabbitURL); err != nil {
		return c, nil, err
	}
	l, err := logger.New(c.LogLevel)
	return c, l, err
}
