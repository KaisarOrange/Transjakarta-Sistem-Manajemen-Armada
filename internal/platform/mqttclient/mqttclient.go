package mqttclient

import (
	"context"
	"fmt"
	"net/url"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Config struct {
	BrokerURL string

	ClientID string

	Username string
	Password string
}

type Client struct{ c paho.Client }

func (c *Client) IsConnected() bool { return c.c.IsConnectionOpen() }

func RedactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "<invalid url>"
	}
	return u.Scheme + "://" + u.Host
}

func Start(lc fx.Lifecycle, cfg Config, l *zap.Logger, filter string, handler func(topic string, payload []byte)) *Client {
	broker := RedactURL(cfg.BrokerURL)
	opts := paho.NewClientOptions().
		AddBroker(cfg.BrokerURL).
		SetClientID(cfg.ClientID).
		SetCleanSession(false).
		SetOrderMatters(true).
		SetKeepAlive(30 * time.Second).
		SetConnectTimeout(5 * time.Second).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(2 * time.Second).
		SetMaxReconnectInterval(30 * time.Second).
		SetConnectionLostHandler(func(_ paho.Client, err error) {
			l.Warn("mqtt connection lost, reconnecting", zap.Error(err))
		}).
		SetOnConnectHandler(func(c paho.Client) {
			tok := c.Subscribe(filter, 1, func(_ paho.Client, m paho.Message) { handler(m.Topic(), m.Payload()) })
			if !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
				l.Error("mqtt subscribe failed", zap.String("filter", filter), zap.Error(tok.Error()))
				return
			}
			l.Info("mqtt subscribed", zap.String("broker", broker), zap.String("filter", filter))
		})
	if cfg.Username != "" {
		opts.SetUsername(cfg.Username).SetPassword(cfg.Password)
	}

	std := zap.NewStdLog(l.With(zap.String("component", "paho")))
	paho.WARN, paho.ERROR, paho.CRITICAL = std, std, std

	client := &Client{c: paho.NewClient(opts)}

	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			tok := client.c.Connect()
			if !tok.WaitTimeout(5 * time.Second) {
				l.Warn("mqtt not connected yet, retrying in the background (check the broker address and credentials)",
					zap.String("broker", broker))
				return nil
			}

			if err := tok.Error(); err != nil {
				return fmt.Errorf("connect to mqtt broker %s: %w", broker, err)
			}
			return nil
		},
		OnStop: func(context.Context) error {
			client.c.Disconnect(1000)
			return nil
		},
	})
	return client
}
