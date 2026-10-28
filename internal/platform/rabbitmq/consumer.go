package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Handler func(messageID string, body []byte) error

const prefetch = 10

func StartConsumer(lc fx.Lifecycle, url string, l *zap.Logger, handle Handler) error {
	if err := ValidateURL(url); err != nil {
		return err
	}
	topology := DefaultTopology()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			go func() {
				defer close(done)
				backoff := time.Second
				for ctx.Err() == nil {
					connected, err := consumeOnce(ctx, url, topology, l, handle)
					if ctx.Err() != nil {
						return
					}
					if connected {
						backoff = time.Second
					}
					l.Warn("rabbitmq consumer stopped, reconnecting", zap.Error(err), zap.Duration("in", backoff))
					select {
					case <-time.After(backoff):
						backoff = min(backoff*2, 30*time.Second)
					case <-ctx.Done():
						return
					}
				}
			}()
			return nil
		},
		OnStop: func(stopCtx context.Context) error {
			cancel()
			select {
			case <-done:
				return nil
			case <-stopCtx.Done():
				return stopCtx.Err()
			}
		},
	})
	return nil
}

func consumeOnce(ctx context.Context, url string, topology Topology, l *zap.Logger, handle Handler) (connected bool, err error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return false, fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return false, fmt.Errorf("open channel: %w", err)
	}
	if err := topology.Declare(ch); err != nil {
		return false, err
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return false, fmt.Errorf("set prefetch: %w", err)
	}
	const tag = "geofence-worker"
	deliveries, err := ch.Consume(topology.Queue, tag, false, false, false, false, nil)
	if err != nil {
		return false, fmt.Errorf("consume: %w", err)
	}
	l.Info("rabbitmq consumer ready", zap.String("queue", topology.Queue), zap.Int("prefetch", prefetch))

	cctx, endCtx := context.WithCancel(ctx)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		<-cctx.Done()
		_ = ch.Cancel(tag, false)
	}()
	defer func() { endCtx(); <-stopped }()

	for d := range deliveries {
		if herr := handle(d.MessageId, d.Body); herr != nil {
			l.Warn("message rejected and dead-lettered", zap.String("message_id", d.MessageId), zap.String("dead_letter_queue", topology.DeadLetterQueue), zap.Error(herr))
			_ = d.Nack(false, false)
			continue
		}
		_ = d.Ack(false)
	}
	if ctx.Err() != nil {
		return true, nil
	}
	return true, errors.New("deliveries channel closed (connection lost)")
}
