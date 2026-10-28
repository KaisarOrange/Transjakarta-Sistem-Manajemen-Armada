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

type Publisher struct {
	url      string
	topology Topology
	log      *zap.Logger
	conn     *amqp.Connection
	ch       *amqp.Channel
	returns  chan amqp.Return
}

func NewPublisher(url string, l *zap.Logger, lc fx.Lifecycle) (*Publisher, error) {
	if err := ValidateURL(url); err != nil {
		return nil, err
	}
	p := &Publisher{url: url, topology: DefaultTopology(), log: l}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { p.reset(); return nil }})
	return p, nil
}

func (p *Publisher) Publish(ctx context.Context, messageID string, body []byte) error {
	if p.ch == nil || p.ch.IsClosed() {
		p.reset()
		if err := p.connect(); err != nil {
			return err
		}
	}
	conf, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, p.topology.Exchange, p.topology.RoutingKey,
		true,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    messageID,
			Timestamp:    time.Now(),
			Body:         body,
		})
	if err != nil {
		p.reset()
		return fmt.Errorf("publish: %w", err)
	}
	acked, err := conf.WaitContext(ctx)
	if err != nil {
		p.reset()
		return fmt.Errorf("wait for confirm: %w", err)
	}
	if !acked {
		p.reset()
		return errors.New("broker nacked the message")
	}

	select {
	case r, open := <-p.returns:
		if !open {
			p.reset()
			return nil
		}
		p.reset()
		return fmt.Errorf("message was not routed to any queue: %s", r.ReplyText)
	default:
		return nil
	}
}

func (p *Publisher) connect() error {
	conn, err := amqp.Dial(p.url)
	if err != nil {
		return fmt.Errorf("connect to rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("open channel: %w", err)
	}
	if err := p.topology.Declare(ch); err != nil {
		_ = conn.Close()
		return err
	}
	if err := ch.Confirm(false); err != nil {
		_ = conn.Close()
		return fmt.Errorf("enable publisher confirms: %w", err)
	}
	p.conn, p.ch = conn, ch
	p.returns = ch.NotifyReturn(make(chan amqp.Return, 1))
	p.log.Info("rabbitmq publisher connected")
	return nil
}

func (p *Publisher) reset() {
	if p.conn != nil {
		_ = p.conn.Close()
	}
	p.conn, p.ch, p.returns = nil, nil, nil
}
