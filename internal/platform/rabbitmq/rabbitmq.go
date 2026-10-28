package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	Exchange        = "fleet.events"
	Queue           = "geofence_alerts"
	RoutingKeyEntry = "geofence.entry"

	DeadLetterExchange = "fleet.events.dlx"
	DeadLetterQueue    = "geofence_alerts.dlq"
)

type Topology struct {
	Exchange, Queue, RoutingKey         string
	DeadLetterExchange, DeadLetterQueue string
}

func DefaultTopology() Topology {
	return Topology{
		Exchange: Exchange, Queue: Queue, RoutingKey: RoutingKeyEntry,
		DeadLetterExchange: DeadLetterExchange, DeadLetterQueue: DeadLetterQueue,
	}
}

func (t Topology) Declare(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(t.DeadLetterExchange, "fanout", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dead-letter exchange: %w", err)
	}
	if _, err := ch.QueueDeclare(t.DeadLetterQueue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dead-letter queue: %w", err)
	}
	if err := ch.QueueBind(t.DeadLetterQueue, "", t.DeadLetterExchange, false, nil); err != nil {
		return fmt.Errorf("bind dead-letter queue: %w", err)
	}

	if err := ch.ExchangeDeclare(t.Exchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}
	args := amqp.Table{"x-dead-letter-exchange": t.DeadLetterExchange}
	if _, err := ch.QueueDeclare(t.Queue, true, false, false, false, args); err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}
	if err := ch.QueueBind(t.Queue, t.RoutingKey, t.Exchange, false, nil); err != nil {
		return fmt.Errorf("bind queue: %w", err)
	}
	return nil
}

func ValidateURL(raw string) error {
	if _, err := amqp.ParseURI(raw); err != nil {
		return fmt.Errorf("RABBITMQ_URL is not a valid amqp:// or amqps:// URL")
	}
	return nil
}
