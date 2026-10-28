//go:build integration

package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type received struct{ messageID, body string }

func rabbitURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		t.Skip("RABBITMQ_URL not set")
	}
	return url
}

func testTopology(t *testing.T, url string) Topology {
	t.Helper()
	id := fmt.Sprintf("test-%d", time.Now().UnixNano())
	tp := Topology{
		Exchange: id + ".ex", Queue: id + ".q", RoutingKey: id + ".key",
		DeadLetterExchange: id + ".dlx", DeadLetterQueue: id + ".dlq",
	}
	t.Cleanup(func() {
		conn, err := amqp.Dial(url)
		if err != nil {
			return
		}
		defer conn.Close()
		ch, err := conn.Channel()
		if err != nil {
			return
		}
		for _, q := range []string{tp.Queue, tp.DeadLetterQueue} {
			_, _ = ch.QueueDelete(q, false, false, false)
		}
		for _, e := range []string{tp.Exchange, tp.DeadLetterExchange} {
			_ = ch.ExchangeDelete(e, false, false)
		}
	})
	return tp
}

func newTestPublisher(url string, tp Topology) *Publisher {
	return &Publisher{url: url, topology: tp, log: zap.NewNop()}
}

func startConsumer(t *testing.T, url string, tp Topology, handle Handler) (<-chan received, func()) {
	t.Helper()
	got := make(chan received, 10)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = consumeOnce(ctx, url, tp, zap.NewNop(), func(id string, body []byte) error {
			got <- received{id, string(body)}
			return handle(id, body)
		})
	}()
	stop := func() { cancel(); <-done }
	t.Cleanup(stop)
	return got, stop
}

func waitFor(t *testing.T, ch <-chan received) received {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for a message")
		return received{}
	}
}

func TestPublishedMessageIsConsumedWithExactBodyAndMessageID(t *testing.T) {
	url := rabbitURL(t)
	tp := testTopology(t, url)
	pub := newTestPublisher(url, tp)
	t.Cleanup(pub.reset)

	got, _ := startConsumer(t, url, tp, func(string, []byte) error { return nil })
	time.Sleep(500 * time.Millisecond)

	const body = `{"vehicle_id":"B1234XYZ","event":"geofence_entry","location":{"latitude":-6.2088,"longitude":106.8456},"timestamp":1715003456}`
	if err := pub.Publish(context.Background(), "B1234XYZ:1715003456", []byte(body)); err != nil {
		t.Fatal(err)
	}
	r := waitFor(t, got)
	if r.body != body || r.messageID != "B1234XYZ:1715003456" {
		t.Fatalf("got %+v, want exact body and message id", r)
	}
}

func TestPublishFailsWhenNoQueueIsBoundThenRepairsItself(t *testing.T) {
	url := rabbitURL(t)
	tp := testTopology(t, url)
	pub := newTestPublisher(url, tp)
	t.Cleanup(pub.reset)

	if err := pub.Publish(context.Background(), "m1", []byte(`{}`)); err != nil {
		t.Fatalf("first publish: %v", err)
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, _ := conn.Channel()
	if _, err := ch.QueueDelete(tp.Queue, false, false, false); err != nil {
		t.Fatal(err)
	}

	err = pub.Publish(context.Background(), "m2", []byte(`{}`))
	if err == nil || !strings.Contains(err.Error(), "not routed") {
		t.Fatalf("publish to an unbound exchange: err = %v, want a 'not routed' error (it must not look like success)", err)
	}

	if err := pub.Publish(context.Background(), "m2", []byte(`{}`)); err != nil {
		t.Fatalf("retry after repair: %v", err)
	}
}

func TestRejectedMessageIsDeadLetteredNotDiscarded(t *testing.T) {
	url := rabbitURL(t)
	tp := testTopology(t, url)
	pub := newTestPublisher(url, tp)
	t.Cleanup(pub.reset)

	got, _ := startConsumer(t, url, tp, func(string, []byte) error { return errors.New("malformed") })
	time.Sleep(500 * time.Millisecond)

	if err := pub.Publish(context.Background(), "bad-1", []byte(`not json`)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, got)

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, _ := conn.Channel()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if d, ok, _ := ch.Get(tp.DeadLetterQueue, true); ok {
			if string(d.Body) != "not json" || d.MessageId != "bad-1" {
				t.Fatalf("dead-lettered message = %q (%s), want the original", d.Body, d.MessageId)
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("rejected message never reached the dead-letter queue")
}
