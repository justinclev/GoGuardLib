package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/kafka/confluent"
)

// runOrders consumes the "orders" topic and calls the payments API for each message.
// When payments is down, messages are written to disk and finished after it recovers.
func runOrders(ctx context.Context, brokers string, payments *breaker.Breaker) error {
	store, err := dlq.OpenWAL("/var/lib/orders/dlq", dlq.WALOptions{})
	if err != nil {
		return fmt.Errorf("open dead-letter log: %w", err)
	}
	defer func() { _ = store.Close() }()

	orders, err := kafka.HandlerBinding(kafka.HandlerConfig{
		Topic:   "orders",
		Breaker: payments,
		Handle: func(ctx context.Context, m kafka.HandledMessage) error {
			return charge(ctx, m.ID, m.Value) // m.ID is stable across retries: use it as an idempotency key
		},
	})
	if err != nil {
		return err
	}

	client, err := confluent.NewClient(ck.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          "orders-service",
		"auto.offset.reset": "earliest",
	}, []string{"orders"})
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer func() { _ = client.Close() }()

	consumer, err := kafka.NewConsumer(kafka.Config{
		Client:   client,
		Store:    store,
		Bindings: []kafka.Binding{orders},
	})
	if err != nil {
		return err
	}

	redriver, err := dlq.NewRedriver(dlq.RedriveConfig{
		Store:    store,
		Handler:  orders.Pipeline.Handler(),
		Breakers: map[string]*breaker.Breaker{"payments": payments},
		Rate:     100, RampUp: 30 * time.Second,
	})
	if err != nil {
		return err
	}

	ctx, stop := context.WithCancel(ctx) // if one loop ends, stop the other
	defer stop()
	errc := make(chan error, 2)
	go func() { errc <- consumer.Run(ctx); stop() }()
	go func() { errc <- redriver.Run(ctx); stop() }()
	return errors.Join(<-errc, <-errc)
}

func charge(context.Context, string, []byte) error { return nil }

func Example_quickstart() {
	_ = runOrders // compiled, not run: it needs a broker
}
