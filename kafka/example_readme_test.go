package kafka_test

import (
	"context"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/kafka/confluent"
)

// chargeOrder stands in for your code that handles one order.
func chargeOrder(ctx context.Context, order []byte) error { return nil }

// This is the README's simple Kafka example. It needs a broker to run, so it only
// has to compile.
func runOrders(ctx context.Context, brokers, dataDir string) error {
	// Where orders wait, on disk, when something goes wrong.
	store, err := dlq.OpenWAL(dataDir, dlq.WALOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	// What to do with each order.
	orders, err := kafka.HandlerBinding(kafka.HandlerConfig{
		Topic: "orders",
		Handle: func(ctx context.Context, m kafka.HandledMessage) error {
			return chargeOrder(ctx, m.Value) // return an error and the order is saved and retried later
		},
	})
	if err != nil {
		return err
	}

	client, err := confluent.NewClient(ck.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          "orders-service",
	}, []string{"orders"})
	if err != nil {
		return err
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

	// The redriver retries saved orders in the background.
	redriver, err := dlq.NewRedriver(dlq.RedriveConfig{Store: store, Handler: orders.Pipeline.Handler()})
	if err != nil {
		return err
	}
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = redriver.Run(ctx) }()

	return consumer.Run(ctx)
}

func Example_readmeSimple() {
	_ = runOrders
}
