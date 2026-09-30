package kafka_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/health"
	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/kafka/confluent"
	"github.com/justinclev/GoGuardLib/retry"
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

// newRedriver is the README's "several topics" example.
func newRedriver(store dlq.Store, bindings []kafka.Binding) (*dlq.Redriver, error) {
	rd, err := kafka.RedriveFor(bindings)
	if err != nil {
		return nil, err
	}
	return dlq.NewRedriver(dlq.RedriveConfig{Store: store, Handler: rd.Handler, Breakers: rd.Breakers})
}

func Example_readmeSeveralTopics() {
	_ = newRedriver
}

// chargePayments sends one order to the payments service. A 4xx means the order itself is wrong
// (a declined card, say), so retrying can never help: it is marked permanent and set aside for a
// person. A 5xx or a connection error is temporary.
func chargePayments(ctx context.Context, url string, m kafka.HandledMessage) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/charge", bytes.NewReader(m.Value))
	if err != nil {
		return retry.Permanent(err)
	}
	req.Header.Set("Idempotency-Key", m.ID) // the same on every attempt, so a repeat is recognised
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode < 300:
		return nil
	case resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests:
		return retry.Permanent(fmt.Errorf("payments answered %d", resp.StatusCode))
	default:
		return fmt.Errorf("payments answered %d", resp.StatusCode)
	}
}

// runGuardedOrders is the README's "in a real service" example: the same consumer as runOrders,
// with a circuit breaker on the call to payments.
func runGuardedOrders(ctx context.Context, brokers, dataDir, paymentsURL string) error {
	store, err := dlq.OpenWAL(dataDir, dlq.WALOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	// Ask payments' own health endpoint whether it is back, instead of guessing.
	check, err := health.HTTP(paymentsURL + "/health")
	if err != nil {
		return err
	}
	payments := breaker.New(breaker.Config{
		Name:             "payments",
		FailureThreshold: 0.5,                                                     // open when half of the recent calls fail...
		MinSamples:       20,                                                      // ...but only after at least 20 calls
		IsFailure:        func(err error) bool { return !retry.IsPermanent(err) }, // a declined card is not an outage
		Health:           &health.Config{Check: check},
	})
	defer payments.Close()

	orders, err := kafka.HandlerBinding(kafka.HandlerConfig{
		Topic:   "orders",
		Breaker: payments, // while this circuit is open the topic pauses and messages wait in Kafka
		Timeout: 5 * time.Second,
		Handle: func(ctx context.Context, m kafka.HandledMessage) error {
			return chargePayments(ctx, paymentsURL, m)
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
		Workers:  4, // messages handled at once; offsets are still committed in order
	})
	if err != nil {
		return err
	}

	// The redriver retries saved orders. Given the breaker, it leaves them alone while the circuit
	// is open, so a long outage does not use up their retries.
	redriver, err := dlq.NewRedriver(dlq.RedriveConfig{
		Store:    store,
		Handler:  orders.Pipeline.Handler(),
		Breakers: map[string]*breaker.Breaker{"payments": payments},
	})
	if err != nil {
		return err
	}

	// Run both until the context ends or one of them stops with an error.
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	errs := make(chan error, 2)
	go func() { errs <- redriver.Run(ctx) }()
	go func() { errs <- consumer.Run(ctx) }()
	err = <-errs
	stop()
	<-errs
	return err
}

func Example_readmeGuarded() {
	_ = runGuardedOrders
}
