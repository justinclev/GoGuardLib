package kafka_test

import (
	"bytes"
	"context"
	"net/http"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	goguard "github.com/justinclev/GoGuardLib"
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
	return kafka.Run(ctx, kafka.ServiceConfig{
		NewClient: confluent.NewClientFunc(ck.ConfigMap{"bootstrap.servers": brokers, "group.id": "orders-service"}),
		DataDir:   dataDir, // orders that fail wait here, on disk
		Handlers: []kafka.HandlerConfig{
			{Topic: "orders", Handle: kafka.HandleValue(chargeOrder)}, // an error: saved, retried later
		},
	})
}

func Example_readmeSimple() {
	_ = runOrders
}

// runOrdersByHand is the same service wired piece by piece, for when you want to own each part.
func runOrdersByHand(ctx context.Context, brokers, dataDir string) error {
	store, err := dlq.OpenWAL(dataDir, dlq.WALOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	orders, err := kafka.HandlerBinding(kafka.HandlerConfig{
		Topic: "orders",
		Handle: func(ctx context.Context, m kafka.HandledMessage) error {
			return chargeOrder(ctx, m.Value)
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

	consumer, err := kafka.NewConsumer(kafka.Config{Client: client, Store: store, Bindings: []kafka.Binding{orders}})
	if err != nil {
		return err
	}
	redriver, err := dlq.NewRedriver(dlq.RedriveConfig{Store: store, Handler: orders.Pipeline.Handler()})
	if err != nil {
		return err
	}
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = redriver.Run(ctx) }()
	return consumer.Run(ctx)
}

func Example_readmeByHand() {
	_ = runOrdersByHand
}

// newRedriver is the README's "several topics" example.
func newRedriver(store dlq.Store, bindings []kafka.Binding) (*dlq.Redriver, error) {
	rd, err := kafka.RedriveFor(bindings)
	if err != nil {
		return nil, err
	}
	return dlq.NewRedriver(dlq.RedriveConfig{Store: store, Handler: rd.Handler, Breakers: rd.Breakers})
}

// runTwoTopics is the README's "several topics" example: each topic has its own handler, and a
// saved message is finished by the handler of its own topic.
func runTwoTopics(ctx context.Context, cfg kafka.ServiceConfig, refund func(context.Context, []byte) error) error {
	cfg.Handlers = append(cfg.Handlers, kafka.HandlerConfig{Topic: "refunds", Handle: kafka.HandleValue(refund)})
	return kafka.Run(ctx, cfg)
}

func Example_readmeSeveralTopics() {
	_ = newRedriver
	_ = runTwoTopics
}

// chargePayments sends one order to the payments service. retry.FromHTTP sorts the answer: success,
// temporary (a 5xx, a timeout) or permanent (a declined card), which is set aside for a person.
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
	return retry.FromHTTP(resp, nil)
}

// runGuardedOrders is the README's "in a real service" example: the same service, with a circuit
// breaker on the call to payments that asks payments' health endpoint when it is back.
func runGuardedOrders(ctx context.Context, brokers, dataDir, paymentsURL string) error {
	return kafka.Run(ctx, kafka.ServiceConfig{
		NewClient: confluent.NewClientFunc(ck.ConfigMap{"bootstrap.servers": brokers, "group.id": "orders-service"}),
		DataDir:   dataDir,
		Handlers: []kafka.HandlerConfig{{
			Topic:      "orders",
			HealthURL:  paymentsURL + "/health", // the topic pauses while payments is down
			Dependency: "payments",
			Timeout:    5 * time.Second,
			Handle: func(ctx context.Context, m kafka.HandledMessage) error {
				return chargePayments(ctx, paymentsURL, m)
			},
		}},
		Consumer: kafka.Config{Workers: 4}, // four orders at once; offsets are still committed in order
	})
}

// runTunedOrders builds the breaker itself, for thresholds and health settings of your own.
func runTunedOrders(ctx context.Context, brokers, dataDir, paymentsURL string) error {
	payments := breaker.New(breaker.Config{
		Name:             "payments",
		FailureThreshold: 0.3,
		MinSamples:       50,
		IsFailure:        retry.IsOutage,
		Health:           health.MustURL(paymentsURL + "/health"),
	})
	defer payments.Close()
	return kafka.Run(ctx, kafka.ServiceConfig{
		NewClient: confluent.NewClientFunc(ck.ConfigMap{"bootstrap.servers": brokers, "group.id": "orders-service"}),
		DataDir:   dataDir,
		Handlers: []kafka.HandlerConfig{{
			Topic: "orders", Breaker: payments,
			Handle: func(ctx context.Context, m kafka.HandledMessage) error { return chargePayments(ctx, paymentsURL, m) },
		}},
	})
}

func Example_readmeGuarded() {
	_ = runGuardedOrders
	_ = runTunedOrders
}

// runWithWebhooks adds saved HTTP requests to the same service: one guarded client, and the service
// sends what could not be sent once the partner is back.
func runWithWebhooks(ctx context.Context, brokers, dataDir string) error {
	hooks, err := goguard.NewClient(goguard.Policy{HealthPath: "/health", Defer: &goguard.Defer{}})
	if err != nil {
		return err
	}
	defer func() { _ = hooks.Close() }()
	return kafka.Run(ctx, kafka.ServiceConfig{
		NewClient: confluent.NewClientFunc(ck.ConfigMap{"bootstrap.servers": brokers, "group.id": "orders-service"}),
		DataDir:   dataDir,
		HTTP:      hooks, // saved requests go into the service's log and are sent from it
		Handlers:  []kafka.HandlerConfig{{Topic: "orders", Handle: kafka.HandleValue(chargeOrder)}},
	})
}

func Example_readmeWebhooks() {
	_ = runWithWebhooks
}

// proposalExample is the short version used in the proposal; runGuardedOrders is the same thing
// with the supporting code.
func proposalExample(ctx context.Context, newClient func([]string) (kafka.Client, error), dataDir, paymentsURL string) error {
	return kafka.Run(ctx, kafka.ServiceConfig{
		NewClient: newClient,
		DataDir:   dataDir, // failed messages wait here, on disk
		Handlers: []kafka.HandlerConfig{{
			Topic:     "orders",
			HealthURL: paymentsURL + "/health",        // the topic pauses while payments is down
			Handle:    kafka.HandleValue(chargeOrder), // return an error: saved to disk, retried later
		}},
	})
}

func Example_readmeProposal() {
	_ = proposalExample
}
