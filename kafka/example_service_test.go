package kafka_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/health"
	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/kafka/confluent"
	"github.com/justinclev/GoGuardLib/pipeline"
	"github.com/justinclev/GoGuardLib/retry"
	"github.com/justinclev/GoGuardLib/secure"
)

// This file is the orders service from the README, kept here so that it compiles.
//
// For every order on the "orders" topic the service does two things:
//  1. reserves the stock, by calling the inventory service
//  2. charges the card, by calling the payments service
//
// If either service is down, the order is saved to disk with a note of how far it
// got, and finished later, starting from the step that failed.

// serviceConfig is what the service needs from its environment.
type serviceConfig struct {
	Brokers        string // "kafka-1:9092,kafka-2:9092"
	DataDir        string // where the dead-letter log lives: a persistent volume, not the container's own disk
	InventoryURL   string // "http://inventory.internal"
	PaymentsURL    string // "http://payments.internal"
	EncryptionKey  []byte // 32 bytes, from your secret store
	OrderKeyPepper []byte // another secret; hides the Kafka key (the customer) in the log
}

// runOrdersService runs until ctx ends, or returns the error that made it unsafe to go on.
func runOrdersService(ctx context.Context, cfg serviceConfig) error {
	// One breaker per service we call.
	inventory, err := newBreaker("inventory", cfg.InventoryURL)
	if err != nil {
		return err
	}
	defer inventory.Close()
	payments, err := newBreaker("payments", cfg.PaymentsURL)
	if err != nil {
		return err
	}
	defer payments.Close()

	// The durable log where orders wait when a service is down.
	store, err := openStore(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	// The two steps every order goes through, each behind its own breaker.
	orders, err := pipeline.New("orders", "v1", []pipeline.Step{
		{Name: "reserve", Breaker: inventory, Timeout: 5 * time.Second, Run: post(cfg.InventoryURL + "/reserve")},
		{Name: "charge", Breaker: payments, Timeout: 5 * time.Second, Run: post(cfg.PaymentsURL + "/charge")},
	})
	if err != nil {
		return fmt.Errorf("building the pipeline: %w", err)
	}

	client, err := confluent.NewClient(ck.ConfigMap{
		"bootstrap.servers": cfg.Brokers,
		"group.id":          "orders-service",
		"auto.offset.reset": "earliest",
	}, []string{"orders"})
	if err != nil {
		return fmt.Errorf("creating the Kafka client: %w", err)
	}
	defer func() { _ = client.Close() }()

	// The consumer reads orders and runs them through the pipeline.
	consumer, err := kafka.NewConsumer(kafka.Config{
		Client:   client,
		Store:    store,
		Bindings: []kafka.Binding{{Topic: "orders", Pipeline: orders}},
	})
	if err != nil {
		return fmt.Errorf("creating the consumer: %w", err)
	}

	// The redriver finishes stored orders once the service they waited for is healthy.
	redriver, err := dlq.NewRedriver(dlq.RedriveConfig{
		Store:    store,
		Handler:  orders.Handler(),
		Breakers: map[string]*breaker.Breaker{"inventory": inventory, "payments": payments},
		Rate:     100, // orders per second, so a service that just came back is not flooded
		RampUp:   30 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("creating the redriver: %w", err)
	}

	return runTogether(ctx, consumer.Run, redriver.Run)
}

// newBreaker guards one service. While its circuit is open, the service's /health
// endpoint is probed. When it answers, one real call is tried, and only if that
// succeeds does normal traffic resume.
func newBreaker(name, baseURL string) (*breaker.Breaker, error) {
	check, err := health.HTTP(baseURL + "/health")
	if err != nil {
		return nil, fmt.Errorf("health check for %s: %w", name, err)
	}
	return breaker.New(breaker.Config{
		Name:             name,
		FailureThreshold: 0.5, // open when half of the recent calls fail...
		MinSamples:       20,  // ...but only after at least 20 calls, so one early error does not trip it
		// A 4xx means the request was wrong, not that the service is unhealthy.
		IsFailure: func(err error) bool { return !retry.IsPermanent(err) },
		Health:    &health.Config{Check: check, Interval: 5 * time.Second, SuccessThreshold: 2},
	}), nil
}

// post is one pipeline step: send the order to a service.
//
// The idempotency key is the same for a given order and step on every attempt, so if
// the step runs twice (after a crash, say) the service can recognise the repeat.
func post(url string) func(context.Context, *pipeline.Exec) error {
	return func(ctx context.Context, x *pipeline.Exec) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(x.Value))
		if err != nil {
			return retry.Permanent(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", x.IdempotencyKey())

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err // the service is unreachable: a temporary problem
		}
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(io.Discard, resp.Body)

		switch {
		case resp.StatusCode < 300:
			return nil
		case resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests:
			// The order itself is wrong (a declined card, say). Retrying cannot help, so
			// mark it permanent: it is parked for a person instead of retried for ever.
			return retry.Permanent(fmt.Errorf("%s answered %d", url, resp.StatusCode))
		default:
			return fmt.Errorf("%s answered %d", url, resp.StatusCode) // temporary: retried later
		}
	}
}

// openStore opens the dead-letter log on disk, with every stored order encrypted.
func openStore(cfg serviceConfig) (dlq.Store, error) {
	wal, err := dlq.OpenWAL(cfg.DataDir, dlq.WALOptions{MaxBytes: 2 << 30}) // 2 GiB: your disk budget
	if err != nil {
		return nil, fmt.Errorf("opening the dead-letter log: %w", err)
	}
	enc, err := secure.NewAESGCM(secure.Key{ID: "2026-09", Material: cfg.EncryptionKey})
	if err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("building the encryptor: %w", err)
	}
	store, err := dlq.Secure(wal, dlq.SecureOptions{Encryptor: enc, OrderKeyPepper: cfg.OrderKeyPepper})
	if err != nil {
		_ = wal.Close()
		return nil, err
	}
	return store, nil
}

// runTogether runs the loops until ctx ends. If one stops, it stops the rest, and it
// returns every error.
func runTogether(ctx context.Context, loops ...func(context.Context) error) error {
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	results := make(chan error, len(loops))
	for _, loop := range loops {
		go func() {
			results <- loop(ctx)
			stop()
		}()
	}
	var errs []error
	for range loops {
		errs = append(errs, <-results)
	}
	return errors.Join(errs...)
}

// parkedOrder is an order waiting for a person, and how far it got.
type parkedOrder struct {
	RecordID string
	Reason   string
	Done     []string // steps that finished
	Next     string   // the step it stopped at
}

// listParked returns the orders that need a person. They are never retried and never
// deleted on their own, so this list is the operator's to-do list.
func listParked(ctx context.Context, store dlq.Store, orders *pipeline.Pipeline) ([]parkedOrder, error) {
	records, err := store.Parked(ctx, dlq.ParkedQuery{Limit: 100})
	if err != nil {
		return nil, fmt.Errorf("listing parked orders: %w", err)
	}
	var parked []parkedOrder
	for _, rec := range records {
		progress, err := orders.Describe(rec.Checkpoint)
		if err != nil {
			return nil, fmt.Errorf("reading the checkpoint of %s: %w", rec.ID, err)
		}
		parked = append(parked, parkedOrder{RecordID: rec.ID, Reason: rec.LastError, Done: progress.Completed, Next: progress.Next})
	}
	return parked, nil
}

// resumeParked puts a parked order back in the queue at the given step, once whatever
// stopped it has been fixed. Steps before that one are not repeated.
func resumeParked(ctx context.Context, store dlq.Store, orders *pipeline.Pipeline, recordID, fromStep string) error {
	return orders.Redrive(ctx, store, recordID, fromStep)
}

// Example_ordersService only makes sure the service above compiles; it needs a broker to run.
func Example_ordersService() {
	_ = runOrdersService
	_ = listParked
	_ = resumeParked
}
