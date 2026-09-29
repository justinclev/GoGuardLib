package kafka_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/health"
	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/kafka/confluent"
	"github.com/justinclev/GoGuardLib/obs"
	"github.com/justinclev/GoGuardLib/pipeline"
	"github.com/justinclev/GoGuardLib/retry"
	"github.com/justinclev/GoGuardLib/secure"
)

// ordersService is everything the orders service needs to run.
type ordersService struct {
	Brokers      string
	DataDir      string // must be on a persistent volume
	EncryptKey   []byte // 32 bytes, from your secret store; never stored beside the data
	InventoryURL string
	PaymentsURL  string
	Events       obs.Sink // circuit and redrive events for your metrics; may be nil
}

// A complete service: it consumes orders, calls two dependencies for each, sets aside
// what cannot finish, and completes it when the dependency recovers.
func Example_ordersService() {
	svc := ordersService{
		Brokers:      "kafka:9092",
		DataDir:      "/var/lib/orders/dlq",
		InventoryURL: "http://inventory.internal",
		PaymentsURL:  "http://payments.internal",
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	_ = svc.Run(ctx) // returns when ctx ends, or with the error that made continuing unsafe
}

// Run wires the components and runs the consumer and the redriver until ctx ends.
func (s ordersService) Run(ctx context.Context) error {
	// The breakers need the redriver's wake-up sink and the redriver needs the
	// breakers, so the sink reaches it through a pointer that is set once it exists.
	var redriver atomic.Pointer[dlq.Redriver]
	events := obs.Multi(s.Events, obs.SinkFunc(func(e obs.Event) {
		if r := redriver.Load(); r != nil {
			r.Sink().Emit(e)
		}
	}))

	inventory, err := newBreaker("inventory", s.InventoryURL+"/health", events)
	if err != nil {
		return err
	}
	defer inventory.Close()
	payments, err := newBreaker("payments", s.PaymentsURL+"/health", events)
	if err != nil {
		return err
	}
	defer payments.Close()

	store, err := s.openStore()
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()

	orders, err := pipeline.New("orders", "v1", []pipeline.Step{
		{Name: "reserve", Breaker: inventory, Timeout: 5 * time.Second, Run: post(s.InventoryURL + "/reserve")},
		{Name: "charge", Breaker: payments, Timeout: 5 * time.Second, Run: post(s.PaymentsURL + "/charge")},
	})
	if err != nil {
		return fmt.Errorf("building the pipeline: %w", err)
	}

	producer, err := confluent.NewProducer(ck.ConfigMap{"bootstrap.servers": s.Brokers})
	if err != nil {
		return fmt.Errorf("creating the dead-letter producer: %w", err)
	}
	defer producer.Close()
	mirror, err := kafka.NewDLQPublisher(kafka.PublisherConfig{Producer: producer}) // "orders.dlq"
	if err != nil {
		return err
	}

	rd, err := dlq.NewRedriver(dlq.RedriveConfig{
		Store:    store,
		Handler:  orders.Handler(),
		Breakers: map[string]*breaker.Breaker{"inventory": inventory, "payments": payments},
		Rate:     100, RampUp: 30 * time.Second, // do not flood a dependency that just came back
		OnPark: mirror.Publish,
		Events: s.Events,
	})
	if err != nil {
		return fmt.Errorf("creating the redriver: %w", err)
	}
	redriver.Store(rd)

	client, err := confluent.NewClient(ck.ConfigMap{
		"bootstrap.servers": s.Brokers,
		"group.id":          "orders-service",
		"auto.offset.reset": "earliest",
	}, []string{"orders"})
	if err != nil {
		return fmt.Errorf("creating the consumer client: %w", err)
	}
	defer func() { _ = client.Close() }()

	consumer, err := kafka.NewConsumer(kafka.Config{
		Client:         client,
		Store:          store,
		Bindings:       []kafka.Binding{{Topic: "orders", Pipeline: orders}},
		Workers:        4,
		ProcessTimeout: 30 * time.Second,
		Mirror:         mirror,
	})
	if err != nil {
		return fmt.Errorf("creating the consumer: %w", err)
	}

	return runBoth(ctx, consumer.Run, rd.Run)
}

// newBreaker guards one dependency. While its circuit is open, its health endpoint is
// probed; real traffic resumes, one canary first, only after it answers.
func newBreaker(name, healthURL string, events obs.Sink) (*breaker.Breaker, error) {
	check, err := health.HTTP(healthURL)
	if err != nil {
		return nil, fmt.Errorf("health check for %s: %w", name, err)
	}
	return breaker.New(breaker.Config{
		Name:             name,
		FailureThreshold: 0.5,
		MinSamples:       20,
		SleepWindow:      30 * time.Second,
		// A 4xx says the request was wrong, not that the service is unhealthy.
		IsFailure: func(err error) bool { return !retry.IsPermanent(err) },
		Health:    &health.Config{Check: check, Interval: 5 * time.Second, SuccessThreshold: 2},
		Events:    events,
	}), nil
}

// openStore opens the durable log that holds messages which could not finish, with
// their payloads encrypted at rest.
func (s ordersService) openStore() (dlq.Store, error) {
	wal, err := dlq.OpenWAL(s.DataDir, dlq.WALOptions{MaxBytes: 2 << 30}) // your disk budget
	if err != nil {
		return nil, fmt.Errorf("opening the dead-letter log: %w", err)
	}
	enc, err := secure.NewAESGCM(secure.Key{ID: "2026-09", Material: s.EncryptKey})
	if err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("building the encryptor: %w", err)
	}
	store, err := dlq.Secure(wal, dlq.SecureOptions{Encryptor: enc})
	if err != nil {
		_ = wal.Close()
		return nil, err
	}
	return store, nil
}

// post is one pipeline step: send the message to a dependency. The idempotency key is
// the same for this message and step on every attempt, so a repeat does no harm.
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
			return err // the dependency is unreachable: transient
		}
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(io.Discard, resp.Body)

		switch {
		case resp.StatusCode < 300:
			return nil
		case resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests:
			// The message itself is wrong; retrying cannot help. It is parked for a person.
			return retry.Permanent(fmt.Errorf("%s answered %d", url, resp.StatusCode))
		default:
			return fmt.Errorf("%s answered %d", url, resp.StatusCode)
		}
	}
}

// runBoth runs the given loops, stops the rest when one ends, and returns every error.
func runBoth(ctx context.Context, loops ...func(context.Context) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(loops))
	for _, loop := range loops {
		go func() { results <- loop(ctx) }()
	}
	var errs []error
	for range loops {
		if err := <-results; err != nil {
			errs = append(errs, err)
		}
		cancel()
	}
	return errors.Join(errs...)
}
