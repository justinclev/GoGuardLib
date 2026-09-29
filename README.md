# GoGuardLib

When a service you depend on breaks, your code shouldn't keep calling it, hang waiting for it, or lose the work it was doing. GoGuardLib does three things about that:

1. **Stops calling a broken service.** After a few failures it refuses further calls right away, instead of letting each one wait and time out.
2. **Notices when the service is back.** It checks the service's health endpoint and lets calls through again, carefully.
3. **Saves work you couldn't finish.** For Kafka, a message that can't be handled yet is written to disk and finished later, from the step where it stopped. Nothing is lost.

Go 1.25 or newer. The core has no dependencies beyond the standard library.

```bash
go get github.com/justinclev/GoGuardLib        # HTTP, any function, retries, saved work
go get github.com/justinclev/GoGuardLib/kafka  # the Kafka consumer (needs cgo)
```

There are no release tags yet, so add `@<commit>` (or `@Kafka-changes`) to pin a version.

## Where do I start?

| I want to protect... | Go to |
|---|---|
| calls my code makes over HTTP | [Step 2](#step-2-protect-http-calls) |
| any other call: a database, gRPC, an SDK | [Step 1](#step-1-protect-any-function) |
| a Kafka consumer I'm writing | [Step 4](#step-4-a-kafka-consumer-that-loses-nothing) |
| a job with several steps (reserve stock, then charge) | [Step 5](#step-5-a-job-with-several-steps) |
| a Kafka loop I already have and don't want to rewrite | [docs/INTEGRATION.md](docs/INTEGRATION.md) |

The examples all use one made-up service: an online shop that charges cards through a `payments` service.

## Step 1: protect any function

A *circuit breaker* watches how often a call fails. When too many fail, it "opens": for a while, calls are refused immediately without trying, which gives the broken service room to recover.

```go
func newPayments() *breaker.Breaker {
	return breaker.New(breaker.Config{
		Name:             "payments",
		FailureThreshold: 0.5, // open the circuit when half the recent calls fail...
		MinSamples:       3,   // ...but only after this many calls (use 20 or more in production)
	})
}
```

```go
func placeOrder(ctx context.Context, payments *breaker.Breaker, order string) (string, error) {
	receipt, err := breaker.Call(ctx, payments, func(ctx context.Context) (string, error) {
		return chargeCard(ctx, order)
	})
	if errors.Is(err, breaker.ErrOpen) {
		return "", errors.New("payments is having trouble, please try again in a minute")
	}
	return receipt, err
}
```

The first calls reach payments and get its errors. Once enough have failed, the breaker opens and the next call returns `breaker.ErrOpen` straight away, without contacting payments.

## Step 2: protect HTTP calls

If you use an `http.Client`, give it a guarded transport. Everything else the client sends is left alone; only the hosts you name are protected.

```go
func newClient() (*http.Client, error) {
	guard, err := goguard.New(goguard.Config{},
		goguard.WithEndpoint("payments", goguard.Host("payments.internal"), goguard.Policy{
			RequestTimeout: 2 * time.Second,
			MaxRetries:     2, // only for GET and HEAD requests, which are safe to repeat
		}),
	)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: guard}, nil
}
```

Use the client as you always do. When the circuit is open, the error matches `goguard.ErrCircuitOpen`:

```go
func getQuote(client *http.Client) error {
	resp, err := client.Get("https://payments.internal/quote")
	if errors.Is(err, goguard.ErrCircuitOpen) {
		return errors.New("payments is having trouble, please try again in a minute")
	}
	if err != nil {
		return err
	}
	return resp.Body.Close()
}
```

`MaxRetries` repeats a failed call, but only for GET and HEAD requests. A POST is never repeated automatically, because you might charge a card twice.

## Step 3: let the service tell you it's back

By default the circuit closes again after a timer. It's better to ask the service. Add a health path and the library calls it while the circuit is open, then lets a real request through as a test before trusting it fully:

```go
func newClientWithHealthCheck() (*http.Client, error) {
	guard, err := goguard.New(goguard.Config{},
		goguard.WithEndpoint("payments", goguard.Host("payments.internal"), goguard.Policy{
			RequestTimeout: 2 * time.Second,
			HealthPath:     "/health", // while the circuit is open, ask payments if it is back
		}),
	)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: guard}, nil
}
```

The same works for Step 1 with `Health: &health.Config{...}` on the breaker (see [Step 5](#step-5-a-job-with-several-steps)).

## Step 4: a Kafka consumer that loses nothing

A normal consumer has two bad choices when payments is down: fail the message or skip it. This one has a third: it saves the message to disk, moves on, and a background "redriver" retries it once payments is healthy.

```go
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
```

What that gives you:

- If `chargeOrder` returns an error, the order is saved to disk and retried later. Orders from the same customer stay in order.
- If the process crashes, saved orders are still there when it restarts.
- If you return `retry.Permanent(err)`, the order is set aside for a person instead of being retried forever.
- Add `Breaker: yourBreaker` to `HandlerConfig` and the consumer pauses the topic while the circuit is open. Messages wait safely in Kafka instead of piling up on disk.

Your `Handle` function can run twice for the same order (after a crash, say), so make it safe to repeat. `m.ID` is the same every time, so you can use it to spot a repeat.

## Step 5: a job with several steps

Say each order has to reserve stock, then charge the card. If charging fails, you don't want to reserve the stock a second time. A *pipeline* remembers how far each order got, and retries start from the step that failed.

Give each service its own breaker, with a health check:

```go
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
```

Describe the steps, each with its breaker:

```go
orders, err := pipeline.New("orders", "v1", []pipeline.Step{
	{Name: "reserve", Breaker: inventory, Timeout: 5 * time.Second, Run: post(cfg.InventoryURL + "/reserve")},
	{Name: "charge", Breaker: payments, Timeout: 5 * time.Second, Run: post(cfg.PaymentsURL + "/charge")},
})
if err != nil {
	return fmt.Errorf("building the pipeline: %w", err)
}
```

Keep the saved orders encrypted on disk:

```go
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
```

(`post` sends the order to a service, and `cfg` holds your settings. Both are in the complete file linked below.)

Then pass `orders` to `kafka.Binding{Topic: "orders", Pipeline: orders}` and start the consumer and redriver as in Step 4. The complete, working file is [kafka/example_service_test.go](kafka/example_service_test.go), including how to list orders waiting for a person and resume them.

When payments goes down in this setup:

1. A few orders fail at the charge step. The payments breaker opens.
2. The consumer stops reading the topic. New orders wait in Kafka, not on disk.
3. Orders already in progress are saved with a note that says "stock reserved, charge not done".
4. Payments recovers. Its health check passes, one real call succeeds, and the circuit closes.
5. The redriver finishes the saved orders, starting at the charge step, at a gentle pace.

## Before you go to production

- **Put the saved-orders directory on a persistent local disk**, not the container's temporary storage and not a network share.
- **Encrypt what's saved.** Step 5 shows how; [docs/SECURITY.md](docs/SECURITY.md) lists everything else worth turning on.
- **Watch the queue.** Alert if the oldest waiting order gets old, or if any order is waiting for a person. [docs/OPERATIONS.md](docs/OPERATIONS.md) says what each signal means and what to do.
- **Use `MinSamples` of 20 or more.** With a small number, one early error can open the circuit.
- **Make your handlers safe to repeat.** Anything can run twice after a crash.

## More documentation

- [The full guide](docs/GUIDE.md): a complete worked example, how each part works with diagrams, and every option.
- [Adding it to an existing Kafka loop](docs/INTEGRATION.md)
- [Running it in production](docs/OPERATIONS.md) and [security](docs/SECURITY.md)
- [A live demo](demo/README.md) with a diagram: `cd demo && docker compose up --build`
- [Audit history](docs/AUDIT.md) and the [changelog](CHANGELOG.md)

## License

See [LICENSE](LICENSE).
