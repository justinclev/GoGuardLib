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

Build a guarded client with one call. Every host it talks to gets its own circuit:

```go
client, err := goguard.NewClient(goguard.Policy{
	RequestTimeout: 2 * time.Second,
	MaxRetries:     2, // only for GET and HEAD requests, which are safe to repeat
})
if err != nil {
	return err
}
defer client.Close()
```

`client` is an `*http.Client` with a guarded transport. To protect only some hosts, or give a host its own settings, add endpoints: `goguard.NewClient(policy, goguard.WithEndpoint("payments", goguard.Host("payments.internal"), paymentsPolicy))`. For full control of the transport, use `goguard.New` and wrap it in an `http.Client` yourself.

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

### Saving requests for later

Some HTTP calls don't need an answer right now: sending a webhook, a notification, a background update. For those, you can have the library save a request it can't send, and send it once the service is back. The saved requests use the same store and redriver as Kafka.

Choose where they wait when you build the store: `dlq.NewMemoryStore(...)` keeps them until the process exits, `dlq.OpenWAL(dir, ...)` keeps them on disk across restarts.

```go
hooks, err := goguard.NewClient(goguard.Policy{
	HealthPath: "/health",
	Defer:      &goguard.Defer{Store: store}, // save what can't be sent right now
})
```

Send as usual. A request is only saved if it carries an idempotency key, so that sending it twice (once before the failure, once from the store) can't do the work twice:

```go
func sendWebhook(client *goguard.Client, eventID string, payload []byte) error {
	req, err := http.NewRequest(http.MethodPost, "https://hooks.partner.com/events", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Idempotency-Key", eventID) // a request without one is never saved
	req.Header.Set("Authorization", "Bearer "+currentToken())

	resp, err := client.Do(req)
	if errors.Is(err, goguard.ErrDeferred) {
		return nil // saved: it will be sent when the partner is back
	}
	if err != nil {
		return err
	}
	return resp.Body.Close()
}
```

`goguard.ErrDeferred` means "not sent, but saved". If you run a Kafka service, hand the client to it (`kafka.ServiceConfig{HTTP: hooks}`, see Step 4) and the saved requests are sent for you. Without Kafka, run a redriver yourself. `BreakerSource` tells it which circuits to watch, so it leaves saved requests alone while their circuit is open:

```go
redriver, err := dlq.NewRedriver(dlq.RedriveConfig{
	Store:         store,
	Handler:       hooks.Replay(func(ctx context.Context, req *http.Request) error {
		req.Header.Set("Authorization", "Bearer "+currentToken()) // credentials are never saved
		return nil
	}),
	BreakerSource: hooks.Breakers,
})
```

Things to know:

- **Only use this for calls that can wait.** A caller that needs the response can't use it, because the answer would arrive long after the caller has gone.
- **Passwords and tokens are never saved.** `Authorization`, cookies and API-key headers are dropped, so add them again in `Prepare`. The URL and body are saved, so put `dlq.Secure` around a disk store if they're sensitive.
- **If a request can't be saved** (no idempotency key, a body that can't be read twice, a body over 1 MiB, or a full store), it fails exactly as it would without this feature. You are never told "saved" when it isn't.
- After the service returns, saved requests are sent at the redriver's pace. A 4xx answer (a bad request) is set aside for a person instead of retried forever.

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
	return kafka.Run(ctx, kafka.ServiceConfig{
		NewClient: confluent.NewClientFunc(ck.ConfigMap{"bootstrap.servers": brokers, "group.id": "orders-service"}),
		DataDir:   dataDir, // orders that fail wait here, on disk
		Handlers: []kafka.HandlerConfig{
			{Topic: "orders", Handle: kafka.HandleValue(chargeOrder)}, // an error: saved, retried later
		},
	})
}
```

`chargeOrder` is your function, `func(ctx context.Context, order []byte) error`. If you want the message ID, key or headers, write the handler as `func(ctx, m kafka.HandledMessage) error` and drop `HandleValue`.

`kafka.Run` opens the on-disk log and the Kafka client, connects the consumer and the redriver, runs both until the context ends or Ctrl-C/SIGTERM, and releases what it opened. If you need the service itself, for `Stats` or to inspect the consumer, use `svc, err := kafka.NewService(cfg)`, then `defer svc.Close()` and `svc.RunUntilSignal(ctx)`; `kafka.Run` is exactly that.

What that gives you:

- If `chargeOrder` returns an error, the order is saved to disk and retried later. Orders from the same customer stay in order.
- If the process crashes, saved orders are still there when it restarts.
- If you return `retry.Permanent(err)`, the order is set aside for a person instead of being retried forever.
- Give the handler a circuit breaker (next section) and the consumer pauses the topic while the circuit is open. Messages wait safely in Kafka instead of piling up on disk.

### In a real service

The example above works, but a saved order is only retried a limited number of times (10 by default) before it is set aside for a person. If the service you call is down for a long time and nothing tells the library so, every retry fails and the orders get set aside. Give the handler a circuit breaker and the redriver knows the service is down: while the circuit is open, saved orders wait without using up their retries.

```go
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
	defer resp.Body.Close()
	return retry.FromHTTP(resp, nil) // 2xx ok; 5xx and timeouts retry; a declined card is set aside
}

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
```

What this adds to the simple version:

- **A circuit breaker with a health check** on the call to payments: `HealthURL` is all it takes. The service builds a breaker that opens when half of the recent calls fail (after at least 20; a declined card does not count) and asks `HealthURL` when payments is back. For other thresholds, build the breaker yourself and pass it as `Breaker` (`health.MustURL` builds the health check from a URL in one expression). The topic pauses while it is open, and saved orders wait instead of failing. The service hands the breaker to the redriver for you.
- **`retry.FromHTTP`** turns the answer into the right kind of error, so you don't write the status switch. A permanent one (a declined card) is set aside for a person, and `retry.IsOutage` keeps it from counting as the service being down.
- **`svc.Stats(ctx)` returns the consumer, redriver and store numbers together, for a health endpoint.
- **`Consumer: kafka.Config{...}`** is the full consumer configuration. `ServiceConfig` also takes `Redriver` (`dlq.RedriveConfig`), `WAL` (`dlq.WALOptions`) and `Secure` (encryption), so nothing is out of reach.

Your `Handle` function can run twice for the same order (after a crash, say), so make it safe to repeat. `m.ID` is the same every time, so you can use it to spot a repeat.

### Reading several topics

Add one handler per topic. A saved message is finished by the handler of its own topic, and every handler's breaker is watched:

```go
err := kafka.Run(ctx, kafka.ServiceConfig{
	NewClient: confluent.NewClientFunc(cfg),
	DataDir:   dataDir,
	Handlers: []kafka.HandlerConfig{
		{Topic: "orders", Handle: kafka.HandleValue(chargeOrder)},
		{Topic: "refunds", Handle: kafka.HandleValue(refund)},
	},
})
```

A topic whose work has several steps goes in `Pipelines` instead (see Step 5). A message from a topic you no longer read is set aside for a person, never run through the wrong handler.

### Saved HTTP requests in the same service

Give the service the guarded client from Step 2 and it also finishes the requests that client saved. A `Defer` with no `Store` uses the log the service opened:

```go
hooks, err := goguard.NewClient(goguard.Policy{Defer: &goguard.Defer{}})
svc, err := kafka.NewService(kafka.ServiceConfig{ /* as above */ HTTP: hooks })
```

If you would rather open the store yourself, give the same `Store` to the client's `Defer` and to the service.

### Wiring it yourself

`NewService` is built from `kafka.NewConsumer`, `dlq.NewRedriver` and `kafka.RedriveFor`, which stay available if you want to own each part (a different store per topic, your own run loop). `kafka/example_readme_test.go` has the same service wired by hand.

## Step 5: a job with several steps

Say each order has to reserve stock, then charge the card. If charging fails, you don't want to reserve the stock a second time. A *pipeline* remembers how far each order got, and retries start from the step that failed.

### The short way

When each step is one HTTP call, you can describe it instead of writing it. This is the whole service:

```go
checkout, err := pipeline.New("checkout", "v1", []pipeline.Step{
	{Name: "fetch-user", HealthURL: usersURL + "/health", Retries: 2,
		HTTP: pipeline.Get(usersURL + "/account/{key}"), SaveAs: "account"},
	{Name: "reserve", HealthURL: inventoryURL + "/health",
		HTTP: pipeline.Post(inventoryURL + "/reserve")},
	{Name: "charge", HealthURL: paymentsURL + "/health",
		HTTP: pipeline.Post(paymentsURL + "/charge/{account.id}")},
})
if err != nil {
	return err
}
return kafka.Run(ctx, kafka.ServiceConfig{
	NewClient:     confluent.NewClientFunc(kafkaConfig),
	DataDir:       dataDir,
	EncryptionKey: key, // 32 bytes, from your secret store
	Pipelines:     []kafka.Binding{{Topic: "checkouts", Pipeline: checkout}},
})
```

What each part does:

- **`HealthURL`** gives the step its own circuit breaker. It opens when half of the recent calls fail, ignores a declined card, and asks the URL when the service is back. While it is open the topic pauses.
- **`Retries: 2`** retries a call that is safe to repeat. Leave it off for a charge or a write: if that fails, the message is saved and the redriver retries it later from that step only.
- **`pipeline.Get` and `pipeline.Post`** send the request with an `Idempotency-Key`, and sort the answer: success, temporary (retried later) or permanent (a 4xx; set aside for a person). A POST sends the Kafka message as its body.
- **`SaveAs: "account"`** keeps the answer for later steps. `{key}` is the message key, `{account.id}` a field of what was saved.
- **`EncryptionKey`** encrypts what is saved to disk.

### Full control

Every part has a longer form with the same behaviour and more settings: a step with `Run` instead of `HTTP`, your own `breaker.Config`, a `retry.Policy`, `Secure` with key rotation, and a `Prepare` function on an HTTP step for credentials. [kafka/example_full_test.go](kafka/example_full_test.go) is this same service written that way.

### Step by step

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

### Passing data from one step to the next

A step can hand data to the next one with `Set` and `Get`. It is saved with the step's progress, so it is still there if a later step fails and the message is resumed hours later. Here step 1 asks one API for an account, and step 2 sends it to another:

```go
checkout, err := pipeline.New("checkout", "v1", []pipeline.Step{
	{Name: "fetch-user", Breaker: usersBr, Run: func(ctx context.Context, x *pipeline.Exec) error {
		account, err := call(ctx, http.MethodGet, users.URL, "")
	
```

If the orders API is down, step 2 fails and the message is saved with `accountId` in its checkpoint. When orders is back, the redriver resumes at step 2, and `Get` still returns the account. Step 1 is not called again. The full example, with two fake APIs, is [examples/stepdata](examples/stepdata/stepdata_test.go).

To pass an object, use the JSON helpers instead of marshalling yourself:

```go
{Name: "fetch-user", Breaker: usersBr, Run: func(ctx context.Context, x *pipeline.Exec) error {
	account, err := fetchAccount(ctx) // any struct
	if err != nil {
		return err
	}
	return pipeline.SetJSON(x, "account", account)
}},
{Name: "charge", Breaker: paymentsBr, Run: func(ctx context.Context, x *pipeline.Exec) error {
	account, err := pipeline.RequireJSON[Account](x, "account")
	if err != nil {
		return err // missing or unreadable data is a permanent error
	}
	return charge(ctx, account)
}},
```

`GetJSON` is the same without the missing-key error: it returns `ok=false` when nothing was saved. A value that cannot be marshalled, or saved data that no longer decodes into your type (a deploy changed the struct), comes back as a `retry.Permanent` error: it is set aside for a person instead of retried for ever. Adding a field is safe; for a breaking change, bump the pipeline version.

Only what you `Set` is saved, so a variable in your closure is lost on a resume. Keep it small (IDs, not payloads): it counts against the record size limit. It is stored in clear unless you wrap the store in `dlq.Secure`.

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
