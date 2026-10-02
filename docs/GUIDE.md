# GoGuardLib: the full guide

The [README](../README.md) is the quick start. This is the long version: a complete example, how each part works, and the reference for every option.

Circuit breakers, retries and a durable dead-letter log for Go services that call other services.

- **HTTP:** wrap your `http.Client`. Calls to a failing service are refused immediately instead of hanging.
- **Kafka:** a consumer that never loses a message. If a downstream service is down, the message is written to disk with its progress, and finished later, starting from the step that failed.
- **Any other call:** `breaker.Do(ctx, fn)` for a database, a gRPC client, anything.

The core module uses only the standard library and never logs or prints. State changes and results are typed events that you send wherever you like, or read with `Stats()`. The Kafka support is a separate module, so HTTP-only users don't pull in cgo.

There is a runnable demo with a live diagram in [`demo/`](../demo/README.md): `cd demo && docker compose up --build`.
Adding this to a service that already has a poll-and-process consumer: [`docs/INTEGRATION.md`](INTEGRATION.md).

```bash
go get github.com/justinclev/GoGuardLib        # HTTP, breaker, retry, dlq, pipeline
go get github.com/justinclev/GoGuardLib/kafka  # Kafka consumer (needs cgo, uses confluent-kafka-go v2)
```

## Example: an orders service

Every example below is one service. It handles each order in two steps:

1. reserve the stock, by calling the **inventory** service
2. charge the card, by calling the **payments** service

Orders arrive on a Kafka topic. The same service also makes a plain HTTP call to payments to get a price quote. That gives two kinds of work with two different needs:

- **The quote** is an HTTP call that someone is waiting for. When payments is down, the right answer is a fast error.
- **An order** must not be lost. When payments is down, the right answer is to save the order and finish it later.

The code below is copied from files that compile: [`examples/httpclient/httpclient_test.go`](../examples/httpclient/httpclient_test.go) for the HTTP part and [`kafka/example_service_test.go`](../kafka/example_service_test.go) for the Kafka part.

### The quote: an HTTP call that fails fast

Two functions set it up. `newGuardedClient` returns an ordinary `http.Client` in which only calls to the payments host are guarded, and everything else goes straight through. `getQuote` is the code that uses it.

```go
// newGuardedClient returns an http.Client whose calls to payments are protected.
// Everything else the client sends passes straight through, untouched.
func newGuardedClient(paymentsHost string, events obs.Sink) (*http.Client, func() error, error) {
	guard, err := goguard.New(goguard.Config{},
		goguard.WithEvents(events),
		goguard.WithEndpoint("payments", onHost(paymentsHost), goguard.Policy{
			FailureThreshold: 0.5,              // open at a 50% failure rate...
			MinSamples:       3,                // ...once this many calls have been seen (20 or more in production)
			SleepWindow:      30 * time.Second, // how long to stay open without a health check
			RequestTimeout:   2 * time.Second,
			MaxRetries:       2, // idempotent requests only (GET, HEAD, OPTIONS, TRACE)
			RetryBackoff:     retry.Jitter(retry.Exponential(100*time.Millisecond, time.Second), 0.3),
		}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("configuring the guard: %w", err)
	}
	return &http.Client{Transport: guard}, guard.Close, nil
}

// onHost matches requests to one host and port.
func onHost(host string) goguard.Matcher {
	return func(r *http.Request) bool { return r.URL.Host == host }
}
```

```go
// ErrPaymentsUnavailable means payments is failing and the call was not attempted.
var ErrPaymentsUnavailable = errors.New("payments is unavailable, try again shortly")

// getQuote asks payments for a price. When payments is failing, it returns at once
// with ErrPaymentsUnavailable instead of waiting for a timeout.
func getQuote(client *http.Client, paymentsURL string) (string, error) {
	resp, err := client.Get(paymentsURL + "/quote")
	if err != nil {
		if errors.Is(err, goguard.ErrCircuitOpen) {
			return "", ErrPaymentsUnavailable // the call was refused: payments was not contacted
		}
		return "", fmt.Errorf("asking payments for a quote: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("payments answered %d", resp.StatusCode)
	}
	price, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading the quote: %w", err)
	}
	return string(price), nil
}
```

Now run `getQuote` four times while payments answers 503 to everything:

```go
for call := 1; call <= 4; call++ {
	_, err := getQuote(client, payments.URL)
	fmt.Printf("call %d: %v\n", call, err)
}
```

It prints:

```
call 1: payments answered 503
call 2: payments answered 503
call 3: payments answered 503
call 4: payments is unavailable, try again shortly
}
```

- Calls 1 to 3 reached payments, which answered 503. The caller gets that as an ordinary error, and the breaker counts a failure. (`MaxRetries: 2` means each call also retried its GET twice on the 503, out of sight. Only idempotent methods are ever retried.)
- After `MinSamples` calls with at least half failing, the circuit opened.
- Call 4 was refused at once, without calling payments, so the caller isn't left waiting on a timeout.
- Nothing is stored. Over HTTP, what to do about a refused request is up to the caller.
- The breaker checks whether payments is back on its own. See [The circuit breaker](#the-circuit-breaker).

### The orders: process them without losing any

The Kafka side has five parts. Each one is a short function.

**1. What the service needs from its environment.**

```go
// serviceConfig is what the service needs from its environment.
type serviceConfig struct {
	Brokers        string // "kafka-1:9092,kafka-2:9092"
	DataDir        string // where the dead-letter log lives: a persistent volume, not the container's own disk
	InventoryURL   string // "http://inventory.internal"
	PaymentsURL    string // "http://payments.internal"
	EncryptionKey  []byte // 32 bytes, from your secret store
	OrderKeyPepper []byte // another secret; hides the Kafka key (the customer) in the log
}
```

**2. One breaker per service.** A breaker watches the recent results of calls to one service and stops calling it when it is clearly failing.

```go
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
```

**3. The step that calls a service.** Both steps use the same function. Its return value tells the library what to do with an order that failed:

```go
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
```

| The step returns | What the library does with the order |
|---|---|
| `nil` | goes on to the next step |
| an ordinary error | the service had a problem: save the order, retry it later |
| `retry.Permanent(err)` | the order itself is bad: park it for a person |

**4. The place where waiting orders are kept.** An order that can't finish is written here, with a note of which steps are done. The log is on disk and every order in it is encrypted.

```go
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
```

`DataDir` must be a persistent volume. Anything stored in a container's own filesystem is gone when the container is replaced.

**5. Put it together.** This is the whole service:

```go
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

	// The durable, encrypted log where orders wait when a service is down.
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

	// The service reads orders, runs them through the pipeline, saves what fails, and finishes
	// saved orders once the service they waited for is healthy.
	svc, err := kafka.NewService(kafka.ServiceConfig{
		NewClient: confluent.NewClientFunc(ck.ConfigMap{
			"bootstrap.servers": cfg.Brokers,
			"group.id":          "orders-service",
			"auto.offset.reset": "earliest",
		}),
		Store:     store,
		Pipelines: []kafka.Binding{{Topic: "orders", Pipeline: orders}},
		Redriver: dlq.RedriveConfig{
			Rate:   100, // orders per second, so a service that just came back is not flooded
			RampUp: 30 * time.Second,
		},
	})
	if err != nil {
		return err
	}
	defer func() { _ = svc.Close() }()
	return svc.Run(ctx)
}
```

Call `runOrdersService` from `main` with a context that ends on shutdown (`signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)`), or use `svc.RunUntilSignal(ctx)` in its place. `Run` runs the consumer and the redriver side by side and stops one if the other stops. The service finds the pipeline's breakers by itself, so the redriver watches `inventory` and `payments` without being told.

Everything the service builds can still be configured: `Consumer` takes a `kafka.Config` (workers, backoff, the dead-letter mirror and so on), `Redriver` takes a `dlq.RedriveConfig`, and `WAL` and `Secure` take the log and encryption options if you give it a `DataDir` instead of a store. If you want to own each part, build them yourself with `kafka.NewConsumer`, `dlq.NewRedriver` and `kafka.RedriveFor`.

### What happens when payments goes down

Follow one stretch of traffic through the service above:

1. Orders keep arriving. The `reserve` step works, but the `charge` step gets 503s. Each order that fails is saved to the log with a note: *reserve done, charge next*. Its Kafka offset is then committed, because the order is now safe on disk.
2. After enough failures, the payments breaker opens. The consumer pauses the topic. New orders stay in Kafka, which holds them for free. The service never calls payments while its breaker is open.
3. Every few seconds the library calls `PaymentsURL/health`. Nothing else touches payments.
4. Payments answers healthy twice in a row. The breaker lets one real call through as a trial. It succeeds, so the breaker closes.
5. The consumer resumes, and the redriver replays the saved orders at a controlled rate, starting at `charge`. Stock is not reserved a second time. Orders for the same customer stay in the order they were placed.
6. If the service restarts at any point, the log is read back from disk and nothing is lost.

Delivery is at-least-once. After a crash a step can run twice, which is why `post` sends an idempotency key: the inventory and payments services can recognise a repeat.

### An order that can never succeed

Say payments refuses a card. Retrying can't help, so `post` returns `retry.Permanent`. The order is **parked**: kept in the log for a person, never retried and never deleted automatically.

A parked order also holds back later orders with the same Kafka key (here, the same customer) until someone deals with it. That keeps each customer's orders in sequence. Other customers are not affected.

An operator lists what is parked, fixes the cause, and resumes the order:

```go
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
```

Resume at `charge` and the order carries on from there. The stock reservation is not repeated. You can also discard a parked order (`store.Discard`).

## How it works

### The circuit breaker

Every dependency gets a breaker. It counts recent successes and failures and is in one of three states:

```
                     failure rate >= FailureThreshold
                     and at least MinSamples calls seen
       ┌────────┐ ────────────────────────────────────▶ ┌────────┐
       │ CLOSED │                                       │  OPEN  │  every call is refused
       │ calls  │                                       │        │  with ErrOpen; the
       │  flow  │                                       └───┬────┘  service isn't touched
       └────────┘                                           │
          ▲                                                 │ it leaves OPEN when either
          │ the canary call succeeds                        │  · the health check passes
          │                                                 │    SuccessThreshold times in a row
          │                                                 │  · or, with no health check,
          │                                                 │    SleepWindow has passed
          │                                                 ▼
          │                                            ┌───────────┐
          └─────────────────────────────────────────── │ HALF-OPEN │  one real call, the
                                                       └─────┬─────┘  canary, is let through
                         the canary fails ───────────────────┘
                         (straight back to OPEN)
```

- **The window.** Results go into a rolling window (`SamplingWindow`, 10 s by default), split into buckets. Old buckets fall off, so the failure rate only reflects the recent past.
- **Opening.** The circuit opens when the window holds at least `MinSamples` calls and the failure rate has reached `FailureThreshold`. With `MinSamples: 20` and `FailureThreshold: 0.5`, 10 failures out of 20 calls opens it. 3 failures out of 3 don't: that's too few calls to judge.
- **What counts as a failure.** Any error, unless you set `IsFailure`. Do set it: a 404 or a validation error means the service answered, so it shouldn't count against it. An error where `IsFailure` returns false is recorded as a success. If the caller cancels its own context, the call is *abandoned* and isn't counted either way. A panic inside the call is counted as a failure and then re-raised. For HTTP the default rule is a transport error or a status of 500 or more. `MaxLatency` and `OutlierFactor` can also treat slow responses as failures.
- **While open**, nothing reaches the dependency. HTTP callers get a `*goguard.CircuitError` (matches `goguard.ErrCircuitOpen`), other callers a `*breaker.OpenError` (matches `breaker.ErrOpen`). Neither is retried.
- **Leaving OPEN without a health check.** After `SleepWindow`, plus up to 10% jitter so a fleet doesn't move together, the next real call becomes the canary.
- **Leaving OPEN with a health check.** A background prober calls your check every `Interval`, backing off while it fails, and real calls aren't used to test a service that hasn't said it's back. After `SuccessThreshold` good checks in a row the circuit goes half-open. A healthy service gets no probe traffic.
- **The canary is a real call.** A service can answer `/health` and still fail real work, so by default only a successful real call closes the circuit. `TrustHealth` skips that step. Only one call is admitted while half-open. If it never reports back, another call takes over after `SleepWindow`, so the circuit can't get stuck.
- **A health check that lies** can't block traffic forever: after `MaxOpen` (5 minutes by default) the timer-based canary resumes.
- **Closing resets the window**, so old failures can't reopen a recovered circuit on its first error.

Two limits sit next to the breaker:

- **Bulkhead** (`MaxInflight`): at most this many calls at once. A call over the limit waits up to `WaitTimeout` (`BulkheadWaitTimeout` in an HTTP `Policy`), then fails with `breaker.ErrBulkhead`. Priority requests skip the bulkhead but never the circuit.
- **Retry budget** (`RetryBudget`): retries may make up at most this share of recent traffic. Failures count as traffic, so a full outage can't turn into a retry storm.

`SetOverride` forces a breaker open or closed by hand. `DryRun: true` tracks everything and emits events but never refuses a call, which is a good way to roll a breaker out and watch it for a day first.

Starting values, if you have no better numbers:

| Setting | Start with | Too low | Too high |
|---|---|---|---|
| `MinSamples` | 20 (default 10) | one bad call trips it | never opens on quiet services |
| `FailureThreshold` | 0.5 | flaps on normal noise | slow to protect |
| `SamplingWindow` | 10 s | reacts to blips | slow to notice an outage |
| `SleepWindow` (no health check) | 30 s | probes a dead service too often | slow to resume |
| Health `Interval` | 5 s | probe load during an outage | slow to resume |
| `SuccessThreshold` | 2 | one lucky probe reopens traffic | slow to resume |

### An HTTP request

```
  client.Do(req)
        │
        ▼
  ┌──────────────────────────┐
  │ pick the endpoint        │  Skip(ctx)? Use(ctx, "name")? first matching Endpoint?
  └────────────┬─────────────┘  GuardAll? If none apply, the request goes straight
               │                through, with no breaker, no retry and no state.
               ▼
  ┌──────────────────────────┐
  │ find the circuit         │  one per endpoint, or one per host with PerHost
  └────────────┬─────────────┘
               ▼
  ┌──────────────────────────┐  circuit open ───▶ refused: *CircuitError
  │ ask the breaker          │  bulkhead full ──▶ refused (marked retryable)
  └────────────┬─────────────┘  (the request body is closed either way)
               │ admitted
               ▼
  ┌──────────────────────────┐
  │ send, with RequestTimeout│◀──────────────────────────────┐
  └────────────┬─────────────┘                               │
               ▼                                             │
  ┌──────────────────────────┐                               │
  │ was it a failure?        │  error, 5xx, or too slow      │
  └───┬───────────┬──────────┘                               │
      │ no        │ yes                                      │
      │           ▼                                          │
      │   retry? only if all of these hold: idempotent       │
      │   method, timeout or 503/504, replayable body,       │
      │   MaxRetries left, retry budget allows it            │
      │           │ yes: wait (backoff + jitter) ────────────┘
      │           │ no
      ▼           ▼
  tell the breaker: success (with latency), failure, or abandoned
      │
      ▼
  the caller gets the response, or a *CircuitError
```

Only GET, HEAD, OPTIONS and TRACE are retried, and only after a timeout, a 503 or a 504. The transport never retries a POST, because repeating it could charge a card twice.

### A Kafka message

```
  Kafka topic
      │  poll. Auto-commit is off: this library decides when an offset is safe.
      ▼
  ┌───────────────────────────────┐
  │ per-partition, per-key queues │  same key: one at a time, in order
  │ and a pool of Workers         │  different keys: in parallel
  └───────────────┬───────────────┘
                  │  is an earlier message with this key waiting in the store?
                  ├──────── yes ──▶ store this one behind it, to keep order ───────┐
                  │ no                                                             │
                  ▼                                                                │
  ┌───────────────────────────────┐                                                │
  │ run the pipeline: step 1,     │  every step has its own breaker, a timeout,    │
  │ step 2, ... and save a        │  optional in-process retries, and an           │
  │ checkpoint after each one     │  idempotency key                               │
  └────┬───────────────┬──────────┘                                                │
       │               │ a step fails                                              │
   all steps ok        ▼                                                           │
       │     ┌─────────────────────────────┐                                       │
       │     │ what kind of failure?       │                                       │
       │     └───┬───────────┬──────────┬──┘                                       │
       │    transient or  permanent    the caller is                               │
       │    circuit open  (retry.      shutting down                               │
       │         │        Permanent)        │                                       │
       ▼         ▼           ▼              ▼                                       │
     DONE    DEFERRED     PARKED       nothing stored,                              │
       │         │           │         nothing blamed                               │
       │         └─────┬─────┘                                                      │
       │               ▼                                                            │
       │   ┌──────────────────────────────┐                                         │
       │   │ Store.Append (fsynced log):  │◀────────────────────────────────────────┘
       │   │ the message, its checkpoint, │  idempotent by topic + partition + offset
       │   │ what it waits on, order key  │
       │   └───────────────┬──────────────┘
       │                   │ once it's on disk, the message counts as safe
       ▼                   ▼
  ┌───────────────────────────────┐   if the write fails, the partition is rewound
  │ commit the offset, but only   │   to that message and paused for a backoff.
  │ as far as every earlier       │   Nothing is skipped.
  │ message is safe               │
  └───────────────────────────────┘

  In parallel: while a circuit used by a topic's pipeline is OPEN, the topic's
  partitions are paused. Messages wait in Kafka, the client keeps polling so the
  group doesn't think the consumer died, and consumption resumes once the
  circuit leaves OPEN.
```

A message can end up in three places, and all three count as safe to acknowledge: **done**, **deferred** (stored with its progress) or **parked** (stored for a person). If a message can't be processed or stored, its offset is never committed.

The offset rule matters with several workers. A later message can finish before an earlier one, so the consumer only commits up to the highest offset where every earlier message in the partition is safe. A slow message holds back the commit, not the other messages. After a crash, messages that were already safe may be delivered again. That's harmless: storing is idempotent and steps carry idempotency keys.

### A stored message coming back

```
    stored: pending, blocked on "payments", checkpoint = "reserve" done
         │
         │  payments' circuit is OPEN: the redriver skips these records, so a
         │  long outage costs no attempts
         │
         │  the circuit leaves OPEN (health checks passed)
         ▼
  ┌────────────────────────────────────────┐
  │ the redriver leases records, limited   │  Rate and Burst cap the speed. RampUp starts
  │ by Rate. The lease is logged before    │  at 10% after a recovery, so a service that
  │ the record leaves the store.           │  just came back isn't flooded.
  └───────────────────┬────────────────────┘
                      ▼
  ┌────────────────────────────────────────┐
  │ pipeline.Handler reads the checkpoint  │  "reserve" doesn't run again.
  │ and runs only the steps not yet done   │  "charge" onwards runs, with the same
  └───────────────────┬────────────────────┘  idempotency keys as before
                      ▼
      ┌───────────────┼─────────────────┬────────────────────┬──────────────┐
   success      circuit open or     permanent            other error      shutdown
      │         blocked again       error                     │               │
      ▼               ▼                ▼                      ▼               ▼
  acknowledged:   back in the        PARKED          nacked with backoff;   released
  removed from    queue, waiting     for a person    PARKED after           untouched
  the log         on the                             MaxAttempts
                  dependency
```

A record that has been leased too many times without ever finishing is parked as a probable crasher. Parked records are never retried or deleted on their own. An operator lists them, looks at where each stopped, fixes the cause, and resumes it at a chosen step (or discards it). See [Operating on parked records](#operating-on-parked-records).

### Side by side

| | HTTP | Kafka |
|---|---|---|
| Purpose | Fail fast, protect both sides | Don't lose messages; resume where they stopped |
| Dependency down | Call refused, the caller decides | Message and checkpoint written to disk, topic paused |
| Retries | Idempotent methods only, with a budget | In-process per step, then redrive with backoff up to `MaxAttempts` |
| Ordering | n/a | Per key: later messages wait behind a stored one |
| Duplicates | n/a | Possible after a crash, so make handlers idempotent |
| Recovery | Health probe, then one canary call | Health probe, canary, then a rate-limited redrive from the checkpoint |

Limits to know about:

- The store is bounded by `MaxBytes` and `MaxRecords`. When it's full, `Append` returns `ErrFull` and the consumer pauses. It doesn't skip messages.
- One process per store directory (it's locked), and Unix only.
- The consumer is tested against a simulated broker. `KAFKA_BROKERS=host:9092 make kafka-integration` runs the same scenarios against a real one (processing and commits, pausing through an outage without leaving the group, redrive and dead-letter mirroring, restart, and rebalance).

## Reference

### HTTP: choosing what is protected

A transport guards only the endpoints you register, each with its own `Policy`. Everything else goes straight to the underlying transport.

```go
rt, err := goguard.New(goguard.Config{
    OnStateChange: func(circuit string, from, to goguard.State) {
        slog.Warn("circuit changed", "circuit", circuit, "from", from.String(), "to", to.String())
    },
},
    goguard.WithEndpoint("payments", goguard.Host("payments.internal"), goguard.Policy{
        FailureThreshold: 0.5,             // open at a 50% failure rate...
        MinSamples:       20,              // ...once at least 20 requests were seen
        SleepWindow:      30 * time.Second,
        MaxRetries:       2,               // idempotent requests only
        RetryBackoff:     retry.Jitter(retry.Exponential(100*time.Millisecond, 2*time.Second), 0.5),
        RequestTimeout:   3 * time.Second,
    }),
    goguard.WithEndpoint("search", goguard.HostPath("api.internal", "/search/"), goguard.Policy{MaxInflight: 50}),
)
if err != nil {
    return fmt.Errorf("invalid guard configuration: %w", err)
}
defer rt.Close()

client := &http.Client{Transport: rt}
```

**Selecting endpoints**

| | |
|---|---|
| `Host("a.test", "b.test")`, `HostPath(host, "/prefix/")`, or any `func(*http.Request) bool` | Matchers. The first registered endpoint that matches wins. |
| `Endpoint.PerHost` | Give each distinct host its own circuit. By default all requests an endpoint matches share one. |
| `goguard.GuardAll(policy)` | Explicitly protect everything not matched, one circuit per host. |
| `goguard.Use(ctx, "payments")` | Send one request through a named endpoint, whatever the matchers say. Unknown names fail with `ErrUnknownEndpoint`. |
| `goguard.Skip(ctx)` | Let one request bypass protection. |
| `goguard.PriorityKey` | Set to `true` in the context to bypass the bulkhead (never the circuit). |

**Good to know**

- Hosts are compared in canonical form (lower case, no trailing dot, no default port), so `Host("example.com")` matches `https://EXAMPLE.com:443/`, and both spellings share one circuit.
- The default transport enables HTTP/2 and keeps 100 idle connections (16 per host, 90 s); set `Config.MaxIdleConns`, `MaxIdleConnsPerHost` and `IdleConnTimeout`, or supply your own `Transport`. `http.Client.CloseIdleConnections` reaches it through the guard.
- Request bodies are closed on every path, including requests rejected by an open circuit.
- When an endpoint's circuits reach `MaxBreakers`, closed circuits are evicted before open ones, so a burst of new hosts cannot wash away the knowledge that a dependency is down.
- `HealthHeader` (a credential for `/health`) is refused with `GuardAll`, which would send it to every host you call.

**Observing**

- `Config.OnStateChange` is delivered in order on its own goroutine, so a slow callback cannot stall requests.
  If it falls `EventBuffer` events behind, events are dropped and counted in `Stats().DroppedEvents`.
- `Config.Events` receives every `obs.Event` synchronously; it must not block. Wrap slow consumers in `obs.NewDispatcher`.
- `rt.Stats()` returns a snapshot of every circuit: state, window counts, failure rate, in-flight, rejections, opens.
- Events carry metadata only. Payloads, headers and credentials never appear in them.


### Any call: the `breaker` package

```go
b := breaker.New(breaker.Config{Name: "orders-db", FailureThreshold: 0.5, MinSamples: 20})

err := b.Do(ctx, func(ctx context.Context) error { return db.PingContext(ctx) }) // db is your *sql.DB
switch {
case errors.Is(err, breaker.ErrOpen):     // the dependency is down: fail fast
case errors.Is(err, breaker.ErrBulkhead): // too many calls in flight
}

rows, err := breaker.Call(ctx, b, func(ctx context.Context) (*sql.Rows, error) {
    return db.QueryContext(ctx, "SELECT id, status FROM orders")
})
```

For work whose outcome you only know later, `Acquire` returns a `Permit`. Finish it with `Success`, `Failure` or `Abandon`, exactly once.

### Retrying: the `retry` package

```go
// reserveStock is your call to the inventory service; inventory is its breaker.
err := retry.Do(ctx, retry.Policy{
    MaxRetries: 3,
    Backoff:    retry.Jitter(retry.Exponential(50*time.Millisecond, time.Second), 1),
    Budget:     retry.NewBudget(0.1, 10), // retries may add about 10% load, and drain during an outage
}, func(ctx context.Context) error {
    return inventory.Do(ctx, reserveStock)
})
```

Return `retry.Permanent(err)` to stop at once. A breaker's `*OpenError` is never retried.

### Recovery: ask the dependency, don't guess

Give a circuit a health check and recovery stops depending on a timer:

```go
goguard.WithEndpoint("payments", goguard.Host("payments.internal"), goguard.Policy{
    HealthPath: "/health", // probed on the same scheme and host as the traffic
    Health:     &health.Config{Interval: 5 * time.Second, SuccessThreshold: 2},
})

// or, for any call, with a check you write (a Check is a func(ctx context.Context) error):
check, err := health.HTTP("http://inventory.internal/health") // healthy on a 2xx answer
if err != nil {
    return err
}
inventory := breaker.New(breaker.Config{Name: "inventory", Health: &health.Config{Check: check}})
defer inventory.Close() // stops the prober
```

`Health` needs a `Check` (or, for HTTP, a `HealthPath`); without one it is ignored. How probing and the canary work is described under [The circuit breaker](#the-circuit-breaker). Details about the probes themselves:

- Probing starts when the circuit opens and stops when it closes. A healthy dependency sees none.
- Probes go through the underlying transport, not the guarded one, and don't touch the circuit's counts.
- They send no body, follow no redirects, read at most a few KiB, and send only the headers you set in `HealthHeader`.
- `obs.ProbeResult` events carry the outcome, status and latency, never the URL or error text.

### Dead-letter records: the `dlq` package

A `dlq.Record` is a piece of work that couldn't be finished (a Kafka message, say), plus where it came from and what it was waiting for. A `dlq.Store` keeps records until they can be retried. Every store follows this contract:

- **`Append` is durable and idempotent.** It returns once the record would survive a crash. Appending the same `ID` again does nothing, so use `dlq.KafkaID(topic, partition, offset)` and a redelivered message maps to the same record.
- **A full store refuses (`ErrFull`).** It never drops or evicts. The caller has to pause.
- **Records are leased, not removed.** `Lease` hands out due records under a fencing token and counts the attempt. If the worker dies, the lease expires and the record is offered again. A stale worker can't ack, nack or overwrite a record someone else holds (`ErrLeaseLost`).
- **`Checkpoint`** saves progress on a leased record, so a retry resumes where the last attempt stopped.
- **`Release`** returns a record without counting an attempt (the dependency was still down). **`Nack`** counts it and delays the next try.
- **`Park`** sets a record aside for a person. It's never retried or deleted automatically. `Parked` lists them (paged, oldest first, with the reason), `Get` inspects any record, and `Requeue`, `RequeueWith` and `Discard` are the ways out.
- **`OrderKey`**: records that share a key are handed out one at a time, in order.
- Delivery is at-least-once, so handlers must tolerate seeing a record twice.

| | Survives a crash | Survives power loss | Use it for |
|---|---|---|---|
| `dlq.NewMemoryStore` | no | no | tests, or work you can lose on restart (capped at 512 MiB unless `MaxBytes` says otherwise; negative removes the cap) |
| `dlq.OpenWAL` | yes | yes (with the default `SyncAlways`) | anything you can't lose |

#### Durable store: `dlq.OpenWAL`

```go
wal, err := dlq.OpenWAL("/var/lib/orders/dlq", dlq.WALOptions{
    MaxBytes: 2 << 30, // payloads live on disk: this is your disk budget
})
if err != nil {
    return fmt.Errorf("opening the dead-letter log: %w", err) // the sections below explain the errors
}
defer wal.Close()
```

A segmented, checksummed write-ahead log: every change is one CRC-32C-framed entry, and the queue is rebuilt from the log on open.

**What it guarantees**

- **Acknowledged means on disk.** With `SyncAlways` (the default) an operation returns only after its entry is fsynced. Concurrent operations share one fsync (group commit): on a test machine, about 280 µs per append when called one at a time, about 18 µs (55,000 a second) with concurrent callers.
- **It fails closed.** If a write or fsync fails, the store stops serving (`ErrStoreFailed`) and releases everyone waiting; it never retries a failed fsync, because after one the disk's state is unknown. A failed write is rolled back so half an entry never sits in front of later data. Close and reopen: recovery restores every acknowledged operation.
- **Recovery repairs what a crash can cause and refuses everything else.** A torn final entry is cut back to the last complete one, and the cut bytes are first copied to a `.quarantine-*` file: nothing is destroyed. A damaged entry that is *followed by valid entries* is bit rot, not an interrupted write, so `OpenWAL` refuses (`ErrCorrupt`) rather than discard entries that may have been acknowledged; `ForceRepair` overrides that deliberately. Damage in earlier segments, a gap in the sequence, a missing segment or a bad snapshot always refuses and modifies nothing. `FailOnTruncation` refuses even the torn-tail repair.
- **Poison pills are counted across crashes.** A lease is logged before the record leaves the store, so a record that kills its worker keeps its attempt count and can be parked. The lease itself dies with the process.
- **One writer, owner-only.** The directory is locked against a second store (`ErrLocked`), created `0700` with `0600` files, and an existing directory or file that others can read or write is refused (`ErrInsecurePermissions`, override with `AllowInsecurePermissions`).
- **A full disk is backpressure, not a failed write.** `MinFreeBytes` (default 64 MiB) is kept free on the log's volume: below it `Append` and `Checkpoint` return an error matching both `ErrFull` and `ErrDiskLow` (so consumers pause), while acks, parks and requeues keep working. Compaction, which needs room for a snapshot as large as the live data, waits for it. `WALStats()` reports `DiskFreeBytes` and `DiskLow`.
- **Tamper evidence (optional).** With `WALOptions.Signer` (a `secure.NewHMAC` signer, its key kept off the volume) every entry and snapshot record is signed over its LSN, type and body. A forged entry is refused on open (`ErrTampered`, also `ErrCorrupt`) and never repaired, and a payload changed on disk is never handed out. To move an existing log to signing, open it with `AcceptUnsignedLegacy` until `Recovery().UnsignedFiles` reaches zero after a compaction, then turn it off (while it is on, someone who can rewrite the files can strip signatures). Signing cannot show that the newest entries were cut off the end of the log; keep backups if that matters.
- **Bounded disk.** The log is compacted into a snapshot and old segments are deleted as it grows (`CompactMinBytes`, `CompactRatio`, or call `Compact`). Compaction copies payloads without holding the store's lock, so appends and leases carry on while it runs.
- **Payloads stay on disk.** Memory holds a small index per record (about 300 bytes) and the payload (key, value, headers, checkpoint) is read back from the log when a record is leased, fetched or listed. 100 records of 256 KiB grow the heap by about 50 KiB instead of 25 MiB. Every read is checked (CRC, frame type, record ID). A payload that cannot be read (bit rot, a deleted or truncated file) is never handed out: the record is parked with a fixed reason, counted in `WALStats().UnreadablePayloads`, and listed by `Parked` without a payload so an operator can find it; restore the files from a backup, or `Discard` it. Compaction refuses to run past such a record rather than drop it. Payloads are read outside the store's lock (a lease picks and reserves records, reads them, then confirms nothing changed and logs the lease), so a slow disk does not stall `Append`. Leasing costs roughly 4 µs more per record than with `PayloadsInMemory`.

**Durability policy** (`WALOptions.Sync`): `SyncAlways` survives power loss. `SyncInterval` fsyncs every `SyncEvery` and survives a process crash but can lose that window on power loss. `SyncNone` fsyncs only on rotation and `Close`, for tests.

**Limits to plan for**

- **`MaxBytes` (default 512 MiB) bounds the live data on disk** and `MaxRecords` (default 1,000,000) bounds the index in memory; beyond either, `Append` returns `ErrFull`. Size them for the worst outage you want to ride out. Set `PayloadsInMemory` to keep everything in RAM (faster leases, and then `MaxBytes` is also your memory budget); compaction then blocks other operations while it writes.
- **Compaction briefly holds a copy of every record's metadata** (about the size of the in-memory index, up to roughly 300 MB at the default `MaxRecords`) while it copies payloads without the lock.
- **Unix only.** Directory locking is not implemented elsewhere, so `OpenWAL` refuses to run without it.
- Quarantine files are kept until you remove them. They hold whatever was in the damaged tail, in the clear unless the store is wrapped with `dlq.Secure`.
- Wrap it with `dlq.Secure` to encrypt payloads at rest; the log itself only checksums.

#### Encrypting and signing: the `secure` package

```go
// key32 and oldKey32 are 32-byte secrets from your secret store; pepper is another.
enc, err := secure.NewAESGCM(secure.Key{ID: "2025-06", Material: key32}, // seals new data
    secure.Key{ID: "2024-11", Material: oldKey32})                       // still opens old data
if err != nil {
    return err
}

// wal is what dlq.OpenWAL returned. Wrap any store the same way.
store, err := dlq.Secure(wal, dlq.SecureOptions{
    Encryptor:      enc,
    OrderKeyPepper: pepper, // keeps the Kafka key (the customer) out of the log while ordering still works
})
```

`dlq.Secure` wraps any store:

- `Key`, `Value`, header values and `Checkpoint` are sealed with **AES-256-GCM**, bound to the record ID and field so ciphertext cannot be moved between records. Sealed data names its key, so **keys rotate** without rewriting the queue. The `Encryptor` interface lets you delegate to a KMS or HSM.
- `LastError`, `Nack` errors and `Park` reasons pass through a **`Redactor`** that removes bearer tokens, JWTs, `user:pass@` URLs, and `password=`/`token=`-style values, and truncates long text so a payload cannot leak through an error message.
- A record that **cannot be decrypted** (a lost key, tampering) is parked and reported through `OnUndecryptable`. It is never returned as ciphertext, never dropped, and never left to crash-loop a worker.
- What stays in clear so stores can index and operators can inspect: IDs, source, header names, timestamps, attempt counts and (without a pepper) `OrderKey`.

`secure.NewHMAC` provides key-ID-aware HMAC-SHA256 signing for stores that keep data unencrypted.

The redactor recognises common credential shapes. It is a safety net for diagnostics, not a guarantee; encrypt payloads rather than relying on it.

### Redriving stored work: `dlq.Redriver`

```go
redriver, err := dlq.NewRedriver(dlq.RedriveConfig{
    Store:       store,
    Handler:     orders.Handler(), // finishes each stored order, starting at the step that failed
    Breakers:    map[string]*breaker.Breaker{"payments": payments},
    Rate:        200, RampUp: 30 * time.Second, // don't flood a dependency that just came back
    MaxAttempts: 10,
})
if err != nil {
    return err
}
go func() { _ = redriver.Run(ctx) }() // returns when ctx ends, after finishing or releasing what it holds
```

What it does with each record:

- Records are tagged with the dependency that blocked them (`BlockedOn`). While that dependency's circuit is open, its records aren't leased at all, so a long outage costs no attempts.
- Replay is rate-limited. After a recovery the rate starts at 10% and climbs to `Rate` over `RampUp`. Add `r.Sink()` to your breakers' `Events` and the redriver reacts to a recovery immediately instead of at its next poll.
- With a pipeline, `orders.Handler()` is the handler. For any other kind of record, write your own `func(ctx context.Context, item *dlq.Item) error`. Its return value decides the outcome:

| Handler returns | Result |
|---|---|
| `nil` | the record is removed |
| `*dlq.BlockedError`, or an error matching `breaker.ErrOpen` | held until that dependency recovers (an open circuit doesn't cost an attempt) |
| `retry.Permanent(err)` | parked for a person |
| any other error | retried after a backoff |

- A record is parked after `MaxAttempts` failures. It's also parked if its lease has been taken `MaxAttempts` times without finishing, which catches a handler that crashes on it. A rejection by an open circuit is never counted against the record. A `BlockedError` for a call that was made and failed is counted, so a message that itself breaks a dependency can't loop forever. Set `Refund` on it only when the call was never made.
- A `BlockedError` for a failed call also backs off (`Backoff`), so a struggling dependency isn't hit every second.
- A store hiccup (timeout, dropped connection) doesn't stop the redriver. It backs off, retries and counts the error in `Stats().StoreErrors`. Only a store that has failed for good (`ErrStoreFailed`, `ErrClosed`, `ErrCorrupt`) ends `Run` with an error.
- A record is never handled twice at once, even by a handler that outlives its lease. The handler's context ends before the lease does. A handler that panics counts as a failure, and the panic text isn't stored. Stored error text is redacted. On shutdown, unfinished records are released without penalty.
- `obs.Redrive` events and `Stats()` report every outcome. Per-key order holds with any number of workers.

### Multi-step work that resumes: the `pipeline` package

When one message triggers several calls, a failure in the third shouldn't restart the first two or lose the message.

```go
// Each Run and Compensate is a func(ctx context.Context, x *pipeline.Exec) error, like post above.
orders, err := pipeline.New("orders", "v1", []pipeline.Step{
    {Name: "reserve", Breaker: inventory, Run: reserveStock, Compensate: releaseStock},
    {Name: "charge",  Breaker: payments,  Run: chargeCard,   Compensate: refundCard},
    {Name: "ship",    Breaker: shipping,  Run: createShipment},
}, pipeline.Saga())
if err != nil {
    return err
}

// The consumer does this for every message. Call Execute yourself only if you have
// your own message loop; msg is a kafka.Message.
result, err := orders.Execute(ctx, store, pipeline.Input{
    ID:    dlq.KafkaID(msg.Topic, msg.Partition, msg.Offset),
    Key:   msg.Key,
    Value: msg.Value,
})
// Done, Deferred and Parked are safe to acknowledge. Failed is not: do not commit the offset.

// Elsewhere, once:
redriver, err := dlq.NewRedriver(dlq.RedriveConfig{Store: store, Handler: orders.Handler(), Breakers: breakers})
```

- Each step is checkpointed as it finishes, along with anything it saved with `x.Set`. If `charge` can't finish, the message and its progress are stored, tagged with `charge`'s dependency. When that recovers, the redriver resumes at `charge`. `reserve` doesn't run again, and `ship` can still read what `reserve` produced.
- Only the blocking dependency matters. Records blocked on `payments` wait while its circuit is open. Records blocked on `shipping` are unaffected.
- Delivery is at-least-once. If the process dies after a step's work but before its checkpoint is durable, that step runs again. `x.IdempotencyKey()` is the same for a given message and step on every attempt and restart, so pass it to the downstream call and a repeat does no harm. Progress made live is stored only when a step defers, so a crash mid-run redelivers the message and reruns it from the top with the same keys.
- `Saga()` undoes the completed steps, newest first, when a step fails permanently. Each compensation is checkpointed, so a crash or outage part-way resumes compensating and never undoes a step twice. The record is then parked so a person can see what happened. A compensation that itself fails permanently says so.
- A checkpoint that doesn't match the deployed pipeline is never resumed. Renaming, reordering or removing a step that a stored record already completed, or a checkpoint from another pipeline, parks the record with `ErrPipelineMismatch`. Adding steps at the end or changing the version label is fine.
- Steps can have a `Timeout`, in-process `Retry` and a `Breaker`. An open circuit defers the message without calling the dependency.

#### Operating on parked records

Parked records wait for a person. You can list them, see where each stopped, and resume it:

```go
parked, err := store.Parked(ctx, dlq.ParkedQuery{Limit: 50}) // what is stuck, and why (LastError)
if err != nil {
    return err
}
for _, rec := range parked {
    progress, err := orders.Describe(rec.Checkpoint) // steps done, the step it stopped at; no message data
    if err != nil {
        continue // a checkpoint that doesn't fit the deployed pipeline needs a human look
    }
    fmt.Println(rec.ID, rec.LastError, progress.Completed, progress.Next)
}

// The cause is fixed. Resume one order at a chosen step; the steps before it are not repeated.
err = orders.Redrive(ctx, store, parked[0].ID, "charge")
```

- `Redrive` is atomic. The checkpoint replacement and the requeue are one durable log entry (`Store.RequeueWith`), so a crash can't leave the record pending with its old checkpoint. It works through `dlq.Secure` (the new checkpoint is sealed like any other). It changes nothing and returns an error if the record isn't parked or the step isn't valid.
- It won't skip work. Resuming at a step whose earlier steps haven't completed is refused (`ErrCannotSkipForward`). So is resuming after a step that was already compensated, since its effects are gone (`ErrStepUndone`). Rewind to that step or earlier instead.
- Steps re-run without having been compensated keep their idempotency keys, because a downstream service sees a genuine repeat. If you rewind past steps that a saga already undid, the pipeline starts a new epoch and their keys change. Otherwise a service that deduplicates would treat the redo as already done. (Epoch 0 keys are exactly what earlier versions produced.)
- A compensated record can't just be requeued as it was, because it would finish compensating and park again. Give `Redrive` the step to resume at.
- Data that earlier steps saved with `x.Set` is kept through a rewind.

### Kafka consumers: the `kafka` module

Kafka support lives in its own Go module, `github.com/justinclev/GoGuardLib/kafka`, so HTTP-only users never pull in the Kafka client or cgo. The consumer logic (package `kafka`) is written against a small `Client` interface and imports no Kafka library; package `kafka/confluent` adapts [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) (which bundles librdkafka; it needs cgo).

```go
client, err := confluent.NewClient(ck.ConfigMap{
    "bootstrap.servers": brokers, "group.id": "orders-service", "auto.offset.reset": "earliest",
    // security.protocol, sasl.*, ssl.*: yours to set; nothing here logs them
}, []string{"orders"})
if err != nil {
    return err
}
defer client.Close()

// Optional: copy messages that get parked to a dead-letter topic ("orders.dlq").
producer, err := confluent.NewProducer(ck.ConfigMap{"bootstrap.servers": brokers})
if err != nil {
    return err
}
defer producer.Close()
mirror, err := kafka.NewDLQPublisher(kafka.PublisherConfig{Producer: producer})
if err != nil {
    return err
}

// store is the durable dlq.Store from above; orders is the pipeline.
consumer, err := kafka.NewConsumer(kafka.Config{
    Client:   client,
    Store:    store,
    Bindings: []kafka.Binding{{Topic: "orders", Pipeline: orders}},
    Mirror:   mirror,
})
if err != nil {
    return err
}
err = consumer.Run(ctx) // returns nil when ctx ends; an error only if it can no longer be safe
```

Only the topics you bind are consumed and guarded. For every message the consumer builds a stable ID (`dlq.KafkaID(topic, partition, offset)`) and runs it through the topic's pipeline.

How the consumer behaves:

- **Commits.** An offset is committed only when its message is safe: fully processed, or durably stored with its progress. Commits are batched (`CommitInterval`, `CommitBatch`) and always happen on rebalance and shutdown. After a crash, messages that were already safe may be redelivered. Storing is idempotent by ID and steps carry idempotency keys, so that's harmless.
- **A dependency is down.** If a breaker used by a topic's pipeline is open, the topic's partitions are paused and the messages stay in Kafka. Kafka holds a backlog far more cheaply than a local queue, and the consumer keeps polling so the group doesn't think it died (`TestRealBrokerPausesThroughAnOutageAndStaysInTheGroup` checks this against a real broker). When the circuit goes half-open, consumption resumes and a real message is the canary.
- **A message can't finish.** A failing step defers the message, with its checkpoint, to the `Store`. The topic keeps flowing, and a `dlq.Redriver` running `pipeline.Handler()` completes it later, from the step that failed.
- **Nothing is skipped.** If a message can be neither processed nor stored (the store is full or unavailable), the partition is rewound to it and paused for a backoff, and nothing behind it runs first (`Stats().Backpressure` counts the store-full cases). The consumer also refuses any message that isn't exactly the next offset, so a gap or a stale delivery can't be committed past. If the store has failed for good, or the consumer can't seek back, `Run` returns an error instead of continuing past a message it can't protect.
- **A message that hangs.** If processing outlives `ProcessTimeout`, the message is stored with its progress like any other failure and the offset moves on. The redriver counts its attempts and parks it if it keeps hanging. Cancelling the context is different: that's you stopping, and nothing is stored.
- **Ordering.** While a message with a key waits in the store, later messages with the same key are stored behind it (`dlq.Store.HasOrderKey`) and replayed in order. Other keys are unaffected. `IgnoreKeyOrder` turns this off.
- **Rebalances.** Progress on revoked partitions is committed before they're released, and pauses are re-applied to partitions that come back. Eager and cooperative protocols both work. On shutdown the final commit keeps polling and retrying (`ShutdownTimeout`), because Kafka refuses a commit during a rebalance.
- **Dead-letter topic (optional).** `kafka.NewDLQPublisher` copies parked messages to `<topic>.dlq` with the original key, value and headers plus `x-goguard-*` metadata (source topic, partition, offset, reason, attempts, record ID). Pass `publisher.Publish` as `dlq.RedriveConfig.OnPark`, and set `kafka.Config.Mirror` for messages the pipeline parks. It's best effort, since the message is already parked in the store. The producer is idempotent with `acks=all`. The copy leaves the store, so it's opt-in. Set `PublisherConfig.Encryptor` to seal the key, value and header values before they leave the process (header names and the metadata stay readable, and an `x-goguard-sealed` header is added). Readers open them with `kafka.OpenDeadLetter` and the same keys.

`confluent.NewClient` takes `confluent.WithErrorHandler(func(error))` for errors librdkafka retries on its own (brokers unreachable, authentication failing). Without it, an unreachable cluster looks like an idle topic. A message that arrives carrying an error stops the consumer instead of being skipped.

**Concurrency.** By default the consumer handles one message at a time, in offset order. Set `Config.Workers` to process several at once. A pool handles them, and `MaxInFlight` (default 4 per worker) caps what's been fetched but not finished; beyond it, partitions pause. Commits stay contiguous: an offset is committed only when it and everything before it in its partition is safe. Messages with the same key run one at a time in offset order, and a message that fails keeps later messages with its key from starting. Different keys never wait for each other. A failed message is retried from its offset after a backoff; messages after it that already finished are remembered and not run again. If a partition is revoked mid-run, only the finished contiguous prefix is committed and the new owner re-reads the rest. Your steps run concurrently, so they must be safe to.

**Requirements.** Processing one message has to finish within `max.poll.interval.ms` (`ProcessTimeout` bounds it). Set `auto.offset.reset` yourself. Auto-commit and offset storing are forced off. Run the integration tests against a broker with `KAFKA_BROKERS=host:9092 make kafka-integration`.


## Upgrading from the guard-everything transport

This release changes behaviour deliberately:

- **Nothing is guarded unless registered.** To keep the old behaviour, add `goguard.GuardAll(policy)`.
- Breaker settings moved from `Config` to `Policy`; `WithTimeout`, `WithRetries`, `WithRetryBudget` and `WithBulkhead` now refine a `Policy`.
- `MinSamples` defaults to 10 and `FailureThreshold` to 0.5 (previously 0, which opened on the first failure).
- Latency-outlier detection is off unless `Policy.OutlierFactor` is set (previously any response over twice the average latency counted as a failure).
- `NewResilientTransport` panics on invalid configuration; use `New` to receive the error.
- `CircuitError.Host` is the circuit name: the endpoint name, or the host for per-host circuits.
- A bulkhead rejection is now reported as `breaker.ErrBulkhead`, not `ErrCircuitOpen`.
- The retry budget now counts failed requests as traffic, so it limits retries when every request is failing.
- `Policy.HeartbeatInterval` and `HeartbeatFunc` are replaced by `Policy.Health` / `HealthPath` (see "Recovery"), and `breaker.RecordProbe` is gone: probes are driven by package `health`.
- A circuit that closes starts with a clean sampling window, so failures from before an outage cannot reopen it on the first new error.
- `Breaker.Close` now exists; call it if the breaker has a health check.
- `dlq.OpenWAL` caps the in-memory index at 1,000,000 records by default when payloads are on disk (`MaxRecords`; negative removes the cap).
- `dlq.NewMemoryStore` is capped at 512 MiB by default (`ErrFull` beyond it, which the consumer turns into backpressure); set `MaxBytes` negative for the old unlimited behaviour.
- `Store.Parked` may return a page shorter than `Limit` when the payload would exceed about 32 MiB; page until an empty result.
- `goguard.Host` and `HostPath` ignore the default ports (`:80`, `:443`) and a trailing dot; circuit names for per-host circuits use the same canonical form.
- A `pipeline` run that outlives its context deadline is stored against its step (it used to fail without being stored); a cancelled context still stores nothing.
- `dlq.Store` gained `HasOrderKey`, `Get`, `Parked` and `RequeueWith` (implement them in custom stores), plus `NackOptions.Refund` and `LeaseRequest.Skip`.

## Review

`docs/AUDIT.md` records a full review of the project (bottlenecks, data loss, security, usability, reliability, scalability): what was found, what was fixed and how it was verified, and what is deliberately left open.

## Roadmap

Possible follow-up: metrics adapters (Prometheus / OpenTelemetry) as separate modules.

## License

MIT
