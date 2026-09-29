# GoGuardLib

Circuit breaking, bulkheads, timeouts and safe retries for Go services.

- `goguard` guards outbound **HTTP** as an `http.RoundTripper`.
- `breaker` is a general circuit breaker for any call: a database query, a gRPC client, a message handler.
- `retry` provides backoff, jitter, retry budgets and permanent-error marking.
- `obs` defines the events the library emits.

The library has no dependencies outside the standard library and **never logs or prints**.
Everything observable is delivered as a typed event to a sink you supply, or read on demand with `Stats()`.
A lint rule (`forbidigo`) fails the build if logging or printing is added to non-test code.

## Installation

```bash
go get github.com/justinclev/GoGuardLib
```

## HTTP: you choose what is protected

Protection is **opt-in**. A transport guards only the endpoints you register, each with its own `Policy`.
Every other request passes straight to the underlying transport with no breaker, no retry and no allocated state.

```go
rt, err := goguard.New(goguard.Config{
    OnStateChange: func(circuit string, from, to goguard.State) {
        metrics.RecordTransition(circuit, from.String(), to.String())
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
if err != nil { /* invalid configuration */ }
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

**Observing**

- `Config.OnStateChange` is delivered in order on its own goroutine, so a slow callback cannot stall requests.
  If it falls `EventBuffer` events behind, events are dropped and counted in `Stats().DroppedEvents`.
- `Config.Events` receives every `obs.Event` synchronously; it must not block. Wrap slow consumers in `obs.NewDispatcher`.
- `rt.Stats()` returns a snapshot of every circuit: state, window counts, failure rate, in-flight, rejections, opens.
- Events carry metadata only. Payloads, headers and credentials never appear in them.

## Any call: the `breaker` package

```go
b := breaker.New(breaker.Config{Name: "inventory-db", FailureThreshold: 0.5, MinSamples: 20})

err := b.Do(ctx, func(ctx context.Context) error { return db.PingContext(ctx) })
switch {
case errors.Is(err, breaker.ErrOpen):     // dependency is down: fail fast
case errors.Is(err, breaker.ErrBulkhead): // too many calls in flight
}

rows, err := breaker.Call(ctx, b, func(ctx context.Context) (*sql.Rows, error) { return db.QueryContext(ctx, q) })
```

- The caller cancelling its own context is never counted as a dependency failure.
- Use `IsFailure` to say which errors mean the dependency is unhealthy (a "not found" usually does not).
- For work whose outcome is known later, `Acquire` returns a `Permit` to finish with `Success`, `Failure` or `Abandon`.

## Retrying: the `retry` package

```go
err := retry.Do(ctx, retry.Policy{
    MaxRetries: 3,
    Backoff:    retry.Jitter(retry.Exponential(50*time.Millisecond, time.Second), 1),
    Budget:     retry.NewBudget(0.1, 10), // retries may add ~10% load; drains during an outage
}, func(ctx context.Context) error {
    return b.Do(ctx, call)
})
```

Return `retry.Permanent(err)` from the function to stop immediately. A breaker's `*OpenError` is never retried.

## Dead-letter records: the `dlq` package

A `dlq.Record` captures work that could not be finished (a Kafka message, a request) together with where it came from and what was blocking it. A `dlq.Store` keeps records safe until they can be retried. The contract every store honours:

- **`Append` is durable and idempotent.** It returns only once the record would survive a crash, and appending the same `ID` again is a no-op. Use `dlq.KafkaID(topic, partition, offset)` so a message redelivered after a crash maps to the same record.
- **A full store refuses (`ErrFull`); it never drops or evicts.** The caller must pause instead of losing data.
- **Records are leased, not removed.** `Lease` hands out due records under a fencing token and counts the attempt. If the worker crashes, the lease expires and the record is offered again. A stale worker cannot ack, nack or overwrite a record someone else now holds (`ErrLeaseLost`).
- **`Checkpoint`** saves progress on a leased record so a retry resumes where the last attempt stopped.
- **`Release`** returns a record without counting the attempt (for when the dependency was still down); **`Nack`** counts it and delays the next try.
- **`Park`** sets a record aside for a human. Parked records are never retried or deleted automatically; `Requeue` and `Discard` are explicit.
- **`OrderKey`** gives per-key ordering: records sharing a key are handed out one at a time, in order.
- Delivery is **at-least-once**. Handlers must tolerate seeing a record twice.

`dlq.NewMemoryStore` is the reference implementation. It is **not durable** and suits tests and work you can afford to lose on restart; a durable write-ahead-log store and a Kafka store are next on the roadmap. Any store can be checked against the contract with `storetest.Run` from `dlq/storetest`.

## Security: the `secure` package

```go
enc, _ := secure.NewAESGCM(secure.Key{ID: "2025-06", Material: key32}, // seals new data
    secure.Key{ID: "2024-11", Material: oldKey32})                     // still opens old data

store, _ := dlq.Secure(dlq.NewMemoryStore(dlq.MemoryOptions{}), dlq.SecureOptions{
    Encryptor:      enc,
    OrderKeyPepper: pepper, // keeps message keys out of the store while ordering still works
})
```

`dlq.Secure` wraps any store:

- `Key`, `Value`, header values and `Checkpoint` are sealed with **AES-256-GCM**, bound to the record ID and field so ciphertext cannot be moved between records. Sealed data names its key, so **keys rotate** without rewriting the queue. The `Encryptor` interface lets you delegate to a KMS or HSM.
- `LastError`, `Nack` errors and `Park` reasons pass through a **`Redactor`** that removes bearer tokens, JWTs, `user:pass@` URLs, and `password=`/`token=`-style values, and truncates long text so a payload cannot leak through an error message.
- A record that **cannot be decrypted** (a lost key, tampering) is parked and reported through `OnUndecryptable`. It is never returned as ciphertext, never dropped, and never left to crash-loop a worker.
- What stays in clear so stores can index and operators can inspect: IDs, source, header names, timestamps, attempt counts and (without a pepper) `OrderKey`.

`secure.NewHMAC` provides key-ID-aware HMAC-SHA256 signing for stores that keep data unencrypted.

The redactor recognises common credential shapes. It is a safety net for diagnostics, not a guarantee; encrypt payloads rather than relying on it.

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

## Roadmap

Next: a durable write-ahead-log store, health-check-driven recovery with automatic redrive, multi-step pipelines that resume from the failed step, and a Kafka consumer adapter.

## License

MIT
