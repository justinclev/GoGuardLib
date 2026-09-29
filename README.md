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

## Recovery: ask the dependency, don't guess

Every dependency's API can tell you whether it is back. Give a circuit a health check and recovery stops depending on a timer:

```go
goguard.WithEndpoint("payments", goguard.Host("payments.internal"), goguard.Policy{
    HealthPath: "/health", // probed on the same scheme and host as the traffic
    Health:     &health.Config{Interval: 5 * time.Second, SuccessThreshold: 2},
})

// or, for any call:
b := breaker.New(breaker.Config{Name: "inventory", Health: &health.Config{Check: pingInventory}})
defer b.Close() // stops the prober
```

- **A healthy dependency sees no probe traffic.** Probing starts when the circuit opens and stops when it closes.
- **Probes back off with jitter** while the dependency is down (`Interval` up to `MaxInterval`), so a struggling service is not hammered and many instances do not probe in lockstep.
- **Health opens the door; real traffic proves it.** After `SuccessThreshold` healthy checks the circuit moves to half-open, and the next real request is a canary that closes it. A dependency can answer `/health` and still fail real work. `TrustHealth` skips the canary if you would rather trust the check.
- **A health check that lies cannot block traffic forever.** After `MaxOpen` (default 5 minutes; negative disables) the timer-based canary resumes whatever the check says.
- **Probes are not traffic.** They go through the underlying transport, never the guarded one, and never touch the circuit's counts. They send no payload, follow no redirects, read a few KiB at most, and send only the headers you set (`HealthHeader`). Probe events (`obs.ProbeResult`) carry the outcome, status and latency, never the URL or error text.
- Without `Health`, a circuit still recovers on its `SleepWindow` timer.

## Dead-letter records: the `dlq` package

A `dlq.Record` captures work that could not be finished (a Kafka message, a request) together with where it came from and what was blocking it. A `dlq.Store` keeps records safe until they can be retried. The contract every store honours:

- **`Append` is durable and idempotent.** It returns only once the record would survive a crash, and appending the same `ID` again is a no-op. Use `dlq.KafkaID(topic, partition, offset)` so a message redelivered after a crash maps to the same record.
- **A full store refuses (`ErrFull`); it never drops or evicts.** The caller must pause instead of losing data.
- **Records are leased, not removed.** `Lease` hands out due records under a fencing token and counts the attempt. If the worker crashes, the lease expires and the record is offered again. A stale worker cannot ack, nack or overwrite a record someone else now holds (`ErrLeaseLost`).
- **`Checkpoint`** saves progress on a leased record so a retry resumes where the last attempt stopped.
- **`Release`** returns a record without counting the attempt (for when the dependency was still down); **`Nack`** counts it and delays the next try.
- **`Park`** sets a record aside for a human. Parked records are never retried or deleted automatically. `Parked` lists them (paged, oldest first, with the reason), `Get` inspects any record, and `Requeue`, `RequeueWith` and `Discard` are the explicit ways out.
- **`OrderKey`** gives per-key ordering: records sharing a key are handed out one at a time, in order.
- Delivery is **at-least-once**. Handlers must tolerate seeing a record twice.

### Choosing a store

| | Survives a process crash | Survives power loss | Use it for |
|---|---|---|---|
| `dlq.NewMemoryStore` | no | no | tests, or work you can afford to lose on restart |
| `dlq.OpenWAL` | yes | yes (with the default `SyncAlways`) | anything you must not lose |

Any store can be checked against the contract with `storetest.Run` from `dlq/storetest`.

### Durable store: `dlq.OpenWAL`

```go
store, err := dlq.OpenWAL("/var/lib/myapp/dlq", dlq.WALOptions{
    MaxBytes: 2 << 30, // the live queue is held in memory: this is your RAM budget
})
if err != nil { /* see below */ }
defer store.Close()
```

A segmented, checksummed write-ahead log: every change is one CRC-32C-framed entry, and the queue is rebuilt from the log on open.

**What it guarantees**

- **Acknowledged means on disk.** With `SyncAlways` (the default) an operation returns only after its entry is fsynced. Concurrent operations share one fsync (group commit): on a test machine, about 280 µs per append when called one at a time, about 18 µs (55,000 a second) with concurrent callers.
- **It fails closed.** If a write or fsync fails, the store stops serving (`ErrStoreFailed`) and releases everyone waiting; it never retries a failed fsync, because after one the disk's state is unknown. A failed write is rolled back so half an entry never sits in front of later data. Close and reopen: recovery restores every acknowledged operation.
- **Recovery repairs what a crash can cause and refuses everything else.** A torn final entry is cut back to the last complete one, and the cut bytes are first copied to a `.quarantine-*` file: nothing is destroyed. A damaged entry that is *followed by valid entries* is bit rot, not an interrupted write, so `OpenWAL` refuses (`ErrCorrupt`) rather than discard entries that may have been acknowledged; `ForceRepair` overrides that deliberately. Damage in earlier segments, a gap in the sequence, a missing segment or a bad snapshot always refuses and modifies nothing. `FailOnTruncation` refuses even the torn-tail repair.
- **Poison pills are counted across crashes.** A lease is logged before the record leaves the store, so a record that kills its worker keeps its attempt count and can be parked. The lease itself dies with the process.
- **One writer, owner-only.** The directory is locked against a second store (`ErrLocked`), created `0700` with `0600` files, and an existing directory or file that others can read or write is refused (`ErrInsecurePermissions`, override with `AllowInsecurePermissions`).
- **Bounded disk.** The log is compacted into a snapshot and old segments are deleted as it grows (`CompactMinBytes`, `CompactRatio`, or call `Compact`).

**Durability policy** (`WALOptions.Sync`): `SyncAlways` survives power loss. `SyncInterval` fsyncs every `SyncEvery` and survives a process crash but can lose that window on power loss. `SyncNone` fsyncs only on rotation and `Close`, for tests.

**Limits to plan for**

- **The whole live queue is held in memory**, capped by `MaxBytes` (default 512 MiB); beyond it `Append` returns `ErrFull`. Size it for the worst outage you want to ride out.
- **Compaction blocks other operations** while it writes the snapshot, for a time proportional to the live data.
- **Unix only.** Directory locking is not implemented elsewhere, so `OpenWAL` refuses to run without it.
- Quarantine files are kept until you remove them. They hold whatever was in the damaged tail, in the clear unless the store is wrapped with `dlq.Secure`.
- Wrap it with `dlq.Secure` to encrypt payloads at rest; the log itself only checksums.

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

## Redriving stored work: `dlq.Redriver`

```go
r, _ := dlq.NewRedriver(dlq.RedriveConfig{
    Store:    store,
    Handler:  func(ctx context.Context, item *dlq.Item) error { return reprocess(ctx, item.Record) },
    Breakers: map[string]*breaker.Breaker{"payments": paymentsBreaker},
    Rate:     200, RampUp: 30 * time.Second, // don't flood a dependency that just came back
    MaxAttempts: 10,
})
go r.Run(ctx) // returns when ctx ends, after finishing or releasing what it holds
```

- **Records wait for their dependency.** A record is tagged with what blocked it (`BlockedOn`). While that dependency's circuit is open its records are not even leased: a long outage costs no attempts and no churn.
- **Recovery drains the backlog gently.** A rate limit spreads the replay, and after a recovery the rate ramps up from 10% over `RampUp`. Add `r.Sink()` to your breakers' events to react to recovery at once instead of at the next poll.
- **The handler's return value decides what happens:** `nil` removes the record; a `*dlq.BlockedError` or a `breaker.ErrOpen` holds it for that dependency (an open circuit costs no attempt); `retry.Permanent(err)` parks it for a human; any other error retries with backoff.
- **Poison pills park; outages don't.** A record that fails `MaxAttempts` times is parked, and so is one whose lease was taken `MaxAttempts` times without finishing (a handler that keeps crashing on it never sees it again). Being rejected by an open circuit is not the record's fault and is never counted. A `BlockedError` for a call that was made and failed *is* counted, so a message that itself breaks a dependency cannot loop forever; set `Refund` only when the call was never made.
- **Safe under failure.** One record is never handled twice at once, even by a handler that outlives its lease; the handler's context ends before the lease does; a handler that panics is treated as a failure without keeping the panic text; stored error text is redacted; on shutdown, unfinished records are released without penalty.
- Events (`obs.Redrive`) and `Stats()` report every outcome. Per-key ordering (`OrderKey`) is preserved with any number of workers.

## Multi-step work that resumes: the `pipeline` package

A message that triggers several calls should not start over, or be lost, when the third is down.

```go
p, _ := pipeline.New("orders", "v3", []pipeline.Step{
    {Name: "reserve", Breaker: inventory, Run: reserveStock},
    {Name: "charge",  Breaker: payments,  Run: chargeCard, Compensate: refund},
    {Name: "ship",    Breaker: shipping,  Run: createShipment},
    {Name: "notify",  Run: sendEmail},
}, pipeline.Saga())

// A message arrives (for example from Kafka):
res, err := p.Execute(ctx, store, pipeline.Input{ID: dlq.KafkaID(topic, part, off), Key: k, Value: v})
// Done, Deferred and Parked are safe to acknowledge. Failed is not: do not commit the offset.

// Elsewhere, once:
r, _ := dlq.NewRedriver(dlq.RedriveConfig{Store: store, Handler: p.Handler(), Breakers: breakers})
```

- **Each step is checkpointed as it finishes**, along with the data it saved with `x.Set`. If `charge` cannot finish, the message and its progress are stored, tagged with `charge`'s dependency. When that recovers, the redriver resumes at `charge`: `reserve` is **not** run again, and `ship` can still read what `reserve` produced.
- **Only the blocking dependency matters.** Records blocked on `payments` wait while its circuit is open; records blocked on `shipping` are unaffected.
- **At-least-once, made safe.** If the process dies after a step's work but before its checkpoint is durable, that step runs again. `x.IdempotencyKey()` is stable for the message and step across every attempt and restart: pass it to the downstream call so a repeat is harmless. Progress made *live* is only stored when a step defers, so a crash mid-run redelivers the message and reruns from the top with the same keys.
- **`Saga()`** undoes completed steps, newest first, when a step fails permanently. Each compensation is checkpointed, so an outage or crash mid-way resumes compensating and never undoes a step twice. The record is then parked, so a person can see what happened; a compensation that itself fails permanently says so.
- **A checkpoint that does not fit the deployed pipeline is never resumed.** Renaming, reordering or removing a step that a stored record already completed, or a checkpoint from another pipeline, parks the record with `ErrPipelineMismatch`. Adding steps at the end, or changing the version label, resumes fine.
- Steps can have a `Timeout`, in-process `Retry`, and a `Breaker`; an open circuit defers the message without calling the dependency.

### Operating on parked records

Parked records are where an operator comes in, so they are visible and fixable:

```go
list, _ := store.Parked(ctx, dlq.ParkedQuery{Limit: 50}) // what is stuck, and why (LastError)
for _, rec := range list {
    pr, _ := ordersPipeline.Describe(rec.Checkpoint) // steps done, the step it stopped at, phase; no message data
    fmt.Println(rec.ID, rec.LastError, pr.Completed, pr.Next)
}

// The cause is fixed. Resume at a chosen step; the steps before it are not repeated.
err := ordersPipeline.Redrive(ctx, store, rec.ID, "charge")
```

- **`Redrive` is one atomic step.** The checkpoint replacement and the requeue are a single durable log entry (`Store.RequeueWith`), so a crash can never leave the record pending with the old checkpoint. It works through `dlq.Secure` (the new checkpoint is sealed like any other) and is rejected, changing nothing, if the record is not parked or the step is not valid.
- **It will not skip work.** Resuming at a step whose predecessors have not completed is refused (`ErrCannotSkipForward`), and so is resuming after a step that was already compensated, since its effects no longer exist (`ErrStepUndone`). Rewind to that step or earlier instead.
- **Redoing undone work gets fresh idempotency keys.** Steps re-run *without* having been compensated keep their keys, because to a downstream they are a genuine repeat. But if you rewind past steps a saga already undid, the pipeline starts a new *epoch* and their keys change, otherwise a service that deduplicates would treat the redo as already done. (Epoch 0 keys are exactly what earlier versions produced.)
- A record that was compensated cannot just be requeued as it was: it would finish compensating and park again. Give `Redrive` the step to resume at.
- Data the earlier steps saved with `x.Set` is kept through a rewind.

## Kafka consumers: the `kafka` module

Kafka support lives in its own Go module, `github.com/justinclev/GoGuardLib/kafka`, so HTTP-only users never pull in the Kafka client or cgo. The consumer logic (package `kafka`) is written against a small `Client` interface and imports no Kafka library; package `kafka/confluent` adapts [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) (which bundles librdkafka; it needs cgo).

```go
client, _ := confluent.NewClient(ck.ConfigMap{
    "bootstrap.servers": brokers, "group.id": "orders-service", "auto.offset.reset": "earliest",
    // security.protocol, sasl.*, ssl.*: yours to set; nothing here logs them
}, []string{"orders"})
defer client.Close()

consumer, _ := kafka.NewConsumer(kafka.Config{
    Client:   client,
    Store:    walStore, // a durable dlq.Store
    Bindings: []kafka.Binding{{Topic: "orders", Pipeline: ordersPipeline}},
    Mirror:   dlqPublisher, // optional: copy parked messages to "orders.dlq"
})
err := consumer.Run(ctx) // returns nil when ctx ends; an error only if it can no longer be safe
```

Only the topics you bind are consumed and guarded. For every message the consumer builds a stable ID (`dlq.KafkaID(topic, partition, offset)`) and runs it through the topic's pipeline.

- **An offset is committed only when its message is safe:** fully processed, or durably stored in the `Store` together with its progress. Commits are batched (`CommitInterval`, `CommitBatch`) and always made on rebalance and shutdown. A crash between commits redelivers messages that were already safe, which is harmless: storing is idempotent by ID and pipeline steps carry idempotency keys.
- **While a dependency is down, the topic pauses and the messages stay in Kafka.** If any circuit breaker used by a topic's pipeline is open, its partitions are paused. Kafka retains the backlog far more cheaply than a local queue, nothing is copied out, and the consumer keeps polling, so the group does not think it died (verified against a real broker for longer than both `session.timeout.ms` and `max.poll.interval.ms`). When the circuit half-opens, consumption resumes and a real message is the canary.
- **A message that cannot finish is stored, not blocking.** A failing step defers the message with its checkpoint to the `Store`; the topic keeps flowing and a `dlq.Redriver` running `pipeline.Handler()` completes it when its dependency recovers, resuming at the failed step.
- **A message is never skipped.** If a message can neither be processed nor stored (the store is full, a timeout), the partition is pointed back at it and paused for a backoff (`Stats().Backpressure` counts store-full cases); nothing behind it is processed first. The consumer also refuses any message that is not exactly the next offset, so a gap or a stale delivery can never be committed past. If the store fails closed, or the consumer cannot seek back, `Run` stops with an error instead of continuing past a message it cannot protect.
- **Per-key ordering.** While a message with a key is waiting in the store, later messages with the same key are stored behind it (`dlq.Store.HasOrderKey`) instead of being processed ahead of it, and the redriver replays them in order. Other keys are unaffected. `IgnoreKeyOrder` turns this off.
- **Rebalances are safe.** Progress on partitions being revoked is committed before they are released; pauses are re-applied to partitions that come back. Both the eager and cooperative protocols work. On shutdown the final commit keeps polling and retrying (`ShutdownTimeout`), because a commit is refused while a rebalance is in progress.
- **Parked messages can be mirrored to a dead-letter topic** (`kafka.NewDLQPublisher`, default `<topic>.dlq`) with the original key, value and headers plus `x-goguard-*` metadata (original topic, partition, offset, reason, attempts, record ID). Pass `publisher.Publish` as `dlq.RedriveConfig.OnPark` for messages the redriver parks. The mirror is best effort: the message is already parked in the store. The producer is idempotent with `acks=all`. The mirrored payload leaves the store, so this is opt-in.

**Requirements and limits.** Processing is sequential per consumer (scale by running more consumers in the group), and one message's processing must stay under `max.poll.interval.ms` (`ProcessTimeout` bounds it). Set `auto.offset.reset` yourself; auto-commit and offset-storing are forced off. Run the integration tests against a broker with `KAFKA_BROKERS=host:9092 make kafka-integration`.

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
- `dlq.Store` gained `HasOrderKey`, `Get`, `Parked` and `RequeueWith` (implement them in custom stores), plus `NackOptions.Refund` and `LeaseRequest.Skip`.

## Roadmap

Possible follow-ups: keeping payloads on disk instead of in memory for very large queues, and metrics adapters (Prometheus / OpenTelemetry) as separate modules.

## License

MIT
