# GoGuardLib

Circuit breaking, bulkheads, timeouts and safe retries for Go services.

- `goguard` guards outbound **HTTP** as an `http.RoundTripper`.
- `breaker` is a general circuit breaker for any call: a database query, a gRPC client, a message handler.
- `retry` provides backoff, jitter, retry budgets and permanent-error marking.
- `obs` defines the events the library emits.

The library has no dependencies outside the standard library and **never logs or prints**.
Everything observable is delivered as a typed event to a sink you supply, or read on demand with `Stats()`.
A lint rule (`forbidigo`) fails the build if logging or printing is added to non-test code.

Adding this to an existing consumer: [`docs/INTEGRATION.md`](docs/INTEGRATION.md).

A runnable, animated demo (real Kafka, four simulated APIs you can take down, and a live diagram) is in [`demo/`](demo/README.md): `cd demo && docker compose up --build`.

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
| `dlq.NewMemoryStore` | no | no | tests, or work you can afford to lose on restart (holds up to 512 MiB unless `MaxBytes` says otherwise; a negative `MaxBytes` removes the cap) |
| `dlq.OpenWAL` | yes | yes (with the default `SyncAlways`) | anything you must not lose |

Any store can be checked against the contract with `storetest.Run` from `dlq/storetest`.

### Durable store: `dlq.OpenWAL`

```go
store, err := dlq.OpenWAL("/var/lib/myapp/dlq", dlq.WALOptions{
    MaxBytes: 2 << 30, // payloads live on disk: this is your disk budget
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
- **Bounded impact on the dependency.** A `BlockedError` for a call that was made and failed backs off like any failure (`Backoff`), so a struggling dependency is not hit every second and the attempt budget is not spent in seconds. A store that hiccups (a timeout, a dropped connection) does not stop the redriver: it backs off, retries, and counts the error in `Stats().StoreErrors`; only a store that has failed for good (`ErrStoreFailed`, `ErrClosed`, `ErrCorrupt`) ends `Run` with an error.
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
- **A message is never skipped.** If a message can neither be processed nor stored (the store is full or unavailable), the partition is pointed back at it and paused for a backoff (`Stats().Backpressure` counts store-full cases); nothing behind it is processed first. The consumer also refuses any message that is not exactly the next offset, so a gap or a stale delivery can never be committed past. If the store fails closed, or the consumer cannot seek back, `Run` stops with an error instead of continuing past a message it cannot protect.
- **A message that hangs cannot pin a partition.** If processing outlives `ProcessTimeout`, the message is stored with its progress like any other failed step and the offset moves on; the redriver then counts its attempts and parks it if it keeps hanging. (Cancelling the context is different: that is you stopping, and stores nothing.)
- **Per-key ordering.** While a message with a key is waiting in the store, later messages with the same key are stored behind it (`dlq.Store.HasOrderKey`) instead of being processed ahead of it, and the redriver replays them in order. Other keys are unaffected. `IgnoreKeyOrder` turns this off.
- **Rebalances are safe.** Progress on partitions being revoked is committed before they are released; pauses are re-applied to partitions that come back. Both the eager and cooperative protocols work. On shutdown the final commit keeps polling and retrying (`ShutdownTimeout`), because a commit is refused while a rebalance is in progress.
- **Parked messages can be mirrored to a dead-letter topic** (`kafka.NewDLQPublisher`, default `<topic>.dlq`) with the original key, value and headers plus `x-goguard-*` metadata (original topic, partition, offset, reason, attempts, record ID). Pass `publisher.Publish` as `dlq.RedriveConfig.OnPark` for messages the redriver parks. The mirror is best effort: the message is already parked in the store. The producer is idempotent with `acks=all`. The mirrored payload leaves the store, so this is opt-in. Set `PublisherConfig.Encryptor` to seal the key, value and header values before they leave the process (header names and the `x-goguard-*` metadata stay readable, and a `x-goguard-sealed` marker is added); readers call `kafka.OpenDeadLetter` with the same keys.

`confluent.NewClient` takes `confluent.WithErrorHandler(func(error))` for the errors librdkafka retries by itself (brokers unreachable, authentication failing): without it an unreachable cluster looks like an idle topic. A message that arrives carrying an error stops the consumer rather than being skipped.

**Concurrency.** By default a consumer handles one message at a time, in offset order. Set `Config.Workers` to process several at once: messages are handled by a pool (`MaxInFlight`, default 4 per worker, bounds what is fetched but unfinished; beyond it the partitions are paused) while commits stay contiguous, so an offset is committed only when it and everything before it in its partition is safe. Messages with the same key in a partition run one at a time in offset order, and a message that fails stops the later ones with its key from starting; different keys never wait for each other. A failed message is retried from its offset after a backoff, and the messages after it that already finished are remembered and not run again. If a partition is revoked while messages are running, only the contiguous finished prefix is committed and the new owner re-reads the rest (storing is idempotent and steps carry idempotency keys). Your pipeline steps run concurrently, so they must be safe for that.

**Requirements and limits.** One message's processing must stay under `max.poll.interval.ms` (`ProcessTimeout` bounds it). Set `auto.offset.reset` yourself; auto-commit and offset-storing are forced off. Run the integration tests against a broker with `KAFKA_BROKERS=host:9092 make kafka-integration`.

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
