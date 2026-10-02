# Adding GoGuardLib to an existing poll-and-process consumer

For services that already have a Kafka poller and a `Process(ctx, msg)` function and want resilience around the calls to other services, without adopting the pipeline-based consumer in the `kafka` module.

Every snippet here is exercised by `examples/integration/integration_test.go`, so it compiles and behaves as described.

## What you need, and what you do not

| You need | You do not need |
|---|---|
| The core module only: `breaker`, `retry`, `health`, `obs` (standard library only). Go 1.25.3 or newer. | The `kafka` module, `confluent-kafka-go` v2, or any change to your poller. |

The `kafka` module (guarded consumer, per-key ordering, resume-from-step) needs `confluent-kafka-go` v2 and a different processing model. Adopt it later if you want it; this guide does not depend on it.

**Version.** The library is not tagged yet, so `v0.0.0-latest` is not a valid version. Depend on a commit (`go get github.com/justinclev/GoGuardLib@<commit>`) or ask for a tag.

## 1. One breaker per dependency

```go
check, err := health.HTTP("https://rules.internal/health") // Check is required
if err != nil { return err }

rules := breaker.New(breaker.Config{
    Name:             "rules-engine",
    FailureThreshold: 0.5,
    MinSamples:       20,
    SleepWindow:      time.Minute,
    MaxInflight:      50, // optional bulkhead
    IsFailure:        func(err error) bool { return !retry.IsPermanent(err) },
    Health: &health.Config{Check: check, Interval: 5 * time.Second, SuccessThreshold: 2},
    Events: sink,
})
defer rules.Close() // stops the health prober
```

Three things the first draft of the migration guide got wrong:

- **`Health` is ignored unless `Health.Check` is set.** Without it recovery is timer-based and there is no error to tell you.
- **Set `IsFailure`.** By default every error counts against the dependency, so a burst of bad input (HTTP 4xx) can open the circuit for everyone. Count only errors that say the service is unhealthy.
- **The library never logs.** A state change is a typed event delivered to `Events`; without a sink you learn about it only by polling `Stats()`.

## 2. Wrap each call: retry outside, breaker inside

```go
func classify(err error) error { // 4xx (except 429) will never succeed on retry
    var se *statusError
    if errors.As(err, &se) && se.code >= 400 && se.code < 500 && se.code != 429 {
        return retry.Permanent(err)
    }
    return err
}

err := retry.Do(ctx, retry.Policy{
    MaxRetries: 2,
    Backoff:    retry.Jitter(retry.Exponential(100*time.Millisecond, 2*time.Second), 0.3),
    Budget:     retryBudget, // one shared retry.NewBudget(0.15, 10) per dependency
}, func(ctx context.Context) error {
    return rules.Do(ctx, func(ctx context.Context) error {
        var err error
        result, err = client.Evaluate(ctx, req)
        return classify(err)
    })
})
```

- An open circuit returns an error matching `breaker.ErrOpen` and is **not retried**; a full bulkhead matches `breaker.ErrBulkhead`.
- A permanent error is not retried and, with the `IsFailure` above, not counted against the dependency. `retry.Do` returns the original error (the marker is removed).
- Give each dependency's `Budget` one shared instance, not one per call, or it limits nothing.

## 3. Deciding what a failed message does

This is the part that decides whether you lose data, and it depends on **your poller**, not on this library:

- If the poller **commits the offset even when `Process` returns an error**, returning the error *drops the message*. "Kafka will retry" is false in that case.
- If it does not commit, one bad message stops the partition until it is retried, so you need a bounded retry and somewhere to put the message when the retries are spent.

Confirm which one you have before you rely on either. An open circuit means "the dependency is down, try later", which is a reason to pause consuming, not to skip messages.

## 4. Keeping failed messages: use the durable log, not files

A hand-written file DLQ (`os.WriteFile(..., 0644)`) has three problems for regulated data: the file is readable by other users, a crash can leave a half-written file (no fsync, no atomic rename), and the payload is in clear text on disk.

`dlq.OpenWAL` with `dlq.Secure` gives you an fsynced, checksummed, owner-only (0700/0600) log with encrypted payloads, idempotent re-append, and the ability to replay later:

```go
wal, err := dlq.OpenWAL("/var/lib/consumer/dlq", dlq.WALOptions{MaxBytes: 2 << 30})
enc, _ := secure.NewAESGCM(secure.Key{ID: "2026-09", Material: key32}) // from your secret store
store, _ := dlq.Secure(wal, dlq.SecureOptions{Encryptor: enc})

err = store.Append(ctx, dlq.Record{
    ID:     dlq.KafkaID(topic, partition, offset), // the same offset appended twice is stored once
    Source: dlq.Source{Kind: "kafka", Name: topic, Partition: partition, Offset: offset},
    Key:    msg.Key, Value: msg.Value,
})
// Only after Append returns nil (it is on disk) is it safe to commit the offset.
```

- **The directory must be a persistent volume.** In Kubernetes an `emptyDir` is lost when the pod is rescheduled.
- Keep the key outside the volume and outside the image.
- Replay with `dlq.Redriver` (see the README); it rate-limits and waits for the dependency's breaker to close.

## 4b. Or let the consumer do it: `kafka.HandlerBinding`

If you can move to `confluent-kafka-go` v2, you do not need to write your own commit and retry loop. `HandlerBinding` turns your existing `Process`-style function into a binding for `kafka.NewConsumer`:

```go
binding, err := kafka.HandlerBinding(kafka.HandlerConfig{
    Topic:   "correspondence-events",
    Breaker: rules,
    Handle: func(ctx context.Context, m kafka.HandledMessage) error {
        return process(ctx, m.Value) // idempotent; use m.ID to deduplicate downstream calls
    },
})
consumer, err := kafka.NewConsumer(kafka.Config{Client: client, Store: store /* durable */, Bindings: []kafka.Binding{binding}})
```

For the whole service in one call (client, log, consumer, redriver and routing), use `kafka.NewService` instead of building each part:

```go
svc, err := kafka.NewService(kafka.ServiceConfig{
    NewClient: confluent.NewClientFunc(kafkaConfig),
    DataDir:   "/var/lib/correspondence", // a persistent volume
    Handlers:  []kafka.HandlerConfig{{Topic: "correspondence-events", Breaker: rules, Handle: handle}},
})
if err != nil {
    return err
}
defer svc.Close()
return svc.RunUntilSignal(ctx)
```

You get contiguous offset commits, a durable store for messages that could not be handled, per-key ordering behind a deferred message, a paused topic while the breaker is open, and resume after a crash. `retry.Permanent(err)` parks a message for a person instead of retrying it.

**Why there is no "simple consumer" that skips the store.** A loop that commits after each success and merely logs failures loses the failed message the moment a later one succeeds (its higher offset is committed), and one that drops a message when the circuit opens loses that too. Handling rebalances, contiguous commits and rewinding correctly is what `kafka.Consumer` exists for, so it is the only consumer offered. `Config.Store` is required with no default on purpose: an in-memory default would lose deferred messages on every restart. Use `dlq.OpenWAL` with `dlq.Secure`.

**`confluent-kafka-go` v1.** Not supported: both major versions bundle librdkafka, the adapter cannot be tested without a broker, and the upgrade for the calls a poller uses is mostly the import path (`.../confluent-kafka-go/v2/kafka`). If a service truly cannot upgrade, use sections 1 to 4 (they need no Kafka client at all).

## 5. Watching it

`Stats()` gives `State`, `Success`, `Failure`, `FailureRateBps` (basis points, 10000 = 100%), `Inflight`, `Rejected`, `RejectedBulkhead`, `Opens`, `LastTransition` and `AvgLatency`. `obs.StateChanged` and `obs.ProbeResult` events arrive on `Events`. **The library exports no Prometheus metrics**; map these to your metrics library yourself (an `obs.Sink` for transitions, a scrape callback for `Stats()`). Wrap a slow sink in `obs.NewDispatcher` because `Emit` must not block.

## Rollout

1. Add the breakers in `DryRun: true` first: everything is recorded and evented, nothing is rejected.
2. Turn on rejection for one dependency, run a chaos test (stop it, watch the circuit open, restore it, watch it close).
3. Only then add the durable log, after step 3 above is settled.
