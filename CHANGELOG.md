# Changelog

All notable changes. The library is pre-1.0: minor versions may change behaviour,
and every behaviour change that is opt-in today is listed under "Planned defaults
for v1.0" so it can be adopted early.

## Unreleased

### Less code to write (additive; nothing was removed or made less configurable)
- `kafka.NewService(kafka.ServiceConfig{...})` builds the store (`DataDir`, or your own
  `Store`), the Kafka client (`NewClient`, or your own `Client`), the consumer, the redriver and
  the per-topic routing, and `Service.Run` runs consumer and redriver together; `Close`
  releases what the service opened. A Kafka service that took about 84 lines is now about 15.
  `Consumer`, `Redriver`, `WAL` and `Secure` embed the existing config structs, so every
  setting is still reachable.
- `confluent.NewClientFunc(cfg, opts...)` is `NewClient` with the topics left to the service.
- `pipeline.SetJSON(x, key, v)`, `GetJSON[T]` and `RequireJSON[T]` pass an object between steps
  without marshalling by hand; JSON that cannot be written or read (and, for `RequireJSON`, a
  missing key) is a permanent error.
- `goguard.Defer.BindLater` and `Client.BindStore`: a guarded client can be built before the
  store exists, and `kafka.NewService` binds the log it opened (so `DataDir` and saved HTTP
  requests work together). Nothing is saved until a store is bound.
- `Service.Stats(ctx)` returns the consumer, redriver and store numbers together;
  `Service.RunUntilSignal` is `Run` that also stops on SIGINT/SIGTERM; `Service.Close` now stops
  a running `Run` and waits for it instead of closing the log under it.
- `health.MustURL(url)` builds a `health.Config` for `breaker.Config.Health` in one expression
  (panics on a malformed URL, like `regexp.MustCompile`; `health.HTTP` still returns an error).
- `goguard.NewClient(policy, opts...)` returns an `*http.Client` with a guarded transport in
  one call (`Guard`, `Breakers`, `Replay`, `Close`); `NewClientWithConfig` takes a `Config`.
  `ServiceConfig.HTTP` hands it to the service, which then also finishes the requests it saved.
- `retry.FromHTTP(resp, err)` classifies an HTTP answer (2xx/3xx ok, 408/425/429/5xx temporary,
  other 4xx permanent) and `retry.IsOutage` is the matching `breaker.Config.IsFailure`; this
  replaces the status switch every handler wrote. `goguard.ReplayHandler` uses it.

### Reliability
- **Fixed: a breaker without a health check could keep a consumer or redriver stalled for ever.**
  An open breaker only moves to half-open when a request reaches it, but the consumer pauses a
  topic while the circuit is open and the redriver skips records whose circuit is open, so
  nothing sent that request. `Breaker.Refusing()` says whether an open circuit would admit a
  canary now (sleep window passed, or the health check's `MaxOpen` ran out); both now use it.
  Health-checked breakers behave as before until `MaxOpen`, which is now honoured by them too.
- HTTP: `Policy.Defer` saves requests that cannot be sent (circuit open, or failed after
  retries) to any `dlq.Store`, memory or disk, and `goguard.ReplayHandler` sends them
  through the redriver when the service is healthy. Opt-in per endpoint; needs an
  idempotency key; never stores credentials; `ErrDeferred` tells the caller.
  `ResilientTransport.Breakers()` feeds the redriver through the new
  `RedriveConfig.BreakerSource` (circuits of a guarded client appear as traffic reaches
  them, so a fixed map would miss them); saved requests are tagged with their circuit's
  name so the redriver holds them while it is open. `Defer.Enabled` switches deferral on
  and off at run time.
- `kafka.RedriveFor(bindings)`: one redrive handler and breaker map for a consumer with
  several topics. Before, a redriver built from one pipeline parked the other topics'
  records as mismatched, or ran records with no progress through the wrong pipeline.
- Consumer: `OnUnstorable` (halt by default, or skip and count) for a message the
  store can never hold; `IDNamespace` and `dlq.KafkaIDIn` so a recreated topic or a
  failover cannot dedupe a new message against an old one; `MirrorTimeout`,
  `OnParkTimeout`.
- WAL: the fsync wait honours the caller's context; `SyncTimeout` fails the store
  closed on a stalled disk; transient read errors no longer park healthy records.
- HTTP: `Retry-After` is honoured on retries (capped at 30s); the default transport
  bounds the wait for response headers (`ResponseHeaderTimeout`, 30s).
- Kafka client: `auto.offset.reset` defaults to `earliest` when unset;
  `Stats.Health` and `BrokersDownTimeout` for a cluster that has become unreachable.

### Security
- Redactor: prefixed keys (`access_token`, `clientSecret`), AWS, GitHub, Slack,
  Google, Stripe keys and PEM blocks.
- `secure.NewAESGCMDerived` (a key per message), `SecureOptions.OrderKeyPeppers`
  (rotation), `dlq.Reseal` (retire a key), `dlq.Audited` and `dlq.WithActor`
  (operator audit trail), `PublisherConfig.RequireEncryption`,
  `confluent.WithRequireTLS`, `kafka.Config.RequireDurableStore`.
- WAL refuses files owned by another user and network filesystems (overridable).
- `obs.Emit` contains a panicking sink.

### Operations
- `StoreStats.OldestParked`, `BlockedKeys`, `ByDependency`; `obs.StoreEvent`,
  `obs.ConsumerEvent`, `obs.OperatorAction`.
- `dlq.VerifyWAL`, `WALStore.Backup`, `dlq.OpenWALCopy` and the `dlqctl` command.
- CI builds with the latest Go 1.25 patch (the scan flagged 18 standard-library
  advisories fixed by 1.25.13; `toolchain go1.25.13` in both go.mod files), and
  pins govulncheck v1.7.0 (v1.8.0 needs Go 1.26). Also: read-only token, CodeQL, tagged releases; SECURITY.md,
  CODEOWNERS, CONTRIBUTING.md, docs/OPERATIONS.md, docs/SECURITY.md.

### Documentation
- README rewritten as a step-by-step quick start (any function, HTTP, health checks, a Kafka
  consumer, a multi-step job) with code compiled from `examples/readme` and `kafka`; the
  previous README is now docs/GUIDE.md.

### Planned defaults for v1.0
`kafka.Config.RequireDurableStore`, `PublisherConfig.RequireEncryption` and
`BrokersDownTimeout` on; `WALOptions.SyncTimeout` 30s; derived keys by default.
