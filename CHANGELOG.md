# Changelog

All notable changes. The library is pre-1.0: minor versions may change behaviour,
and every behaviour change that is opt-in today is listed under "Planned defaults
for v1.0" so it can be adopted early.

## Unreleased

### Reliability
- HTTP: `Policy.Defer` saves requests that cannot be sent (circuit open, or failed after
  retries) to any `dlq.Store`, memory or disk, and `goguard.ReplayHandler` sends them
  through the redriver when the service is healthy. Opt-in per endpoint; needs an
  idempotency key; never stores credentials; `ErrDeferred` tells the caller.
  `ResilientTransport.Breakers()` feeds the redriver.
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
