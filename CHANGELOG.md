# Changelog

All notable changes. The library is pre-1.0: minor versions may change behaviour,
and every behaviour change that is opt-in today is listed under "Planned defaults
for v1.0" so it can be adopted early.

## Unreleased

### Reliability
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
- CI: read-only token, pinned govulncheck, CodeQL, tagged releases; SECURITY.md,
  CODEOWNERS, CONTRIBUTING.md, docs/OPERATIONS.md, docs/SECURITY.md.

### Planned defaults for v1.0
`kafka.Config.RequireDurableStore`, `PublisherConfig.RequireEncryption` and
`BrokersDownTimeout` on; `WALOptions.SyncTimeout` 30s; derived keys by default.
