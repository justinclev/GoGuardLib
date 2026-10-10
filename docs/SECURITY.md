# Threat model

What GoGuardLib defends against, what it leaves to you, and how to turn each control on.

## Assets

1. **Message payloads** in the dead-letter store, its backups and the mirror topic: they
   are customer data.
2. **Integrity of the queue**: a record must not be altered, dropped or replayed without
   the operator knowing.
3. **Availability**: a bad message or a broken dependency must not stop unrelated work.

## Who we assume can do what

| Actor | Can | Cannot |
|---|---|---|
| Someone with read access to the disk or a backup | read the files | (with encryption on) read payloads |
| Someone who can write to the log directory | rewrite files | (with signing on) change a record without detection |
| A client sending messages | send oversized, malformed or hostile payloads | crash the process, exhaust memory, or forge dead-letter metadata |
| An operator | requeue and discard | do so without a trace (with `dlq.Audited`) |
| The network path to Kafka | observe or alter traffic | (with TLS required) read or modify it |

Out of scope: an attacker who runs code as the service's user or as root on the host,
and the confidentiality of data in the process's memory.

## Controls

| Control | Protects | How to turn it on | Default |
|---|---|---|---|
| AES-256-GCM sealing of key, value, headers, checkpoint, bound to record and field | payload confidentiality and integrity at rest | `dlq.Secure(store, SecureOptions{Encryptor})` | off |
| Derived per-message keys | removes the 2^32-messages-per-key bound | `secure.NewAESGCMDerived` | off |
| HMAC of every log frame | detects edits, not just damage | `WALOptions.Signer` | off |
| Keyed hash of order keys | keeps customer identifiers off disk | `SecureOptions.OrderKeyPeppers` | off (order keys are stored in clear) |
| Redaction of error text (tokens, keys, PEM, URLs with credentials) | secrets leaking into stored errors | on inside `Secure` and the redriver | on |
| Sealed dead-letter mirror; refuse plaintext | payloads on a widely readable topic | `PublisherConfig{Encryptor, RequireEncryption}` | off |
| Require TLS | traffic and SASL credentials | `confluent.WithRequireTLS`, `WithProducerRequireTLS` | off |
| Refuse a store that can lose acknowledged data | silent loss after commit | `kafka.Config.RequireDurableStore` | off |
| File and directory mode 0700/0600 and ownership | local tampering | always (override: `AllowInsecurePermissions`) | on |
| Refuse network filesystems | corruption from broken locks | always (override: `AllowNetworkFilesystem`) | on |
| Operator audit trail | unaccountable release or deletion | `dlq.Audited` + `dlq.WithActor` | off |
| Saved HTTP requests: credential headers dropped, idempotency key required | credentials and duplicate side effects when `Policy.Defer` is on | `goguard.Policy{Defer: ...}` (opt-in per endpoint); the URL and body are stored, so wrap disk stores in `dlq.Secure` | off |
| Key retirement | leaked or expired encryption keys | `dlq.Reseal` | manual |

A production deployment should turn on: sealing, signing, a pepper, `RequireEncryption`,
`RequireTLS`, `RequireDurableStore`, and `Audited`. The ones listed "off" become defaults
at v1.0 (see CHANGELOG.md).

## Guarantees by design

- The library never logs. Events and errors carry IDs, names, counts and timings; not
  payloads, keys, headers, URLs or credentials.
- Decryption failures are uniform (`ErrDecrypt`), so a wrong key, a wrong context and
  tampering look the same to a probe.
- Reads are bounded: frames larger than the limit are rejected before allocation, and
  redaction bounds its input, so a hostile payload cannot force unbounded memory or CPU.
- A record that cannot be decrypted or read is parked for an operator, never returned as
  ciphertext, never silently dropped, never a crash loop.
- A failed write or fsync stops the store; a failed fsync is never retried.
- Panics in steps, handlers, health checks and event sinks are contained.

## Residual risks

- Data is on one machine's disk after the offset is committed. A lost volume loses the
  records not yet handled. Back up (`WALStore.Backup`) and keep the volume replicated;
  a replicated store is on the roadmap.
- Signing detects edits but not the removal of a whole suffix of the log or its
  replacement with an older signed copy; keep the signing key and backups elsewhere.
- Order keys are stored in clear unless a pepper is set, and a stored `LastError` is
  redacted on a best-effort basis: it recognises common credential shapes, not every secret.
- A compromised encryption key exposes everything sealed under it until `dlq.Reseal`
  moves the data to a new key; backups made before that stay readable with the old key.

## Reporting

See [../SECURITY.md](../SECURITY.md).
