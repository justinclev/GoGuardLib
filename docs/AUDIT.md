# GoGuardLib audit

A review of every non-test source file (about 7,000 lines across the core module and the `kafka` module) for bottlenecks, data loss, security, usability, reliability and scalability, followed by fixes. Each finding says what was wrong, why it matters, what was done, and how it was verified.

**Method.** Every file was read in full. Suspected problems were confirmed by reading the calling code and, where a claim could be measured, by a benchmark or a test written to fail first. Fixes that touch safety-critical paths were checked with mutation tests: the fix was removed and the new test had to fail.

**Scope of verification.** `make all` (gofmt, vet, golangci-lint with `forbidigo`, `go test -race`, build) and `make kafka` pass; the core, `dlq`, `pipeline` and `kafka` suites were also run three times under `-race` to look for flakes. The real-broker integration suite (`make kafka-integration`) and `govulncheck` could not be run in this environment (no broker; the vulnerability database is unreachable), so the `confluent` adapter changes are compile- and vet-checked only. Both run in CI.

## Summary

| # | Finding | Area | Severity | Status |
|---|---|---|---|---|
| 1 | Lease, Stats and Parked scan the whole queue under the store's global lock | Scalability | High | Fixed |
| 2 | `Parked` returns unbounded payload per page | Reliability / memory | High | Fixed |
| 3 | Kafka: a message that outlives `ProcessTimeout` is retried for ever and pins its partition | Reliability | High | Fixed |
| 4 | `Redriver.Run` ends on the first transient store error | Reliability | High | Fixed |
| 5 | Blocked-and-failed records retried every second, parked after ~10 s of outage | Reliability / data | High | Fixed |
| 6 | Half-open circuit wedged for ever by a probe that never reports back | Reliability | High | Fixed |
| 7 | Request body leaked on every rejected request | Reliability | High | Fixed |
| 8 | HTTP/2 silently disabled; connection pool limits unbounded or tiny | Performance | Medium | Fixed |
| 9 | `HealthHeader` credential sent to every host under `GuardAll` | Security | Medium | Fixed |
| 10 | Open circuit can be evicted by a burst of new hosts | Security / reliability | Medium | Fixed |
| 11 | Shared circuit hashes and locks a shard on every request | Scalability | Medium | Fixed |
| 12 | `MemoryStore` unbounded by default (OOM loses the queue) | Data loss | Medium | Fixed |
| 13 | WAL in-memory index unbounded for tiny records | Reliability | Medium | Fixed |
| 14 | Rolling window stretches under sparse traffic | Correctness | Low-Medium | Fixed |
| 15 | Hosts with default ports / trailing dots miss matchers and split circuits | Usability | Medium | Fixed |
| 16 | No `CloseIdleConnections` through the guard | Usability | Low | Fixed |
| 17 | Redactor runs every pattern over unbounded text | Security / CPU | Low-Medium | Fixed |
| 18 | Ordered-key removal O(n) | Scalability | Low | Fixed |
| 19 | Confluent adapter hides broker errors and message-level errors | Reliability / usability | Medium | Fixed |
| 20 | No dependency vulnerability scanning or update automation | Security | Medium | Added (unverified locally) |
| 21 | Lease, Get and Parked read payloads from disk under the store lock | Scalability | Medium | Fixed (two-phase lease) |
| 22 | Kafka consumer processes one message at a time | Scalability | Medium | Open (design) |
| 23 | Records are protected by CRC only; `Secure` does not authenticate metadata | Security | Medium | Open (documented) |
| 24 | No free-disk-space guard; compaction needs up to 2x live data | Reliability | Medium | Open (documented) |
| 25 | Dead-letter mirror and `OnPark` send payloads in clear | Security | Info | By design, documented |

## Findings that were fixed

### 1. Queue scans under the global lock (High, scalability)

**Was.** `machine.selectLease` walked every record in `Seq` order on every `Lease`, `stats` walked every record, and `Parked` walked every record. All three run under the store's single mutex, which `Append` also needs. The redriver polls every 500 ms, so during an outage, when most of the queue is blocked on a dependency whose circuit is open, every poll held the lock for time proportional to the backlog. Measured against the previous commit with a 100,000-record backlog: **4.86 ms per `Lease` poll** (and the same for a backlog of delayed retries), stalling every `Append` behind it. Both `MemoryStore` and `WALStore` share this state machine.

**Now.** `dlq/machine.go` keeps an incremental index: pending and leased items live in per-dependency groups in `Seq` order, parked items in their own ordered list, and state counters are maintained on every transition. A `Lease` skips a group for an open dependency in O(1), a group that a scan found to have nothing due remembers when to look again, and `Stats` reads counters. Moving a record between `Pending` and `Leased` keeps its place, so leasing and returning the head of a huge group stays O(1). The cached "nothing due until" time is cleared by every event that can make something eligible sooner and is never believed for more than one second, so a missed clear can delay work by at most that long, never starve it.

**Result.** `BenchmarkLeaseWithBlockedBacklog/100000`: 4.86 ms to **0.18 µs**; `BenchmarkLeaseWithDelayedBacklog` (100,000 delayed): 4.63 ms to **3.6 µs**.

**Verified by.** `TestQueueIndexAgreesWithAFullScan`: 40 seeds of 600 random operations (append, lease with filters and skips, ack, nack that moves dependency, release, park, requeue, discard, time passing) checked after every step against a naive full-scan specification for selection, counters, group membership, ordering, the parked list and the oldest-pending statistic. Three mutations (skip the clear on in-group moves, skip the successor clear when an ordered head leaves, set the wake time even when items were found) each fail it. The existing conformance suites for the memory, WAL, secure and payload-in-memory stores all pass.

### 2. Unbounded `Parked` pages (High, memory)

**Was.** `Parked(Limit: 1000)` hydrated up to 1000 records with payloads up to 8 MiB each: several gigabytes in one call, under the store lock, triggered by an operator or dashboard.

**Now.** A page is cut once it would carry 32 MiB of payload (always at least one record). Callers page with `After` until an empty page. Documented on `ParkedQuery` and in the README's upgrade notes. `TestParkedPagesAreBoundedByBytes`.

### 3. A message that hangs pins its partition (High, reliability)

**Was.** If processing outlived `ProcessTimeout`, `pipeline.Execute` returned `Failed` without storing anything, and the consumer seeked back and retried with a capped backoff. A message that always hangs its dependency was retried for ever, never counted, never parked, and everything behind it on the partition waited.

**Now.** A run that outlives its own deadline is held against the step it was in: the message is stored with its progress and the offset moves on. The redriver then counts its attempts and parks it after `MaxAttempts`. A cancelled context (the caller stopping) still stores nothing. `TestAMessageThatHangsPastItsDeadlineIsStoredWithItsProgress`, `TestACancelledRunIsNotHeldAgainstTheMessage`, and in the Kafka module `TestAMessageThatTimesOutIsStoredNotRetriedForEver` (which replaces the old test that asserted the endless retry). Reverting the change fails the first.

### 4. `Redriver.Run` ends on any store error (High, reliability)

**Was.** One `Lease` error (a timeout or dropped connection with a remote store) made `Run` return, and redriving stopped until the application noticed and restarted it.

**Now.** Only stores that have failed for good (`ErrStoreFailed`, `ErrClosed`, `ErrCorrupt`) end `Run`. Other errors are counted in `Stats().StoreErrors`, and the redriver backs off (100 ms doubling to 30 s) and continues. Rate-limit tokens reserved for a failed lease are returned. `TestRedriverSurvivesTransientStoreErrors`; the existing fatal-error test still passes.

### 5. Blocked-and-failed records retried too fast (High, reliability and data)

**Was.** A `BlockedError` with `Refund: false` (the call was made and failed) was requeued with a fixed delay of twice the poll interval (1 s). With the default `MaxAttempts` of 10, a dependency outage that its circuit had not yet opened for parked every record in the queue after about ten seconds, turning a transient outage into manual work for a human, while hitting the struggling dependency once a second per record.

**Now.** A failed call backs off with the configured `Backoff` (exponential 1 s to 5 min with jitter by default). Refunded blocks (circuit open, call never made) keep the short hold. `TestABlockThatCountsEventuallyParksTheRecord` still passes with an explicit fast backoff.

### 6. Half-open wedge (High, reliability)

**Was.** In half-open, one call is admitted as the probe and every other call is rejected until it reports. `Acquire` hands out a `Permit` that the caller must finish; a caller that leaked one (an early return, a lost goroutine) left the circuit half-open for ever, rejecting all traffic.

**Now.** The admission time of the probe is recorded; after one sleep window without a report another call takes over as the probe. `TestLeakedProbeIsReplacedAfterASleepWindow`. A leaked permit still holds a bulkhead slot; that is documented on `Acquire`.

### 7. Request bodies leaked on rejection (High, reliability)

**Was.** `http.RoundTripper` must close the request body on every path. The guard returned early without closing it when a circuit was open, an endpoint was unknown, or the transport was closed. With a file or stream body, every request rejected during an outage leaked a descriptor or buffer.

**Now.** `closeRequestBody` on all three paths. `TestRejectedRequestsCloseTheirBody`; removing the fix fails it.

### 8. HTTP/2 disabled, connection pool (Medium, performance)

**Was.** The default transport set a custom `DialContext` without `ForceAttemptHTTP2`, which switches HTTP/2 off, and left `MaxIdleConns` unlimited and `MaxIdleConnsPerHost` at Go's default of 2 unless configured.

**Now.** HTTP/2 forced on, defaults of 100 idle connections, 16 per host, 90 s idle timeout, and `Expect: 100-continue` timeout of 1 s; all overridable. `TestDefaultTransportEnablesHTTP2AndSensibleIdleLimits`.

### 9. Health credential sent to every host (Medium, security)

**Was.** `Policy.HealthHeader` (meant for a `/health` auth token) was sent with every probe. Under `GuardAll`, circuits are per host for whatever host the application calls, so the token went to all of them.

**Now.** `New` refuses `GuardAll` with a `HealthHeader`; `Policy.HealthHeader` documents that it goes to every host an endpoint probes. `TestGuardAllRefusesAHealthHeader`.

### 10. Eviction could hide an outage (Medium, security and reliability)

**Was.** When a shard reached `MaxBreakers`, the least recently used circuit was evicted regardless of state. With per-host circuits (`GuardAll`, `PerHost`) and attacker- or data-influenced hosts, a burst of new hosts could evict an open circuit, and the protection for a dead dependency would silently reset.

**Now.** Eviction prefers a closed circuit among the 16 least recently used. `TestEvictionPrefersClosedCircuits`; removing the preference fails it.

### 11. Shared circuit on the shard path (Medium, scalability)

**Was.** An endpoint without `PerHost` (the default, one circuit per endpoint) still hashed the endpoint name, took a shard read lock, and consulted an LRU on every request.

**Now.** The single circuit of such an endpoint is kept in an atomic pointer on the endpoint, created once. It needs no eviction (there is at most one per endpoint). `Stats` and `Close` include it. `TestSharedCircuitIsOneObjectAndIsListed`.

### 12. `MemoryStore` unbounded by default (Medium, data loss)

**Was.** With the zero value a long outage grew the store until the process was killed for memory, losing everything.

**Now.** It holds up to 512 MiB (`DefaultMemoryMaxBytes`) and returns `ErrFull`, which the consumer already turns into backpressure. A negative `MaxBytes` removes the cap. `TestMemoryStoreIsBoundedByDefault`.

### 13. WAL index bounded only by bytes (Medium, reliability)

**Was.** With payloads on disk, `MaxBytes` bounds disk, but the in-memory index costs about 300 bytes per record regardless of size. Millions of tiny records fit the byte limit and could need gigabytes of index.

**Now.** `MaxRecords` defaults to 1,000,000 when payloads are on disk (negative removes the cap; unchanged with `PayloadsInMemory`). `TestTheIndexIsBoundedByDefaultWhenPayloadsAreOnDisk`.

### 14. Rolling window drift (Low-Medium, correctness)

**Was.** After a rotation the window's clock was reset to "now", discarding the part of a bucket that had already elapsed. With traffic arriving less often than a bucket, buckets effectively lasted longer than configured and old failures counted for up to 50% longer than the sampling window.

**Now.** The clock advances by whole buckets. `TestWindowKeepsItsLengthUnderSparseTraffic`; reverting fails it.

### 15. Host canonicalisation (Medium, usability)

**Was.** `Host("example.com")` did not match `https://example.com:443/` or `EXAMPLE.com.`, and per-host circuits treated those spellings as different services.

**Now.** Matchers, circuit keys and circuit names use one canonical form (lower case, no trailing dot, no default port). Non-default ports still distinguish services. `TestHostsAreMatchedAndKeyedCanonically`.

### 16. `CloseIdleConnections` (Low, usability)

`http.Client.CloseIdleConnections` looks for the method on the transport; the guard did not forward it, so idle connections could not be closed through it. `TestCloseIdleConnectionsReachesTheUnderlyingTransport`.

### 17. Redactor CPU (Low-Medium, security)

**Was.** Every pattern ran over the whole error text before truncation. An error embedding a large payload (a rejected 50 MiB body) burned CPU on every failure, and redaction runs on the failure path.

**Now.** Input is bounded to 16 times the output limit before the patterns run; the secret in the retained part is still redacted, and the result is still truncated. `TestHugeInputIsBoundedAndStillRedacted`.

### 18. Ordered-key removal (Low, scalability)

Removing the head of a key's list copied the whole list, so draining a hot key with many queued records was quadratic. The head now leaves in O(1) and wakes its successor.

### 19. Confluent adapter visibility (Medium, reliability)

**Was.** Non-fatal librdkafka errors (brokers unreachable, authentication failing) were dropped silently, so an unreachable cluster looked like an idle topic, and a message carrying an error was treated as a normal message.

**Now.** `confluent.WithErrorHandler(func(error))` receives the retried errors. A message that arrives with an error stops the consumer with that error rather than being skipped. Not covered by unit tests (needs a broker); verified with `go vet` including the `integration` tag.

### 20. Dependency scanning (Medium, security)

Added a `vuln` CI job (`go mod verify` plus `govulncheck` on both modules), a `make vuln` target and weekly Dependabot updates for both Go modules and the GitHub Actions. Not run locally (the vulnerability database is blocked in this environment), so treat the first CI run as its verification.

### 21. Payload reads under the store lock (Medium, scalability)

**Was.** `Lease` had to read a record's payload before it could log the lease, and `Get` and `Parked` read payloads too, all while holding the lock `Append` needs. The reads are small, but a slow disk stretched the pause and `Append` tail latency inherited it.

**Now.** A lease works in three steps: pick and *reserve* records under the lock (other leases skip reserved records, so concurrent leasers get disjoint sets), read their payloads with the lock released, then take the lock again, confirm each record is unchanged (a version counter, bumped by every state or checkpoint change), log the lease and hand the records out. A record that changed in between (an old worker finishing an expired lease) is dropped rather than handed out stale, and a read that failed is retried once under the lock before a record is ever parked as unreadable, so a compaction that deletes a file mid-read is not mistaken for corruption. `Get` and `Parked` read unlocked with the same locked retry. The file cache now hands out reference-counted handles and preads outside its own mutex, so readers do not serialise, and evicted or closed handles are closed by their last reader.

**Verified by.** With every lease read stalled for 20 ms, the worst `Append` took 0.27 ms (`TestAppendLatencyIsIndependentOfSlowReads`); other operations and a second lease proceed while a read is in flight; twelve concurrent leasers over 300 records never share one; a checkpoint, ack or park by an old worker mid-read is never handed out stale; a compaction mid-read returns intact payloads and parks nothing; reservations are released on cancellation, close and unreadable payloads; the file cache survives concurrent eviction, drops and close under `-race`. Five mutations each fail a test: skipping the version check, the reservation, the locked retry, the release on error, and holding the lock across the read. Single-threaded lease cost is unchanged within noise (about 9 µs for a lease and release on disk).

## Findings that remain, and why

### 22. Sequential consumer (Medium, open by design)

One `Consumer` processes one message at a time, so throughput per consumer is `1 / message latency`. Scale by running more consumers in the group (each owns its partitions). A per-partition worker pool would have to preserve per-key ordering, commit only contiguous safe offsets, and stay correct across rebalances; that is a feature with its own design and test matrix, not an audit fix.

### 23. Integrity of stored records (Medium, open)

The WAL protects against torn and rotted writes with CRC-32C, not against deliberate modification, and `dlq.Secure` authenticates each sealed payload field (bound to record ID and field) but not the metadata beside them (state, `BlockedOn`, `OrderKey`, timestamps). Someone able to write the WAL directory can alter that metadata; they could also delete the directory, so the directory permissions (`0700`, enforced at open) are the control. If tamper evidence matters, the HMAC signer in `secure` could sign whole frames; that changes the on-disk format and is a deliberate decision, not a patch.

### 24. Disk space (Medium, open)

`MaxBytes` bounds live data, not free space. Compaction writes a full snapshot beside the old files, so disk use can briefly reach about twice the live data plus the log. A full disk fails the write safely (the write is rolled back, the store keeps serving reads, the consumer backs off) and fails compaction cleanly (temporary file removed), but there is no early warning. Alert on free space and size the volume at 2x `MaxBytes` plus segment headroom. `WALStats().LastCompactionErr` surfaces a compaction that cannot finish.

### 25. Payloads leaving the store (Info)

`Redriver.OnPark` and the Kafka dead-letter mirror pass the message payload to code and topics you choose. That is the purpose, is opt-in and is documented; encrypt with `dlq.Secure` before mirroring if the topic is less trusted than the store.

## What was checked and found sound

Recorded so the next reviewer does not repeat it.

- **WAL write path and recovery.** Frames carry the CRC over length, LSN, type and body; a torn tail is quarantined before it is cut; damage followed by valid frames refuses to open; write and fsync failures fail the store closed and a failed fsync is never retried; segment creation syncs the directory; the directory is locked with `flock` and permissions are checked. Compaction's swap only re-points references unchanged since capture, and unreadable payloads are parked, not returned. The fuzzers and crash tests cover these.
- **Encryption.** AES-256-GCM with per-message random nonces, key IDs for rotation, AAD binding ciphertext to record and field, uniform decryption errors, empty plaintext round-trips. The 2^32-messages-per-key nonce bound is documented; rotate well before it.
- **No logging.** `forbidigo` bans `log`, `slog` and `fmt.Print*` in non-test code; events and errors carry names, counts and timings, never payloads, URLs or headers. Health probes send only configured headers, follow no redirects and read at most 4 KiB.
- **Concurrency.** The breaker's admission is a CAS loop, the dispatcher never blocks an emitter, the redriver never handles a record twice at once, and `Close` paths wait for their goroutines. All suites pass under `-race`.

## Suggested next steps

1. Run `make kafka-integration` against a broker and the new `vuln` job once in CI.
2. Decide on finding 23 (frame signing) if the threat model includes someone with write access to the volume.
3. Add Prometheus / OpenTelemetry adapters as separate modules so `Stats()` and events are scraped without glue code.
