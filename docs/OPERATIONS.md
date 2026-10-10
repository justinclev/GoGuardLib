# Running GoGuardLib in production

What to watch, what each alert means, and what to do. Every name below is in the
code; nothing here needs a metrics library, only the `Stats` methods and the event
sinks you already have.

## The five numbers to alert on

| Signal | Where | Alert when | Means |
|---|---|---|---|
| `StoreStats.OldestPending` | `store.Stats` | above what a normal outage lasts (say 30 min) | The queue is not draining: a dependency is still down, or the redriver is not running. |
| `StoreStats.OldestParked` | `store.Stats` | above 0 for longer than your on-call response time | A person has to act. Parked records are never retried. |
| `StoreStats.BlockedKeys` | `store.Stats` | above 0 | That many keys (customers, accounts) have every later message waiting behind a parked one. |
| `Consumer.Stats().Health.AllBrokersDown` | consumer | true | The cluster is unreachable. To Kafka's clients this looks like an idle topic. |
| `WALStats.Failed`, `obs.StoreFailedClosed` | WAL | non-nil / seen | The store stopped after a disk error. Restart the process; recovery restores everything acknowledged. |

`Stats` methods are cheap except `BlockedKeys`, which scans the parked records; read
it every few seconds or minutes, not per message.

## Events worth routing

Give every component an `Events` sink (wrap slow ones in `obs.Dispatcher`):

- `obs.StateChanged`: a breaker opened or closed. Page on a critical dependency staying open.
- `obs.StoreEvent`: `compaction_failed` (the log grows until it is fixed; check disk),
  `failed_closed`, `sync_timeout`, `disk_low`, `quarantined` (a damaged tail was set
  aside at start-up; the file is named in `RecoveryReport.QuarantineFile`).
- `obs.ConsumerEvent`: `parked`, `retry`, `backpressure`, `commit_failed`,
  `mirror_failed`, `unstorable`, `paused` (with `Reason`: `breaker_open`,
  `retry_backoff`, `backlog`).
- `obs.OperatorAction`: every requeue and discard, with who did it.

## Runbook

### A dependency is down
Nothing to do. Breakers open, the consumer pauses the topic (messages wait in Kafka),
work already in flight is stored, and the redriver resumes it when the health check
passes. Watch `OldestPending`; it should fall after recovery. If it does not, the
redriver is not running or its handler is failing: check `RedriveStats`.

### `BlockedKeys > 0`: parked records are holding customers
1. `dlqctl inspect DIR` lists the parked records (ID, source, attempts, reason). It
   works on a copy, so it is safe beside the running service.
2. Read the reason. If the cause is fixed (a bad deploy, a missing SKU), requeue:
   `store.Requeue` in code, or `dlqctl requeue -actor you DIR ID...` with the service stopped.
3. If the message is genuinely bad, `Discard` it. This is the only way data leaves the
   store unhandled, and it is recorded as an `obs.OperatorAction`.
Everything queued behind a parked message is released the moment it leaves.

### The consumer stopped with `ErrUnstorable`
A message failed and the store will never accept it (usually it is larger than
`MaxRecordBytes`). Nothing was lost and no offset was committed past it. Either raise
the store's `MaxRecordBytes` (and the producer-side limit), fix the producer, or restart
with `OnUnstorable: kafka.UnstorableSkip` after deciding it may go (it is counted in
`Stats.Unstorable` and copied to the mirror if one is set).

### The consumer stopped with `ErrBrokersDown`
`BrokersDownTimeout` passed with no reachable broker. Check the network, DNS and the
cluster; the process is safe to restart, offsets are only committed for safe messages.

### The store failed closed (`ErrStoreFailed`)
A write or fsync failed, or `SyncTimeout` passed. The store refuses everything because
the on-disk state of the last writes is unknown. Fix the disk, restart. On start the log
is replayed; every acknowledged record is restored and a torn tail is quarantined, not
deleted. If start-up reports `ErrCorrupt`, restore from a backup (below).

### The disk is filling
`obs.StoreDiskLow` and `ErrDiskLow`: appends are refused (the consumer applies
backpressure) but acks and parks still work. Free space, or let the redriver drain the
queue. If `compaction_failed` fired, compaction is not reclaiming space: look at
`WALStats.LastCompactionErr`.

### Backups
- Live: `WALStore.Backup(ctx, dir)` copies a consistent log while the store serves.
  Schedule it and keep copies off the volume.
- Check them: `dlqctl verify DIR` reads every frame, checks signatures and the sequence
  and changes nothing. A backup that has not been verified is not a backup.
- Restore: stop the service, put the verified files in an empty directory, start.

### Rotating keys
- Encryption key: add the new key as active with `NewAESGCM(new, old)`; old data still
  opens. To retire the old key for good, `dlq.Reseal` into a new empty store, verify it,
  switch over, delete the old directory.
- Order-key pepper: put the new pepper first in `OrderKeyPeppers`; ordering holds across
  the change. Drop the old one when no stored record uses it.
- Signing key: `NewHMAC(new, old)` the same way.

## Sizing and defaults worth knowing

- `SyncAlways` (the default) fsyncs before acknowledging, with group commit. It is the only
  policy `RequireDurableStore` accepts.
- `Workers` is the parallelism; with slow steps, throughput is roughly `Workers / step latency`.
- Put the WAL on local block storage. NFS, SMB and FUSE are refused because `flock` and
  `fsync` are not reliable on them.
- The default HTTP transport waits at most 30s for response headers; set
  `Policy.RequestTimeout` to bound the whole call.
- A new consumer group starts from `earliest` unless you set `auto.offset.reset`.
