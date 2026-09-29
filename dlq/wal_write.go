package dlq

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ---- failing closed ----

// setFailure records the first fatal error. After it, every operation is refused.
func (w *WALStore) setFailure(cause error) error {
	e := fmt.Errorf("%w: %w", ErrStoreFailed, cause)
	w.failure.CompareAndSwap(nil, &e)
	return *w.failure.Load()
}

// failAndWake records a failure and releases anyone waiting for durability.
func (w *WALStore) failAndWake(cause error) error {
	err := w.setFailure(cause)
	w.syncMu.Lock()
	w.syncDone.Broadcast()
	w.syncMu.Unlock()
	return err
}

func (w *WALStore) begin(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if w.closed {
		return ErrClosed
	}
	if f := w.failure.Load(); f != nil {
		return *f
	}
	return nil
}

// ---- segments ----

// createSegment makes a new segment file with a durable header and returns it
// open for appending. On failure nothing is left behind, because a stray empty
// segment would be mistaken for the start of the next one.
func (w *WALStore) createSegment(start uint64) (walFile, error) {
	path := filepath.Join(w.dir, segmentName(start))
	f, err := w.opts.openFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (walFile, error) {
		_ = f.Close()
		if rerr := os.Remove(path); rerr != nil && !os.IsNotExist(rerr) {
			return nil, w.setFailure(fmt.Errorf("cannot clean up new segment: %w (after %w)", rerr, err))
		}
		return nil, err
	}
	if _, err := f.Write(encodeHeader(walMagic, start)); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := syncDir(w.dir); err != nil {
		return fail(err)
	}
	return f, nil
}

// newSegmentLocked starts a segment and makes it the active one (recovery only).
func (w *WALStore) newSegmentLocked(start uint64) error {
	f, err := w.createSegment(start)
	if err != nil {
		return err
	}
	w.active = f
	w.activeSize = headerSize
	w.segments = append(w.segments, segment{start: start, path: filepath.Join(w.dir, segmentName(start))})
	return nil
}

// rotateLocked seals the active segment and starts a new one. Everything written
// so far is durable when it returns.
func (w *WALStore) rotateLocked() error {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	for w.syncing { // never close a file the syncer is still flushing
		w.syncDone.Wait()
	}
	if err := w.active.Sync(); err != nil {
		_ = w.setFailure(fmt.Errorf("fsync on rotation: %w", err))
		w.syncDone.Broadcast()
		return *w.failure.Load()
	}
	if w.written > w.durable {
		w.durable = w.written
	}
	nf, err := w.createSegment(w.nextLSN)
	if err != nil {
		w.syncDone.Broadcast()
		return err // the old segment is intact and still active
	}
	old := w.active
	w.active, w.syncFile = nf, nf
	w.activeSize = headerSize
	w.segments = append(w.segments, segment{start: w.nextLSN, path: filepath.Join(w.dir, segmentName(w.nextLSN))})
	_ = old.Close() // already synced
	w.syncDone.Broadcast()
	return nil
}

// ---- appending to the log ----

// logLocked writes one entry and returns its position in the group-commit
// sequence and where the frame landed on disk. The caller applies the change to memory only after this succeeds, so
// memory never runs ahead of what the log accepted.
func (w *WALStore) logLocked(typ byte, body []byte) (uint64, blobRef, error) {
	if len(body) > w.opts.maxEntryBytes {
		return 0, blobRef{}, fmt.Errorf("%w: entry is %d bytes, limit is %d", ErrInvalidRecord, len(body), w.opts.maxEntryBytes)
	}
	frameBytes := appendFrame(nil, w.nextLSN, typ, body)
	if w.activeSize > headerSize && w.activeSize+int64(len(frameBytes)) > w.opts.SegmentBytes {
		if err := w.rotateLocked(); err != nil {
			return 0, blobRef{}, err
		}
	}
	ref := blobRef{kind: typ, file: w.segments[len(w.segments)-1].start, off: w.activeSize, n: int32(len(frameBytes))}
	n, err := w.active.Write(frameBytes)
	if err != nil || n != len(frameBytes) {
		if err == nil {
			err = fmt.Errorf("short write: %d of %d bytes", n, len(frameBytes))
		}
		// Cut the segment back to the last complete frame. If that is not possible
		// the file may hold half a frame in front of later data, which recovery
		// could not tell from corruption, so stop here.
		if terr := w.active.Truncate(w.activeSize); terr != nil {
			return 0, blobRef{}, w.failAndWake(fmt.Errorf("write failed (%w) and rollback failed (%w)", err, terr))
		}
		return 0, blobRef{}, fmt.Errorf("dlq: writing log entry: %w", err)
	}
	w.activeSize += int64(n)
	w.logBytes += int64(n)
	w.nextLSN++

	w.syncMu.Lock()
	w.written++
	seq := w.written
	w.syncWake.Signal()
	w.syncMu.Unlock()
	return seq, ref, nil
}

// currentSeq is the position of everything written so far.
func (w *WALStore) currentSeq() uint64 {
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	return w.written
}

// waitDurable blocks until the entry at seq has been fsynced (SyncAlways), or
// returns the failure that prevented it.
func (w *WALStore) waitDurable(seq uint64) error {
	if w.opts.Sync != SyncAlways {
		if f := w.failure.Load(); f != nil {
			return *f
		}
		return nil
	}
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	for w.durable < seq && w.failure.Load() == nil {
		w.syncDone.Wait()
	}
	if w.durable >= seq {
		return nil
	}
	return *w.failure.Load()
}

// syncLoopAlways is the group committer: while one fsync runs, later operations
// queue up and are all covered by the next one.
func (w *WALStore) syncLoopAlways() {
	defer w.wg.Done()
	w.syncMu.Lock()
	defer w.syncMu.Unlock()
	for {
		for w.written == w.durable && !w.syncStop && w.failure.Load() == nil {
			w.syncWake.Wait()
		}
		if w.failure.Load() != nil || (w.syncStop && w.written == w.durable) {
			w.syncDone.Broadcast()
			return
		}
		target, f := w.written, w.syncFile
		w.syncing = true
		w.syncMu.Unlock()
		err := f.Sync()
		w.syncMu.Lock()
		w.syncing = false
		if err != nil {
			// Never retry a failed fsync: the kernel may have dropped the dirty
			// pages, so a later success would prove nothing.
			_ = w.setFailure(fmt.Errorf("fsync: %w", err))
		} else if target > w.durable {
			w.durable = target
		}
		w.syncDone.Broadcast()
	}
}

func (w *WALStore) syncLoopInterval() {
	defer w.wg.Done()
	t := time.NewTicker(w.opts.SyncEvery)
	defer t.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-t.C:
			w.syncOnce()
		}
	}
}

func (w *WALStore) syncOnce() {
	w.syncMu.Lock()
	if w.written == w.durable || w.failure.Load() != nil {
		w.syncMu.Unlock()
		return
	}
	target, f := w.written, w.syncFile
	w.syncing = true
	w.syncMu.Unlock()
	err := f.Sync()
	w.syncMu.Lock()
	w.syncing = false
	if err != nil {
		_ = w.setFailure(fmt.Errorf("fsync: %w", err))
	} else if target > w.durable {
		w.durable = target
	}
	w.syncDone.Broadcast()
	w.syncMu.Unlock()
}

// ---- Store ----

// finish releases the lock and waits for durability of seq.
func (w *WALStore) finish(seq uint64) error {
	w.mu.Unlock()
	return w.waitDurable(seq)
}

func (w *WALStore) maybeCompactLocked() {
	if w.opts.DisableAutoCompact {
		return
	}
	threshold := int64(w.opts.CompactRatio * float64(w.m.bytes))
	if threshold < w.opts.CompactMinBytes {
		threshold = w.opts.CompactMinBytes
	}
	if w.logBytes >= threshold {
		select {
		case w.compactCh <- struct{}{}:
		default:
		}
	}
}

// Append implements Store. It returns after the record is on stable storage.
func (w *WALStore) Append(ctx context.Context, r Record) error {
	w.mu.Lock()
	if err := w.begin(ctx); err != nil {
		w.mu.Unlock()
		return err
	}
	rec, dup, err := w.m.prepare(r, w.opts.Clock())
	if err != nil {
		w.mu.Unlock()
		return err
	}
	if dup {
		// The original may still be waiting for its fsync: do not report the
		// record safe before it is.
		return w.finish(w.currentSeq())
	}
	var e encoder
	encodeRecord(&e, &rec)
	seq, ref, err := w.logLocked(entAppend, e.b)
	if err != nil {
		w.mu.Unlock()
		return err
	}
	w.insertRecord(rec, ref)
	w.maybeCompactLocked()
	return w.finish(seq)
}

// Lease implements Store.
func (w *WALStore) Lease(ctx context.Context, req LeaseRequest) ([]Lease, error) {
	if err := validateLease(req); err != nil {
		return nil, err
	}
	w.mu.Lock()
	if err := w.begin(ctx); err != nil {
		w.mu.Unlock()
		return nil, err
	}
	now := w.opts.Clock()
	items := w.m.selectLease(now, req)
	if len(items) == 0 {
		w.mu.Unlock()
		return nil, nil
	}
	// Read payloads before logging anything. A record whose payload cannot be read
	// is parked instead of leased: handing out a truncated message would be worse
	// than holding it for an operator.
	hydrated := make([]Record, 0, len(items))
	good := items[:0:0]
	var bad []*memItem
	for _, it := range items {
		rec, err := w.hydrate(it)
		if err != nil {
			bad = append(bad, it)
			continue
		}
		good = append(good, it)
		hydrated = append(hydrated, rec)
	}
	var lastSeq uint64
	for _, it := range bad {
		seq, err := w.parkUnreadableLocked(it, now)
		if err != nil {
			w.mu.Unlock()
			return nil, err
		}
		lastSeq = seq
	}
	if len(good) == 0 {
		w.maybeCompactLocked()
		return nil, w.finish(lastSeq)
	}
	var e encoder
	e.uvarint(uint64(len(good)))
	for _, it := range good {
		e.str(it.rec.ID)
	}
	// The attempt count is logged before the record leaves the store, so a worker
	// that crashes on a poison pill cannot reset its own counter.
	seq, _, err := w.logLocked(entLease, e.b)
	if err != nil {
		w.mu.Unlock()
		return nil, err
	}
	leases := w.m.grant(good, now, req.TTL)
	for i := range leases {
		h := hydrated[i]
		leases[i].Record.Key, leases[i].Record.Value = h.Key, h.Value
		leases[i].Record.Headers, leases[i].Record.Checkpoint = h.Headers, h.Checkpoint
	}
	w.maybeCompactLocked()
	if err := w.finish(seq); err != nil {
		return nil, err
	}
	return leases, nil
}

// unreadableReason is the fixed text a record is parked with when its payload
// cannot be read back. It carries no payload and no file names.
const unreadableReason = "payload unreadable on disk (corrupt or missing): restore the log files from a backup"

// parkUnreadableLocked logs and applies parking of a record whose payload is gone.
func (w *WALStore) parkUnreadableLocked(it *memItem, now time.Time) (uint64, error) {
	var e encoder
	e.str(it.rec.ID)
	e.time(now)
	e.str(unreadableReason)
	seq, _, err := w.logLocked(entPark, e.b)
	if err != nil {
		return 0, err
	}
	w.m.applyPark(it, now, unreadableReason)
	w.unreadable.Add(1)
	return seq, nil
}

// Checkpoint implements Store.
func (w *WALStore) Checkpoint(ctx context.Context, id, token string, checkpoint []byte) error {
	w.mu.Lock()
	if err := w.begin(ctx); err != nil {
		w.mu.Unlock()
		return err
	}
	it, err := w.m.held(id, token)
	if err == nil {
		err = w.m.checkCheckpoint(it, checkpoint)
	}
	if err != nil {
		w.mu.Unlock()
		return err
	}
	var e encoder
	e.str(id)
	e.nullable(checkpoint)
	seq, ref, err := w.logLocked(entCheckpoint, e.b)
	if err != nil {
		w.mu.Unlock()
		return err
	}
	w.setCheckpointAt(it, checkpoint, ref)
	w.maybeCompactLocked()
	return w.finish(seq)
}

// withLease runs a lease-fenced state change: verify, log, apply.
func (w *WALStore) withLease(ctx context.Context, id, token string, typ byte, body func(*encoder), apply func(*memItem)) error {
	w.mu.Lock()
	if err := w.begin(ctx); err != nil {
		w.mu.Unlock()
		return err
	}
	it, err := w.m.held(id, token)
	if err != nil {
		w.mu.Unlock()
		return err
	}
	var e encoder
	e.str(id)
	if body != nil {
		body(&e)
	}
	seq, _, err := w.logLocked(typ, e.b)
	if err != nil {
		w.mu.Unlock()
		return err
	}
	apply(it)
	w.maybeCompactLocked()
	return w.finish(seq)
}

// Ack implements Store.
func (w *WALStore) Ack(ctx context.Context, id, token string) error {
	return w.withLease(ctx, id, token, entAck, nil, func(it *memItem) { w.m.remove(it) })
}

// Release implements Store.
func (w *WALStore) Release(ctx context.Context, id, token string) error {
	return w.withLease(ctx, id, token, entRelease, nil, func(it *memItem) { w.m.applyRelease(it) })
}

// Nack implements Store.
func (w *WALStore) Nack(ctx context.Context, id, token string, o NackOptions) error {
	now := w.opts.Clock()
	next := now.Add(o.Delay)
	return w.withLease(ctx, id, token, entNack,
		func(e *encoder) {
			e.time(now)
			e.time(next)
			e.str(o.Err)
			e.str(o.BlockedOn)
			if o.Refund { // optional trailing flag: entries written before it existed still decode
				e.u8(1)
			}
		},
		func(it *memItem) { w.m.applyNack(it, now, next, o.Err, o.BlockedOn, o.Refund) })
}

// Park implements Store.
func (w *WALStore) Park(ctx context.Context, id, token, reason string) error {
	now := w.opts.Clock()
	return w.withLease(ctx, id, token, entPark,
		func(e *encoder) {
			e.time(now)
			e.str(reason)
		},
		func(it *memItem) { w.m.applyPark(it, now, reason) })
}

// withParked runs a change that applies to a parked record.
func (w *WALStore) withParked(ctx context.Context, id string, typ byte, apply func(*memItem)) error {
	w.mu.Lock()
	if err := w.begin(ctx); err != nil {
		w.mu.Unlock()
		return err
	}
	it, ok := w.m.items[id]
	switch {
	case !ok:
		w.mu.Unlock()
		return ErrNotFound
	case it.rec.State != Parked:
		w.mu.Unlock()
		return ErrNotParked
	}
	var e encoder
	e.str(id)
	seq, _, err := w.logLocked(typ, e.b)
	if err != nil {
		w.mu.Unlock()
		return err
	}
	apply(it)
	w.maybeCompactLocked()
	return w.finish(seq)
}

// Requeue implements Store.
func (w *WALStore) Requeue(ctx context.Context, id string) error {
	return w.RequeueWith(ctx, id, RequeueOptions{})
}

// RequeueWith implements Store. The checkpoint replacement is part of the same log
// entry as the requeue, so a crash can never leave the record pending with the old
// checkpoint.
func (w *WALStore) RequeueWith(ctx context.Context, id string, o RequeueOptions) error {
	w.mu.Lock()
	if err := w.begin(ctx); err != nil {
		w.mu.Unlock()
		return err
	}
	it, ok := w.m.items[id]
	switch {
	case !ok:
		w.mu.Unlock()
		return ErrNotFound
	case it.rec.State != Parked:
		w.mu.Unlock()
		return ErrNotParked
	}
	if o.ReplaceCheckpoint {
		if err := w.m.checkCheckpoint(it, o.Checkpoint); err != nil {
			w.mu.Unlock()
			return err
		}
	}
	var e encoder
	e.str(id)
	if o.ReplaceCheckpoint { // optional trailing fields: entries written before them still decode
		e.u8(1)
		e.nullable(o.Checkpoint)
	}
	seq, ref, err := w.logLocked(entRequeue, e.b)
	if err != nil {
		w.mu.Unlock()
		return err
	}
	w.m.applyRequeue(it, false, nil)
	if o.ReplaceCheckpoint {
		w.setCheckpointAt(it, o.Checkpoint, ref)
	}
	w.maybeCompactLocked()
	return w.finish(seq)
}

// Get implements Store.
func (w *WALStore) Get(ctx context.Context, id string) (Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.begin(ctx); err != nil {
		return Record{}, err
	}
	it, ok := w.m.items[id]
	if !ok {
		return Record{}, ErrNotFound
	}
	return w.hydrate(it)
}

// Parked implements Store.
func (w *WALStore) Parked(ctx context.Context, q ParkedQuery) ([]Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.begin(ctx); err != nil {
		return nil, err
	}
	items := w.m.parkedItems(q.After, q.Limit)
	out := make([]Record, 0, len(items))
	for _, it := range items {
		rec, err := w.hydrate(it)
		if err != nil {
			// Still list it, so an operator can see it and discard it, but without
			// a payload we cannot vouch for.
			rec = it.rec.Clone()
			rec.Key, rec.Value, rec.Headers, rec.Checkpoint = nil, nil, nil, nil
		}
		out = append(out, rec)
	}
	return out, nil
}

// Discard implements Store.
func (w *WALStore) Discard(ctx context.Context, id string) error {
	return w.withParked(ctx, id, entDiscard, func(it *memItem) { w.m.remove(it) })
}

// HasOrderKey implements Store.
func (w *WALStore) HasOrderKey(ctx context.Context, orderKey string) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.begin(ctx); err != nil {
		return false, err
	}
	return w.m.hasOrderKey(orderKey), nil
}

// Stats implements Store.
func (w *WALStore) Stats(ctx context.Context) (StoreStats, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.begin(ctx); err != nil {
		return StoreStats{}, err
	}
	return w.m.stats(w.opts.Clock()), nil
}

// Close stops the store after making everything written durable, whatever the
// sync policy. Records stay on disk. It is safe to call more than once.
func (w *WALStore) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	close(w.stopCh)
	w.mu.Unlock()

	// Let a compaction in progress finish or give up before the files go away.
	w.compactMu.Lock()
	defer w.compactMu.Unlock()

	w.syncMu.Lock()
	w.syncStop = true
	w.syncWake.Broadcast()
	w.syncMu.Unlock()
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()
	var err error
	if w.failure.Load() == nil {
		if serr := w.active.Sync(); serr != nil {
			err = w.failAndWake(fmt.Errorf("fsync on close: %w", serr))
		} else {
			w.syncMu.Lock()
			w.durable = w.written
			w.syncDone.Broadcast()
			w.syncMu.Unlock()
		}
	}
	if cerr := w.active.Close(); cerr != nil && err == nil {
		err = cerr
	}
	w.files.closeAll()
	w.lock.unlock()
	return err
}
