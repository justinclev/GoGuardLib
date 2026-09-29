package dlq

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// A snapshot is the whole live queue at one LSN. It lets the log be cut: once a
// snapshot is durable, every segment before it is redundant and is deleted, so
// the log's size follows the live data, not the history.

// loadSnapshot fills the machine from a snapshot. Any defect is fatal: unlike a
// torn log tail, a snapshot is written whole and renamed into place, so damage
// means bit rot, and guessing would risk silently dropping records.
func (w *WALStore) loadSnapshot(path string, lsn uint64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	size := info.Size()

	hdr := make([]byte, headerSize)
	n, _ := io.ReadFull(f, hdr)
	got, flags, herr := decodeHeaderFlags(hdr[:n], snapMagic)
	if herr != nil || got != lsn {
		if herr == nil {
			herr = fmt.Errorf("header says LSN %d", got)
		}
		return corrupt(path, 0, "bad snapshot header: %v", herr)
	}
	signed, ferr := w.checkFileFlags(flags)
	if ferr != nil {
		return fmt.Errorf("%w: %s: %w", ErrCorrupt, filepath.Base(path), ferr)
	}

	r := bufio.NewReaderSize(f, 1<<20)
	off := int64(headerSize)
	var count uint64
	for {
		fr, ferr := readFrame(r, size-off, w.opts.maxEntryBytes)
		if ferr != nil {
			return corrupt(path, off, "snapshot ends without a valid footer: %v", ferr)
		}
		frameStart := off
		off += fr.size
		body, serr := w.openBody(signed, true, lsn, fr.typ, fr.body)
		if serr != nil {
			return tampered(path, frameStart, serr)
		}
		fr.body = body
		switch fr.typ {
		case snapRecord:
			d := decoder{b: fr.body}
			rec := decodeRecord(&d)
			if err := d.done(); err != nil {
				return corrupt(path, off, "undecodable record: %v", err)
			}
			if _, dup := w.m.items[rec.ID]; dup {
				return corrupt(path, off, "duplicate record in snapshot")
			}
			w.insertRecord(rec, blobRef{kind: snapRecord, snap: true, signed: signed, file: lsn, off: frameStart, n: int32(fr.size)})
			count++
		case snapEnd:
			d := decoder{b: fr.body}
			want, nextSeq := d.uvarint(), d.uvarint()
			if err := d.done(); err != nil || want != count {
				return corrupt(path, off, "footer says %d records, read %d", want, count)
			}
			if off != size {
				return corrupt(path, off, "unexpected data after the footer")
			}
			if nextSeq > w.m.seq {
				w.m.seq = nextSeq
			}
			return nil
		default:
			return corrupt(path, off, "unexpected entry type %d in snapshot", fr.typ)
		}
	}
}

// Compact snapshots the live queue and deletes the log segments it makes
// redundant. It runs automatically as the log grows (see WALOptions). When
// payloads are on disk the snapshot is copied without holding the store's lock,
// so appends and leases continue while it runs; with PayloadsInMemory it blocks
// other operations while it writes, in proportion to the live data.
func (w *WALStore) Compact(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return w.compactOnce(ctx)
}

// compactOnce runs one compaction; only one runs at a time.
func (w *WALStore) compactOnce(ctx context.Context) error {
	w.compactMu.Lock()
	defer w.compactMu.Unlock()
	if w.offload {
		return w.compactOffload(ctx)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if f := w.failure.Load(); f != nil {
		return *f
	}
	err := w.compactLocked()
	w.compactErr = err
	return err
}

func (w *WALStore) compactLoop() {
	defer w.wg.Done()
	for {
		select {
		case <-w.stopCh:
			return
		case <-w.compactCh:
			w.mu.Lock()
			skip := w.closed || w.failure.Load() != nil || !time.Now().After(w.compactHold)
			w.mu.Unlock()
			if skip {
				continue
			}
			if err := w.compactOnce(context.Background()); err != nil {
				w.mu.Lock()
				w.compactHold = time.Now().Add(compactRetryAfter)
				w.mu.Unlock()
			}
		}
	}
}

// snapEntry is what a compaction captures about one record while it holds the
// lock: the record's metadata at LSN L and where its payload is.
type snapEntry struct {
	rec              Record // metadata only
	bodyRef, ckptRef blobRef
	bodyN, ckptN     int
	newRef           blobRef // where the copy landed in the snapshot
}

// compactOffload is compaction for stores whose payloads live on disk. It has
// three phases: capture the metadata and the list of old files under the lock,
// copy payloads into the snapshot with the lock released, then swap under the
// lock. Operations that happen during the copy are in the new segments, which
// recovery replays on top of the snapshot; the swap only re-points references
// that did not change in the meantime. The caller holds compactMu.
func (w *WALStore) compactOffload(ctx context.Context) (err error) {
	defer func() {
		if errors.Is(err, ErrClosed) {
			return
		}
		w.mu.Lock()
		w.compactErr = err
		w.mu.Unlock()
	}()
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	if f := w.failure.Load(); f != nil {
		w.mu.Unlock()
		return *f
	}
	if w.nextLSN-1 <= w.snapLSN {
		w.mu.Unlock()
		return nil
	}
	if err := w.checkDiskLocked(uint64(w.m.bytes)); err != nil { // the snapshot is as big as the live data
		w.mu.Unlock()
		return err
	}
	if w.activeSize > headerSize {
		if err := w.rotateLocked(); err != nil {
			w.mu.Unlock()
			return err
		}
	}
	last := w.nextLSN - 1
	seq := w.m.seq
	logAtCapture := w.logBytes
	oldSegs := append([]segment(nil), w.segments[:len(w.segments)-1]...)
	oldSnap := w.snapLSN
	entries := make([]snapEntry, 0, len(w.m.items))
	for el := w.m.order.Front(); el != nil; el = el.Next() {
		it := el.Value.(*memItem)
		e := snapEntry{rec: it.rec, bodyRef: it.bodyRef, ckptRef: it.ckptRef, bodyN: it.bodyN, ckptN: it.ckptN}
		if e.rec.State == Leased {
			e.rec.State = Pending // a lease belongs to a worker that will not survive a restart
		}
		entries = append(entries, e)
	}
	w.mu.Unlock()

	if h := w.opts.compactHook; h != nil {
		h("captured")
	}

	final := filepath.Join(w.dir, snapshotName(last))
	tmp := final + ".tmp"
	if err := w.writeSnapshotOffload(ctx, tmp, last, seq, entries); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := syncDir(w.dir); err != nil {
		return w.failAndWake(fmt.Errorf("syncing directory after snapshot: %w", err))
	}
	if h := w.opts.compactHook; h != nil {
		h("copied")
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if f := w.failure.Load(); f != nil {
		return *f
	}
	// Re-point whatever still refers to what was captured. A reference that has
	// changed since (a newer checkpoint, say) lives in a newer segment and stays.
	for i := range entries {
		e := &entries[i]
		it, ok := w.m.items[e.rec.ID]
		if !ok {
			continue
		}
		if it.bodyRef == e.bodyRef {
			it.bodyRef = e.newRef
		}
		if e.ckptRef.valid() && it.ckptRef == e.ckptRef {
			it.ckptRef = e.newRef
		}
	}
	for _, sg := range oldSegs {
		w.files.drop(fileKey{id: sg.start})
		_ = os.Remove(sg.path)
	}
	if oldSnap != 0 {
		w.files.drop(fileKey{snap: true, id: oldSnap})
		_ = os.Remove(filepath.Join(w.dir, snapshotName(oldSnap)))
	}
	_ = syncDir(w.dir)
	w.segments = append([]segment(nil), w.segments[len(oldSegs):]...)
	w.snapLSN = last
	w.logBytes -= logAtCapture
	if w.logBytes < 0 {
		w.logBytes = 0
	}
	return nil
}

// writeSnapshotOffload writes the snapshot, reading each record's payload from
// the old files, and records where each frame landed. A payload that cannot be
// read fails the compaction: dropping it would destroy the only evidence a backup
// restore could use.
func (w *WALStore) writeSnapshotOffload(ctx context.Context, path string, last, seq uint64, entries []snapEntry) error {
	f, err := w.opts.openFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(fileWriter{f}, 1<<20)
	fail := func(err error) error {
		_ = f.Close()
		return err
	}
	if _, err := bw.Write(encodeHeaderFlags(snapMagic, last, w.fileFlags())); err != nil {
		return fail(err)
	}
	off := int64(headerSize)
	var frameBuf []byte
	for i := range entries {
		select {
		case <-w.stopCh:
			return fail(ErrClosed)
		case <-ctx.Done():
			return fail(ctx.Err())
		default:
		}
		e := &entries[i]
		tmp := memItem{rec: e.rec, bodyRef: e.bodyRef, ckptRef: e.ckptRef}
		rec, err := w.hydrate(&tmp)
		if err != nil {
			return fail(fmt.Errorf("compaction cannot copy record %s: %w", e.rec.ID, err))
		}
		var enc encoder
		encodeRecord(&enc, &rec)
		frameBuf = appendFrame(frameBuf[:0], 0, snapRecord, w.sealBody(true, last, snapRecord, enc.b))
		if _, err := bw.Write(frameBuf); err != nil {
			return fail(err)
		}
		e.newRef = blobRef{kind: snapRecord, snap: true, signed: w.opts.Signer != nil, file: last, off: off, n: int32(len(frameBuf))}
		off += int64(len(frameBuf))
	}
	var enc encoder
	enc.uvarint(uint64(len(entries)))
	enc.uvarint(seq)
	frameBuf = appendFrame(frameBuf[:0], 0, snapEnd, w.sealBody(true, last, snapEnd, enc.b))
	if _, err := bw.Write(frameBuf); err != nil {
		return fail(err)
	}
	if err := bw.Flush(); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	return f.Close()
}

func (w *WALStore) compactLocked() error {
	if w.nextLSN-1 <= w.snapLSN {
		return nil // nothing new since the last snapshot
	}
	if err := w.checkDiskLocked(uint64(w.m.bytes)); err != nil { // the snapshot is as big as the live data
		return err
	}
	// Seal the active segment so every entry up to L sits in closed segments and
	// the new active segment starts at L+1.
	if w.activeSize > headerSize {
		if err := w.rotateLocked(); err != nil {
			return err
		}
	}
	last := w.nextLSN - 1

	final := filepath.Join(w.dir, snapshotName(last))
	tmp := final + ".tmp"
	if err := w.writeSnapshot(tmp, last); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := syncDir(w.dir); err != nil {
		// The rename may not be durable, so do not delete anything it replaces.
		return w.failAndWake(fmt.Errorf("syncing directory after snapshot: %w", err))
	}

	// The snapshot is durable and complete: what it covers can go. Failures here
	// only leave redundant files, which recovery removes.
	active := w.segments[len(w.segments)-1]
	for _, sg := range w.segments[:len(w.segments)-1] {
		_ = os.Remove(sg.path)
	}
	if w.snapLSN != 0 {
		_ = os.Remove(filepath.Join(w.dir, snapshotName(w.snapLSN)))
	}
	_ = syncDir(w.dir)
	w.segments = []segment{active}
	w.snapLSN = last
	w.logBytes = 0
	return nil
}

func (w *WALStore) writeSnapshot(path string, last uint64) error {
	f, err := w.opts.openFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(fileWriter{f}, 1<<20)
	if _, err := bw.Write(encodeHeaderFlags(snapMagic, last, w.fileFlags())); err != nil {
		_ = f.Close()
		return err
	}
	var count uint64
	var frameBuf []byte
	for el := w.m.order.Front(); el != nil; el = el.Next() {
		rec := el.Value.(*memItem).rec // shallow copy: only read below
		if rec.State == Leased {
			rec.State = Pending // a lease belongs to a worker that will not survive a restart
		}
		var e encoder
		encodeRecord(&e, &rec)
		frameBuf = appendFrame(frameBuf[:0], 0, snapRecord, w.sealBody(true, last, snapRecord, e.b))
		if _, err := bw.Write(frameBuf); err != nil {
			_ = f.Close()
			return err
		}
		count++
	}
	var e encoder
	e.uvarint(count)
	e.uvarint(w.m.seq)
	frameBuf = appendFrame(frameBuf[:0], 0, snapEnd, w.sealBody(true, last, snapEnd, e.b))
	if _, err := bw.Write(frameBuf); err != nil {
		_ = f.Close()
		return err
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// fileWriter adapts a walFile to io.Writer for bufio.
type fileWriter struct{ f walFile }

func (fw fileWriter) Write(p []byte) (int, error) { return fw.f.Write(p) }
