package dlq

import (
	"bufio"
	"context"
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
	got, herr := decodeHeader(hdr[:n], snapMagic)
	if herr != nil || got != lsn {
		if herr == nil {
			herr = fmt.Errorf("header says LSN %d", got)
		}
		return corrupt(path, 0, "bad snapshot header: %v", herr)
	}

	r := bufio.NewReaderSize(f, 1<<20)
	off := int64(headerSize)
	var count uint64
	for {
		fr, ferr := readFrame(r, size-off, w.opts.maxEntryBytes)
		if ferr != nil {
			return corrupt(path, off, "snapshot ends without a valid footer: %v", ferr)
		}
		off += fr.size
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
			w.m.insert(rec)
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
// redundant. It runs automatically as the log grows (see WALOptions), and
// blocks other operations while it writes, in proportion to the live data.
func (w *WALStore) Compact(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
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
			if !w.closed && w.failure.Load() == nil && time.Now().After(w.compactHold) {
				err := w.compactLocked()
				w.compactErr = err
				if err != nil {
					w.compactHold = time.Now().Add(compactRetryAfter)
				}
			}
			w.mu.Unlock()
		}
	}
}

func (w *WALStore) compactLocked() error {
	if w.nextLSN-1 <= w.snapLSN {
		return nil // nothing new since the last snapshot
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
	if _, err := bw.Write(encodeHeader(snapMagic, last)); err != nil {
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
		frameBuf = appendFrame(frameBuf[:0], 0, snapRecord, e.b)
		if _, err := bw.Write(frameBuf); err != nil {
			_ = f.Close()
			return err
		}
		count++
	}
	var e encoder
	e.uvarint(count)
	e.uvarint(w.m.seq)
	frameBuf = appendFrame(frameBuf[:0], 0, snapEnd, e.b)
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
