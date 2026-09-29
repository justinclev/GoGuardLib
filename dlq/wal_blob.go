package dlq

import (
	"bufio"
	"bytes"
	"container/list"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// blobRef says where on disk part of a record's payload lives: a frame in a
// segment or snapshot file. kind is the type of that frame, which says how to
// decode it. The zero value means "nothing stored".
type blobRef struct {
	kind byte   // entAppend / snapRecord (a whole record), entCheckpoint, entRequeue
	snap bool   // the file is a snapshot (file = its LSN) rather than a segment (file = its first LSN)
	file uint64 //
	off  int64  // offset of the frame in the file
	n    int32  // length of the frame in bytes, header included
}

func (r blobRef) valid() bool { return r.kind != 0 }

type fileKey struct {
	snap bool
	id   uint64
}

type cachedFile struct {
	key     fileKey
	f       *os.File
	refs    int  // readers using f right now
	evicted bool // no longer in the cache: close f when the last reader is done
}

// fileCache keeps a bounded number of log files open for reading. Log files are
// immutable once sealed, and the active segment only grows, so a positioned read of
// a frame that was written earlier is always safe, including while other goroutines
// append. A file that has been deleted after we opened it stays readable through
// the open handle, which is what lets compaction copy from old segments without
// holding the store's lock.
//
// Reads run outside the cache's mutex, so readers do not serialise; a handle that
// is evicted, dropped or closed while a reader is using it is closed by that
// reader's release, never under it.
type fileCache struct {
	mu     sync.Mutex
	dir    string
	max    int
	files  map[fileKey]*list.Element
	lru    *list.List // *cachedFile, most recently used first
	closed bool
}

func newFileCache(dir string, max int) *fileCache {
	if max <= 0 {
		max = 32
	}
	return &fileCache{dir: dir, max: max, files: map[fileKey]*list.Element{}, lru: list.New()}
}

func (c *fileCache) path(k fileKey) string {
	if k.snap {
		return filepath.Join(c.dir, snapshotName(k.id))
	}
	return filepath.Join(c.dir, segmentName(k.id))
}

var errCacheClosed = errors.New("dlq: file cache is closed")

// acquire returns an open handle for k and counts the caller as a user of it.
func (c *fileCache) acquire(k fileKey) (*cachedFile, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errCacheClosed
	}
	if el, ok := c.files[k]; ok {
		c.lru.MoveToFront(el)
		cf := el.Value.(*cachedFile)
		cf.refs++
		return cf, nil
	}
	f, err := os.Open(c.path(k))
	if err != nil {
		return nil, err
	}
	cf := &cachedFile{key: k, f: f, refs: 1}
	c.files[k] = c.lru.PushFront(cf)
	for c.lru.Len() > c.max {
		c.retireLocked(c.lru.Back())
	}
	return cf, nil
}

// retireLocked takes an element out of the cache and closes its file unless a
// reader still holds it.
func (c *fileCache) retireLocked(el *list.Element) {
	cf := el.Value.(*cachedFile)
	c.lru.Remove(el)
	delete(c.files, cf.key)
	cf.evicted = true
	if cf.refs == 0 {
		_ = cf.f.Close()
	}
}

func (c *fileCache) release(cf *cachedFile) {
	c.mu.Lock()
	cf.refs--
	if cf.evicted && cf.refs == 0 {
		_ = cf.f.Close()
	}
	c.mu.Unlock()
}

// readAt reads exactly n bytes at off.
func (c *fileCache) readAt(k fileKey, off int64, n int) ([]byte, error) {
	cf, err := c.acquire(k)
	if err != nil {
		return nil, err
	}
	defer c.release(cf)
	buf := make([]byte, n)
	read, err := cf.f.ReadAt(buf, off)
	if read == n {
		return buf, nil // a full read that ends exactly at the end of the file reports io.EOF; that is fine
	}
	if err == nil || errors.Is(err, io.EOF) {
		return nil, io.ErrUnexpectedEOF // the file is shorter than the reference says
	}
	return nil, err
}

// drop forgets the handle for a file that is about to be, or has been, deleted.
func (c *fileCache) drop(k fileKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.files[k]; ok {
		c.retireLocked(el)
	}
}

// closeAll closes every handle (those in use as soon as their readers finish) and
// refuses further opens.
func (c *fileCache) closeAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	for c.lru.Len() > 0 {
		c.retireLocked(c.lru.Back())
	}
}

func unreadable(format string, args ...any) error {
	return fmt.Errorf("%w: payload unreadable: %s", ErrCorrupt, fmt.Sprintf(format, args...))
}

// loadFrame reads and verifies the frame a reference points at.
func (w *WALStore) loadFrame(ref blobRef) (frame, error) {
	b, err := w.files.readAt(fileKey{snap: ref.snap, id: ref.file}, ref.off, int(ref.n))
	if err != nil {
		return frame{}, unreadable("%v", err)
	}
	fr, err := readFrame(bufio.NewReader(bytes.NewReader(b)), int64(len(b)), w.opts.maxEntryBytes)
	if err != nil {
		return frame{}, unreadable("%v", err)
	}
	if fr.typ != ref.kind {
		return frame{}, unreadable("frame type %d, expected %d", fr.typ, ref.kind)
	}
	return fr, nil
}

// loadRecord decodes the record frame ref points at and checks it is the record we
// think it is, so a stale or wrong reference cannot return another record's data.
func (w *WALStore) loadRecord(ref blobRef, id string) (Record, error) {
	fr, err := w.loadFrame(ref)
	if err != nil {
		return Record{}, err
	}
	d := decoder{b: fr.body}
	rec := decodeRecord(&d)
	if err := d.done(); err != nil {
		return Record{}, unreadable("%v", err)
	}
	if rec.ID != id {
		return Record{}, unreadable("frame holds record %q, expected %q", rec.ID, id)
	}
	return rec, nil
}

func (w *WALStore) loadCheckpoint(ref blobRef, id string) ([]byte, error) {
	switch ref.kind {
	case entAppend, snapRecord:
		rec, err := w.loadRecord(ref, id)
		return rec.Checkpoint, err
	case entCheckpoint, entRequeue:
		fr, err := w.loadFrame(ref)
		if err != nil {
			return nil, err
		}
		d := decoder{b: fr.body}
		got := d.str()
		var cp []byte
		if fr.typ == entCheckpoint {
			cp = d.nullable()
		} else if len(d.b) > 0 && d.u8() == 1 {
			cp = d.nullable()
		}
		if err := d.done(); err != nil {
			return nil, unreadable("%v", err)
		}
		if got != id {
			return nil, unreadable("frame holds record %q, expected %q", got, id)
		}
		return cp, nil
	}
	return nil, unreadable("unknown reference kind %d", ref.kind)
}

// itemSnap is what an unlocked reader needs about an item: a copy of its metadata
// and where its payload is, taken while the store lock was held.
type itemSnap struct {
	id  string
	tmp memItem
}

func snapshotItem(it *memItem) itemSnap {
	return itemSnap{id: it.rec.ID, tmp: memItem{rec: it.rec, bodyRef: it.bodyRef, ckptRef: it.ckptRef}}
}

// hydrateSnap reads a snapshot's payload. It touches no shared state, so it may run
// without the store lock; a failure may only mean the files moved (a compaction), so
// callers retry under the lock before believing it.
func (w *WALStore) hydrateSnap(s *itemSnap) (Record, error) { return w.hydrate(&s.tmp) }

// hydrate returns a full copy of the item's record, reading its payload from disk
// when it lives there.
func (w *WALStore) hydrate(it *memItem) (Record, error) {
	rec := it.rec.Clone()
	if it.bodyRef.valid() {
		full, err := w.loadRecord(it.bodyRef, rec.ID)
		if err != nil {
			return Record{}, err
		}
		rec.Key, rec.Value, rec.Headers = full.Key, full.Value, full.Headers
		if it.ckptRef == it.bodyRef { // the checkpoint is in the same frame: one read
			rec.Checkpoint = full.Checkpoint
			return rec, nil
		}
	}
	if it.ckptRef.valid() {
		cp, err := w.loadCheckpoint(it.ckptRef, rec.ID)
		if err != nil {
			return Record{}, err
		}
		rec.Checkpoint = cp
	}
	return rec, nil
}
