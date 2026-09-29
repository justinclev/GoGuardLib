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
	key fileKey
	f   *os.File
}

// fileCache keeps a bounded number of log files open for reading. Log files are
// immutable once sealed, and the active segment only grows, so a positioned read of
// a frame that was written earlier is always safe, including while other goroutines
// append. A file that has been deleted after we opened it stays readable through
// the open handle, which is what lets compaction copy from old segments without
// holding the store's lock.
type fileCache struct {
	mu    sync.Mutex
	dir   string
	max   int
	files map[fileKey]*list.Element
	lru   *list.List // *cachedFile, most recently used first
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

// readAt reads exactly n bytes at off.
func (c *fileCache) readAt(k fileKey, off int64, n int) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var f *os.File
	if el, ok := c.files[k]; ok {
		c.lru.MoveToFront(el)
		f = el.Value.(*cachedFile).f
	} else {
		var err error
		if f, err = os.Open(c.path(k)); err != nil {
			return nil, err
		}
		c.files[k] = c.lru.PushFront(&cachedFile{key: k, f: f})
		for c.lru.Len() > c.max {
			old := c.lru.Back()
			c.lru.Remove(old)
			cf := old.Value.(*cachedFile)
			delete(c.files, cf.key)
			_ = cf.f.Close()
		}
	}
	buf := make([]byte, n)
	read, err := f.ReadAt(buf, off)
	if read == n {
		return buf, nil // a full read that ends exactly at the end of the file reports io.EOF; that is fine
	}
	if err == nil || errors.Is(err, io.EOF) {
		return nil, io.ErrUnexpectedEOF // the file is shorter than the reference says
	}
	return nil, err
}

// drop closes the handle for a file that is about to be, or has been, deleted.
func (c *fileCache) drop(k fileKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.files[k]; ok {
		c.lru.Remove(el)
		delete(c.files, k)
		_ = el.Value.(*cachedFile).f.Close()
	}
}

func (c *fileCache) closeAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for el := c.lru.Front(); el != nil; el = el.Next() {
		_ = el.Value.(*cachedFile).f.Close()
	}
	c.files = map[fileKey]*list.Element{}
	c.lru.Init()
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
