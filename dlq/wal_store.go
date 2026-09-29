package dlq

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrCorrupt means the on-disk log is damaged in a way recovery will not paper
	// over: a bad frame before the tail, a gap in the sequence, or a bad snapshot.
	// The store refuses to open; nothing is modified. Restore from backup or
	// inspect the named file.
	ErrCorrupt = errors.New("dlq: WAL is corrupt")
	// ErrLocked means another store already has the directory open.
	ErrLocked = errors.New("dlq: WAL directory is locked by another store")
	// ErrStoreFailed means a disk write or fsync failed. The store stops serving
	// (fail closed) because after a failed fsync the on-disk state is unknown.
	// Close it and reopen: recovery restores every acknowledged record.
	ErrStoreFailed = errors.New("dlq: WAL store failed")
	// ErrInsecurePermissions means the directory or a file in it is readable or
	// writable by other users.
	ErrInsecurePermissions = errors.New("dlq: WAL directory has insecure permissions")
)

// SyncPolicy chooses when written data is forced to stable storage.
type SyncPolicy int

const (
	// SyncAlways makes every operation wait for an fsync before returning.
	// Concurrent operations share one fsync (group commit). An acknowledged
	// record survives a crash and a power failure. This is the default.
	SyncAlways SyncPolicy = iota
	// SyncInterval fsyncs in the background every SyncEvery. Operations return once
	// the data is written to the OS: it survives a process crash but a power
	// failure can lose up to SyncEvery of acknowledged operations.
	SyncInterval
	// SyncNone fsyncs only when segments rotate and on Close. For tests and
	// disposable data.
	SyncNone
)

const (
	// DefaultWALMaxBytes bounds the live data a store holds when
	// WALOptions.MaxBytes is zero. Payloads live on disk by default, so this is a
	// disk limit; with PayloadsInMemory it is also the memory the queue can use.
	DefaultWALMaxBytes = 512 << 20
	// DefaultWALMaxRecords bounds the in-memory index (about 300 bytes a record) when
	// payloads are on disk and WALOptions.MaxRecords is zero. Without it a flood of
	// tiny records could fit the byte limit and still need gigabytes of index.
	DefaultWALMaxRecords  = 1_000_000
	defaultSegmentBytes   = 64 << 20
	defaultCompactMin     = 64 << 20
	defaultCompactRatio   = 2.0
	defaultSyncEvery      = 100 * time.Millisecond
	defaultMaxRecordBytes = 8 << 20
	compactRetryAfter     = 30 * time.Second
)

// walFile is the part of *os.File the store writes through; tests replace it to
// inject disk faults.
type walFile interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
	Truncate(int64) error
}

// WALOptions configures OpenWAL. The zero value is a safe production default.
type WALOptions struct {
	// MaxRecords caps the number of records; Append beyond it returns ErrFull. With
	// payloads on disk it bounds memory and defaults to DefaultWALMaxRecords (a
	// negative value removes the cap); with PayloadsInMemory it defaults to no cap.
	MaxRecords int
	// MaxBytes caps the estimated bytes of live records, payload included; Append
	// and Checkpoint beyond it return ErrFull. Payloads are on disk by default, so
	// this bounds disk; set MaxRecords to bound memory. Default DefaultWALMaxBytes.
	MaxBytes int64
	// MaxRecordBytes rejects any single record larger than this. Default 8 MiB.
	MaxRecordBytes int

	// Sync selects the durability policy. Default SyncAlways.
	Sync SyncPolicy
	// SyncEvery is the fsync period for SyncInterval. Default 100ms.
	SyncEvery time.Duration

	// SegmentBytes is the size at which the log rolls to a new file. Default 64 MiB.
	SegmentBytes int64
	// CompactMinBytes and CompactRatio decide when to snapshot and drop old
	// segments: once the log written since the last snapshot exceeds both
	// CompactMinBytes and CompactRatio times the live data size. Defaults 64 MiB
	// and 2.
	CompactMinBytes int64
	CompactRatio    float64
	// DisableAutoCompact turns off background compaction; call Compact yourself.
	DisableAutoCompact bool

	// FailOnTruncation makes Open fail instead of repairing a torn tail. Use it
	// if you would rather investigate than auto-recover.
	FailOnTruncation bool
	// ForceRepair lets Open cut a damaged tail even when valid entries follow the
	// damage. Normally that pattern means bit rot rather than an interrupted
	// write, and Open refuses because cutting would discard entries that were
	// acknowledged. The cut bytes are still quarantined.
	ForceRepair bool
	// AllowInsecurePermissions skips the permission checks on the directory and
	// files (for volumes mounted with fixed modes).
	AllowInsecurePermissions bool

	// PayloadsInMemory keeps every record's key, value, headers and checkpoint in
	// memory, as earlier versions did: Lease is faster, but memory grows with the
	// data. By default only a small index per record is kept in memory and payloads
	// are read back from the log when needed, so MaxBytes bounds disk, and memory is
	// bounded by MaxRecords (about 300 bytes per record).
	PayloadsInMemory bool
	// FileCacheSize is how many log files are kept open for reading payloads.
	// Default 32.
	FileCacheSize int

	// Clock replaces time.Now, for tests.
	Clock func() time.Time

	// Test hooks.
	maxEntryBytes int
	openFile      func(name string, flag int, perm os.FileMode) (walFile, error)
	compactHook   func(stage string) // "captured" and "copied", to interleave work with a compaction
	leaseHook     func(stage string) // "selected" and "read", to interleave work with a lease's unlocked read
}

func (o *WALOptions) applyDefaults() error {
	if o.MaxRecords == 0 && !o.PayloadsInMemory {
		o.MaxRecords = DefaultWALMaxRecords
	}
	if o.MaxBytes == 0 {
		o.MaxBytes = DefaultWALMaxBytes
	}
	if o.MaxRecordBytes == 0 {
		o.MaxRecordBytes = defaultMaxRecordBytes
	}
	if o.SyncEvery <= 0 {
		o.SyncEvery = defaultSyncEvery
	}
	if o.SegmentBytes <= 0 {
		o.SegmentBytes = defaultSegmentBytes
	}
	if o.CompactMinBytes <= 0 {
		o.CompactMinBytes = defaultCompactMin
	}
	if o.CompactRatio <= 0 {
		o.CompactRatio = defaultCompactRatio
	}
	if o.Clock == nil {
		o.Clock = time.Now
	}
	if o.maxEntryBytes == 0 {
		o.maxEntryBytes = DefaultMaxEntryBytes
	}
	if o.openFile == nil {
		o.openFile = func(name string, flag int, perm os.FileMode) (walFile, error) {
			return os.OpenFile(name, flag, perm)
		}
	}
	if o.MaxRecordBytes > o.maxEntryBytes-1024 {
		return fmt.Errorf("dlq: MaxRecordBytes %d must be below the entry limit %d", o.MaxRecordBytes, o.maxEntryBytes)
	}
	return nil
}

// RecoveryReport says what OpenWAL found and repaired.
type RecoveryReport struct {
	// SnapshotLSN is the last entry covered by the snapshot loaded, 0 if none.
	SnapshotLSN uint64
	// Segments is the number of log files replayed.
	Segments int
	// Entries is the number of log entries applied on top of the snapshot.
	Entries uint64
	// Records is the number of records live after recovery.
	Records int
	// TruncatedBytes is how many bytes of torn tail were cut from the last
	// segment. They are copied to QuarantineFile first, never just deleted.
	TruncatedBytes int64
	QuarantineFile string
	Duration       time.Duration
}

// WALStats describes the log itself.
type WALStats struct {
	Segments          int
	LogBytes          int64  // bytes written since the last snapshot
	NextLSN           uint64 // LSN the next entry will get
	SnapshotLSN       uint64
	LastCompactionErr error
	Failed            error // non-nil once the store has failed closed
	// PayloadsOnDisk is true unless the store was opened with PayloadsInMemory.
	PayloadsOnDisk bool
	// UnreadablePayloads counts records whose payload could not be read back from
	// disk (corruption or a missing file). Such records are parked, not lost from
	// the index, so an operator can see them.
	UnreadablePayloads uint64
}

type segment struct {
	start uint64
	path  string
}

// WALStore is a durable Store: a segmented, checksummed write-ahead log with the
// live queue held in memory and rebuilt on open.
//
// Every state change is one log entry. An operation returns only after its entry
// is fsynced (per Sync), so acknowledged work survives a crash. If a write or
// fsync ever fails the store fails closed: it stops serving rather than continue
// on state it can no longer vouch for, and recovery on reopen restores every
// acknowledged operation.
//
// The directory is locked against a second store and created 0700 with 0600
// files. The whole live queue is held in memory, bounded by MaxBytes.
type WALStore struct {
	dir  string
	opts WALOptions
	lock *dirLock

	failure atomic.Pointer[error] // set once, fail closed

	offload    bool // payloads live on disk
	files      *fileCache
	unreadable atomic.Uint64
	compactMu  sync.Mutex // one compaction at a time; held for its whole run

	mu          sync.Mutex // state, log position and files
	m           *machine
	closed      bool
	active      walFile
	activeSize  int64
	segments    []segment // oldest first; the last is active
	nextLSN     uint64
	snapLSN     uint64
	logBytes    int64
	compactErr  error
	compactHold time.Time
	recovery    RecoveryReport

	// Group commit. Lock order: mu, then syncMu.
	syncMu   sync.Mutex
	syncWake *sync.Cond
	syncDone *sync.Cond
	written  uint64
	durable  uint64
	syncing  bool
	syncStop bool
	syncFile walFile

	stopCh    chan struct{}
	compactCh chan struct{}
	wg        sync.WaitGroup
}

var _ Store = (*WALStore)(nil)

func (w *WALStore) leaseHook(stage string) {
	if h := w.opts.leaseHook; h != nil {
		h(stage)
	}
}

func segmentName(start uint64) string { return fmt.Sprintf("wal-%020d.log", start) }
func snapshotName(lsn uint64) string  { return fmt.Sprintf("snapshot-%020d.snap", lsn) }

func parseName(name, prefix, suffix string) (uint64, bool) {
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return 0, false
	}
	digits := name[len(prefix) : len(name)-len(suffix)]
	if len(digits) != 20 {
		return 0, false
	}
	n, err := strconv.ParseUint(digits, 10, 64)
	return n, err == nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

func checkPerm(path string, info os.FileInfo, dir bool, o *WALOptions) error {
	if o.AllowInsecurePermissions {
		return nil
	}
	mask := os.FileMode(0o077)
	if dir {
		mask = 0o022 // other users may traverse, but must not write
	}
	if info.Mode().Perm()&mask != 0 {
		return fmt.Errorf("%w: %s is %#o", ErrInsecurePermissions, path, info.Mode().Perm())
	}
	return nil
}

// OpenWAL opens or creates a durable store in dir, recovering whatever a previous
// run left behind.
func OpenWAL(dir string, opts WALOptions) (*WALStore, error) {
	if err := opts.applyDefaults(); err != nil {
		return nil, err
	}
	start := time.Now()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("dlq: %s is not a directory", dir)
	}
	if err := checkPerm(dir, info, true, &opts); err != nil {
		return nil, err
	}
	lk, err := lockDir(dir)
	if err != nil {
		return nil, err
	}

	w := &WALStore{
		dir:       dir,
		opts:      opts,
		lock:      lk,
		offload:   !opts.PayloadsInMemory,
		files:     newFileCache(dir, opts.FileCacheSize),
		stopCh:    make(chan struct{}),
		compactCh: make(chan struct{}, 1),
	}
	w.syncWake = sync.NewCond(&w.syncMu)
	w.syncDone = sync.NewCond(&w.syncMu)

	if err := w.recover(); err != nil {
		w.releaseFiles()
		return nil, err
	}
	w.recovery.Duration = time.Since(start)

	w.syncFile = w.active
	switch opts.Sync {
	case SyncAlways:
		w.wg.Add(1)
		go w.syncLoopAlways()
	case SyncInterval:
		w.wg.Add(1)
		go w.syncLoopInterval()
	}
	if !opts.DisableAutoCompact {
		w.wg.Add(1)
		go w.compactLoop()
	}
	return w, nil
}

func (w *WALStore) releaseFiles() {
	if w.active != nil {
		_ = w.active.Close()
	}
	w.files.closeAll()
	w.lock.unlock()
}

// Recovery reports what OpenWAL did.
func (w *WALStore) Recovery() RecoveryReport { return w.recovery }

// WALStats describes the log.
func (w *WALStore) WALStats() WALStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := WALStats{
		Segments:           len(w.segments),
		LogBytes:           w.logBytes,
		NextLSN:            w.nextLSN,
		SnapshotLSN:        w.snapLSN,
		LastCompactionErr:  w.compactErr,
		PayloadsOnDisk:     w.offload,
		UnreadablePayloads: w.unreadable.Load(),
	}
	if f := w.failure.Load(); f != nil {
		st.Failed = *f
	}
	return st
}

// ---- recovery ----

func corrupt(path string, off int64, format string, args ...any) error {
	return fmt.Errorf("%w: %s at offset %d: %s", ErrCorrupt, filepath.Base(path), off, fmt.Sprintf(format, args...))
}

func (w *WALStore) recover() error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}
	var segs []segment
	var snaps []uint64
	for _, e := range entries {
		name := e.Name()
		if info, err := e.Info(); err == nil && !e.IsDir() && name != "LOCK" {
			if err := checkPerm(filepath.Join(w.dir, name), info, false, &w.opts); err != nil {
				return err
			}
		}
		switch {
		case strings.HasSuffix(name, ".tmp"):
			_ = os.Remove(filepath.Join(w.dir, name)) // an unfinished snapshot: never valid
		default:
			if n, ok := parseName(name, "wal-", ".log"); ok {
				segs = append(segs, segment{start: n, path: filepath.Join(w.dir, name)})
			} else if n, ok := parseName(name, "snapshot-", ".snap"); ok {
				snaps = append(snaps, n)
			}
		}
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].start < segs[j].start })
	sort.Slice(snaps, func(i, j int) bool { return snaps[i] < snaps[j] })

	w.m = newMachine(limits{maxRecords: w.opts.MaxRecords, maxBytes: w.opts.MaxBytes, maxRecordBytes: w.opts.MaxRecordBytes})

	if len(snaps) > 0 {
		lsn := snaps[len(snaps)-1]
		path := filepath.Join(w.dir, snapshotName(lsn))
		if err := w.loadSnapshot(path, lsn); err != nil {
			return err
		}
		w.snapLSN = lsn
		w.recovery.SnapshotLSN = lsn
		for _, old := range snaps[:len(snaps)-1] {
			_ = os.Remove(filepath.Join(w.dir, snapshotName(old)))
		}
	}
	expected := w.snapLSN + 1

	// Segments wholly covered by the snapshot are leftovers of an interrupted
	// compaction; the snapshot is durable, so they can go.
	for len(segs) > 1 && segs[1].start <= expected {
		_ = os.Remove(segs[0].path)
		segs = segs[1:]
	}

	var lastGoodEnd int64
	for i, sg := range segs {
		isLast := i == len(segs)-1
		end, next, err := w.replaySegment(sg, expected, isLast)
		if err != nil {
			return err
		}
		if end < 0 { // last segment had an unusable header and was set aside
			segs = segs[:i]
			break
		}
		expected = next
		lastGoodEnd = end
		w.recovery.Segments++
	}

	w.nextLSN = expected
	w.segments = segs
	if len(segs) == 0 {
		if err := w.newSegmentLocked(expected); err != nil {
			return err
		}
	} else {
		last := segs[len(segs)-1]
		f, err := w.opts.openFile(last.path, os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return err
		}
		w.active = f
		w.activeSize = lastGoodEnd
	}
	for _, sg := range w.segments {
		if info, err := os.Stat(sg.path); err == nil && sg.start > w.snapLSN {
			w.logBytes += info.Size() - headerSize
		}
	}
	w.m.endReplay()
	w.recovery.Records = len(w.m.items)
	return nil
}

// replaySegment applies the entries of one segment. It returns the offset of the
// end of the last good entry (or -1 if the segment was discarded) and the LSN
// that should come next.
func (w *WALStore) replaySegment(sg segment, expected uint64, isLast bool) (end int64, next uint64, err error) {
	f, err := os.Open(sg.path)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	size := info.Size()

	hdr := make([]byte, headerSize)
	n, _ := io.ReadFull(f, hdr)
	startLSN, herr := decodeHeader(hdr[:n], walMagic)
	if herr != nil || startLSN != sg.start {
		if isLast && herr != nil && sg.start >= expected {
			// The crash hit while the file was being created: it holds no entries.
			if err := w.quarantine(sg.path, 0, size); err != nil {
				return 0, 0, err
			}
			if err := os.Remove(sg.path); err != nil {
				return 0, 0, err
			}
			return -1, expected, nil
		}
		if herr == nil {
			herr = fmt.Errorf("header says segment starts at %d", startLSN)
		}
		return 0, 0, corrupt(sg.path, 0, "bad segment header: %v", herr)
	}
	if sg.start > expected {
		return 0, 0, corrupt(sg.path, 0, "log entries %d to %d are missing", expected, sg.start-1)
	}

	r := bufio.NewReaderSize(f, 1<<20)
	off := int64(headerSize)
	next = expected
	for {
		fr, ferr := readFrame(r, size-off, w.opts.maxEntryBytes)
		if errors.Is(ferr, io.EOF) {
			return off, next, nil
		}
		if ferr != nil {
			if !isLast {
				return 0, 0, corrupt(sg.path, off, "%v", ferr)
			}
			return w.repairTail(sg.path, off, size, next)
		}
		switch {
		case fr.lsn <= w.snapLSN:
			// already covered by the snapshot
		case fr.lsn == next:
			ref := blobRef{kind: fr.typ, file: sg.start, off: off, n: int32(fr.size)}
			if aerr := w.applyEntry(fr, ref); aerr != nil {
				return 0, 0, corrupt(sg.path, off, "entry %d: %v", fr.lsn, aerr)
			}
			next++
			w.recovery.Entries++
		default:
			return 0, 0, corrupt(sg.path, off, "expected entry %d but found %d", next, fr.lsn)
		}
		off += fr.size
	}
}

// repairTail handles a damaged frame in the last segment. A crash mid-write
// leaves exactly this. The damaged bytes are saved to a quarantine file before
// the segment is cut back, so a repair never destroys evidence or data.
func (w *WALStore) repairTail(path string, off, size int64, next uint64) (int64, uint64, error) {
	if w.opts.FailOnTruncation {
		return 0, 0, corrupt(path, off, "damaged tail (%d bytes) and FailOnTruncation is set", size-off)
	}
	if !w.opts.ForceRepair {
		if found, err := validFrameAfter(path, off, size, next, w.opts.maxEntryBytes); err != nil {
			return 0, 0, err
		} else if found {
			return 0, 0, corrupt(path, off, "damage is followed by valid log entries, so this is not an interrupted write; "+
				"refusing to discard them (set ForceRepair to cut the log here and quarantine the rest)")
		}
	}
	if err := w.quarantine(path, off, size); err != nil {
		return 0, 0, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = f.Close() }()
	if err := f.Truncate(off); err != nil {
		return 0, 0, err
	}
	if err := f.Sync(); err != nil {
		return 0, 0, err
	}
	w.recovery.TruncatedBytes = size - off
	return off, next, nil
}

// quarantine copies bytes [from,to) of path into a side file and syncs it.
func (w *WALStore) quarantine(path string, from, to int64) error {
	if to <= from {
		return nil
	}
	src, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	if _, err := src.Seek(from, io.SeekStart); err != nil {
		return err
	}
	qpath := fmt.Sprintf("%s.quarantine-%d", path, time.Now().UnixNano())
	dst, err := os.OpenFile(qpath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.CopyN(dst, src, to-from); err != nil {
		_ = dst.Close()
		return fmt.Errorf("saving damaged tail before repair: %w", err)
	}
	if err := dst.Sync(); err != nil {
		_ = dst.Close()
		return err
	}
	if err := dst.Close(); err != nil {
		return err
	}
	w.recovery.QuarantineFile = qpath
	return syncDir(w.dir)
}

func (w *WALStore) applyEntry(fr frame, ref blobRef) error {
	d := decoder{b: fr.body}
	switch fr.typ {
	case entAppend:
		rec := decodeRecord(&d)
		if err := d.done(); err != nil {
			return err
		}
		if rec.State == Leased {
			return errBadEntry
		}
		w.insertRecord(rec, ref)
	case entLease:
		n := d.uvarint()
		if n > uint64(len(d.b)) {
			return errBadEntry
		}
		ids := make([]string, 0, n)
		for i := uint64(0); i < n; i++ {
			ids = append(ids, d.str())
		}
		if err := d.done(); err != nil {
			return err
		}
		for _, id := range ids {
			w.m.replayLease(id)
		}
	case entCheckpoint:
		id, cp := d.str(), d.nullable()
		if err := d.done(); err != nil {
			return err
		}
		w.replayCheckpoint(id, cp, ref)
	case entRequeue:
		id := d.str()
		replace, cp := false, []byte(nil)
		if len(d.b) > 0 {
			replace = d.u8() == 1
			if replace {
				cp = d.nullable()
			}
		}
		if err := d.done(); err != nil {
			return err
		}
		w.replayRequeue(id, replace, cp, ref)
	case entAck, entRelease, entDiscard:
		id := d.str()
		if err := d.done(); err != nil {
			return err
		}
		switch fr.typ {
		case entAck:
			w.m.replayAck(id)
		case entRelease:
			w.m.replayRelease(id)
		case entDiscard:
			w.m.replayDiscard(id)
		}
	case entNack:
		id, at, next, text, blocked := d.str(), d.time(), d.time(), d.str(), d.str()
		refund := false
		if len(d.b) > 0 {
			refund = d.u8() == 1
		}
		if err := d.done(); err != nil {
			return err
		}
		w.m.replayNack(id, at, next, text, blocked, refund)
	case entPark:
		id, at, reason := d.str(), d.time(), d.str()
		if err := d.done(); err != nil {
			return err
		}
		w.m.replayPark(id, at, reason)
	default:
		return fmt.Errorf("unknown entry type %d (written by a newer version?)", fr.typ)
	}
	return nil
}

// resyncWindow bounds how far past damage recovery looks for valid entries.
const resyncWindow = 8 << 20

// validFrameAfter reports whether any byte offset after off, within the resync
// window, starts a frame that passes its checksum and carries an LSN at or after
// next. A checksum match at a random offset has odds of about 1 in 2^32, so a
// hit means real entries follow the damage.
func validFrameAfter(path string, off, size int64, next uint64, maxEntry int) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	start := off + 1
	if start >= size {
		return false, nil
	}
	n := size - start
	if n > resyncWindow {
		n = resyncWindow
	}
	buf := make([]byte, n)
	if _, err := f.ReadAt(buf, start); err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	for p := 0; p+frameOverhead+frameMeta <= len(buf); p++ {
		length := int64(binary.LittleEndian.Uint32(buf[p:]))
		if length < frameMeta || length > int64(maxEntry)+frameMeta || int64(p)+frameOverhead+length > int64(len(buf)) {
			continue
		}
		payload := buf[p+frameOverhead : p+frameOverhead+int(length)]
		c := crc32.Update(0, castagnoli, buf[p:p+4])
		c = crc32.Update(c, castagnoli, payload)
		if c != binary.LittleEndian.Uint32(buf[p+4:]) {
			continue
		}
		if binary.LittleEndian.Uint64(payload) >= next && payload[8] >= entAppend && payload[8] <= entDiscard {
			return true, nil
		}
	}
	return false, nil
}

// insertRecord stores a record whose frame is at ref: its payload goes to disk (and
// only the reference is kept) unless the store keeps payloads in memory.
func (w *WALStore) insertRecord(rec Record, ref blobRef) {
	if w.offload {
		w.m.insertOffloaded(rec, ref)
		return
	}
	w.m.insert(rec)
}

// setCheckpointAt records a new checkpoint whose frame is at ref.
func (w *WALStore) setCheckpointAt(it *memItem, cp []byte, ref blobRef) {
	if !w.offload {
		w.m.setCheckpoint(it, cp)
		return
	}
	if cp == nil {
		ref = blobRef{}
	}
	w.m.setCheckpointRef(it, len(cp), ref)
}

func (w *WALStore) replayCheckpoint(id string, cp []byte, ref blobRef) {
	if it, ok := w.m.items[id]; ok {
		w.setCheckpointAt(it, cp, ref)
	}
}

func (w *WALStore) replayRequeue(id string, replace bool, cp []byte, ref blobRef) {
	it, ok := w.m.items[id]
	if !ok || it.rec.State != Parked {
		return
	}
	w.m.applyRequeue(it, false, nil)
	if replace {
		w.setCheckpointAt(it, cp, ref)
	}
}
