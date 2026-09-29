package dlq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- fault injection ----

type faults struct {
	mu           sync.Mutex
	failWrite    func(name string) (partial int, err error) // write `partial` bytes, then fail
	failSync     func(name string) error
	failTruncate func(name string) error
	failOpen     func(name string, flag int) error
	syncDelay    time.Duration
	syncs        atomic.Int32
}

func (fl *faults) set(f func(*faults)) {
	fl.mu.Lock()
	f(fl)
	fl.mu.Unlock()
}

type faultFile struct {
	*os.File
	fl   *faults
	name string
}

func (fl *faults) open(name string, flag int, perm os.FileMode) (walFile, error) {
	fl.mu.Lock()
	fo := fl.failOpen
	fl.mu.Unlock()
	if fo != nil {
		if err := fo(name, flag); err != nil {
			return nil, err
		}
	}
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &faultFile{File: f, fl: fl, name: name}, nil
}

func (f *faultFile) Write(p []byte) (int, error) {
	f.fl.mu.Lock()
	fw := f.fl.failWrite
	f.fl.mu.Unlock()
	if fw != nil {
		if partial, err := fw(f.name); err != nil {
			if partial > len(p) {
				partial = len(p)
			}
			n, _ := f.File.Write(p[:partial])
			return n, err
		}
	}
	return f.File.Write(p)
}

func (f *faultFile) Sync() error {
	f.fl.mu.Lock()
	fs, delay := f.fl.failSync, f.fl.syncDelay
	f.fl.mu.Unlock()
	f.fl.syncs.Add(1)
	if delay > 0 {
		time.Sleep(delay)
	}
	if fs != nil {
		if err := fs(f.name); err != nil {
			return err
		}
	}
	return f.File.Sync()
}

func (f *faultFile) Truncate(n int64) error {
	f.fl.mu.Lock()
	ft := f.fl.failTruncate
	f.fl.mu.Unlock()
	if ft != nil {
		if err := ft(f.name); err != nil {
			return err
		}
	}
	return f.File.Truncate(n)
}

var errDisk = errors.New("injected disk failure")

func isSegment(name string) bool  { return strings.Contains(filepath.Base(name), "wal-") }
func isSnapshot(name string) bool { return strings.Contains(filepath.Base(name), "snapshot-") }

func openF(t testing.TB, dir string, fl *faults, o WALOptions) *WALStore {
	t.Helper()
	if fl != nil {
		o.openFile = fl.open
	}
	s, err := OpenWAL(dir, o)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	return s
}

func addRecords(t testing.TB, s *WALStore, ids ...string) {
	t.Helper()
	for _, id := range ids {
		if err := s.Append(context.Background(), Record{ID: id, Value: []byte("v-" + id)}); err != nil {
			t.Fatalf("Append(%s): %v", id, err)
		}
	}
}

func drainIDs(t testing.TB, s *WALStore) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for {
		ls, err := s.Lease(context.Background(), LeaseRequest{Max: 100, TTL: time.Hour})
		if err != nil {
			t.Fatalf("Lease: %v", err)
		}
		if len(ls) == 0 {
			return out
		}
		for _, l := range ls {
			out[l.Record.ID] = true
		}
	}
}

func lastSeg(t testing.TB, dir string) string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if len(m) == 0 {
		t.Fatal("no segment")
	}
	return m[len(m)-1] // zero-padded names sort by LSN
}

// ---- disk errors ----

// A write that fails halfway must not leave half a frame in front of later
// data: it is rolled back, and the store keeps working.
func TestPartialWriteIsRolledBack(t *testing.T) {
	dir := t.TempDir()
	fl := &faults{}
	s := openF(t, dir, fl, WALOptions{})
	addRecords(t, s, "r0", "r1")

	fl.set(func(f *faults) {
		f.failWrite = func(name string) (int, error) {
			if isSegment(name) {
				return 20, errDisk // 20 bytes of the frame reach the disk
			}
			return 0, nil
		}
	})
	if err := s.Append(context.Background(), Record{ID: "r2"}); err == nil {
		t.Fatal("Append succeeded despite the failed write")
	}
	fl.set(func(f *faults) { f.failWrite = nil })

	addRecords(t, s, "r3") // the store recovered by itself
	if s.WALStats().Failed != nil {
		t.Fatal("a rolled-back write should not fail the store")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openF(t, dir, nil, WALOptions{})
	defer s.Close()
	got := drainIDs(t, s)
	if len(got) != 3 || !got["r0"] || !got["r1"] || !got["r3"] || got["r2"] {
		t.Fatalf("recovered %v, want exactly the acknowledged r0 r1 r3", got)
	}
	if rep := s.Recovery(); rep.TruncatedBytes != 0 {
		t.Fatalf("the failed write left %d garbage bytes behind", rep.TruncatedBytes)
	}
}

// If the rollback also fails the file may hold half a frame ahead of later
// data, so the store stops; recovery on reopen repairs the tail.
func TestFailedRollbackStopsTheStore(t *testing.T) {
	dir := t.TempDir()
	fl := &faults{}
	s := openF(t, dir, fl, WALOptions{})
	addRecords(t, s, "r0", "r1")

	fl.set(func(f *faults) {
		f.failWrite = func(name string) (int, error) {
			if isSegment(name) {
				return 20, errDisk
			}
			return 0, nil
		}
		f.failTruncate = func(string) error { return errDisk }
	})
	if err := s.Append(context.Background(), Record{ID: "r2"}); !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("err = %v, want ErrStoreFailed", err)
	}
	ctx := context.Background()
	if err := s.Append(ctx, Record{ID: "r3"}); !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("later Append: %v; a failed store must refuse work", err)
	}
	if _, err := s.Lease(ctx, LeaseRequest{Max: 1, TTL: time.Second}); !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("Lease: %v", err)
	}
	if _, err := s.Stats(ctx); !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("Stats: %v", err)
	}
	if s.WALStats().Failed == nil {
		t.Fatal("WALStats does not report the failure")
	}
	_ = s.Close()

	s = openF(t, dir, nil, WALOptions{})
	defer s.Close()
	got := drainIDs(t, s)
	if len(got) != 2 || !got["r0"] || !got["r1"] {
		t.Fatalf("recovered %v, want the acknowledged r0 r1", got)
	}
	if s.Recovery().TruncatedBytes != 20 {
		t.Fatalf("report %+v: the 20-byte torn write should have been repaired", s.Recovery())
	}
}

// After a failed fsync the kernel may have dropped the data, so the store must
// stop, release everyone waiting, and never report the write as durable.
func TestFsyncFailureFailsClosed(t *testing.T) {
	dir := t.TempDir()
	fl := &faults{}
	s := openF(t, dir, fl, WALOptions{})
	addRecords(t, s, "acked1", "acked2")

	fl.set(func(f *faults) {
		f.syncDelay = 30 * time.Millisecond // let several writers queue behind the fsync
		f.failSync = func(name string) error {
			if isSegment(name) {
				return errDisk
			}
			return nil
		}
	})
	var wg sync.WaitGroup
	errs := make([]error, 6)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = s.Append(context.Background(), Record{ID: fmt.Sprintf("inflight%d", i)})
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("writers were left waiting after the fsync failed")
	}
	for i, err := range errs {
		if !errors.Is(err, ErrStoreFailed) {
			t.Errorf("writer %d: err = %v, want ErrStoreFailed: it must not be told its record is safe", i, err)
		}
	}
	if err := s.Append(context.Background(), Record{ID: "after"}); !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("Append after failure: %v", err)
	}
	_ = s.Close()

	s = openF(t, dir, nil, WALOptions{})
	defer s.Close()
	got := drainIDs(t, s)
	if !got["acked1"] || !got["acked2"] {
		t.Fatalf("recovered %v: records acknowledged before the failure must survive", got)
	}
}

func TestGroupCommitSharesFsyncs(t *testing.T) {
	dir := t.TempDir()
	fl := &faults{}
	s := openF(t, dir, fl, WALOptions{})
	defer s.Close()
	fl.set(func(f *faults) { f.syncDelay = 10 * time.Millisecond })
	base := fl.syncs.Load()

	const writers = 60
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := s.Append(context.Background(), Record{ID: fmt.Sprintf("w%02d", i)}); err != nil {
				t.Errorf("Append: %v", err)
			}
		}(i)
	}
	wg.Wait()
	syncs := int(fl.syncs.Load() - base)
	if syncs < 1 || syncs > writers/3 {
		t.Fatalf("%d fsyncs for %d concurrent appends; group commit should batch them", syncs, writers)
	}
	if got := drainIDs(t, s); len(got) != writers {
		t.Fatalf("%d of %d records", len(got), writers)
	}
}

func TestRotationFailureLeavesTheStoreUsable(t *testing.T) {
	dir := t.TempDir()
	fl := &faults{}
	s := openF(t, dir, fl, WALOptions{SegmentBytes: 300, DisableAutoCompact: true})
	addRecords(t, s, "a")

	fl.set(func(f *faults) {
		f.failOpen = func(name string, flag int) error {
			if isSegment(name) && flag&os.O_EXCL != 0 {
				return errDisk // creating a new segment fails
			}
			return nil
		}
	})
	var failed int
	for i := 0; i < 10; i++ {
		if err := s.Append(context.Background(), Record{ID: fmt.Sprintf("x%d", i), Value: make([]byte, 60)}); err != nil {
			failed++
			if errors.Is(err, ErrStoreFailed) {
				t.Fatal("a failed rotation must not fail the store")
			}
		}
	}
	if failed == 0 {
		t.Fatal("no append needed to rotate; the test is not exercising rotation")
	}
	fl.set(func(f *faults) { f.failOpen = nil })
	addRecords(t, s, "b")
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(s.Close())

	segs, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	s = openF(t, dir, nil, WALOptions{})
	defer s.Close()
	got := drainIDs(t, s)
	if !got["a"] || !got["b"] {
		t.Fatalf("recovered %v (%d segments)", got, len(segs))
	}
}

func TestSnapshotWriteFailureIsNotFatal(t *testing.T) {
	dir := t.TempDir()
	fl := &faults{}
	s := openF(t, dir, fl, WALOptions{DisableAutoCompact: true})
	defer s.Close()
	addRecords(t, s, "a", "b", "c")

	fl.set(func(f *faults) {
		f.failWrite = func(name string) (int, error) {
			if isSnapshot(name) {
				return 10, errDisk
			}
			return 0, nil
		}
	})
	if err := s.Compact(context.Background()); err == nil {
		t.Fatal("Compact should report the failure")
	}
	if tmp, _ := filepath.Glob(filepath.Join(dir, "*.tmp")); len(tmp) != 0 {
		t.Fatalf("unfinished snapshot left behind: %v", tmp)
	}
	if s.WALStats().LastCompactionErr == nil || s.WALStats().Failed != nil {
		t.Fatalf("stats = %+v; a failed compaction is reported but not fatal", s.WALStats())
	}
	addRecords(t, s, "d") // still serving

	fl.set(func(f *faults) { f.failWrite = nil })
	if err := s.Compact(context.Background()); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if s.WALStats().LastCompactionErr != nil {
		t.Fatal("error not cleared after success")
	}
}

func TestSyncIntervalAcknowledgesBeforeFsyncAndCloseFlushes(t *testing.T) {
	dir := t.TempDir()
	fl := &faults{}
	s := openF(t, dir, fl, WALOptions{Sync: SyncInterval, SyncEvery: 200 * time.Millisecond})
	base := fl.syncs.Load()
	start := time.Now()
	addRecords(t, s, "a", "b", "c")
	if time.Since(start) > 150*time.Millisecond || fl.syncs.Load() != base {
		t.Fatalf("SyncInterval should acknowledge before fsync (took %v, %d syncs)", time.Since(start), fl.syncs.Load()-base)
	}
	deadline := time.Now().Add(3 * time.Second)
	for fl.syncs.Load() == base && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if fl.syncs.Load() == base {
		t.Fatal("the background sync never ran")
	}
	addRecords(t, s, "d")
	pre := fl.syncs.Load()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if fl.syncs.Load() <= pre {
		t.Fatal("Close must flush whatever the sync policy")
	}
}

func TestCloseFsyncFailureIsReported(t *testing.T) {
	fl := &faults{}
	s := openF(t, t.TempDir(), fl, WALOptions{Sync: SyncNone})
	addRecords(t, s, "a")
	fl.set(func(f *faults) { f.failSync = func(string) error { return errDisk } })
	if err := s.Close(); !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("Close = %v; a failed final fsync must not be silent", err)
	}
}

// ---- entries that pass their checksum but make no sense ----

func craft(t testing.TB, dir string, lsn uint64, typ byte, body []byte) {
	t.Helper()
	f, err := os.OpenFile(lastSeg(t, dir), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(appendFrame(nil, lsn, typ, body)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestIntactButNonsensicalEntriesAreCorruptionNotATear(t *testing.T) {
	cases := map[string]func(t *testing.T, dir string){
		"lsn gap":          func(t *testing.T, dir string) { craft(t, dir, 10, entAck, []byte{1, 'x'}) },
		"lsn repeated":     func(t *testing.T, dir string) { craft(t, dir, 2, entAck, []byte{1, 'x'}) },
		"unknown type":     func(t *testing.T, dir string) { craft(t, dir, 4, 99, []byte{1}) },
		"undecodable body": func(t *testing.T, dir string) { craft(t, dir, 4, entAppend, []byte{0xff, 0xff}) },
		"trailing junk":    func(t *testing.T, dir string) { craft(t, dir, 4, entAck, []byte{1, 'x', 0}) },
		"append leased":    func(t *testing.T, dir string) { craft(t, dir, 4, entAppend, leasedRecordBody()) },
	}
	for name, make := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			s := openF(t, dir, nil, WALOptions{})
			addRecords(t, s, "a", "b", "c") // LSNs 1..3
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			make(t, dir)
			_, err := OpenWAL(dir, WALOptions{ForceRepair: true})
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt: an intact frame is not a torn write, so it must never be silently cut", err)
			}
		})
	}
}

func leasedRecordBody() []byte {
	var e encoder
	r := Record{ID: "x", State: Leased}
	encodeRecord(&e, &r)
	return e.b
}

func TestSegmentHeaderMustMatchItsFilename(t *testing.T) {
	dir := t.TempDir()
	s := openF(t, dir, nil, WALOptions{})
	addRecords(t, s, "a")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	seg := lastSeg(t, dir)
	if err := os.Rename(seg, filepath.Join(dir, segmentName(5))); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWAL(dir, WALOptions{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

// Recovery reads bytes that a crash, a bad disk or an attacker produced. Whatever
// it is given it must return an error or a consistent store, and never panic.
func FuzzRecoverSegment(f *testing.F) {
	dir := f.TempDir()
	s, err := OpenWAL(dir, WALOptions{DisableAutoCompact: true})
	if err != nil {
		f.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		_ = s.Append(context.Background(), Record{ID: fmt.Sprintf("r%d", i), Value: []byte("payload"), Checkpoint: []byte("cp")})
	}
	ls, _ := s.Lease(context.Background(), LeaseRequest{Max: 2, TTL: time.Hour})
	_ = s.Ack(context.Background(), ls[0].Record.ID, ls[0].Token)
	_ = s.Close()
	seed, err := os.ReadFile(lastSeg(f, dir))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add(seed[:len(seed)/2])
	f.Add(seed[:headerSize])
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		d := t.TempDir()
		if err := os.WriteFile(filepath.Join(d, segmentName(1)), data, 0o600); err != nil {
			t.Fatal(err)
		}
		s, err := OpenWAL(d, WALOptions{DisableAutoCompact: true})
		if err != nil {
			return // refusing is always acceptable
		}
		defer s.Close()
		// A store that opened must be usable and must survive a reopen.
		if err := s.Append(context.Background(), Record{ID: "probe"}); err != nil {
			t.Fatalf("recovered store cannot append: %v", err)
		}
		before := drainIDs(t, s)
		_ = s.Close()
		s2, err := OpenWAL(d, WALOptions{DisableAutoCompact: true})
		if err != nil {
			t.Fatalf("a store that opened once cannot reopen: %v", err)
		}
		defer s2.Close()
		after := drainIDs(t, s2)
		if len(before) != len(after) {
			t.Fatalf("reopen changed the record count: %d -> %d", len(before), len(after))
		}
	})
}
