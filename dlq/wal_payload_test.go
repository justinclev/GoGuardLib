package dlq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func openInternal(t testing.TB, dir string, o WALOptions) *WALStore {
	t.Helper()
	s, err := OpenWAL(dir, o)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	return s
}

func payload(i int) []byte { return []byte(fmt.Sprintf("payload-%03d", i)) }

func mustNil(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func heapInUse() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func TestPayloadsStayOutOfMemory(t *testing.T) {
	const n, size = 100, 256 << 10
	grow := func(inMemory bool) uint64 {
		s := openInternal(t, t.TempDir(), WALOptions{PayloadsInMemory: inMemory, Sync: SyncNone, DisableAutoCompact: true, MaxBytes: 1 << 30})
		defer func() { _ = s.Close() }()
		before := heapInUse()
		for i := 0; i < n; i++ {
			mustNil(t, s.Append(context.Background(), Record{ID: fmt.Sprintf("r%03d", i), Value: bytes.Repeat([]byte{byte(i)}, size)}))
		}
		after := heapInUse()
		if st := s.WALStats(); st.PayloadsOnDisk == inMemory {
			t.Fatalf("PayloadsOnDisk = %v with PayloadsInMemory = %v", st.PayloadsOnDisk, inMemory)
		}
		if after < before {
			return 0
		}
		return after - before
	}
	disk, mem := grow(false), grow(true)
	t.Logf("heap growth for %d MiB of payload: on disk %d KiB, in memory %d KiB", n*size>>20, disk>>10, mem>>10)
	if disk > 4<<20 {
		t.Fatalf("payloads on disk still grew the heap by %d KiB", disk>>10)
	}
	if mem < n*size/2 {
		t.Fatalf("control failed: PayloadsInMemory grew the heap by only %d KiB", mem>>10)
	}
}

func TestPayloadsRoundTripThroughEveryOperation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openInternal(t, dir, WALOptions{SegmentBytes: 600, DisableAutoCompact: true})
	rec := Record{ID: "a", Key: []byte("k"), Value: []byte("v"), Headers: []Header{{Key: "h", Value: []byte("hv")}}, OrderKey: "o"}
	mustNil(t, s.Append(ctx, rec))
	ls, err := s.Lease(ctx, LeaseRequest{Max: 1, TTL: time.Hour})
	mustNil(t, err)
	got := ls[0].Record
	if string(got.Key) != "k" || string(got.Value) != "v" || len(got.Headers) != 1 || string(got.Headers[0].Value) != "hv" {
		t.Fatalf("leased record lost its payload: %+v", got)
	}
	mustNil(t, s.Checkpoint(ctx, "a", ls[0].Token, []byte("cp1")))
	mustNil(t, s.Park(ctx, "a", ls[0].Token, "why"))
	pk, err := s.Parked(ctx, ParkedQuery{})
	mustNil(t, err)
	if len(pk) != 1 || string(pk[0].Value) != "v" || string(pk[0].Checkpoint) != "cp1" {
		t.Fatalf("parked record: %+v", pk)
	}
	mustNil(t, s.RequeueWith(ctx, "a", RequeueOptions{ReplaceCheckpoint: true, Checkpoint: []byte("cp2")}))
	g, err := s.Get(ctx, "a")
	mustNil(t, err)
	if string(g.Value) != "v" || string(g.Checkpoint) != "cp2" {
		t.Fatalf("after requeue: %+v", g)
	}
	mustNil(t, s.Close())

	s = openInternal(t, dir, WALOptions{SegmentBytes: 600, DisableAutoCompact: true})
	defer func() { _ = s.Close() }()
	g, err = s.Get(ctx, "a")
	mustNil(t, err)
	if string(g.Key) != "k" || string(g.Value) != "v" || string(g.Checkpoint) != "cp2" || g.Headers[0].Key != "h" {
		t.Fatalf("after reopen: %+v", g)
	}
	// A replaced-with-nil checkpoint must read back as none.
	ls, err = s.Lease(ctx, LeaseRequest{Max: 1, TTL: time.Hour})
	mustNil(t, err)
	mustNil(t, s.Checkpoint(ctx, "a", ls[0].Token, nil))
	if g, _ = s.Get(ctx, "a"); g.Checkpoint != nil {
		t.Fatalf("checkpoint = %q, want none", g.Checkpoint)
	}
}

func TestCompactionRepointsReferences(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openInternal(t, dir, WALOptions{SegmentBytes: 400, DisableAutoCompact: true})
	defer func() { _ = s.Close() }()
	for i := 0; i < 20; i++ {
		mustNil(t, s.Append(ctx, Record{ID: fmt.Sprintf("r%02d", i), Value: payload(i)}))
	}
	ls, err := s.Lease(ctx, LeaseRequest{Max: 1, TTL: time.Hour})
	mustNil(t, err)
	mustNil(t, s.Checkpoint(ctx, ls[0].Record.ID, ls[0].Token, []byte("cp")))
	before, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	mustNil(t, s.Compact(ctx))
	after, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if len(after) >= len(before) {
		t.Fatalf("compaction left %d segments (was %d)", len(after), len(before))
	}
	s.mu.Lock()
	for id, it := range s.m.items {
		if !it.bodyRef.snap {
			t.Errorf("%s still points at a segment after compaction", id)
		}
		if it.ckptRef.valid() && !it.ckptRef.snap {
			t.Errorf("%s checkpoint still points at a segment", id)
		}
	}
	s.mu.Unlock()
	// Everything is readable from the snapshot, with no reopen in between.
	for i := 0; i < 20; i++ {
		g, err := s.Get(ctx, fmt.Sprintf("r%02d", i))
		mustNil(t, err)
		if !bytes.Equal(g.Value, payload(i)) {
			t.Fatalf("r%02d: %q", i, g.Value)
		}
	}
	if g, _ := s.Get(ctx, ls[0].Record.ID); string(g.Checkpoint) != "cp" {
		t.Fatalf("checkpoint lost by compaction: %q", g.Checkpoint)
	}
}

func TestCompactionDoesNotBlockAndKeepsConcurrentChanges(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	var s *WALStore
	var tok map[string]string
	done := func(what string, f func()) {
		t.Helper()
		c := make(chan struct{})
		go func() { f(); close(c) }()
		select {
		case <-c:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s blocked while compaction was copying", what)
		}
	}
	o := WALOptions{SegmentBytes: 500, DisableAutoCompact: true}
	o.compactHook = func(stage string) {
		switch stage {
		case "captured":
			done("append", func() {
				mustNil(t, s.Append(ctx, Record{ID: "late1", Value: []byte("late-1")}))
				mustNil(t, s.Checkpoint(ctx, "r0", tok["r0"], []byte("cp-new")))
				mustNil(t, s.Ack(ctx, "r1", tok["r1"]))
				mustNil(t, s.Park(ctx, "r2", tok["r2"], "parked mid-compaction"))
			})
		case "copied":
			done("append", func() {
				mustNil(t, s.Append(ctx, Record{ID: "late2", Value: []byte("late-2")}))
				mustNil(t, s.Nack(ctx, "r3", tok["r3"], NackOptions{Err: "boom"}))
			})
		}
	}
	s = openInternal(t, dir, o)
	for i := 0; i < 8; i++ {
		mustNil(t, s.Append(ctx, Record{ID: fmt.Sprintf("r%d", i), Value: payload(i)}))
	}
	ls, err := s.Lease(ctx, LeaseRequest{Max: 4, TTL: time.Hour})
	mustNil(t, err)
	tok = map[string]string{}
	for _, l := range ls {
		tok[l.Record.ID] = l.Token
	}
	mustNil(t, s.Checkpoint(ctx, "r0", tok["r0"], []byte("cp-old")))

	mustNil(t, s.Compact(ctx))

	verify := func(s *WALStore, leased bool) {
		t.Helper()
		if _, err := s.Get(ctx, "r1"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("r1 was acked during compaction but Get says %v", err)
		}
		g, err := s.Get(ctx, "r0")
		mustNil(t, err)
		if string(g.Checkpoint) != "cp-new" || !bytes.Equal(g.Value, payload(0)) {
			t.Fatalf("r0: checkpoint %q value %q", g.Checkpoint, g.Value)
		}
		if g, _ = s.Get(ctx, "r2"); g.State != Parked || g.LastError != "parked mid-compaction" || !bytes.Equal(g.Value, payload(2)) {
			t.Fatalf("r2: %+v", g)
		}
		if g, _ = s.Get(ctx, "r3"); g.State != Pending || g.LastError != "boom" {
			t.Fatalf("r3: %+v", g)
		}
		for id, want := range map[string]string{"late1": "late-1", "late2": "late-2"} {
			if g, err = s.Get(ctx, id); err != nil || string(g.Value) != want {
				t.Fatalf("%s: %v %q", id, err, g.Value)
			}
		}
		for i := 4; i < 8; i++ {
			if g, err = s.Get(ctx, fmt.Sprintf("r%d", i)); err != nil || !bytes.Equal(g.Value, payload(i)) {
				t.Fatalf("r%d: %v %q", i, err, g.Value)
			}
		}
	}
	verify(s, true)
	// The new segments hold the operations made during the copy; they replay on
	// top of the snapshot.
	mustNil(t, s.Close())
	s = openInternal(t, dir, WALOptions{SegmentBytes: 500, DisableAutoCompact: true})
	defer func() { _ = s.Close() }()
	verify(s, false)
	st := s.WALStats()
	if st.UnreadablePayloads != 0 {
		t.Fatalf("unreadable = %d", st.UnreadablePayloads)
	}
}

// corruptPayload flips a byte inside the frame that holds id's body.
func corruptPayload(t *testing.T, dir string, s *WALStore, id string) {
	t.Helper()
	s.mu.Lock()
	ref := s.m.items[id].bodyRef
	s.mu.Unlock()
	name := segmentName(ref.file)
	if ref.snap {
		name = snapshotName(ref.file)
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_RDWR, 0)
	mustNil(t, err)
	defer func() { _ = f.Close() }()
	b := make([]byte, 1)
	at := ref.off + int64(ref.n) - 2
	_, err = f.ReadAt(b, at)
	mustNil(t, err)
	b[0] ^= 0xff
	_, err = f.WriteAt(b, at)
	mustNil(t, err)
}

func TestUnreadablePayloadIsParkedNotHandedOut(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openInternal(t, dir, WALOptions{DisableAutoCompact: true})
	defer func() { _ = s.Close() }()
	for i := 1; i <= 3; i++ {
		mustNil(t, s.Append(ctx, Record{ID: fmt.Sprintf("r%d", i), Value: payload(i)}))
	}
	corruptPayload(t, dir, s, "r2")

	if _, err := s.Get(ctx, "r2"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get on a corrupt payload: %v, want ErrCorrupt", err)
	}
	ls, err := s.Lease(ctx, LeaseRequest{Max: 10, TTL: time.Hour})
	mustNil(t, err)
	if len(ls) != 2 || ls[0].Record.ID != "r1" || ls[1].Record.ID != "r3" {
		t.Fatalf("leased %d records, want r1 and r3", len(ls))
	}
	if !bytes.Equal(ls[0].Record.Value, payload(1)) || !bytes.Equal(ls[1].Record.Value, payload(3)) {
		t.Fatal("healthy records lost their payload")
	}
	pk, err := s.Parked(ctx, ParkedQuery{})
	mustNil(t, err)
	if len(pk) != 1 || pk[0].ID != "r2" || pk[0].Value != nil || pk[0].LastError != unreadableReason {
		t.Fatalf("parked = %+v", pk)
	}
	if got := s.WALStats().UnreadablePayloads; got != 1 {
		t.Fatalf("UnreadablePayloads = %d", got)
	}
	// It cannot be requeued into a loop, but an operator can discard it.
	mustNil(t, s.Discard(ctx, "r2"))
}

func TestTruncatedSegmentIsUnreadableToo(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openInternal(t, dir, WALOptions{DisableAutoCompact: true, FileCacheSize: 1})
	defer func() { _ = s.Close() }()
	mustNil(t, s.Append(ctx, Record{ID: "r1", Value: payload(1)}))
	s.mu.Lock()
	ref := s.m.items["r1"].bodyRef
	s.mu.Unlock()
	mustNil(t, os.Truncate(filepath.Join(dir, segmentName(ref.file)), ref.off+3))
	if _, err := s.Get(ctx, "r1"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get: %v, want ErrCorrupt", err)
	}
}

func TestCompactionRefusesToDropAnUnreadablePayload(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openInternal(t, dir, WALOptions{DisableAutoCompact: true})
	defer func() { _ = s.Close() }()
	for i := 1; i <= 3; i++ {
		mustNil(t, s.Append(ctx, Record{ID: fmt.Sprintf("r%d", i), Value: payload(i)}))
	}
	corruptPayload(t, dir, s, "r2")
	if err := s.Compact(ctx); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Compact: %v, want ErrCorrupt", err)
	}
	if m, _ := filepath.Glob(filepath.Join(dir, "snapshot-*")); len(m) != 0 {
		t.Fatalf("a failed compaction left files behind: %v", m)
	}
	// The store keeps working and the log is intact.
	mustNil(t, s.Append(ctx, Record{ID: "r4", Value: payload(4)}))
	if g, err := s.Get(ctx, "r3"); err != nil || !bytes.Equal(g.Value, payload(3)) {
		t.Fatalf("r3: %v", err)
	}
}

func TestAWrongReferenceNeverReturnsAnotherRecordsData(t *testing.T) {
	ctx := context.Background()
	s := openInternal(t, t.TempDir(), WALOptions{DisableAutoCompact: true})
	defer func() { _ = s.Close() }()
	mustNil(t, s.Append(ctx, Record{ID: "r1", Value: payload(1)}))
	mustNil(t, s.Append(ctx, Record{ID: "r2", Value: payload(2)}))
	s.mu.Lock()
	orig := s.m.items["r1"].bodyRef
	s.m.items["r1"].bodyRef = s.m.items["r2"].bodyRef // a bug elsewhere, simulated
	s.mu.Unlock()
	if g, err := s.Get(ctx, "r1"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get returned %q, %v; want ErrCorrupt", g.Value, err)
	}
	s.mu.Lock()
	s.m.items["r1"].bodyRef = orig
	s.m.items["r1"].bodyRef.kind = entCheckpoint
	s.mu.Unlock()
	if _, err := s.Get(ctx, "r1"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("a frame of the wrong type was accepted: %v", err)
	}
}

func TestTheIndexIsBoundedByDefaultWhenPayloadsAreOnDisk(t *testing.T) {
	s := openInternal(t, t.TempDir(), WALOptions{})
	if got := s.m.lim.maxRecords; got != DefaultWALMaxRecords {
		t.Fatalf("default MaxRecords = %d, want %d", got, DefaultWALMaxRecords)
	}
	_ = s.Close()
	s = openInternal(t, t.TempDir(), WALOptions{PayloadsInMemory: true})
	if got := s.m.lim.maxRecords; got != 0 {
		t.Fatalf("in-memory default MaxRecords = %d, want none (MaxBytes bounds it)", got)
	}
	_ = s.Close()
	s = openInternal(t, t.TempDir(), WALOptions{MaxRecords: -1})
	if got := s.m.lim.maxRecords; got > 0 {
		t.Fatalf("a negative MaxRecords must remove the cap, got %d", got)
	}
	_ = s.Close()
}
