package dlq_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/dlq/storetest"
)

func openWAL(t testing.TB, dir string, opts dlq.WALOptions) *dlq.WALStore {
	t.Helper()
	s, err := dlq.OpenWAL(dir, opts)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	return s
}

func walFactory(opts dlq.WALOptions) storetest.Factory {
	return func(t *testing.T, now func() time.Time) dlq.Store {
		o := opts
		o.Clock = now
		s := openWAL(t, t.TempDir(), o)
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
}

func TestWALStoreConformance(t *testing.T) {
	storetest.Run(t, walFactory(dlq.WALOptions{}))
}

func TestWALStoreConformancePayloadsInMemory(t *testing.T) {
	storetest.Run(t, walFactory(dlq.WALOptions{PayloadsInMemory: true}))
	storetest.Run(t, walFactory(dlq.WALOptions{PayloadsInMemory: true, SegmentBytes: 512, CompactMinBytes: 1, CompactRatio: 0.01}))
}

func TestWALStoreConformanceWithTinySegmentsAndCompaction(t *testing.T) {
	// Constant rotation and compaction underneath must not change behaviour.
	storetest.Run(t, walFactory(dlq.WALOptions{SegmentBytes: 512, CompactMinBytes: 1, CompactRatio: 0.01}))
}

func TestWALStoreConformanceWithSecure(t *testing.T) {
	storetest.Run(t, func(t *testing.T, now func() time.Time) dlq.Store {
		s := walFactory(dlq.WALOptions{})(t, now)
		return secured(t, s, dlq.SecureOptions{Encryptor: enc(t, newKey(t, "k")), OrderKeyPepper: []byte("p")})
	})
}

func TestWALStoreConformanceSyncModes(t *testing.T) {
	for name, p := range map[string]dlq.SyncPolicy{"interval": dlq.SyncInterval, "none": dlq.SyncNone} {
		t.Run(name, func(t *testing.T) {
			storetest.Run(t, walFactory(dlq.WALOptions{Sync: p, SyncEvery: time.Millisecond}))
		})
	}
}

// ---- helpers for state comparison ----

type snap struct {
	State     dlq.State
	Attempts  int
	Value     string
	Check     string
	BlockedOn string
	LastError string
	Order     string
}

// drain reads every record without changing what a later reader would see: it
// leases with a far-future clock and releases nothing, so use only at the end of
// a test on a store it then discards.
func drain(t testing.TB, s dlq.Store) map[string]snap {
	t.Helper()
	ctx := context.Background()
	out := map[string]snap{}
	for {
		ls, err := s.Lease(ctx, dlq.LeaseRequest{Max: 100, TTL: time.Hour})
		if err != nil {
			t.Fatalf("Lease: %v", err)
		}
		if len(ls) == 0 {
			return out
		}
		for _, l := range ls {
			r := l.Record
			out[r.ID] = snap{r.State, r.Attempts, string(r.Value), string(r.Checkpoint), r.BlockedOn, r.LastError, r.OrderKey}
		}
	}
}

func appendN(t testing.TB, s dlq.Store, n int, prefix string) {
	t.Helper()
	for i := 0; i < n; i++ {
		err := s.Append(context.Background(), dlq.Record{
			ID: fmt.Sprintf("%s%03d", prefix, i), Value: []byte(fmt.Sprintf("value-%s%03d", prefix, i)), BlockedOn: "dep",
		})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
}

func segmentFiles(t testing.TB, dir string) []string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	sort.Strings(m)
	return m
}

func copyDir(t testing.TB, src, dst string) {
	t.Helper()
	if err := os.MkdirAll(dst, 0o700); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() == "LOCK" || e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// ---- durability across restart ----

func TestWALPersistsEveryKindOfChange(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	clock := storetest.NewClock()
	s := openWAL(t, dir, dlq.WALOptions{Clock: clock.Now})

	add := func(id string, mut func(*dlq.Record)) {
		r := dlq.Record{ID: id, Value: []byte("v-" + id), BlockedOn: "payments"}
		if mut != nil {
			mut(&r)
		}
		if err := s.Append(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	add("acked", nil)
	add("nacked", nil)
	add("parked", nil)
	add("released", nil)
	add("checkpointed", func(r *dlq.Record) { r.Checkpoint = []byte("cp0") })
	add("untouched", func(r *dlq.Record) { r.OrderKey = "ok" })
	add("born-parked", func(r *dlq.Record) { r.State = dlq.Parked; r.LastError = "bad" })
	add("discarded", func(r *dlq.Record) { r.State = dlq.Parked })
	add("tomb", func(r *dlq.Record) { r.Value = nil })

	lease := func(id string) dlq.Lease {
		ls, err := s.Lease(ctx, dlq.LeaseRequest{Max: 20, TTL: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		var found *dlq.Lease
		for i := range ls {
			if ls[i].Record.ID == id {
				found = &ls[i]
				continue
			}
			must(t, s.Release(ctx, ls[i].Record.ID, ls[i].Token)) // give back what we did not want
		}
		if found == nil {
			t.Fatalf("could not lease %s", id)
		}
		return *found
	}

	l := lease("acked")
	must(t, s.Ack(ctx, "acked", l.Token))
	l = lease("nacked")
	must(t, s.Nack(ctx, "nacked", l.Token, dlq.NackOptions{Delay: time.Minute, Err: "upstream 503", BlockedOn: "inventory"}))
	l = lease("parked")
	must(t, s.Park(ctx, "parked", l.Token, "poison"))
	l = lease("released")
	must(t, s.Release(ctx, "released", l.Token))
	l = lease("checkpointed")
	must(t, s.Checkpoint(ctx, "checkpointed", l.Token, []byte("cp-step3")))
	must(t, s.Nack(ctx, "checkpointed", l.Token, dlq.NackOptions{}))
	must(t, s.Discard(ctx, "discarded"))
	must(t, s.Close())

	clock.Advance(2 * time.Minute) // the nack delay has passed
	s = openWAL(t, dir, dlq.WALOptions{Clock: clock.Now})
	defer s.Close()
	got := drain(t, s)

	if _, ok := got["acked"]; ok {
		t.Error("an acked record came back")
	}
	if _, ok := got["discarded"]; ok {
		t.Error("a discarded record came back")
	}
	want := map[string]snap{
		"nacked":       {dlq.Leased, 2, "v-nacked", "", "inventory", "upstream 503", ""},
		"released":     {dlq.Leased, 1, "v-released", "", "payments", "", ""},
		"checkpointed": {dlq.Leased, 2, "v-checkpointed", "cp-step3", "payments", "", ""},
		"untouched":    {dlq.Leased, 1, "v-untouched", "", "payments", "", "ok"},
		"tomb":         {dlq.Leased, 1, "", "", "payments", "", ""},
	}
	// "released" was leased once then released (attempt refunded), and the
	// verification lease above is the one that counts now; likewise the others.
	want["released"] = snap{dlq.Leased, 1, "v-released", "", "payments", "", ""}
	for id, w := range want {
		g, ok := got[id]
		if !ok {
			t.Errorf("record %s lost across restart", id)
			continue
		}
		if g != w {
			t.Errorf("%s:\n got  %+v\n want %+v", id, g, w)
		}
	}
	st, _ := s.Stats(ctx)
	if st.Parked != 2 { // "parked" and "born-parked" stay parked
		t.Errorf("parked = %d, want 2", st.Parked)
	}
	// Requeue works on a record that was parked before the restart.
	must(t, s.Requeue(ctx, "parked"))
}

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// A lease belongs to a worker that dies with the process, but its attempt still
// counts, so a poison pill that crashes its worker is eventually caught.
func TestWALLeasesDieWithTheProcessButAttemptsSurvive(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openWAL(t, dir, dlq.WALOptions{})
	appendN(t, s, 1, "p")

	var oldToken string
	for crash := 1; crash <= 3; crash++ {
		ls, err := s.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Hour})
		must(t, err)
		if len(ls) != 1 || ls[0].Record.Attempts != crash {
			t.Fatalf("crash %d: leased %d records, attempts %v", crash, len(ls), ls)
		}
		oldToken = ls[0].Token
		// "Crash": abandon the store without a clean shutdown.
		crashDir := t.TempDir()
		copyDir(t, dir, crashDir)
		_ = s.Close()
		dir = crashDir
		s = openWAL(t, dir, dlq.WALOptions{})
	}
	defer s.Close()
	if err := s.Ack(ctx, dlq.KafkaID("x", 0, 0), oldToken); err == nil {
		t.Fatal("a token from before the crash was honoured")
	}
	ls, _ := s.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Hour})
	if len(ls) != 1 || ls[0].Record.Attempts != 4 {
		t.Fatalf("after 3 crashes the next lease should be attempt 4, got %+v", ls)
	}
	if err := s.Ack(ctx, "p000", oldToken); !errors.Is(err, dlq.ErrLeaseLost) {
		t.Fatalf("stale token: err = %v, want ErrLeaseLost", err)
	}
}

// Everything acknowledged is on disk the moment the call returns: copying the
// directory without any shutdown (what a crash leaves) loses nothing.
func TestWALAcknowledgedWorkSurvivesACrash(t *testing.T) {
	dir := t.TempDir()
	s := openWAL(t, dir, dlq.WALOptions{})
	defer s.Close()
	appendN(t, s, 50, "r")

	crashDir := t.TempDir()
	copyDir(t, dir, crashDir)
	recovered := openWAL(t, crashDir, dlq.WALOptions{})
	defer recovered.Close()

	got := drain(t, recovered)
	if len(got) != 50 {
		t.Fatalf("recovered %d of 50 acknowledged records", len(got))
	}
	if rep := recovered.Recovery(); rep.Records != 50 || rep.TruncatedBytes != 0 || rep.Entries != 50 {
		t.Fatalf("recovery report = %+v", rep)
	}
}

func TestWALAppendIsIdempotentAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	id := dlq.KafkaID("orders", 1, 99)
	s := openWAL(t, dir, dlq.WALOptions{})
	must(t, s.Append(ctx, dlq.Record{ID: id, Value: []byte("first")}))
	must(t, s.Close())

	s = openWAL(t, dir, dlq.WALOptions{})
	defer s.Close()
	must(t, s.Append(ctx, dlq.Record{ID: id, Value: []byte("redelivered")})) // consumer crashed before commit
	got := drain(t, s)
	if len(got) != 1 || got[id].Value != "first" {
		t.Fatalf("got %+v; a redelivered message must not create a second record", got)
	}
}

func TestWALSeqKeepsIncreasingAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openWAL(t, dir, dlq.WALOptions{})
	appendN(t, s, 3, "a")
	ls, _ := s.Lease(ctx, dlq.LeaseRequest{Max: 10, TTL: time.Hour})
	maxSeq := ls[len(ls)-1].Record.Seq
	for _, l := range ls {
		must(t, s.Ack(ctx, l.Record.ID, l.Token)) // even with nothing left, Seq must not restart
	}
	must(t, s.Compact(ctx))
	must(t, s.Close())

	s = openWAL(t, dir, dlq.WALOptions{})
	defer s.Close()
	appendN(t, s, 1, "b")
	ls, _ = s.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Hour})
	if ls[0].Record.Seq <= maxSeq {
		t.Fatalf("Seq %d after restart, must exceed %d", ls[0].Record.Seq, maxSeq)
	}
}

func TestWALCapacityIsEnforcedAndNeverDropsOnReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openWAL(t, dir, dlq.WALOptions{MaxRecords: 3})
	appendN(t, s, 3, "r")
	if err := s.Append(ctx, dlq.Record{ID: "extra"}); !errors.Is(err, dlq.ErrFull) {
		t.Fatalf("err = %v, want ErrFull", err)
	}
	must(t, s.Close())

	// Reopened under a stricter limit: everything already accepted is kept.
	s = openWAL(t, dir, dlq.WALOptions{MaxRecords: 1})
	defer s.Close()
	if st, _ := s.Stats(ctx); st.Total() != 3 {
		t.Fatalf("total = %d; lowering the limit must not drop stored records", st.Total())
	}
	if err := s.Append(ctx, dlq.Record{ID: "extra"}); !errors.Is(err, dlq.ErrFull) {
		t.Fatalf("err = %v, want ErrFull", err)
	}
}

func TestWALRejectsOversizedEntries(t *testing.T) {
	s := openWAL(t, t.TempDir(), dlq.WALOptions{MaxRecordBytes: 1000})
	defer s.Close()
	err := s.Append(context.Background(), dlq.Record{ID: "big", Value: make([]byte, 5000)})
	if !errors.Is(err, dlq.ErrInvalidRecord) {
		t.Fatalf("err = %v, want ErrInvalidRecord", err)
	}
	if _, err := dlq.OpenWAL(t.TempDir(), dlq.WALOptions{MaxRecordBytes: dlq.DefaultMaxEntryBytes}); err == nil {
		t.Fatal("a record limit above the entry limit must be rejected")
	}
}

// ---- locking and permissions ----

func TestWALDirectoryLock(t *testing.T) {
	dir := t.TempDir()
	a := openWAL(t, dir, dlq.WALOptions{})
	if _, err := dlq.OpenWAL(dir, dlq.WALOptions{}); !errors.Is(err, dlq.ErrLocked) {
		t.Fatalf("second open: err = %v, want ErrLocked", err)
	}
	must(t, a.Close())
	must(t, a.Close()) // idempotent
	b := openWAL(t, dir, dlq.WALOptions{})
	must(t, b.Close())
}

func TestWALFilesAreOwnerOnly(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue") // created by the store
	s := openWAL(t, dir, dlq.WALOptions{SegmentBytes: 512, DisableAutoCompact: true})
	appendN(t, s, 40, "r")
	must(t, s.Compact(context.Background()))
	appendN(t, s, 5, "s")
	must(t, s.Close())

	if info, _ := os.Stat(dir); info.Mode().Perm() != 0o700 {
		t.Errorf("directory mode = %#o, want 0700", info.Mode().Perm())
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) < 3 {
		t.Fatalf("expected segments, snapshot and lock, found %d files", len(entries))
	}
	for _, e := range entries {
		info, _ := e.Info()
		if info.Mode().Perm()&0o077 != 0 {
			t.Errorf("%s has mode %#o; only the owner may access it", e.Name(), info.Mode().Perm())
		}
	}
}

func TestWALRefusesInsecurePermissions(t *testing.T) {
	dir := t.TempDir()
	s := openWAL(t, dir, dlq.WALOptions{})
	appendN(t, s, 1, "r")
	must(t, s.Close())

	seg := segmentFiles(t, dir)[0]
	must(t, os.Chmod(seg, 0o644))
	if _, err := dlq.OpenWAL(dir, dlq.WALOptions{}); !errors.Is(err, dlq.ErrInsecurePermissions) {
		t.Fatalf("world-readable segment: err = %v", err)
	}
	if s, err := dlq.OpenWAL(dir, dlq.WALOptions{AllowInsecurePermissions: true}); err != nil {
		t.Fatalf("override rejected: %v", err)
	} else {
		must(t, s.Close())
	}
	must(t, os.Chmod(seg, 0o600))

	must(t, os.Chmod(dir, 0o777))
	if _, err := dlq.OpenWAL(dir, dlq.WALOptions{}); !errors.Is(err, dlq.ErrInsecurePermissions) {
		t.Fatalf("world-writable directory: err = %v", err)
	}
	must(t, os.Chmod(dir, 0o700))
}

// ---- rotation and compaction ----

func TestWALRotationReplaysAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	s := openWAL(t, dir, dlq.WALOptions{SegmentBytes: 1024, DisableAutoCompact: true})
	appendN(t, s, 200, "r")
	must(t, s.Close())

	if n := len(segmentFiles(t, dir)); n < 10 {
		t.Fatalf("only %d segments; rotation is not happening", n)
	}
	s = openWAL(t, dir, dlq.WALOptions{DisableAutoCompact: true})
	defer s.Close()
	if got := drain(t, s); len(got) != 200 {
		t.Fatalf("recovered %d of 200", len(got))
	}
	if s.Recovery().Segments < 10 {
		t.Fatalf("report = %+v", s.Recovery())
	}
}

func TestWALCompactionBoundsTheLogAndPreservesState(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openWAL(t, dir, dlq.WALOptions{SegmentBytes: 2048, DisableAutoCompact: true})

	// Churn: many records come and go, a few stay.
	for i := 0; i < 300; i++ {
		id := fmt.Sprintf("churn%03d", i)
		must(t, s.Append(ctx, dlq.Record{ID: id, Value: []byte("x")}))
		ls, _ := s.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Hour, BlockedOn: ""})
		must(t, s.Ack(ctx, ls[0].Record.ID, ls[0].Token))
	}
	appendN(t, s, 5, "keep")
	before := len(segmentFiles(t, dir))
	must(t, s.Compact(ctx))
	after := len(segmentFiles(t, dir))
	if before < 10 || after != 1 {
		t.Fatalf("segments %d -> %d; compaction should leave only the active one", before, after)
	}
	if st := s.WALStats(); st.SnapshotLSN == 0 || st.LogBytes != 0 {
		t.Fatalf("wal stats = %+v", st)
	}
	snaps, _ := filepath.Glob(filepath.Join(dir, "snapshot-*.snap"))
	if len(snaps) != 1 {
		t.Fatalf("snapshots = %v", snaps)
	}

	// Work continues on top of the snapshot, and a second compaction replaces it.
	appendN(t, s, 5, "later")
	must(t, s.Compact(ctx))
	appendN(t, s, 5, "last")
	must(t, s.Close())
	if snaps, _ = filepath.Glob(filepath.Join(dir, "snapshot-*.snap")); len(snaps) != 1 {
		t.Fatalf("old snapshot not removed: %v", snaps)
	}

	s = openWAL(t, dir, dlq.WALOptions{})
	defer s.Close()
	got := drain(t, s)
	if len(got) != 15 {
		t.Fatalf("recovered %d records, want 15 (5 keep + 5 later + 5 last)", len(got))
	}
	for _, p := range []string{"keep", "later", "last"} {
		for i := 0; i < 5; i++ {
			if _, ok := got[fmt.Sprintf("%s%03d", p, i)]; !ok {
				t.Errorf("record %s%03d lost through compaction", p, i)
			}
		}
	}
	if rep := s.Recovery(); rep.SnapshotLSN == 0 {
		t.Fatalf("recovery did not use the snapshot: %+v", rep)
	}
}

func TestWALAutomaticCompactionKeepsTheLogSmall(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openWAL(t, dir, dlq.WALOptions{SegmentBytes: 4096, CompactMinBytes: 16 << 10, CompactRatio: 1})
	defer s.Close()

	for i := 0; i < 2000; i++ {
		must(t, s.Append(ctx, dlq.Record{ID: fmt.Sprintf("r%04d", i), Value: make([]byte, 100)}))
		ls, _ := s.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Hour})
		must(t, s.Ack(ctx, ls[0].Record.ID, ls[0].Token))
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && s.WALStats().SnapshotLSN == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	st := s.WALStats()
	if st.SnapshotLSN == 0 || st.LastCompactionErr != nil {
		t.Fatalf("automatic compaction did not run: %+v", st)
	}
	var total int64
	for _, f := range segmentFiles(t, dir) {
		info, _ := os.Stat(f)
		total += info.Size()
	}
	if total > 200<<10 { // 2000 append+lease+ack cycles would be ~700 KiB uncompacted
		t.Fatalf("log is %d bytes after compaction; it is not being bounded", total)
	}
}

// ---- concurrency ----

func TestWALConcurrentWorkersAndRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openWAL(t, dir, dlq.WALOptions{SegmentBytes: 8192, CompactMinBytes: 32 << 10, CompactRatio: 1})

	const producers, perProducer = 8, 60
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				err := s.Append(ctx, dlq.Record{ID: fmt.Sprintf("p%d-%03d", p, i), Value: []byte("v")})
				if err != nil {
					t.Errorf("Append: %v", err)
				}
			}
		}(p)
	}
	var acked sync.Map
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 400; i++ {
				ls, err := s.Lease(ctx, dlq.LeaseRequest{Max: 5, TTL: time.Hour})
				if err != nil {
					t.Errorf("Lease: %v", err)
					return
				}
				for _, l := range ls {
					if l.Record.ID[len(l.Record.ID)-1]%2 == 0 {
						if err := s.Ack(ctx, l.Record.ID, l.Token); err != nil {
							t.Errorf("Ack: %v", err)
						}
						acked.Store(l.Record.ID, true)
					} else if err := s.Release(ctx, l.Record.ID, l.Token); err != nil {
						t.Errorf("Release: %v", err)
					}
				}
			}
		}()
	}
	wg.Wait()
	must(t, s.Close())

	s = openWAL(t, dir, dlq.WALOptions{})
	defer s.Close()
	got := drain(t, s)
	for p := 0; p < producers; p++ {
		for i := 0; i < perProducer; i++ {
			id := fmt.Sprintf("p%d-%03d", p, i)
			_, isAcked := acked.Load(id)
			_, present := got[id]
			if isAcked && present {
				t.Errorf("%s was acked but reappeared after restart", id)
			}
			if !isAcked && !present {
				t.Errorf("%s was never acked yet is gone: data loss", id)
			}
		}
	}
	_ = strings.TrimSpace
}

// A requeue is a logged change like any other and must replay.
func TestWALReplaysRequeueAndDiscard(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openWAL(t, dir, dlq.WALOptions{})
	appendN(t, s, 2, "r")
	ls, _ := s.Lease(ctx, dlq.LeaseRequest{Max: 2, TTL: time.Hour})
	must(t, s.Park(ctx, "r000", ls[0].Token, "fixable"))
	must(t, s.Park(ctx, "r001", ls[1].Token, "hopeless"))
	must(t, s.Requeue(ctx, "r000"))
	must(t, s.Discard(ctx, "r001"))
	must(t, s.Close())

	s = openWAL(t, dir, dlq.WALOptions{})
	defer s.Close()
	st, _ := s.Stats(ctx)
	if st.Pending != 1 || st.Parked != 0 || st.Total() != 1 {
		t.Fatalf("stats = %+v, want r000 pending and r001 gone", st)
	}
	got := drain(t, s)
	if g := got["r000"]; g.Attempts != 1 || g.LastError != "fixable" {
		t.Fatalf("r000 = %+v: a requeue resets attempts and keeps the last error", g)
	}
}

func TestWALCompactEdgeCases(t *testing.T) {
	ctx := context.Background()
	s := openWAL(t, t.TempDir(), dlq.WALOptions{DisableAutoCompact: true})
	must(t, s.Compact(ctx)) // nothing has happened yet: a no-op

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.Compact(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	appendN(t, s, 2, "r")
	must(t, s.Compact(ctx))
	must(t, s.Compact(ctx)) // and again: nothing new since the snapshot
	must(t, s.Close())
	if err := s.Compact(ctx); !errors.Is(err, dlq.ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestWALOpenRejectsAFileAsDirectory(t *testing.T) {
	f := filepath.Join(t.TempDir(), "afile")
	must(t, os.WriteFile(f, []byte("x"), 0o600))
	if _, err := dlq.OpenWAL(f, dlq.WALOptions{}); err == nil {
		t.Fatal("expected an error")
	}
}

// Replacing the checkpoint is part of the requeue's single log entry: after a
// restart, or a crash, the record is either still parked with the old checkpoint
// or pending with the new one, never pending with the old.
func TestWALRequeueWithSurvivesRestartAndCrash(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openWAL(t, dir, dlq.WALOptions{})
	must(t, s.Append(ctx, dlq.Record{ID: "r", State: dlq.Parked, Checkpoint: []byte("old"), LastError: "why"}))
	must(t, s.RequeueWith(ctx, "r", dlq.RequeueOptions{ReplaceCheckpoint: true, Checkpoint: []byte("new")}))

	crashDir := t.TempDir()
	copyDir(t, dir, crashDir) // what a crash right now would leave
	must(t, s.Close())

	for name, d := range map[string]string{"clean restart": dir, "crash": crashDir} {
		s := openWAL(t, d, dlq.WALOptions{})
		got, err := s.Get(ctx, "r")
		must(t, err)
		if got.State != dlq.Pending || string(got.Checkpoint) != "new" || got.LastError != "why" {
			t.Fatalf("%s: record = %+v", name, got)
		}
		must(t, s.Close())
	}

	// Clearing the checkpoint also persists, and a plain Requeue leaves it alone.
	s = openWAL(t, dir, dlq.WALOptions{})
	must(t, s.Append(ctx, dlq.Record{ID: "c", State: dlq.Parked, Checkpoint: []byte("x")}))
	must(t, s.Append(ctx, dlq.Record{ID: "k", State: dlq.Parked, Checkpoint: []byte("keep")}))
	must(t, s.RequeueWith(ctx, "c", dlq.RequeueOptions{ReplaceCheckpoint: true}))
	must(t, s.Requeue(ctx, "k"))
	must(t, s.Close())
	s = openWAL(t, dir, dlq.WALOptions{})
	defer s.Close()
	if c, _ := s.Get(ctx, "c"); len(c.Checkpoint) != 0 {
		t.Fatalf("cleared checkpoint came back: %q", c.Checkpoint)
	}
	if k, _ := s.Get(ctx, "k"); string(k.Checkpoint) != "keep" {
		t.Fatalf("checkpoint = %q", k.Checkpoint)
	}
}

func TestWALParkedListingAfterRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openWAL(t, dir, dlq.WALOptions{})
	for i := 0; i < 3; i++ {
		must(t, s.Append(ctx, dlq.Record{ID: fmt.Sprintf("p%d", i), State: dlq.Parked, LastError: "why"}))
	}
	must(t, s.Close())
	s = openWAL(t, dir, dlq.WALOptions{})
	defer s.Close()
	got, err := s.Parked(ctx, dlq.ParkedQuery{})
	if err != nil || len(got) != 3 || got[0].ID != "p0" || got[2].LastError != "why" {
		t.Fatalf("Parked = %+v, %v", got, err)
	}
}
