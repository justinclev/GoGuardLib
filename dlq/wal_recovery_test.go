package dlq_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
)

func fileSize(t testing.TB, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func lastSegment(t testing.TB, dir string) string {
	t.Helper()
	segs := segmentFiles(t, dir)
	if len(segs) == 0 {
		t.Fatal("no segments")
	}
	return segs[len(segs)-1]
}

func flipByte(t testing.TB, path string, off int64) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[off] ^= 0x55
	must(t, os.WriteFile(path, b, 0o600))
}

// dirDigest fingerprints every file so a test can prove a failed Open changed nothing.
func dirDigest(t testing.TB, dir string) string {
	t.Helper()
	h := sha256.New()
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if e.Name() == "LOCK" {
			continue
		}
		b, _ := os.ReadFile(filepath.Join(dir, e.Name()))
		fmt.Fprintf(h, "%s:%d:", e.Name(), len(b))
		h.Write(b)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func quarantineFiles(t testing.TB, dir string) []string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(dir, "*.quarantine-*"))
	return m
}

// goldenLog builds a single-segment log of n records and returns its directory,
// plus the size the segment had with n-1 records (the start of the last frame).
func goldenLog(t testing.TB, n int) (dir string, lastFrameStart int64) {
	t.Helper()
	dir = t.TempDir()
	s := openWAL(t, dir, dlq.WALOptions{DisableAutoCompact: true})
	appendN(t, s, n-1, "r")
	must(t, s.Close())
	lastFrameStart = fileSize(t, lastSegment(t, dir))

	s = openWAL(t, dir, dlq.WALOptions{DisableAutoCompact: true})
	must(t, s.Append(context.Background(), dlq.Record{ID: fmt.Sprintf("r%03d", n-1), Value: []byte(fmt.Sprintf("value-r%03d", n-1))}))
	must(t, s.Close())
	return dir, lastFrameStart
}

// A crash can cut the log anywhere inside the entry being written. Whatever the
// cut, recovery must return exactly the entries that were complete, save the
// discarded bytes, and leave a store that works and reopens cleanly.
func TestWALRecoversFromATornTailAtEveryByte(t *testing.T) {
	const n = 10
	golden, start := goldenLog(t, n)
	end := fileSize(t, lastSegment(t, golden))
	if end-start < 20 {
		t.Fatalf("last frame only %d bytes", end-start)
	}

	for cut := start; cut <= end; cut++ {
		dir := t.TempDir()
		copyDir(t, golden, dir)
		must(t, os.Truncate(lastSegment(t, dir), cut))

		s, err := dlq.OpenWAL(dir, dlq.WALOptions{DisableAutoCompact: true})
		if err != nil {
			t.Fatalf("cut at %d of %d: Open failed: %v", cut, end, err)
		}
		wantRecords, wantTruncated := n-1, cut-start
		if cut == end {
			wantRecords, wantTruncated = n, 0
		}
		rep := s.Recovery()
		if rep.Records != wantRecords || rep.TruncatedBytes != wantTruncated {
			t.Fatalf("cut at %d: report %+v, want %d records and %d bytes truncated", cut, rep, wantRecords, wantTruncated)
		}
		if q := quarantineFiles(t, dir); wantTruncated > 0 {
			if len(q) != 1 || fileSize(t, q[0]) != wantTruncated || rep.QuarantineFile != q[0] {
				t.Fatalf("cut at %d: quarantine %v, want one file of %d bytes", cut, q, wantTruncated)
			}
		} else if len(q) != 0 {
			t.Fatalf("cut at %d: unexpected quarantine %v", cut, q)
		}

		// The repaired store is fully usable, and what it writes survives a restart.
		must(t, s.Append(context.Background(), dlq.Record{ID: "after-recovery", Value: []byte("ok")}))
		must(t, s.Close())
		s = openWAL(t, dir, dlq.WALOptions{DisableAutoCompact: true})
		if rep := s.Recovery(); rep.TruncatedBytes != 0 || rep.Records != wantRecords+1 {
			t.Fatalf("cut at %d: second open %+v, want a clean log with %d records", cut, rep, wantRecords+1)
		}
		must(t, s.Close())
	}
}

func TestWALRecoversFromTornTailAcrossSeveralFrames(t *testing.T) {
	golden, _ := goldenLog(t, 10)
	full := fileSize(t, lastSegment(t, golden))
	for cut := int64(40); cut < full; cut += 7 { // through the header and into every entry
		dir := t.TempDir()
		copyDir(t, golden, dir)
		must(t, os.Truncate(lastSegment(t, dir), cut))
		s, err := dlq.OpenWAL(dir, dlq.WALOptions{DisableAutoCompact: true})
		if err != nil {
			t.Fatalf("cut at %d: %v", cut, err)
		}
		got := drain(t, s)
		for id := range got { // whatever survives must be an intact prefix
			var i int
			fmt.Sscanf(id, "r%03d", &i)
			if got[id].Value != fmt.Sprintf("value-r%03d", i) || i >= 10 {
				t.Fatalf("cut at %d: record %s came back damaged: %+v", cut, id, got[id])
			}
		}
		for i := 0; i < len(got); i++ {
			if _, ok := got[fmt.Sprintf("r%03d", i)]; !ok {
				t.Fatalf("cut at %d: recovered %d records but r%03d is missing (not a prefix)", cut, len(got), i)
			}
		}
		must(t, s.Close())
	}
}

func TestWALRecoversFromZeroFilledTail(t *testing.T) {
	golden, _ := goldenLog(t, 5)
	seg := lastSegment(t, golden)
	f, err := os.OpenFile(seg, os.O_APPEND|os.O_WRONLY, 0o600)
	must(t, err)
	_, err = f.Write(make([]byte, 4096)) // a filesystem that extended the file before the data landed
	must(t, err)
	must(t, f.Close())

	s := openWAL(t, golden, dlq.WALOptions{})
	defer s.Close()
	if rep := s.Recovery(); rep.Records != 5 || rep.TruncatedBytes != 4096 {
		t.Fatalf("report = %+v", rep)
	}
}

func TestWALTailDamageIsQuarantinedNotDestroyed(t *testing.T) {
	golden, start := goldenLog(t, 6)
	seg := lastSegment(t, golden)
	flipByte(t, seg, start+12) // inside the last entry

	// Strict mode refuses and touches nothing.
	before := dirDigest(t, golden)
	if _, err := dlq.OpenWAL(golden, dlq.WALOptions{FailOnTruncation: true}); !errors.Is(err, dlq.ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
	if dirDigest(t, golden) != before {
		t.Fatal("a refused Open modified the directory")
	}

	damaged, _ := os.ReadFile(seg)
	s := openWAL(t, golden, dlq.WALOptions{})
	defer s.Close()
	rep := s.Recovery()
	if rep.Records != 5 || rep.TruncatedBytes != int64(len(damaged))-start {
		t.Fatalf("report = %+v", rep)
	}
	saved, _ := os.ReadFile(rep.QuarantineFile)
	if string(saved) != string(damaged[start:]) {
		t.Fatal("the cut bytes were not preserved exactly in the quarantine file")
	}
}

// Damage with valid entries after it is bit rot, not an interrupted write.
// Truncating would silently discard acknowledged records, so Open must refuse.
func TestWALMidLogDamageIsCorruptionNotATornTail(t *testing.T) {
	dir, _ := goldenLog(t, 10)
	seg := lastSegment(t, dir)
	flipByte(t, seg, 32+70) // inside an early entry, far from the tail

	before := dirDigest(t, dir)
	_, err := dlq.OpenWAL(dir, dlq.WALOptions{})
	if !errors.Is(err, dlq.ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
	if dirDigest(t, dir) != before {
		t.Fatal("a refused Open modified the directory")
	}
	if len(quarantineFiles(t, dir)) != 0 {
		t.Fatal("nothing should be quarantined when Open refuses")
	}

	// The lock was released, and an operator can override deliberately.
	s := openWAL(t, dir, dlq.WALOptions{ForceRepair: true})
	defer s.Close()
	if rep := s.Recovery(); rep.TruncatedBytes == 0 || rep.QuarantineFile == "" {
		t.Fatalf("ForceRepair should have cut and quarantined: %+v", rep)
	}
	if fileSize(t, mustOne(t, quarantineFiles(t, dir))) != s.Recovery().TruncatedBytes {
		t.Fatal("quarantine does not hold everything that was cut")
	}
}

func mustOne(t testing.TB, xs []string) string {
	t.Helper()
	if len(xs) != 1 {
		t.Fatalf("want exactly one file, got %v", xs)
	}
	return xs[0]
}

func multiSegmentLog(t testing.TB) string {
	t.Helper()
	dir := t.TempDir()
	s := openWAL(t, dir, dlq.WALOptions{SegmentBytes: 600, DisableAutoCompact: true})
	appendN(t, s, 60, "r")
	must(t, s.Close())
	if n := len(segmentFiles(t, dir)); n < 5 {
		t.Fatalf("only %d segments", n)
	}
	return dir
}

func TestWALDamageInAnEarlierSegmentIsFatal(t *testing.T) {
	for name, damage := range map[string]func(t *testing.T, seg string){
		"bit flip":       func(t *testing.T, seg string) { flipByte(t, seg, 50) },
		"truncated":      func(t *testing.T, seg string) { must(t, os.Truncate(seg, fileSize(t, seg)-5)) },
		"header damaged": func(t *testing.T, seg string) { flipByte(t, seg, 3) },
		"emptied":        func(t *testing.T, seg string) { must(t, os.Truncate(seg, 0)) },
	} {
		t.Run(name, func(t *testing.T) {
			dir := multiSegmentLog(t)
			damage(t, segmentFiles(t, dir)[1])
			before := dirDigest(t, dir)
			if _, err := dlq.OpenWAL(dir, dlq.WALOptions{ForceRepair: true}); !errors.Is(err, dlq.ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt even with ForceRepair (only the tail may be cut)", err)
			}
			if dirDigest(t, dir) != before {
				t.Fatal("a refused Open modified the directory")
			}
		})
	}
}

func TestWALMissingSegmentIsDetected(t *testing.T) {
	for name, idx := range map[string]int{"first": 0, "middle": 2} {
		t.Run(name, func(t *testing.T) {
			dir := multiSegmentLog(t)
			must(t, os.Remove(segmentFiles(t, dir)[idx]))
			if _, err := dlq.OpenWAL(dir, dlq.WALOptions{}); !errors.Is(err, dlq.ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt (a gap means acknowledged entries are gone)", err)
			}
		})
	}
}

func TestWALInterruptedSegmentCreationIsRepaired(t *testing.T) {
	for name, junk := range map[string][]byte{"empty file": {}, "partial header": []byte("GGUARD\x00\x01\x02")} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			s := openWAL(t, dir, dlq.WALOptions{})
			appendN(t, s, 5, "r")
			must(t, s.Close())
			// A crash while rotating leaves a new, unfinished segment file.
			next := filepath.Join(dir, fmt.Sprintf("wal-%020d.log", 6))
			must(t, os.WriteFile(next, junk, 0o600))

			s = openWAL(t, dir, dlq.WALOptions{})
			defer s.Close()
			if got := drain(t, s); len(got) != 5 {
				t.Fatalf("recovered %d of 5", len(got))
			}
			must(t, s.Append(context.Background(), dlq.Record{ID: "more"}))
		})
	}
}

// ---- snapshots ----

func compactedLog(t testing.TB) string {
	t.Helper()
	dir := t.TempDir()
	s := openWAL(t, dir, dlq.WALOptions{SegmentBytes: 800, DisableAutoCompact: true})
	appendN(t, s, 30, "r")
	must(t, s.Compact(context.Background()))
	appendN(t, s, 5, "s")
	must(t, s.Close())
	return dir
}

func TestWALDamagedSnapshotIsFatal(t *testing.T) {
	for name, damage := range map[string]func(t *testing.T, p string){
		"bit flip":  func(t *testing.T, p string) { flipByte(t, p, 100) },
		"truncated": func(t *testing.T, p string) { must(t, os.Truncate(p, fileSize(t, p)-10)) },
		"no footer": func(t *testing.T, p string) { must(t, os.Truncate(p, 200)) },
		"header":    func(t *testing.T, p string) { flipByte(t, p, 2) },
	} {
		t.Run(name, func(t *testing.T) {
			dir := compactedLog(t)
			snaps, _ := filepath.Glob(filepath.Join(dir, "snapshot-*.snap"))
			damage(t, mustOne(t, snaps))
			before := dirDigest(t, dir)
			if _, err := dlq.OpenWAL(dir, dlq.WALOptions{ForceRepair: true}); !errors.Is(err, dlq.ErrCorrupt) {
				t.Fatalf("err = %v, want ErrCorrupt", err)
			}
			if dirDigest(t, dir) != before {
				t.Fatal("a refused Open modified the directory")
			}
		})
	}
}

func TestWALGapAfterSnapshotIsDetected(t *testing.T) {
	dir := compactedLog(t)
	// Entries after the snapshot live in the segments that remain; losing the
	// first of them leaves a hole between the snapshot and the log.
	segs := segmentFiles(t, dir)
	if len(segs) < 2 {
		t.Skip("all post-snapshot entries fit in one segment")
	}
	must(t, os.Remove(segs[0]))
	if _, err := dlq.OpenWAL(dir, dlq.WALOptions{}); !errors.Is(err, dlq.ErrCorrupt) {
		t.Fatalf("err = %v, want ErrCorrupt", err)
	}
}

// A crash after the snapshot is durable but before old segments are deleted
// leaves redundant files. Recovery must accept them, use the snapshot, and clean up.
func TestWALLeftoverFilesFromInterruptedCompactionAreCleaned(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s := openWAL(t, dir, dlq.WALOptions{SegmentBytes: 800, DisableAutoCompact: true})
	appendN(t, s, 30, "r")
	old := map[string][]byte{}
	for _, f := range segmentFiles(t, dir) {
		b, _ := os.ReadFile(f)
		old[f] = b
	}
	must(t, s.Compact(ctx))
	appendN(t, s, 3, "s")
	must(t, s.Close())

	// Put the deleted segments back, as if the process died before removing them,
	// and add an unfinished snapshot too.
	for f, b := range old {
		must(t, os.WriteFile(f, b, 0o600))
	}
	tmp := filepath.Join(dir, fmt.Sprintf("snapshot-%020d.snap.tmp", 99))
	must(t, os.WriteFile(tmp, []byte("half a snapshot"), 0o600))

	s = openWAL(t, dir, dlq.WALOptions{})
	defer s.Close()
	if got := drain(t, s); len(got) != 33 {
		t.Fatalf("recovered %d records, want 33", len(got))
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatal("unfinished snapshot was not removed")
	}
	for f := range old {
		if _, err := os.Stat(f); err == nil && f != segmentFiles(t, dir)[len(segmentFiles(t, dir))-1] {
			t.Fatalf("redundant segment %s not cleaned up", filepath.Base(f))
		}
	}
}

func TestWALRecoveryDurationAndReport(t *testing.T) {
	dir := compactedLog(t)
	s := openWAL(t, dir, dlq.WALOptions{})
	defer s.Close()
	rep := s.Recovery()
	if rep.SnapshotLSN == 0 || rep.Records != 35 || rep.Entries != 5 || rep.Duration <= 0 || rep.Duration > time.Minute {
		t.Fatalf("report = %+v", rep)
	}
}
