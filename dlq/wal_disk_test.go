package dlq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// fakeDisk is a volume whose free space a test controls.
type fakeDisk struct {
	free atomic.Uint64
	err  atomic.Bool
}

func (d *fakeDisk) fn(string) (uint64, error) {
	if d.err.Load() {
		return 0, errors.New("statfs failed")
	}
	return d.free.Load(), nil
}

func openDiskStore(t testing.TB, d *fakeDisk, min int64) *WALStore {
	t.Helper()
	return openInternal(t, t.TempDir(), WALOptions{MinFreeBytes: min, freeFn: d.fn, Sync: SyncNone, DisableAutoCompact: true})
}

// waitCheck lets the store's cached measurement expire.
func waitCheck() { time.Sleep(diskCheckEvery + 20*time.Millisecond) }

func TestALowDiskRefusesGrowthButStillAcceptsWorkThatFreesSpace(t *testing.T) {
	ctx := context.Background()
	d := &fakeDisk{}
	d.free.Store(10 << 20)
	s := openDiskStore(t, d, 1<<20)
	defer func() { _ = s.Close() }()

	mustNil(t, s.Append(ctx, Record{ID: "a", Value: []byte("a")}))
	ls, err := s.Lease(ctx, LeaseRequest{Max: 1, TTL: time.Hour})
	mustNil(t, err)

	d.free.Store(512 << 10) // under the reserve
	waitCheck()
	err = s.Append(ctx, Record{ID: "b", Value: []byte("b")})
	if !errors.Is(err, ErrFull) || !errors.Is(err, ErrDiskLow) {
		t.Fatalf("Append on a nearly full disk = %v, want ErrFull and ErrDiskLow", err)
	}
	if _, gerr := s.Get(ctx, "b"); !errors.Is(gerr, ErrNotFound) {
		t.Fatalf("a refused record must not be stored: %v", gerr)
	}
	if err := s.Checkpoint(ctx, "a", ls[0].Token, []byte("progress")); !errors.Is(err, ErrDiskLow) {
		t.Fatalf("Checkpoint = %v, want ErrDiskLow", err)
	}
	if st := s.WALStats(); !st.DiskLow || st.DiskFreeBytes != 512<<10 {
		t.Fatalf("stats %+v: want DiskLow with 512 KiB free", st)
	}
	// Finishing work frees space and must never be refused.
	mustNil(t, s.Ack(ctx, "a", ls[0].Token))

	d.free.Store(10 << 20) // an operator made room
	waitCheck()
	mustNil(t, s.Append(ctx, Record{ID: "b", Value: []byte("b")}))
	if st := s.WALStats(); st.DiskLow {
		t.Fatalf("stats %+v: DiskLow after room was made", st)
	}
}

// A fast writer must not be able to outrun a stale free-space reading.
func TestBytesWrittenSinceTheLastMeasurementAreCounted(t *testing.T) {
	ctx := context.Background()
	d := &fakeDisk{}
	d.free.Store(1<<20 + 100<<10) // 100 KiB above the reserve
	s := openDiskStore(t, d, 1<<20)
	defer func() { _ = s.Close() }()
	big := bytes.Repeat([]byte("x"), 40<<10)
	var refused int
	for i := 0; i < 10; i++ { // all within one measurement interval
		if err := s.Append(ctx, Record{ID: fmt.Sprintf("r%d", i), Value: big}); errors.Is(err, ErrDiskLow) {
			refused++
		}
	}
	if refused == 0 || refused == 10 {
		t.Fatalf("%d of 10 appends refused: the first few should fit in 100 KiB and the rest not", refused)
	}
}

func TestCompactionWaitsForRoomForItsSnapshot(t *testing.T) {
	ctx := context.Background()
	for _, mem := range []bool{false, true} {
		t.Run(fmt.Sprintf("payloadsInMemory=%v", mem), func(t *testing.T) {
			d := &fakeDisk{}
			d.free.Store(100 << 20)
			s := openInternal(t, t.TempDir(), WALOptions{MinFreeBytes: 1 << 20, freeFn: d.fn, Sync: SyncNone, DisableAutoCompact: true, PayloadsInMemory: mem})
			defer func() { _ = s.Close() }()
			big := bytes.Repeat([]byte("x"), 100<<10)
			for i := 0; i < 50; i++ { // about 5 MiB live
				mustNil(t, s.Append(ctx, Record{ID: fmt.Sprintf("r%02d", i), Value: big}))
			}
			d.free.Store(3 << 20) // above the reserve, below reserve plus a snapshot
			waitCheck()
			if err := s.Compact(ctx); !errors.Is(err, ErrDiskLow) {
				t.Fatalf("Compact = %v, want ErrDiskLow", err)
			}
			if st := s.WALStats(); st.SnapshotLSN != 0 || !errors.Is(st.LastCompactionErr, ErrDiskLow) {
				t.Fatalf("stats %+v: a refused compaction must change nothing and say why", st)
			}
			d.free.Store(100 << 20)
			waitCheck()
			mustNil(t, s.Compact(ctx))
			if st := s.WALStats(); st.SnapshotLSN == 0 {
				t.Fatal("compaction did not run once there was room")
			}
		})
	}
}

func TestTheDiskGuardCanBeDisabledAndIgnoresAFailedMeasurement(t *testing.T) {
	ctx := context.Background()
	d := &fakeDisk{}
	s := openDiskStore(t, d, -1) // free space is 0, but the check is off
	mustNil(t, s.Append(ctx, Record{ID: "a", Value: []byte("a")}))
	_ = s.Close()

	d2 := &fakeDisk{}
	d2.err.Store(true)
	s = openDiskStore(t, d2, 1<<20)
	defer func() { _ = s.Close() }()
	mustNil(t, s.Append(ctx, Record{ID: "a", Value: []byte("a")})) // cannot tell: do not stop work
}
