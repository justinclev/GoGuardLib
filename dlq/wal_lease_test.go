package dlq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testClock is a hand-driven clock.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func newTestClock() *testClock { return &testClock{t: time.Unix(1_700_000_000, 0)} }
func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *testClock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// hookBox lets a test choose, after the store is open, what the lease hook does.
type hookBox struct{ f atomic.Pointer[func(string)] }

func (h *hookBox) set(f func(string)) {
	if f == nil {
		h.f.Store(nil)
		return
	}
	h.f.Store(&f)
}
func (h *hookBox) call(stage string) {
	if f := h.f.Load(); f != nil {
		(*f)(stage)
	}
}

func openLeaseStore(t testing.TB, dir string, box *hookBox, clk *testClock, o WALOptions) *WALStore {
	t.Helper()
	o.DisableAutoCompact = true
	if o.Sync == SyncAlways {
		o.Sync = SyncNone // these tests are about locking, not fsync
	}
	o.leaseHook = box.call
	if clk != nil {
		o.Clock = clk.Now
	}
	return openInternal(t, dir, o)
}

func appendN(t testing.TB, s *WALStore, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		mustNil(t, s.Append(context.Background(), Record{ID: fmt.Sprintf("r%03d", i), Value: payload(i)}))
	}
}

func noReservations(t testing.TB, s *WALStore) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, it := range s.m.items {
		if it.reserved {
			t.Errorf("%s is still reserved after the lease finished", id)
		}
	}
}

func within(t *testing.T, what string, d time.Duration, f func()) {
	t.Helper()
	c := make(chan struct{})
	go func() { f(); close(c) }()
	select {
	case <-c:
	case <-time.After(d):
		t.Fatalf("%s did not finish within %v", what, d)
	}
}

func idsOfLeases(ls []Lease) []string {
	out := make([]string, len(ls))
	for i, l := range ls {
		out[i] = l.Record.ID
	}
	return out
}

// While a lease is reading payloads, everything else must keep moving: that is the
// point of not holding the lock across the read.
func TestOtherOperationsProceedWhileALeaseReads(t *testing.T) {
	ctx := context.Background()
	box := &hookBox{}
	s := openLeaseStore(t, t.TempDir(), box, nil, WALOptions{})
	defer func() { _ = s.Close() }()
	appendN(t, s, 4)

	reading, proceed := make(chan struct{}), make(chan struct{})
	box.set(func(stage string) {
		if stage == "selected" {
			close(reading)
			<-proceed
		}
	})
	type result struct {
		ls  []Lease
		err error
	}
	done := make(chan result, 1)
	go func() {
		ls, err := s.Lease(ctx, LeaseRequest{Max: 2, TTL: time.Hour})
		done <- result{ls, err}
	}()
	<-reading

	within(t, "operations during a lease's read", 2*time.Second, func() {
		mustNil(t, s.Append(ctx, Record{ID: "new", Value: []byte("n")}))
		if g, err := s.Get(ctx, "r003"); err != nil || !bytes_equal(g.Value, payload(3)) {
			t.Errorf("Get during the read: %v %q", err, g.Value)
		}
		if _, err := s.Stats(ctx); err != nil {
			t.Error(err)
		}
		// Records the lease did not pick can be leased by someone else meanwhile.
		box.set(nil)
		other, err := s.Lease(ctx, LeaseRequest{Max: 10, TTL: time.Hour})
		if err != nil {
			t.Error(err)
		}
		for _, l := range other {
			if l.Record.ID == "r000" || l.Record.ID == "r001" {
				t.Errorf("%s was handed to a second lease while the first was reading it", l.Record.ID)
			}
		}
		if len(other) != 3 { // r002, r003 and "new"
			t.Errorf("second lease got %v, want the three records the first did not pick", idsOfLeases(other))
		}
	})
	close(proceed)
	r := <-done
	mustNil(t, r.err)
	if got := idsOfLeases(r.ls); fmt.Sprint(got) != "[r000 r001]" {
		t.Fatalf("first lease got %v, want [r000 r001]", got)
	}
	for i, l := range r.ls {
		if !bytes_equal(l.Record.Value, payload(i)) {
			t.Fatalf("%s has payload %q", l.Record.ID, l.Record.Value)
		}
	}
	noReservations(t, s)
}

func bytes_equal(a, b []byte) bool { return string(a) == string(b) }

func TestConcurrentLeasersNeverShareARecord(t *testing.T) {
	ctx := context.Background()
	box := &hookBox{}
	s := openLeaseStore(t, t.TempDir(), box, nil, WALOptions{})
	defer func() { _ = s.Close() }()
	const n = 300
	appendN(t, s, n)
	box.set(func(string) { time.Sleep(200 * time.Microsecond) }) // widen the window between the steps

	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 12; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				ls, err := s.Lease(ctx, LeaseRequest{Max: 3, TTL: time.Hour})
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				for _, l := range ls {
					seen[l.Record.ID]++
				}
				total := len(seen)
				mu.Unlock()
				if len(ls) == 0 && total == n {
					return
				}
				if len(ls) == 0 {
					time.Sleep(time.Millisecond) // others are holding the rest
				}
			}
		}()
	}
	within(t, "the leasers", 30*time.Second, wg.Wait)
	for id, c := range seen {
		if c != 1 {
			t.Errorf("%s was leased %d times", id, c)
		}
	}
	if len(seen) != n {
		t.Fatalf("leased %d records, want %d", len(seen), n)
	}
	noReservations(t, s)
}

// An old worker finishing an expired lease while a new lease is reading must not
// let the new lease hand out what it read before the change.
func TestAChangeDuringTheReadIsNeverHandedOutStale(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		change func(t *testing.T, s *WALStore, id, oldToken string)
		want   func(t *testing.T, s *WALStore, second []Lease)
	}{
		{
			name: "checkpoint",
			change: func(t *testing.T, s *WALStore, id, tok string) {
				mustNil(t, s.Checkpoint(ctx, id, tok, []byte("new-progress")))
			},
			want: func(t *testing.T, s *WALStore, second []Lease) {
				if len(second) != 0 {
					t.Fatalf("handed out %v that changed while it was being read", idsOfLeases(second))
				}
				ls, err := s.Lease(ctx, LeaseRequest{Max: 1, TTL: time.Hour})
				mustNil(t, err)
				if len(ls) != 1 || string(ls[0].Record.Checkpoint) != "new-progress" {
					t.Fatalf("next lease: %+v, want the record with the new checkpoint", ls)
				}
			},
		},
		{
			name: "ack",
			change: func(t *testing.T, s *WALStore, id, tok string) {
				mustNil(t, s.Ack(ctx, id, tok))
			},
			want: func(t *testing.T, s *WALStore, second []Lease) {
				if len(second) != 0 {
					t.Fatalf("handed out %v that was finished while it was being read", idsOfLeases(second))
				}
				if _, err := s.Get(ctx, "r000"); !errors.Is(err, ErrNotFound) {
					t.Fatalf("Get after ack: %v", err)
				}
			},
		},
		{
			name: "park",
			change: func(t *testing.T, s *WALStore, id, tok string) {
				mustNil(t, s.Park(ctx, id, tok, "operator"))
			},
			want: func(t *testing.T, s *WALStore, second []Lease) {
				if len(second) != 0 {
					t.Fatalf("handed out %v that was parked while it was being read", idsOfLeases(second))
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			box := &hookBox{}
			clk := newTestClock()
			s := openLeaseStore(t, t.TempDir(), box, clk, WALOptions{})
			defer func() { _ = s.Close() }()
			appendN(t, s, 1)
			first, err := s.Lease(ctx, LeaseRequest{Max: 1, TTL: time.Second})
			mustNil(t, err)
			clk.Advance(2 * time.Second) // the lease expires: the record is offered again

			box.set(func(stage string) {
				if stage == "selected" {
					tc.change(t, s, "r000", first[0].Token) // the old worker is still alive
					box.set(nil)
				}
			})
			second, err := s.Lease(ctx, LeaseRequest{Max: 1, TTL: time.Hour})
			mustNil(t, err)
			tc.want(t, s, second)
			noReservations(t, s)
		})
	}
}

// A compaction that runs between a lease's steps moves and deletes the files the
// lease meant to read. That is not corruption: the record must come back intact and
// must not be parked.
func TestACompactionDuringTheReadIsNotMistakenForCorruption(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	box := &hookBox{}
	s := openLeaseStore(t, dir, box, nil, WALOptions{SegmentBytes: 300})
	appendN(t, s, 12)
	mustNil(t, s.Close())
	s = openLeaseStore(t, dir, box, nil, WALOptions{SegmentBytes: 300}) // empty file cache: the reads must open files
	defer func() { _ = s.Close() }()
	before, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))

	box.set(func(stage string) {
		if stage == "selected" {
			mustNil(t, s.Compact(ctx))
			box.set(nil)
		}
	})
	ls, err := s.Lease(ctx, LeaseRequest{Max: 12, TTL: time.Hour})
	mustNil(t, err)
	after, _ := filepath.Glob(filepath.Join(dir, "wal-*.log"))
	if len(after) >= len(before) {
		t.Fatalf("the compaction did not remove segments (%d before, %d after): the test is not testing anything", len(before), len(after))
	}
	if len(ls) != 12 {
		t.Fatalf("leased %d records, want 12", len(ls))
	}
	sort.Slice(ls, func(i, j int) bool { return ls[i].Record.ID < ls[j].Record.ID })
	for i, l := range ls {
		if !bytes_equal(l.Record.Value, payload(i)) {
			t.Fatalf("%s has payload %q, want %q", l.Record.ID, l.Record.Value, payload(i))
		}
	}
	if st := s.WALStats(); st.UnreadablePayloads != 0 {
		t.Fatalf("%d records were parked as unreadable by a compaction race", st.UnreadablePayloads)
	}
	noReservations(t, s)
}

func TestReservationsAreReleasedWhenALeaseGivesUp(t *testing.T) {
	ctx := context.Background()

	t.Run("cancelled", func(t *testing.T) {
		box := &hookBox{}
		s := openLeaseStore(t, t.TempDir(), box, nil, WALOptions{})
		defer func() { _ = s.Close() }()
		appendN(t, s, 3)
		cctx, cancel := context.WithCancel(ctx)
		box.set(func(stage string) {
			if stage == "selected" {
				cancel()
				box.set(nil)
			}
		})
		if _, err := s.Lease(cctx, LeaseRequest{Max: 3, TTL: time.Hour}); !errors.Is(err, context.Canceled) {
			t.Fatalf("Lease = %v, want context.Canceled", err)
		}
		noReservations(t, s)
		ls, err := s.Lease(ctx, LeaseRequest{Max: 3, TTL: time.Hour})
		mustNil(t, err)
		if len(ls) != 3 {
			t.Fatalf("after the cancelled lease %d records could be leased, want 3", len(ls))
		}
	})

	t.Run("store closed mid-read", func(t *testing.T) {
		box := &hookBox{}
		s := openLeaseStore(t, t.TempDir(), box, nil, WALOptions{})
		appendN(t, s, 3)
		box.set(func(stage string) {
			if stage == "selected" {
				mustNil(t, s.Close())
				box.set(nil)
			}
		})
		if _, err := s.Lease(ctx, LeaseRequest{Max: 3, TTL: time.Hour}); !errors.Is(err, ErrClosed) {
			t.Fatalf("Lease = %v, want ErrClosed", err)
		}
		noReservations(t, s)
	})

	t.Run("unreadable payload", func(t *testing.T) {
		dir := t.TempDir()
		box := &hookBox{}
		s := openLeaseStore(t, dir, box, nil, WALOptions{})
		defer func() { _ = s.Close() }()
		appendN(t, s, 3)
		corruptPayload(t, dir, s, "r001")
		ls, err := s.Lease(ctx, LeaseRequest{Max: 3, TTL: time.Hour})
		mustNil(t, err)
		if got := idsOfLeases(ls); fmt.Sprint(got) != "[r000 r002]" {
			t.Fatalf("leased %v, want [r000 r002]", got)
		}
		if g, _ := s.Get(ctx, "r001"); g.State != Parked && s.WALStats().UnreadablePayloads != 1 {
			t.Fatal("the unreadable record was not parked exactly once")
		}
		noReservations(t, s)
	})
}

// Lease must not make Append wait for the read: with every read stalled, Append
// latency stays far below the stall.
func TestAppendLatencyIsIndependentOfSlowReads(t *testing.T) {
	ctx := context.Background()
	box := &hookBox{}
	s := openLeaseStore(t, t.TempDir(), box, nil, WALOptions{})
	defer func() { _ = s.Close() }()
	appendN(t, s, 50)
	box.set(func(stage string) {
		if stage == "selected" {
			time.Sleep(20 * time.Millisecond) // a slow disk
		}
	})
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			ls, err := s.Lease(ctx, LeaseRequest{Max: 1, TTL: time.Hour})
			if err == nil {
				for _, l := range ls {
					_ = s.Release(ctx, l.Record.ID, l.Token)
				}
			}
		}
	}()
	var worst time.Duration
	for i := 0; i < 200; i++ {
		start := time.Now()
		mustNil(t, s.Append(ctx, Record{ID: fmt.Sprintf("a%03d", i), Value: []byte("x")}))
		if d := time.Since(start); d > worst {
			worst = d
		}
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()
	t.Logf("worst Append latency with every lease read stalled 20ms: %v", worst)
	if worst > 10*time.Millisecond {
		t.Fatalf("Append waited %v behind a lease's read", worst)
	}
}

// ---- fileCache ----

func TestFileCacheSurvivesConcurrentEvictionDropAndClose(t *testing.T) {
	dir := t.TempDir()
	const files, size = 8, 4096
	for i := 0; i < files; i++ {
		b := make([]byte, size)
		for j := range b {
			b[j] = byte(i)
		}
		mustNil(t, os.WriteFile(filepath.Join(dir, segmentName(uint64(i+1))), b, 0o600))
	}
	c := newFileCache(dir, 2) // far fewer slots than files: constant eviction

	var seen sync.Map // *cachedFile -> struct{}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := 0; g < 12; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := fileKey{id: uint64((g+i)%files + 1)}
				cf, err := c.acquire(k)
				if err != nil {
					if !errors.Is(err, errCacheClosed) {
						t.Error(err)
					}
					return
				}
				seen.Store(cf, struct{}{})
				buf := make([]byte, 16)
				if _, err := cf.f.ReadAt(buf, int64(i%(size-16))); err != nil {
					t.Errorf("read through an acquired handle failed: %v", err)
				}
				if buf[0] != byte(k.id-1) {
					t.Errorf("file %d returned %d", k.id, buf[0])
				}
				c.release(cf)
			}
		}(g)
	}
	wg.Add(1)
	go func() { // compaction dropping files under the readers
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			c.drop(fileKey{id: uint64(i%files + 1)})
			time.Sleep(50 * time.Microsecond)
		}
	}()
	time.Sleep(300 * time.Millisecond)
	c.closeAll() // Close while reads are in flight
	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()

	if c.lru.Len() != 0 || len(c.files) != 0 {
		t.Fatalf("cache still holds %d files after closeAll", c.lru.Len())
	}
	if _, err := c.readAt(fileKey{id: 1}, 0, 4); !errors.Is(err, errCacheClosed) {
		t.Fatalf("readAt after closeAll = %v, want errCacheClosed", err)
	}
	leaked := 0
	seen.Range(func(k, _ any) bool {
		cf := k.(*cachedFile)
		if cf.refs != 0 {
			t.Errorf("a reader never released its handle (refs=%d)", cf.refs)
		}
		if _, err := cf.f.Stat(); err == nil {
			leaked++
		}
		return true
	})
	if leaked != 0 {
		t.Fatalf("%d file handles were never closed", leaked)
	}
}
