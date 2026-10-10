package dlq

import (
	"context"
	"errors"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/obs"
	"github.com/justinclev/GoGuardLib/secure"
)

// A stalled fsync must not hold a caller past its own deadline.
func TestAppendHonoursContextWhileFsyncStalls(t *testing.T) {
	fl := &faults{}
	s := openF(t, t.TempDir(), fl, WALOptions{})
	defer func() { _ = s.Close() }()
	fl.set(func(f *faults) { f.syncDelay = 1500 * time.Millisecond })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := s.Append(ctx, Record{ID: "a", Value: []byte("v")})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Append = %v, want context deadline exceeded", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Append held the caller for %v past its 100ms deadline", d)
	}
}

// SyncTimeout turns a hung disk into a failed-closed store instead of a hang.
func TestSyncTimeoutFailsClosedAndReports(t *testing.T) {
	fl := &faults{}
	var mu sync.Mutex
	var codes []obs.StoreCode
	sink := obs.SinkFunc(func(e obs.Event) {
		if se, ok := e.(obs.StoreEvent); ok {
			mu.Lock()
			codes = append(codes, se.Code)
			mu.Unlock()
		}
	})
	s := openF(t, t.TempDir(), fl, WALOptions{SyncTimeout: 100 * time.Millisecond, Events: sink})
	fl.set(func(f *faults) { f.syncDelay = 1500 * time.Millisecond })

	start := time.Now()
	err := s.Append(context.Background(), Record{ID: "a", Value: []byte("v")})
	if !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("Append = %v, want ErrStoreFailed", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Append took %v with SyncTimeout 100ms", d)
	}
	if err := s.Append(context.Background(), Record{ID: "b"}); !errors.Is(err, ErrStoreFailed) {
		t.Fatalf("store still serving after a sync timeout: %v", err)
	}
	if st := s.WALStats(); st.Failed == nil {
		t.Fatal("WALStats.Failed is nil after a sync timeout")
	}
	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, c := range codes {
		found = found || c == obs.StoreSyncTimeout
	}
	if !found {
		t.Fatalf("no sync_timeout store event; got %v", codes)
	}
	done := make(chan struct{})
	go func() { _ = s.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung behind the stalled fsync")
	}
}

// A read that fails for a passing reason (no file descriptors, an I/O error)
// says nothing about the record: it must stay queued, not be parked as corrupt.
func TestTransientReadErrorDoesNotParkRecord(t *testing.T) {
	s := openF(t, t.TempDir(), nil, WALOptions{})
	defer func() { _ = s.Close() }()
	addRecords(t, s, "a")

	s.files.openFn = func(string) (*os.File, error) { return nil, &os.PathError{Op: "open", Path: "x", Err: syscall.EMFILE} }
	ls, err := s.Lease(context.Background(), LeaseRequest{Max: 1, TTL: time.Hour})
	if err != nil || len(ls) != 0 {
		t.Fatalf("Lease during the fault = %v, %v; want nothing and no error", ls, err)
	}
	if n := s.WALStats().UnreadablePayloads; n != 0 {
		t.Fatalf("a transient read fault parked %d record(s) as unreadable", n)
	}

	s.files.openFn = nil
	ls, err = s.Lease(context.Background(), LeaseRequest{Max: 1, TTL: time.Hour})
	if err != nil || len(ls) != 1 || ls[0].Record.ID != "a" {
		t.Fatalf("Lease after the fault cleared = %v, %v; want record a", ls, err)
	}
}

// Compaction failing is invisible to a caller until it polls WALStats; the store
// must say so on its own, once when it starts and once when it recovers.
func TestCompactionFailureAndRecoveryAreReported(t *testing.T) {
	fl := &faults{}
	var mu sync.Mutex
	var codes []obs.StoreCode
	sink := obs.SinkFunc(func(e obs.Event) {
		if se, ok := e.(obs.StoreEvent); ok {
			mu.Lock()
			codes = append(codes, se.Code)
			mu.Unlock()
		}
	})
	s := openF(t, t.TempDir(), fl, WALOptions{DisableAutoCompact: true, Events: sink})
	defer func() { _ = s.Close() }()
	addRecords(t, s, "a", "b")

	fl.set(func(f *faults) {
		f.failOpen = func(name string, flag int) error {
			if isSnapshot(name) {
				return errDisk
			}
			return nil
		}
	})
	for i := 0; i < 2; i++ { // a second failure is not a new event
		if err := s.Compact(context.Background()); err == nil {
			t.Fatal("Compact succeeded despite the injected fault")
		}
	}
	fl.set(func(f *faults) { f.failOpen = nil })
	if err := s.Compact(context.Background()); err != nil {
		t.Fatalf("Compact after the fault cleared: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	want := []obs.StoreCode{obs.StoreCompactionFailed, obs.StoreCompactionRecovered}
	if len(codes) != len(want) || codes[0] != want[0] || codes[1] != want[1] {
		t.Fatalf("store events = %v, want %v", codes, want)
	}
}

// Mode bits are not enough: a directory another user owns can be rewritten by
// that user whatever its mode says now.
func TestWALRefusesADirectoryOwnedByAnotherUser(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root to hand a directory to another user")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(dir, 54321, 54321); err != nil {
		t.Skipf("cannot chown here: %v", err)
	}
	if _, err := OpenWAL(dir, WALOptions{}); !errors.Is(err, ErrInsecurePermissions) {
		t.Fatalf("OpenWAL = %v, want ErrInsecurePermissions", err)
	}
	s, err := OpenWAL(dir, WALOptions{AllowInsecurePermissions: true})
	if err != nil {
		t.Fatalf("override should open it: %v", err)
	}
	_ = s.Close()
}

// Rotating the order-key pepper must not let a new message overtake an earlier
// one that is still stored under the old pepper.
func TestPepperRotationKeepsOrderAcrossTheRotation(t *testing.T) {
	inner := NewMemoryStore(MemoryOptions{})
	enc := testEncryptor(t)
	oldP, newP := []byte("old-pepper-old-pepper-old-pepper"), []byte("new-pepper-new-pepper-new-pepper")
	ctx := context.Background()

	before, _ := Secure(inner, SecureOptions{Encryptor: enc, OrderKeyPepper: oldP})
	if err := before.Append(ctx, Record{ID: "r1", OrderKey: "cust", Value: []byte("1")}); err != nil {
		t.Fatal(err)
	}

	after, err := Secure(inner, SecureOptions{Encryptor: enc, OrderKeyPeppers: [][]byte{newP, oldP}})
	if err != nil {
		t.Fatal(err)
	}
	if err := after.Append(ctx, Record{ID: "r2", OrderKey: "cust", Value: []byte("2")}); err != nil {
		t.Fatal(err)
	}
	if err := after.Append(ctx, Record{ID: "r3", OrderKey: "fresh", Value: []byte("3")}); err != nil {
		t.Fatal(err)
	}
	if held, _ := after.HasOrderKey(ctx, "cust"); !held {
		t.Fatal("the old key is no longer found after rotation")
	}
	// r2 shares r1's stored key: only r1 (and the unrelated r3) may be leased.
	ls, _ := after.Lease(ctx, LeaseRequest{Max: 10, TTL: time.Hour})
	got := map[string]bool{}
	for _, l := range ls {
		got[l.Record.ID] = true
	}
	if !got["r1"] || got["r2"] || !got["r3"] {
		t.Fatalf("leased %v: r2 must wait for r1", got)
	}
	if held, _ := before.HasOrderKey(ctx, "fresh"); held {
		t.Fatal("a new key should be stored under the new pepper only")
	}
}

func testEncryptor(t testing.TB) secure.Encryptor {
	t.Helper()
	e, err := secure.NewAESGCM(secure.Key{ID: "k", Material: []byte("0123456789abcdef0123456789abcdef")})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// Reseal must leave nothing readable under the retired key.
func TestResealMovesDataToNewKeysAndKeepsItsState(t *testing.T) {
	ctx := context.Background()
	oldEnc := testEncryptor(t)
	newEnc, err := secure.NewAESGCM(secure.Key{ID: "k2", Material: []byte("fedcba9876543210fedcba9876543210")})
	if err != nil {
		t.Fatal(err)
	}
	src := openF(t, t.TempDir(), nil, WALOptions{})
	defer func() { _ = src.Close() }()
	sec, _ := Secure(src, SecureOptions{Encryptor: oldEnc, OrderKeyPepper: []byte("pepper-pepper-pepper-pepper-1234")})
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(sec.Append(ctx, Record{ID: "a", OrderKey: "cust", Key: []byte("k"), Value: []byte("va"), Headers: []Header{{Key: "h", Value: []byte("hv")}}}))
	must(sec.Append(ctx, Record{ID: "b", Value: []byte("vb")}))
	ls, _ := sec.Lease(ctx, LeaseRequest{Max: 1, TTL: time.Hour})
	must(sec.Checkpoint(ctx, ls[0].Record.ID, ls[0].Token, []byte("progress")))
	must(sec.Park(ctx, ls[0].Record.ID, ls[0].Token, "needs a human"))

	dst := NewMemoryStore(MemoryOptions{})
	rep, err := Reseal(ctx, src, dst, oldEnc, newEnc)
	if err != nil || rep.Records != 2 || rep.Parked != 1 {
		t.Fatalf("Reseal = %+v, %v", rep, err)
	}

	after, _ := Secure(dst, SecureOptions{Encryptor: newEnc})
	got, err := after.Get(ctx, "a")
	if err != nil || string(got.Value) != "va" || string(got.Checkpoint) != "progress" || string(got.Headers[0].Value) != "hv" || got.State != Parked {
		t.Fatalf("record a after reseal = %+v, %v", got, err)
	}
	if _, err := Secure(dst, SecureOptions{Encryptor: oldEnc}); err != nil {
		t.Fatal(err)
	}
	stale, _ := Secure(dst, SecureOptions{Encryptor: oldEnc})
	if _, err := stale.Get(ctx, "a"); err == nil {
		t.Fatal("data is still readable with the retired key")
	}
}

func TestWALRefusesANetworkFilesystemUnlessAllowed(t *testing.T) {
	fake := func(string) (string, bool) { return "nfs", true }
	if _, err := OpenWAL(t.TempDir(), WALOptions{netFSFn: fake}); !errors.Is(err, ErrNetworkFilesystem) {
		t.Fatalf("OpenWAL on NFS = %v, want ErrNetworkFilesystem", err)
	}
	s, err := OpenWAL(t.TempDir(), WALOptions{netFSFn: fake, AllowNetworkFilesystem: true})
	if err != nil {
		t.Fatalf("override should open it: %v", err)
	}
	_ = s.Close()
}
