package breaker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/obs"
)

var errDown = errors.New("down")

func quick(events obs.Sink) Config {
	return Config{
		Name:             "dep",
		FailureThreshold: 0.5,
		MinSamples:       2,
		SamplingWindow:   time.Second,
		BucketDuration:   100 * time.Millisecond,
		SleepWindow:      50 * time.Millisecond,
		Events:           events,
	}
}

func fail(b *Breaker) error {
	return b.Do(context.Background(), func(context.Context) error { return errDown })
}

func TestOpensThenRejectsWithOpenError(t *testing.T) {
	b := New(quick(nil))
	_ = fail(b)
	_ = fail(b)
	if b.State() != StateOpen {
		t.Fatalf("state = %v, want open", b.State())
	}

	called := false
	err := b.Do(context.Background(), func(context.Context) error { called = true; return nil })
	if called {
		t.Fatal("fn ran while the circuit was open")
	}
	var oe *OpenError
	if !errors.As(err, &oe) || !errors.Is(err, ErrOpen) || oe.Name != "dep" {
		t.Fatalf("err = %v, want *OpenError matching ErrOpen", err)
	}
	if !oe.NonRetryable() || oe.Error() == "" {
		t.Fatal("OpenError must be non-retryable and printable")
	}
	if got := b.Stats().Rejected; got != 1 {
		t.Fatalf("Rejected = %d, want 1", got)
	}
}

func TestMinSamplesPreventsEarlyTrip(t *testing.T) {
	cfg := quick(nil)
	cfg.MinSamples = 5
	b := New(cfg)
	for i := 0; i < 4; i++ {
		_ = fail(b)
	}
	if b.State() != StateClosed {
		t.Fatalf("state = %v after 4 failures with MinSamples 5", b.State())
	}
}

func TestRecoversThroughHalfOpen(t *testing.T) {
	b := New(quick(nil))
	_ = fail(b)
	_ = fail(b)
	time.Sleep(120 * time.Millisecond)

	if err := b.Do(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("probe rejected: %v", err)
	}
	if b.State() != StateClosed {
		t.Fatalf("state = %v after successful probe, want closed", b.State())
	}
}

func TestFailedProbeReopens(t *testing.T) {
	b := New(quick(nil))
	_ = fail(b)
	_ = fail(b)
	time.Sleep(120 * time.Millisecond)
	_ = fail(b) // probe fails
	if b.State() != StateOpen {
		t.Fatalf("state = %v after failed probe, want open", b.State())
	}
}

func TestEventsAndStats(t *testing.T) {
	var mu sync.Mutex
	var seen []obs.StateChanged
	b := New(quick(obs.SinkFunc(func(e obs.Event) {
		mu.Lock()
		seen = append(seen, e.(obs.StateChanged))
		mu.Unlock()
	})))
	_ = fail(b)
	_ = fail(b)
	time.Sleep(120 * time.Millisecond)
	_ = b.Do(context.Background(), func(context.Context) error { return nil })

	mu.Lock()
	defer mu.Unlock()
	want := [][2]State{{StateClosed, StateOpen}, {StateOpen, StateHalfOpen}, {StateHalfOpen, StateClosed}}
	if len(seen) != len(want) {
		t.Fatalf("got %d events, want %d: %+v", len(seen), len(want), seen)
	}
	for i, w := range want {
		if seen[i].From != w[0] || seen[i].To != w[1] || seen[i].Dependency != "dep" || seen[i].At.IsZero() {
			t.Fatalf("event %d = %+v, want %v->%v", i, seen[i], w[0], w[1])
		}
	}
	st := b.Stats()
	if st.Opens != 1 || st.LastTransition.IsZero() || st.Name != "dep" || st.State != StateClosed {
		t.Fatalf("stats = %+v", st)
	}
}

func TestCallerCancellationIsNotAFailure(t *testing.T) {
	b := New(quick(nil))
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		_ = b.Do(ctx, func(ctx context.Context) error {
			cancel()
			return ctx.Err()
		})
	}
	st := b.Stats()
	if b.State() != StateClosed || st.Failure != 0 || st.Success != 0 || st.Inflight != 0 {
		t.Fatalf("cancellations were recorded: %+v", st)
	}
}

func TestNonFailureErrorCountsAsSuccess(t *testing.T) {
	cfg := quick(nil)
	notFound := errors.New("not found")
	cfg.IsFailure = func(err error) bool { return !errors.Is(err, notFound) }
	b := New(cfg)
	for i := 0; i < 10; i++ {
		if err := b.Do(context.Background(), func(context.Context) error { return notFound }); !errors.Is(err, notFound) {
			t.Fatalf("err = %v, want the original error", err)
		}
	}
	if b.State() != StateClosed || b.Stats().Success != 10 {
		t.Fatalf("stats = %+v", b.Stats())
	}
}

func TestPanicCountsAsFailureAndPropagates(t *testing.T) {
	b := New(quick(nil))
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("panic was swallowed")
			}
		}()
		_ = b.Do(context.Background(), func(context.Context) error { panic("bug") })
	}()
	st := b.Stats()
	if st.Failure != 1 || st.Inflight != 0 {
		t.Fatalf("stats = %+v, want 1 failure and no leaked slot", st)
	}
}

func TestBulkheadRejectsWithBulkheadError(t *testing.T) {
	cfg := quick(nil)
	cfg.MaxInflight = 1
	b := New(cfg)

	release := make(chan struct{})
	started := make(chan struct{})
	go func() {
		_ = b.Do(context.Background(), func(context.Context) error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started

	err := b.Do(context.Background(), func(context.Context) error { return nil })
	var be *BulkheadError
	if !errors.As(err, &be) || !errors.Is(err, ErrBulkhead) || errors.Is(err, ErrOpen) {
		t.Fatalf("err = %v, want *BulkheadError", err)
	}
	if be.Error() == "" || b.Stats().RejectedBulkhead != 1 {
		t.Fatalf("stats = %+v", b.Stats())
	}
	close(release)
}

func TestBulkheadWaitTimeout(t *testing.T) {
	cfg := quick(nil)
	cfg.MaxInflight = 1
	cfg.WaitTimeout = 200 * time.Millisecond
	b := New(cfg)

	p, err := b.Acquire(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(30 * time.Millisecond)
		p.Success(time.Millisecond)
	}()
	// Freed slot is available when the wait ends.
	if err := b.Do(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("waiting call rejected: %v", err)
	}
}

func TestPermitIsIdempotent(t *testing.T) {
	b := New(quick(nil))
	p, _ := b.Acquire(context.Background(), false)
	p.Success(time.Millisecond)
	p.Failure()
	p.Abandon()
	st := b.Stats()
	if st.Success != 1 || st.Failure != 0 || st.Inflight != 0 {
		t.Fatalf("stats = %+v, want exactly one success", st)
	}
}

func TestOverrides(t *testing.T) {
	b := New(quick(nil))
	b.SetOverride(OverrideForceOpen)
	if err := b.Do(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrOpen) {
		t.Fatalf("forced open: %v", err)
	}
	if b.Stats().Override != OverrideForceOpen {
		t.Fatal("override not reported in stats")
	}
	b.SetOverride(OverrideForceClosed)
	_ = fail(b)
	_ = fail(b)
	if err := b.Do(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("forced closed rejected a call: %v", err)
	}
	b.SetOverride(OverrideNone)
	if b.Stats().Override != OverrideNone {
		t.Fatal("override not cleared")
	}
}

func TestDryRunNeverRejects(t *testing.T) {
	cfg := quick(nil)
	cfg.DryRun = true
	b := New(cfg)
	_ = fail(b)
	_ = fail(b)
	if b.State() != StateOpen {
		t.Fatalf("dry run must still track state, got %v", b.State())
	}
	if err := b.Do(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("dry run rejected a call: %v", err)
	}
}

func TestCallReturnsValue(t *testing.T) {
	b := New(quick(nil))
	v, err := Call(context.Background(), b, func(context.Context) (int, error) { return 42, nil })
	if v != 42 || err != nil {
		t.Fatalf("v=%d err=%v", v, err)
	}
	_, err = Call(context.Background(), b, func(context.Context) (string, error) { return "", errDown })
	if !errors.Is(err, errDown) {
		t.Fatalf("err = %v", err)
	}
}

func TestRetryBudgetAndDefaults(t *testing.T) {
	b := New(Config{Name: "d", RetryBudget: 0.1})
	if !b.CanRetry() {
		t.Fatal("empty window must allow retries")
	}
	b.RecordRetry()
	if b.CanRetry() {
		t.Fatal("budget should be exhausted after a retry with no traffic")
	}
	if b.Name() != "d" || b.AvgLatency() != 0 {
		t.Fatal("accessors")
	}
	d := New(Config{})
	if d.cfg.FailureThreshold != 0.5 || d.cfg.MinSamples != 10 || d.cfg.SleepWindow != 30*time.Second || d.cfg.SamplingWindow != 10*time.Second {
		t.Fatalf("defaults not applied: %+v", d.cfg)
	}
	if d.cfg.IsFailure(nil) || !d.cfg.IsFailure(errDown) {
		t.Fatal("default IsFailure")
	}
}

func TestConcurrentUseIsRaceFree(t *testing.T) {
	b := New(quick(nil))
	var wg sync.WaitGroup
	var n atomic.Int32
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = b.Do(context.Background(), func(context.Context) error {
					if n.Add(1)%3 == 0 {
						return errDown
					}
					return nil
				})
				_ = b.Stats()
			}
		}()
	}
	wg.Wait()
	if b.Stats().Inflight != 0 {
		t.Fatalf("leaked slots: %+v", b.Stats())
	}
}

func TestRecordProbeDoesNotTouchInflight(t *testing.T) {
	b := New(quick(nil))
	b.RecordProbe(false)
	b.RecordProbe(false)
	if b.State() != StateOpen {
		t.Fatalf("state = %v, want open after unhealthy probes", b.State())
	}
	b.RecordProbe(true)
	if b.Stats().Inflight != 0 {
		t.Fatalf("inflight = %d, want 0", b.Stats().Inflight)
	}
}

func TestBulkheadWaitEndsWithContext(t *testing.T) {
	cfg := quick(nil)
	cfg.MaxInflight = 1
	cfg.WaitTimeout = time.Hour
	b := New(cfg)
	p, _ := b.Acquire(context.Background(), false)
	defer p.Abandon()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := b.Acquire(ctx, false)
	if !errors.Is(err, ErrBulkhead) || time.Since(start) > 2*time.Second {
		t.Fatalf("err=%v after %v; the wait must end with the context", err, time.Since(start))
	}
}
