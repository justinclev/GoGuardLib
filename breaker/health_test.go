package breaker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/health"
	"github.com/justinclev/GoGuardLib/obs"
)

type probeControl struct {
	healthy atomic.Bool
	calls   atomic.Int32
}

func (p *probeControl) check(context.Context) error {
	p.calls.Add(1)
	if p.healthy.Load() {
		return nil
	}
	return errors.New("unhealthy")
}

func healthCfg(p *probeControl, sink obs.Sink) Config {
	cfg := quick(sink)
	cfg.SleepWindow = 20 * time.Millisecond // far shorter than the checks: only health may end the outage
	cfg.Health = &health.Config{Check: p.check, Interval: 5 * time.Millisecond, MaxInterval: 10 * time.Millisecond, Jitter: -1, SuccessThreshold: 2}
	return cfg
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func trip(b *Breaker) {
	_ = fail(b)
	_ = fail(b)
}

func TestHealthGatedCircuitIgnoresTheTimer(t *testing.T) {
	p := &probeControl{}
	b := New(healthCfg(p, nil))
	defer b.Close()
	trip(b)
	time.Sleep(150 * time.Millisecond) // many sleep windows

	called := false
	err := b.Do(context.Background(), func(context.Context) error { called = true; return nil })
	if called || !errors.Is(err, ErrOpen) {
		t.Fatalf("a canary was sent to a dependency whose health check is still failing (err %v)", err)
	}
	if b.State() != StateOpen {
		t.Fatalf("state = %v", b.State())
	}
}

func TestHealthyProbesMoveToHalfOpenAndACanaryCloses(t *testing.T) {
	p := &probeControl{}
	b := New(healthCfg(p, nil))
	defer b.Close()
	trip(b)
	p.healthy.Store(true)
	waitFor(t, "half-open", func() bool { return b.State() == StateHalfOpen })

	if b.Stats().Failure != 2 || b.Stats().Success != 0 {
		t.Fatalf("probes leaked into the window: %+v", b.Stats())
	}
	// Health alone does not close the circuit: real traffic has to prove it.
	time.Sleep(30 * time.Millisecond)
	if b.State() != StateHalfOpen {
		t.Fatalf("state = %v; a healthy /health must not close the circuit by itself", b.State())
	}
	if err := b.Do(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("canary rejected: %v", err)
	}
	if b.State() != StateClosed {
		t.Fatalf("state = %v after a successful canary", b.State())
	}
}

func TestProbingHappensOnlyWhileTheCircuitIsOpen(t *testing.T) {
	p := &probeControl{}
	p.healthy.Store(true)
	b := New(healthCfg(p, nil))
	defer b.Close()
	for i := 0; i < 20; i++ {
		_ = b.Do(context.Background(), func(context.Context) error { return nil })
		time.Sleep(3 * time.Millisecond)
	}
	if p.calls.Load() != 0 {
		t.Fatalf("%d health checks ran against a healthy dependency", p.calls.Load())
	}

	for i := 0; i < 40; i++ { // enough failures to outweigh the earlier successes
		_ = fail(b)
	}
	waitFor(t, "probing to start", func() bool { return p.calls.Load() > 0 })
	waitFor(t, "half-open", func() bool { return b.State() == StateHalfOpen })
	_ = b.Do(context.Background(), func(context.Context) error { return nil }) // closes
	time.Sleep(40 * time.Millisecond)
	settled := p.calls.Load()
	time.Sleep(60 * time.Millisecond)
	if p.calls.Load() != settled {
		t.Fatal("the prober kept running after the circuit closed")
	}
}

func TestFailedCanaryReopensAndNeedsFreshHealthyProbes(t *testing.T) {
	p := &probeControl{}
	b := New(healthCfg(p, nil))
	defer b.Close()
	trip(b)
	p.healthy.Store(true)
	waitFor(t, "half-open", func() bool { return b.State() == StateHalfOpen })

	_ = fail(b) // the canary fails
	if b.State() != StateOpen {
		t.Fatalf("state = %v after a failed canary", b.State())
	}
	waitFor(t, "second half-open", func() bool { return b.State() == StateHalfOpen })
}

func TestUnhealthyProbeWhileHalfOpenReopens(t *testing.T) {
	p := &probeControl{}
	b := New(healthCfg(p, nil))
	defer b.Close()
	trip(b)
	p.healthy.Store(true)
	waitFor(t, "half-open", func() bool { return b.State() == StateHalfOpen })
	p.healthy.Store(false) // it relapsed before any traffic arrived
	waitFor(t, "reopen", func() bool { return b.State() == StateOpen })
}

func TestTrustHealthClosesWithoutACanary(t *testing.T) {
	p := &probeControl{}
	cfg := healthCfg(p, nil)
	cfg.Health.TrustHealth = true
	b := New(cfg)
	defer b.Close()
	trip(b)
	p.healthy.Store(true)
	waitFor(t, "closed", func() bool { return b.State() == StateClosed })
}

func TestProbeEventsAreEmittedAndDoNotCountAsTraffic(t *testing.T) {
	var mu sync.Mutex
	var probes []obs.ProbeResult
	p := &probeControl{}
	b := New(healthCfg(p, obs.SinkFunc(func(e obs.Event) {
		if pr, ok := e.(obs.ProbeResult); ok {
			mu.Lock()
			probes = append(probes, pr)
			mu.Unlock()
		}
	})))
	defer b.Close()
	trip(b)
	waitFor(t, "three probes", func() bool { mu.Lock(); defer mu.Unlock(); return len(probes) >= 3 })
	mu.Lock()
	first := probes[0]
	mu.Unlock()
	if first.OK || first.Dependency != "dep" {
		t.Fatalf("event = %+v", first)
	}
	if st := b.Stats(); st.Failure != 2 || st.Success != 0 {
		t.Fatalf("stats = %+v: probes must not be counted", st)
	}
}

// A health check that lies (always unhealthy while the service is fine) must not
// be able to block traffic forever.
func TestMaxOpenFallsBackToTheTimer(t *testing.T) {
	p := &probeControl{} // never healthy
	cfg := healthCfg(p, nil)
	cfg.Health.MaxOpen = 80 * time.Millisecond
	b := New(cfg)
	defer b.Close()
	trip(b)
	if _, err := b.Acquire(context.Background(), false); !errors.Is(err, ErrOpen) {
		t.Fatalf("err = %v, want the circuit gated at first", err)
	}
	time.Sleep(200 * time.Millisecond)
	permit, err := b.Acquire(context.Background(), false)
	if err != nil {
		t.Fatalf("still blocked after MaxOpen: %v", err)
	}
	permit.Abandon()
}

func TestNegativeMaxOpenNeverFallsBack(t *testing.T) {
	p := &probeControl{}
	cfg := healthCfg(p, nil)
	cfg.Health.MaxOpen = -1
	b := New(cfg)
	defer b.Close()
	trip(b)
	time.Sleep(150 * time.Millisecond)
	if _, err := b.Acquire(context.Background(), false); !errors.Is(err, ErrOpen) {
		t.Fatalf("err = %v, want the circuit to stay gated", err)
	}
}

func TestCloseStopsTheProberAndIsIdempotent(t *testing.T) {
	p := &probeControl{}
	b := New(healthCfg(p, nil))
	trip(b)
	waitFor(t, "probing", func() bool { return p.calls.Load() > 1 })

	done := make(chan struct{})
	go func() { b.Close(); b.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return")
	}
	n := p.calls.Load()
	time.Sleep(50 * time.Millisecond)
	if p.calls.Load() != n {
		t.Fatal("the prober ran after Close")
	}
	// Opening after Close must not resurrect a prober.
	b.SetOverride(OverrideNone)
	b.in.BeginProbe()
	trip(b)
	time.Sleep(30 * time.Millisecond)
	if p.calls.Load() != n {
		t.Fatal("a prober started after Close")
	}
}

func TestBreakerWithoutHealthStillUsesTheTimer(t *testing.T) {
	b := New(quick(nil))
	trip(b)
	time.Sleep(120 * time.Millisecond)
	if err := b.Do(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("timer canary rejected: %v", err)
	}
	b.Close() // harmless without a prober
}
