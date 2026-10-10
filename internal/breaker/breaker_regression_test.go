package breaker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The bulkhead must never admit more than maxInflight concurrent requests,
// however many goroutines race for the last slots.
func TestBulkheadNeverOverAdmits(t *testing.T) {
	const limit = 5
	b := NewBreaker(0.5, time.Second, time.Second, 100*time.Millisecond, limit, 0, false, 0)

	var admitted int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var seed uint64 = 1
			<-start
			if b.Allow(context.Background(), 0, &seed, false) {
				atomic.AddInt32(&admitted, 1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := atomic.LoadInt32(&admitted); got != limit {
		t.Fatalf("admitted %d requests, want exactly %d", got, limit)
	}
}

// A request rejected by the circuit must give back the slot it reserved.
func TestRejectedRequestReleasesSlot(t *testing.T) {
	b := NewBreaker(0.5, time.Hour, time.Second, 100*time.Millisecond, 2, 0, false, 0)
	var seed uint64 = 1
	b.MarkFailure() // opens the circuit; also drives inflight to -1, so reset below
	atomic.StoreInt32(&b.inflight, 0)

	for i := 0; i < 10; i++ {
		if b.Allow(context.Background(), 0, &seed, false) {
			t.Fatal("open circuit admitted a request")
		}
	}
	if got := atomic.LoadInt32(&b.inflight); got != 0 {
		t.Fatalf("inflight = %d after rejections, want 0", got)
	}
}

func TestAllowExReportsWhyRejected(t *testing.T) {
	var seed uint64 = 1
	ctx := context.Background()

	b := NewBreaker(0.5, time.Hour, time.Second, 100*time.Millisecond, 1, 0, false, 0)
	if v := b.AllowEx(ctx, 0, &seed, false); v != Admitted {
		t.Fatalf("first request: %v", v)
	}
	if v := b.AllowEx(ctx, 0, &seed, false); v != RejectedBulkhead {
		t.Fatalf("second request: got %v, want RejectedBulkhead", v)
	}
	if v := b.AllowEx(ctx, 0, &seed, true); v != Admitted {
		t.Fatalf("VIP request must bypass the bulkhead, got %v", v)
	}

	b.SetOverride(1)
	if v := b.AllowEx(ctx, 0, &seed, false); v != RejectedOpen {
		t.Fatalf("forced open: got %v", v)
	}
	if b.Inflight() != 2 {
		t.Fatalf("Inflight = %d, want 2", b.Inflight())
	}
}

// An abandoned half-open probe must free the probe slot; otherwise the breaker
// would stay half-open forever.
func TestAbandonedProbeFreesProbeSlot(t *testing.T) {
	var seed uint64 = 1
	ctx := context.Background()
	b := NewBreaker(0.5, 10*time.Millisecond, time.Second, 100*time.Millisecond, 0, 0, false, 0)
	b.MarkFailure() // open
	time.Sleep(60 * time.Millisecond)

	if !b.Allow(ctx, 0, &seed, false) {
		t.Fatal("probe not admitted after sleep window")
	}
	if b.Allow(ctx, 0, &seed, false) {
		t.Fatal("second probe admitted while one is in flight")
	}
	b.Abandon()
	if !b.Allow(ctx, 0, &seed, false) {
		t.Fatal("probe slot not released by Abandon")
	}
	if b.FailureRateBps() != 10000 {
		t.Fatalf("FailureRateBps = %d, want 10000", b.FailureRateBps())
	}
}

// A probe whose caller never reports back must not leave the circuit half-open for
// ever: after a sleep window another call takes over as the probe.
func TestLeakedProbeIsReplacedAfterASleepWindow(t *testing.T) {
	b := NewBreaker(0.5, 50*time.Millisecond, time.Second, 100*time.Millisecond, 0, 1, false, 0)
	ctx := context.Background()
	var seed uint64 = 1
	b.MarkFailure() // MinSamples 1: opens the circuit
	if b.State().String() != "open" {
		t.Fatalf("state %v, want open", b.State())
	}
	time.Sleep(80 * time.Millisecond)
	if b.AllowEx(ctx, 0, &seed, false) != Admitted {
		t.Fatal("the first probe was not admitted after the sleep window")
	}
	// This probe is never finished. Others are turned away while it may still report.
	if b.AllowEx(ctx, 0, &seed, false) == Admitted {
		t.Fatal("a second call was admitted while a probe was in flight")
	}
	time.Sleep(80 * time.Millisecond)
	if b.AllowEx(ctx, 0, &seed, false) != Admitted {
		t.Fatal("the circuit stayed wedged half-open behind a probe that never reported back")
	}
}
