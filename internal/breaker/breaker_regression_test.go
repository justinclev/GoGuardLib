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

// Health probes never held a slot, so recording their result must not drift
// the in-flight counter.
func TestProbeResultsDoNotTouchInflight(t *testing.T) {
	b := NewBreaker(0.5, time.Second, time.Second, 100*time.Millisecond, 3, 0, false, 0)
	for i := 0; i < 5; i++ {
		b.ProbeFailure()
		b.ProbeSuccess()
	}
	if got := atomic.LoadInt32(&b.inflight); got != 0 {
		t.Fatalf("inflight = %d after probes, want 0", got)
	}
}
