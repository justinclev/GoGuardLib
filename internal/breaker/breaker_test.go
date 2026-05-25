package breaker

import (
	"context"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/internal/engine"
)

func TestBreakerTransitions(t *testing.T) {
	b := NewBreaker(0.5, 100*time.Millisecond, 1*time.Second, 100*time.Millisecond, 0, 0, false, 0)
	ctx := context.Background()
	var seed uint64 = 123

	// Closed -> Open
	for i := 0; i < 10; i++ {
		b.MarkFailure()
	}
	if b.state.Get() != engine.StateOpen {
		t.Error("Expected StateOpen after failures")
	}

	// Open -> HalfOpen
	if b.Allow(ctx, 0, &seed, false) {
		t.Error("Expected Allow to be false in Open state")
	}
	time.Sleep(150 * time.Millisecond)
	if !b.Allow(ctx, 0, &seed, false) {
		t.Error("Expected Allow to be true in HalfOpen state")
	}
	if b.state.Get() != engine.StateHalfOpen {
		t.Error("Expected StateHalfOpen")
	}

	// Probing (Single-flight)
	if b.Allow(ctx, 0, &seed, false) {
		t.Error("Expected only one request allowed in HalfOpen")
	}

	// HalfOpen -> Open (failure)
	b.MarkFailure()
	if b.state.Get() != engine.StateOpen {
		t.Error("Expected StateOpen after failure in HalfOpen")
	}
}

func TestBulkhead(t *testing.T) {
	b := NewBreaker(0.5, 1*time.Second, 10*time.Second, 1*time.Second, 2, 0, false, 0)
	ctx := context.Background()
	var seed uint64 = 123
	
	if !b.Allow(ctx, 0, &seed, false) { t.Fatal("Request 1 blocked") }
	if !b.Allow(ctx, 0, &seed, false) { t.Fatal("Request 2 blocked") }
	if b.Allow(ctx, 0, &seed, false) { t.Fatal("Request 3 allowed (should be blocked by bulkhead)") }
	
	// VIP should bypass bulkhead
	if !b.Allow(ctx, 0, &seed, true) { t.Fatal("VIP blocked by bulkhead") }
	
	// Release two requests
	b.MarkSuccess(100 * time.Millisecond) // Release 1 (inflight=2)
	b.MarkSuccess(100 * time.Millisecond) // Release 2 (inflight=1)
	
	if !b.Allow(ctx, 0, &seed, false) { t.Fatal("Request should be allowed after slots freed") }
}

func TestManualOverride(t *testing.T) {
	b := NewBreaker(0.5, 1*time.Second, 10*time.Second, 1*time.Second, 0, 0, false, 0)
	ctx := context.Background()
	var seed uint64 = 123
	
	b.SetOverride(1) // ForceOpen
	if b.Allow(ctx, 0, &seed, false) { t.Fatal("Should be blocked when ForceOpen") }
	
	b.SetOverride(2) // ForceClosed
	if !b.Allow(ctx, 0, &seed, false) { t.Fatal("Should be allowed when ForceClosed") }
}

func TestMinSamples(t *testing.T) {
	b := NewBreaker(0.1, 1*time.Second, 10*time.Second, 1*time.Second, 0, 5, false, 0)
	
	for i := 0; i < 4; i++ {
		b.MarkFailure()
	}
	if b.State() != engine.StateClosed {
		t.Error("Expected StateClosed due to minSamples")
	}
	
	b.MarkFailure()
	if b.State() != engine.StateOpen {
		t.Error("Expected StateOpen after 5th sample")
	}
}

func TestRetryBudget(t *testing.T) {
	b := NewBreaker(0.5, 1*time.Second, 10*time.Second, 1*time.Second, 0, 0, false, 0.1) // 10% budget
	
	// 10 successes
	for i := 0; i < 10; i++ {
		b.MarkSuccess(10 * time.Millisecond)
	}
	
	if !b.CanRetry() { t.Fatal("Should be able to retry (budget empty)") }
	b.RecordRetry()
	if !b.CanRetry() { t.Fatal("Should still be able to retry") }
	
	b.RecordRetry()
	if b.CanRetry() { t.Fatal("Should NOT be able to retry (budget exceeded)") }
}
