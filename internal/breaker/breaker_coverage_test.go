package breaker

import (
	"context"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/internal/engine"
)

func TestBreaker_AvgLatency(t *testing.T) {
	b := NewBreaker(0.5, 1*time.Second, 10*time.Second, 1*time.Second, 0, 0, false, 0)
	b.MarkSuccess(100 * time.Millisecond)
	b.MarkSuccess(200 * time.Millisecond)
	
	avg := b.AvgLatency()
	if avg != 150*time.Millisecond {
		t.Errorf("expected 150ms avg latency, got %v", avg)
	}
}

func TestBreaker_Stats(t *testing.T) {
	b := NewBreaker(0.5, 1*time.Second, 10*time.Second, 1*time.Second, 0, 0, false, 0)
	b.MarkSuccess(100 * time.Millisecond)
	b.MarkFailure()
	
	s, f := b.Stats()
	if s != 1 || f != 1 {
		t.Errorf("expected 1 success and 1 failure, got %d and %d", s, f)
	}
}

func TestBreaker_SetOverride(t *testing.T) {
	b := NewBreaker(0.5, 1*time.Second, 10*time.Second, 1*time.Second, 0, 0, false, 0)
	
	b.SetOverride(1) // Always Open
	if b.Allow(context.TODO(), 0, nil, false) {
		t.Errorf("expected Allow to be false with override 1")
	}
	
	b.SetOverride(2) // Always Closed
	if !b.Allow(context.TODO(), 0, nil, false) {
		t.Errorf("expected Allow to be true with override 2")
	}
}

func TestBreaker_CanRetry(t *testing.T) {
	// 10% budget
	b := NewBreaker(0.5, 1*time.Second, 10*time.Second, 1*time.Second, 0, 0, false, 0.1)
	
	if !b.CanRetry() {
		t.Errorf("should be able to retry initially")
	}
	
	b.MarkSuccess(10 * time.Millisecond)
	b.RecordRetry()
	
	// success=1, retry=1. retry rate = 50% > 10%
	if b.CanRetry() {
		t.Errorf("should NOT be able to retry when over budget")
	}

	b2 := NewBreaker(0.5, 1*time.Second, 10*time.Second, 1*time.Second, 0, 0, false, 0)
	if !b2.CanRetry() {
		t.Errorf("should always be able to retry with 0 budget")
	}
}

func TestBreaker_Bulkhead(t *testing.T) {
	ctx := context.Background()
	b := NewBreaker(0.5, 1*time.Second, 10*time.Second, 1*time.Second, 1, 0, false, 0)
	
	if !b.Allow(ctx, 0, nil, false) {
		t.Errorf("first request should be allowed")
	}
	
	if b.Allow(ctx, 0, nil, false) {
		t.Errorf("second request should be blocked (bulkhead=1)")
	}

	// VIP should bypass
	if !b.Allow(ctx, 0, nil, true) {
		t.Errorf("VIP request should be allowed despite bulkhead")
	}
}

func TestBreaker_HalfOpen(t *testing.T) {
	// Small sleep window
	b := NewBreaker(0.1, 10*time.Millisecond, 10*time.Second, 1*time.Second, 0, 1, false, 0)
	b.MarkFailure()
	
	if b.State() != engine.StateOpen {
		t.Fatalf("expected Open state")
	}
	
	time.Sleep(20 * time.Millisecond)
	
	seed := uint64(1)
	if !b.Allow(context.Background(), 0, &seed, false) {
		t.Errorf("should allow probe in HalfOpen")
	}
	
	if b.State() != engine.StateHalfOpen {
		t.Errorf("expected HalfOpen state after probe allowed")
	}
	
	// Only one probe at a time
	if b.Allow(context.Background(), 0, &seed, false) {
		t.Errorf("should not allow second probe in HalfOpen")
	}
	
	b.MarkSuccess(10 * time.Millisecond)
	if b.State() != engine.StateClosed {
		t.Errorf("expected Closed state after successful probe")
	}
}

func TestBreaker_DryRun(t *testing.T) {
	b := NewBreaker(0.1, 1*time.Hour, 10*time.Second, 1*time.Second, 0, 1, true, 0)
	b.MarkFailure()
	
	if b.State() != engine.StateOpen {
		t.Fatalf("expected Open state")
	}
	
	// Open but DryRun, should allow
	seed := uint64(1)
	if !b.Allow(context.Background(), 0, &seed, false) {
		t.Errorf("DryRun should allow request even if Open")
	}
}
