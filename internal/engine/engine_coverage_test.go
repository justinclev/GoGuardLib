package engine

import (
	"testing"
	"time"
)

func TestRollingWindow_RecordRetry(t *testing.T) {
	w := NewRollingWindow(10*time.Second, 1*time.Second)
	now := time.Now().UnixNano()

	w.RecordRetry(now)
	if w.totalRetries != 1 {
		t.Errorf("expected 1 total retry, got %d", w.totalRetries)
	}
}

func TestRollingWindow_RetryRateBps(t *testing.T) {
	w := NewRollingWindow(10*time.Second, 1*time.Second)
	now := time.Now().UnixNano()

	w.Success(now, 100)
	w.RecordRetry(now)

	// success=1, retry=1. rate = 1 / (1+1) = 0.5 = 5000 bps
	rate := w.RetryRateBps()
	if rate != 5000 {
		t.Errorf("expected 5000 bps retry rate, got %d", rate)
	}
}

func TestRollingWindow_Counts(t *testing.T) {
	w := NewRollingWindow(10*time.Second, 1*time.Second)
	now := time.Now().UnixNano()

	w.Success(now, 100)
	w.Failure(now)

	s, f := w.Counts()
	if s != 1 || f != 1 {
		t.Errorf("expected 1 success and 1 failure, got %d and %d", s, f)
	}
}

func TestRollingWindow_Rotate_Full(t *testing.T) {
	w := NewRollingWindow(2*time.Second, 1*time.Second)
	now := time.Now().UnixNano()

	w.Success(now, 100)
	if w.totalSuccess != 1 {
		t.Errorf("expected 1 success")
	}

	// Rotate past the window
	later := now + int64(3*time.Second)
	w.rotate(later)

	if w.totalSuccess != 0 {
		t.Errorf("expected 0 success after full rotation, got %d", w.totalSuccess)
	}
}

func TestBreakerStateString(t *testing.T) {
	for s, want := range map[BreakerState]string{StateClosed: "closed", StateOpen: "open", StateHalfOpen: "half-open", 42: "unknown"} {
		if s.String() != want {
			t.Errorf("%d -> %q, want %q", int(s), s.String(), want)
		}
	}
}

// When every request fails there are no successes; retries must still count
// against the budget or a full outage would trigger unlimited retries.
func TestRetryRateCountsFailuresAsTraffic(t *testing.T) {
	w := NewRollingWindow(time.Second, 100*time.Millisecond)
	now := time.Now().UnixNano()
	if w.RetryRateBps() != 0 {
		t.Fatal("empty window must report 0")
	}
	for i := 0; i < 8; i++ {
		w.Failure(now)
	}
	for i := 0; i < 2; i++ {
		w.RecordRetry(now)
	}
	if got := w.RetryRateBps(); got != 2000 { // 2 retries / 10 total
		t.Fatalf("RetryRateBps = %d, want 2000", got)
	}
}
