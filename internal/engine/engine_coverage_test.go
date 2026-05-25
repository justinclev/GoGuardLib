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
