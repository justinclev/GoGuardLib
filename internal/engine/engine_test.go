package engine

import (
	"testing"
	"time"
)

func TestRollingWindow(t *testing.T) {
	window := 1 * time.Second
	bucket := 100 * time.Millisecond
	rw := NewRollingWindow(window, bucket)

	now := time.Now().UnixNano()
	rw.Success(now, 100)
	rw.Success(now, 200)
	rw.Failure(now)

	rate := rw.FailureRateBps(now)
	if rate != 3333 {
		t.Errorf("Expected failure rate 3333, got %d", rate)
	}

	if avg := rw.AvgLatencyUs(); avg != 150 {
		t.Errorf("Expected avg latency 150, got %d", avg)
	}

	// Wait for rotation
	future := now + int64(200*time.Millisecond)
	rw.Success(future, 100)

	rate = rw.FailureRateBps(future)
	if rate != 2500 {
		t.Errorf("Expected failure rate 2500 after rotation, got %d", rate)
	}
}

func TestAtomicState(t *testing.T) {
	s := NewAtomicState()
	if s.Get() != StateClosed {
		t.Error("Initial state not StateClosed")
	}

	if !s.Transition(StateClosed, StateOpen) {
		t.Error("Failed to transition Closed -> Open")
	}
	if s.Get() != StateOpen {
		t.Error("State not Open")
	}
}
