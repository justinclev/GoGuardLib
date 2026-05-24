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
	rw.Success(now)
	rw.Success(now)
	rw.Failure(now)

	rate := rw.FailureRate(now)
	if rate != 1.0/3.0 {
		t.Errorf("Expected failure rate 0.33, got %f", rate)
	}

	// Wait for rotation
	future := now + int64(200*time.Millisecond)
	rw.Success(future)

	rate = rw.FailureRate(future)
	if rate != 1.0/4.0 {
		t.Errorf("Expected failure rate 0.25 after rotation, got %f", rate)
	}

	// Wait for full window to expire
	expiry := future + int64(1*time.Second)
	rate = rw.FailureRate(expiry)
	if rate != 0.0 {
		t.Errorf("Expected failure rate 0.0 after window expiry, got %f", rate)
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
