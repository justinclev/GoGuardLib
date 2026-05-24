package state

import (
	"testing"
)

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

	if s.Transition(StateClosed, StateHalfOpen) {
		t.Error("Invalid transition allowed")
	}
}
