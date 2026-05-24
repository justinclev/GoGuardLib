package breaker

import (
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/internal/state"
)

func TestBreakerTransitions(t *testing.T) {
	b := NewBreaker(0.5, 100*time.Millisecond, 1*time.Second)

	// Closed -> Open
	for i := 0; i < 10; i++ {
		b.MarkFailure()
	}
	if b.state.Get() != state.StateOpen {
		t.Error("Expected StateOpen after failures")
	}

	// Open -> HalfOpen
	if b.Allow() {
		t.Error("Expected Allow to be false in Open state")
	}
	time.Sleep(150 * time.Millisecond)
	if !b.Allow() {
		t.Error("Expected Allow to be true in HalfOpen state")
	}
	if b.state.Get() != state.StateHalfOpen {
		t.Error("Expected StateHalfOpen")
	}

	// HalfOpen -> Open (failure)
	b.MarkFailure()
	if b.state.Get() != state.StateOpen {
		t.Error("Expected StateOpen after failure in HalfOpen")
	}

	// Recover to Closed
	time.Sleep(150 * time.Millisecond)
	b.Allow()
	b.MarkSuccess()
	if b.state.Get() != state.StateClosed {
		t.Error("Expected StateClosed after success in HalfOpen")
	}
}
