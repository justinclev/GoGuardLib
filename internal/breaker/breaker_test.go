package breaker

import (
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/internal/engine"
)

func TestBreakerTransitions(t *testing.T) {
	b := NewBreaker(0.5, 100*time.Millisecond, 1*time.Second, 100*time.Millisecond)

	// Closed -> Open
	for i := 0; i < 10; i++ {
		b.MarkFailure()
	}
	if b.state.Get() != engine.StateOpen {
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
	if b.state.Get() != engine.StateHalfOpen {
		t.Error("Expected StateHalfOpen")
	}

	// Probing (Single-flight)
	if b.Allow() {
		t.Error("Expected only one request allowed in HalfOpen")
	}

	// HalfOpen -> Open (failure)
	b.MarkFailure()
	if b.state.Get() != engine.StateOpen {
		t.Error("Expected StateOpen after failure in HalfOpen")
	}
}
