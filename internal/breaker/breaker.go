package breaker

import (
	"sync"
	"time"

	"github.com/justinclev/GoGuardLib/internal/metrics"
	"github.com/justinclev/GoGuardLib/internal/state"
)

type Breaker struct {
	mu               sync.Mutex
	state            *state.AtomicState
	metrics          *metrics.RollingWindow
	failureThreshold float64
	sleepWindow      time.Duration
	lastFailure      time.Time
}

func NewBreaker(failureThreshold float64, sleepWindow, samplingWindow time.Duration) *Breaker {
	return &Breaker{
		state:            state.NewAtomicState(),
		metrics:          metrics.NewRollingWindow(samplingWindow, samplingWindow/10),
		failureThreshold: failureThreshold,
		sleepWindow:      sleepWindow,
	}
}

func (b *Breaker) Allow() bool {
	currentState := b.state.Get()

	if currentState == state.StateOpen {
		if time.Since(b.lastFailure) > b.sleepWindow {
			if b.state.Transition(state.StateOpen, state.StateHalfOpen) {
				return true
			}
		}
		return false
	}
	return true
}

func (b *Breaker) MarkSuccess() {
	currentState := b.state.Get()
	b.metrics.Success()

	if currentState == state.StateHalfOpen {
		b.state.Transition(state.StateHalfOpen, state.StateClosed)
	}
}

func (b *Breaker) MarkFailure() {
	b.metrics.Failure()
	b.lastFailure = time.Now()

	if b.state.Get() == state.StateHalfOpen {
		b.state.Transition(state.StateHalfOpen, state.StateOpen)
		return
	}

	if b.metrics.FailureRate() >= b.failureThreshold {
		b.state.Transition(state.StateClosed, state.StateOpen)
	}
}
