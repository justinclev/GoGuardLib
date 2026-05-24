package breaker

import (
	"sync/atomic"
	"time"

	"github.com/justinclev/GoGuardLib/internal/engine"
)

type Breaker struct {
	state            *engine.AtomicState
	metrics          *engine.RollingWindow
	failureThreshold float64
	sleepWindow      time.Duration
	lastFailure      int64 // UnixNano
	probing          int32 // Atomic flag for single-flight in HalfOpen
}

func NewBreaker(failureThreshold float64, sleepWindow, samplingWindow, bucketDuration time.Duration) *Breaker {
	return &Breaker{
		state:            engine.NewAtomicState(),
		metrics:          engine.NewRollingWindow(samplingWindow, bucketDuration),
		failureThreshold: failureThreshold,
		sleepWindow:      sleepWindow,
	}
}

func (b *Breaker) Allow() bool {
	now := time.Now().UnixNano()
	currentState := b.state.Get()

	if currentState == engine.StateOpen {
		last := atomic.LoadInt64(&b.lastFailure)
		if now-last > int64(b.sleepWindow) {
			if b.state.Transition(engine.StateOpen, engine.StateHalfOpen) {
				atomic.StoreInt32(&b.probing, 1)
				return true
			}
		}
		return false
	}

	if currentState == engine.StateHalfOpen {
		// Only allow one request to probe
		return atomic.CompareAndSwapInt32(&b.probing, 0, 1)
	}

	return true
}

func (b *Breaker) MarkSuccess() {
	now := time.Now().UnixNano()
	b.metrics.Success(now)

	if b.state.Get() == engine.StateHalfOpen {
		if b.state.Transition(engine.StateHalfOpen, engine.StateClosed) {
			atomic.StoreInt32(&b.probing, 0)
		}
	}
}

func (b *Breaker) MarkFailure() {
	now := time.Now().UnixNano()
	b.metrics.Failure(now)
	atomic.StoreInt64(&b.lastFailure, now)

	if b.state.Get() == engine.StateHalfOpen {
		if b.state.Transition(engine.StateHalfOpen, engine.StateOpen) {
			atomic.StoreInt32(&b.probing, 0)
		}
		return
	}

	if b.metrics.FailureRate(now) >= b.failureThreshold {
		b.state.Transition(engine.StateClosed, engine.StateOpen)
	}
}
