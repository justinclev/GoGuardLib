package breaker

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/justinclev/GoGuardLib/internal/engine"
)

func xorshift64(seed *uint64) uint64 {
	x := atomic.LoadUint64(seed)
	if x == 0 {
		x = uint64(time.Now().UnixNano())
	}
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	atomic.StoreUint64(seed, x)
	return x
}

type Breaker struct {
	state            *engine.AtomicState
	metrics          *engine.RollingWindow
	lastFailure      int64
	minSamples       int64
	seed             uint64
	failureThreshold int64
	retryBudgetBps   int64
	sleepWindow      time.Duration
	
	inflight       int32
	maxInflight    int32
	probing          int32
	override       int32
	dryRun         int32

	OnStateChange func(from, to engine.BreakerState)
}

func NewBreaker(failureThreshold float64, sleepWindow, samplingWindow, bucketDuration time.Duration, maxInflight int, minSamples int64, dryRun bool, retryBudget float64) *Breaker {
	dr := int32(0)
	if dryRun { dr = 1 }
	return &Breaker{
		state:            engine.NewAtomicState(),
		metrics:          engine.NewRollingWindow(samplingWindow, bucketDuration),
		failureThreshold: int64(failureThreshold * 10000),
		retryBudgetBps:   int64(retryBudget * 10000),
		sleepWindow:      sleepWindow,
		maxInflight:      int32(maxInflight),
		minSamples:       minSamples,
		seed:             uint64(time.Now().UnixNano()),
		dryRun:           dr,
	}
}

func (b *Breaker) transition(from, to engine.BreakerState) bool {
	if b.state.Transition(from, to) {
		if b.OnStateChange != nil {
			b.OnStateChange(from, to)
		}
		return true
	}
	return false
}

func (b *Breaker) Allow(ctx context.Context, waitTimeout time.Duration, shardSeed *uint64, isVIP bool) bool {
	override := atomic.LoadInt32(&b.override)
	if override == 1 { return false }
	if override == 2 {
		atomic.AddInt32(&b.inflight, 1)
		return true
	}

	// 1. Bulkhead Check
	if b.maxInflight > 0 && !isVIP {
		if atomic.LoadInt32(&b.inflight) >= b.maxInflight {
			if waitTimeout <= 0 { return false }
			t := time.NewTimer(waitTimeout)
			defer t.Stop()
			select {
			case <-ctx.Done(): return false
			case <-t.C:
				// Re-check after waiting
				if atomic.LoadInt32(&b.inflight) >= b.maxInflight {
					return false
				}
			}
		}
	}

	dryRun := atomic.LoadInt32(&b.dryRun) == 1
	now := time.Now().UnixNano()
	currentState := b.state.Get()

	// 2. Circuit Check
	if currentState == engine.StateOpen {
		last := atomic.LoadInt64(&b.lastFailure)
		jitterRange := int64(b.sleepWindow) / 10
		var jitter int64
		if jitterRange > 0 {
			jitter = int64(xorshift64(shardSeed) % uint64(jitterRange))
		}
		if now-last > int64(b.sleepWindow) + jitter {
			if b.transition(engine.StateOpen, engine.StateHalfOpen) {
				if atomic.CompareAndSwapInt32(&b.probing, 0, 1) {
					atomic.AddInt32(&b.inflight, 1)
					return true
				}
			}
		}
		if dryRun {
			atomic.AddInt32(&b.inflight, 1)
			return true
		}
		return false
	}

	if currentState == engine.StateHalfOpen {
		if atomic.CompareAndSwapInt32(&b.probing, 0, 1) {
			atomic.AddInt32(&b.inflight, 1)
			return true
		}
		if dryRun {
			atomic.AddInt32(&b.inflight, 1)
			return true
		}
		return false
	}

	atomic.AddInt32(&b.inflight, 1)
	return true
}

func (b *Breaker) CanRetry() bool {
	if b.retryBudgetBps <= 0 { return true }
	return b.metrics.RetryRateBps() < b.retryBudgetBps
}

func (b *Breaker) RecordRetry() {
	b.metrics.RecordRetry(time.Now().UnixNano())
}

func (b *Breaker) MarkSuccess(latency time.Duration) {
	atomic.AddInt32(&b.inflight, -1)
	now := time.Now().UnixNano()
	lUs := latency.Microseconds()
	if lUs <= 0 {
		b.metrics.Success(now, -1)
	} else {
		b.metrics.Success(now, lUs)
	}

	if b.state.Get() == engine.StateHalfOpen {
		if b.transition(engine.StateHalfOpen, engine.StateClosed) {
			atomic.StoreInt32(&b.probing, 0)
		}
	}
}

func (b *Breaker) MarkFailure() {
	atomic.AddInt32(&b.inflight, -1)
	now := time.Now().UnixNano()
	b.metrics.Failure(now)
	atomic.StoreInt64(&b.lastFailure, now)

	if b.state.Get() == engine.StateHalfOpen {
		if b.transition(engine.StateHalfOpen, engine.StateOpen) {
			atomic.StoreInt32(&b.probing, 0)
		}
		return
	}

	success, failure := b.metrics.Counts()
	if b.minSamples > 0 && (success+failure) < b.minSamples {
		return
	}

	if b.metrics.FailureRateBps(now) >= b.failureThreshold {
		b.transition(engine.StateClosed, engine.StateOpen)
	}
}

func (b *Breaker) AvgLatency() time.Duration {
	return time.Duration(b.metrics.AvgLatencyUs()) * time.Microsecond
}

func (b *Breaker) State() engine.BreakerState { return b.state.Get() }
func (b *Breaker) Stats() (success, failure int64) { return b.metrics.Counts() }
func (b *Breaker) SetOverride(v int) { atomic.StoreInt32(&b.override, int32(v)) }
