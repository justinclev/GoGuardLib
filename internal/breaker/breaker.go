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

	inflight    int32
	maxInflight int32
	probing     int32
	override    int32
	dryRun      int32
	healthGated int32 // 1: only a health check may end the open state (until maxOpen)
	maxOpen     time.Duration

	OnStateChange func(from, to engine.BreakerState)
}

func NewBreaker(failureThreshold float64, sleepWindow, samplingWindow, bucketDuration time.Duration, maxInflight int, minSamples int64, dryRun bool, retryBudget float64) *Breaker {
	dr := int32(0)
	if dryRun {
		dr = 1
	}
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
		if to == engine.StateClosed {
			b.metrics.Reset() // start the recovered circuit from a clean slate
		}
		if b.OnStateChange != nil {
			b.OnStateChange(from, to)
		}
		return true
	}
	return false
}

// acquire reserves an in-flight slot. The reservation is a CAS loop so the
// bulkhead limit can never be exceeded, even under contention.
func (b *Breaker) acquire(ctx context.Context, waitTimeout time.Duration, isVIP bool) bool {
	if b.maxInflight <= 0 || isVIP {
		atomic.AddInt32(&b.inflight, 1)
		return true
	}
	if b.tryAcquire() {
		return true
	}
	if waitTimeout <= 0 {
		return false
	}
	t := time.NewTimer(waitTimeout)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return b.tryAcquire()
	}
}

func (b *Breaker) tryAcquire() bool {
	for {
		cur := atomic.LoadInt32(&b.inflight)
		if cur >= b.maxInflight {
			return false
		}
		if atomic.CompareAndSwapInt32(&b.inflight, cur, cur+1) {
			return true
		}
	}
}

// circuitAllows decides whether the circuit admits a request. It does not touch
// the in-flight counter.
func (b *Breaker) circuitAllows(shardSeed *uint64) bool {
	dryRun := atomic.LoadInt32(&b.dryRun) == 1

	switch b.state.Get() {
	case engine.StateOpen:
		now := time.Now().UnixNano()
		last := atomic.LoadInt64(&b.lastFailure)
		if atomic.LoadInt32(&b.healthGated) == 1 && (b.maxOpen <= 0 || now-last < int64(b.maxOpen)) {
			// A health check owns the way out of the open state. Real requests are
			// not spent probing a dependency that has not said it is back.
			return dryRun
		}
		jitterRange := int64(b.sleepWindow) / 10
		var jitter int64
		if jitterRange > 0 {
			jitter = int64(xorshift64(shardSeed) % uint64(jitterRange))
		}
		if now-last > int64(b.sleepWindow)+jitter {
			if b.transition(engine.StateOpen, engine.StateHalfOpen) {
				if atomic.CompareAndSwapInt32(&b.probing, 0, 1) {
					return true
				}
			}
		}
		return dryRun
	case engine.StateHalfOpen:
		if atomic.CompareAndSwapInt32(&b.probing, 0, 1) {
			return true
		}
		return dryRun
	default:
		return true
	}
}

// Verdict is the outcome of an admission decision.
type Verdict int

const (
	Admitted Verdict = iota
	RejectedOpen
	RejectedBulkhead
)

// AllowEx decides whether a request is admitted and says why not when it is
// rejected. An admitted request holds an in-flight slot until MarkSuccess,
// MarkFailure or Abandon.
func (b *Breaker) AllowEx(ctx context.Context, waitTimeout time.Duration, shardSeed *uint64, isVIP bool) Verdict {
	switch atomic.LoadInt32(&b.override) {
	case 1:
		return RejectedOpen
	case 2:
		atomic.AddInt32(&b.inflight, 1)
		return Admitted
	}

	if !b.acquire(ctx, waitTimeout, isVIP) {
		return RejectedBulkhead
	}
	if !b.circuitAllows(shardSeed) {
		atomic.AddInt32(&b.inflight, -1)
		return RejectedOpen
	}
	return Admitted
}

func (b *Breaker) Allow(ctx context.Context, waitTimeout time.Duration, shardSeed *uint64, isVIP bool) bool {
	return b.AllowEx(ctx, waitTimeout, shardSeed, isVIP) == Admitted
}

// Abandon releases an admitted request's slot without recording an outcome,
// for requests cancelled by the caller rather than failed by the dependency.
// A half-open probe that is abandoned frees the probe slot so another request
// can take over.
func (b *Breaker) Abandon() {
	atomic.AddInt32(&b.inflight, -1)
	if b.state.Get() == engine.StateHalfOpen {
		atomic.StoreInt32(&b.probing, 0)
	}
}

// Inflight returns the number of admitted requests that have not finished.
func (b *Breaker) Inflight() int32 { return atomic.LoadInt32(&b.inflight) }

// FailureRateBps returns the failure rate over the sampling window in basis points.
func (b *Breaker) FailureRateBps() int64 { return b.metrics.FailureRateBps(time.Now().UnixNano()) }

func (b *Breaker) CanRetry() bool {
	if b.retryBudgetBps <= 0 {
		return true
	}
	return b.metrics.RetryRateBps() < b.retryBudgetBps
}

func (b *Breaker) RecordRetry() {
	b.metrics.RecordRetry(time.Now().UnixNano())
}

// MarkSuccess records a successful request and releases its in-flight slot.
func (b *Breaker) MarkSuccess(latency time.Duration) {
	atomic.AddInt32(&b.inflight, -1)
	b.recordSuccess(latency)
}

// MarkFailure records a failed request and releases its in-flight slot.
func (b *Breaker) MarkFailure() {
	atomic.AddInt32(&b.inflight, -1)
	b.recordFailure()
}

// SetHealthGate hands the way out of the open state to a health check: while
// gated, an open circuit stays open until BeginProbe or ResetClosed is called.
// After maxOpen (if positive) the timer-based canary resumes anyway, so a health
// check that never reports healthy cannot block traffic forever.
func (b *Breaker) SetHealthGate(maxOpen time.Duration) {
	b.maxOpen = maxOpen
	atomic.StoreInt32(&b.healthGated, 1)
}

// BeginProbe moves an open circuit to half-open so the next request is admitted
// as a canary. It reports whether it changed anything.
func (b *Breaker) BeginProbe() bool {
	atomic.StoreInt32(&b.probing, 0)
	return b.transition(engine.StateOpen, engine.StateHalfOpen)
}

// ResetClosed closes an open or half-open circuit without waiting for a canary.
func (b *Breaker) ResetClosed() bool {
	atomic.StoreInt32(&b.probing, 0)
	return b.transition(engine.StateHalfOpen, engine.StateClosed) || b.transition(engine.StateOpen, engine.StateClosed)
}

// ReopenFromProbe returns a half-open circuit to open because a health check
// found the dependency unhealthy again. It records nothing in the window.
func (b *Breaker) ReopenFromProbe() bool {
	// Only a real reopening restarts the open clock. Refreshing it on every
	// unhealthy probe of an already open circuit would keep the MaxOpen fallback
	// from ever expiring.
	if b.transition(engine.StateHalfOpen, engine.StateOpen) {
		atomic.StoreInt64(&b.lastFailure, time.Now().UnixNano())
		atomic.StoreInt32(&b.probing, 0)
		return true
	}
	return false
}

func (b *Breaker) recordSuccess(latency time.Duration) {
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

func (b *Breaker) recordFailure() {
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

func (b *Breaker) State() engine.BreakerState      { return b.state.Get() }
func (b *Breaker) Stats() (success, failure int64) { return b.metrics.Counts() }
func (b *Breaker) SetOverride(v int)               { atomic.StoreInt32(&b.override, int32(v)) }
