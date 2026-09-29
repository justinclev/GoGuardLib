// Package breaker is a circuit breaker for any operation that can fail: an
// HTTP call, a database query, a message handler.
//
// Wrap calls with Do or Call, or use Acquire when success or failure is only
// known later. While a dependency is failing the breaker opens and rejects
// calls immediately instead of piling load onto it; after a sleep window it
// lets a single probe through and closes again when the probe succeeds.
//
// The package never logs. State transitions are delivered to Config.Events and
// counters are read with Stats.
package breaker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	ibreaker "github.com/justinclev/GoGuardLib/internal/breaker"
	"github.com/justinclev/GoGuardLib/obs"
)

// State is the state of a circuit breaker.
type State = obs.State

const (
	StateClosed   = obs.StateClosed
	StateOpen     = obs.StateOpen
	StateHalfOpen = obs.StateHalfOpen
)

var (
	// ErrOpen matches errors returned when the circuit rejects a call.
	ErrOpen = errors.New("circuit breaker is open")
	// ErrBulkhead matches errors returned when the concurrency limit rejects a call.
	ErrBulkhead = errors.New("bulkhead is full")
)

// OpenError is returned when the circuit is open. It matches ErrOpen with
// errors.Is and is never worth retrying immediately.
type OpenError struct {
	Name  string
	State State
}

func (e *OpenError) Error() string {
	return fmt.Sprintf("goguard: circuit breaker %q is %s", e.Name, e.State)
}
func (e *OpenError) Is(target error) bool { return target == ErrOpen }
func (e *OpenError) NonRetryable() bool   { return true }

// BulkheadError is returned when too many calls are already in flight. It
// matches ErrBulkhead with errors.Is.
type BulkheadError struct{ Name string }

func (e *BulkheadError) Error() string {
	return fmt.Sprintf("goguard: bulkhead %q is full", e.Name)
}
func (e *BulkheadError) Is(target error) bool { return target == ErrBulkhead }

// Override forces a breaker's behaviour regardless of measured health.
type Override int

const (
	OverrideNone        Override = 0 // normal operation
	OverrideForceOpen   Override = 1 // reject everything
	OverrideForceClosed Override = 2 // admit everything
)

// Config configures a Breaker. The zero value of every field selects a default.
type Config struct {
	// Name identifies the breaker in errors, events and stats.
	Name string
	// FailureThreshold is the failure rate (0 to 1) that opens the circuit. Default 0.5.
	FailureThreshold float64
	// MinSamples is the number of calls required in the window before the
	// circuit may open, so a single early failure cannot trip it. Default 10.
	MinSamples int64
	// SamplingWindow is the period over which failures are measured. Default 10s.
	SamplingWindow time.Duration
	// BucketDuration is the resolution of the window. Default SamplingWindow/10.
	BucketDuration time.Duration
	// SleepWindow is how long the circuit stays open before probing. Default 30s.
	SleepWindow time.Duration
	// MaxInflight caps concurrent calls (bulkhead); 0 means unlimited.
	MaxInflight int
	// WaitTimeout is how long a call waits for a bulkhead slot; 0 rejects at once.
	WaitTimeout time.Duration
	// RetryBudget (0 to 1) is the share of recent traffic that may be retries,
	// consulted by CanRetry. 0 disables the check.
	RetryBudget float64
	// DryRun records everything and emits events but never rejects a call.
	DryRun bool
	// IsFailure decides whether an error counts against the dependency.
	// Default: any non-nil error. Errors caused by the caller cancelling its own
	// context are never counted.
	IsFailure func(error) bool
	// Events receives StateChanged events. Emit must not block; wrap slow
	// consumers in an obs.Dispatcher.
	Events obs.Sink
}

func (c *Config) applyDefaults() {
	if c.FailureThreshold <= 0 {
		c.FailureThreshold = 0.5
	}
	if c.MinSamples <= 0 {
		c.MinSamples = 10
	}
	if c.SamplingWindow <= 0 {
		c.SamplingWindow = 10 * time.Second
	}
	if c.BucketDuration <= 0 {
		c.BucketDuration = c.SamplingWindow / 10
	}
	if c.BucketDuration <= 0 {
		c.BucketDuration = time.Second
	}
	if c.SleepWindow <= 0 {
		c.SleepWindow = 30 * time.Second
	}
	if c.IsFailure == nil {
		c.IsFailure = func(err error) bool { return err != nil }
	}
}

// Stats is a point-in-time view of a breaker.
type Stats struct {
	Name             string
	State            State
	Success          int64 // in the sampling window
	Failure          int64 // in the sampling window
	FailureRateBps   int64 // failure rate in basis points (10000 = 100%)
	Inflight         int32
	Rejected         uint64 // calls rejected by the open circuit, lifetime
	RejectedBulkhead uint64 // calls rejected by the bulkhead, lifetime
	Opens            uint64 // times the circuit opened, lifetime
	LastTransition   time.Time
	AvgLatency       time.Duration
	Override         Override
}

// Breaker is safe for concurrent use.
type Breaker struct {
	cfg      Config
	in       *ibreaker.Breaker
	seed     uint64
	override int32

	rejected     atomic.Uint64
	rejectedBulk atomic.Uint64
	opens        atomic.Uint64
	lastChange   atomic.Int64
}

// New creates a Breaker.
func New(cfg Config) *Breaker {
	cfg.applyDefaults()
	b := &Breaker{cfg: cfg, seed: uint64(time.Now().UnixNano())}
	b.in = ibreaker.NewBreaker(cfg.FailureThreshold, cfg.SleepWindow, cfg.SamplingWindow,
		cfg.BucketDuration, cfg.MaxInflight, cfg.MinSamples, cfg.DryRun, cfg.RetryBudget)
	b.in.OnStateChange = func(from, to State) {
		now := time.Now()
		b.lastChange.Store(now.UnixNano())
		if to == StateOpen {
			b.opens.Add(1)
		}
		obs.Emit(cfg.Events, obs.StateChanged{Dependency: cfg.Name, From: from, To: to, At: now})
	}
	return b
}

// Name returns the configured name.
func (b *Breaker) Name() string { return b.cfg.Name }

// State returns the current state.
func (b *Breaker) State() State { return b.in.State() }

// SetOverride forces the breaker open or closed, or restores normal operation.
func (b *Breaker) SetOverride(o Override) {
	atomic.StoreInt32(&b.override, int32(o))
	b.in.SetOverride(int(o))
}

// Stats returns a snapshot of the breaker.
func (b *Breaker) Stats() Stats {
	success, failure := b.in.Stats()
	var last time.Time
	if ns := b.lastChange.Load(); ns != 0 {
		last = time.Unix(0, ns)
	}
	return Stats{
		Name:             b.cfg.Name,
		State:            b.in.State(),
		Success:          success,
		Failure:          failure,
		FailureRateBps:   b.in.FailureRateBps(),
		Inflight:         b.in.Inflight(),
		Rejected:         b.rejected.Load(),
		RejectedBulkhead: b.rejectedBulk.Load(),
		Opens:            b.opens.Load(),
		LastTransition:   last,
		AvgLatency:       b.in.AvgLatency(),
		Override:         Override(atomic.LoadInt32(&b.override)),
	}
}

// AvgLatency returns the mean latency of successful calls in the window.
func (b *Breaker) AvgLatency() time.Duration { return b.in.AvgLatency() }

// CanRetry reports whether the retry budget allows another retry.
func (b *Breaker) CanRetry() bool { return b.in.CanRetry() }

// RecordRetry counts a retry against the retry budget.
func (b *Breaker) RecordRetry() { b.in.RecordRetry() }

// RecordProbe records the result of an out-of-band health probe. Probes never
// held a call slot, so they do not touch the in-flight count. A healthy probe
// closes a half-open circuit; an unhealthy one reopens it.
func (b *Breaker) RecordProbe(healthy bool) {
	if healthy {
		b.in.ProbeSuccess()
	} else {
		b.in.ProbeFailure()
	}
}

// Permit is an admitted call. Exactly one of Success, Failure or Abandon takes
// effect; later calls are ignored.
type Permit struct {
	b    *Breaker
	done atomic.Bool
}

// Success records a successful call and how long it took.
func (p *Permit) Success(latency time.Duration) {
	if p.done.CompareAndSwap(false, true) {
		p.b.in.MarkSuccess(latency)
	}
}

// Failure records a failed call.
func (p *Permit) Failure() {
	if p.done.CompareAndSwap(false, true) {
		p.b.in.MarkFailure()
	}
}

// Abandon releases the call without recording an outcome, for calls the caller
// cancelled itself.
func (p *Permit) Abandon() {
	if p.done.CompareAndSwap(false, true) {
		p.b.in.Abandon()
	}
}

// Acquire asks the breaker to admit a call. On success the caller must finish
// the returned Permit. vip calls bypass the bulkhead (never the circuit).
// Rejections are *OpenError or *BulkheadError.
func (b *Breaker) Acquire(ctx context.Context, vip bool) (*Permit, error) {
	switch b.in.AllowEx(ctx, b.cfg.WaitTimeout, &b.seed, vip) {
	case ibreaker.Admitted:
		return &Permit{b: b}, nil
	case ibreaker.RejectedBulkhead:
		b.rejectedBulk.Add(1)
		return nil, &BulkheadError{Name: b.cfg.Name}
	default:
		b.rejected.Add(1)
		return nil, &OpenError{Name: b.cfg.Name, State: b.in.State()}
	}
}

// Do runs fn if the breaker admits it and records the outcome. A panic in fn
// counts as a failure and is re-raised.
func (b *Breaker) Do(ctx context.Context, fn func(context.Context) error) error {
	p, err := b.Acquire(ctx, false)
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			p.Failure()
			panic(r)
		}
	}()

	start := time.Now()
	err = fn(ctx)
	b.finish(ctx, p, err, time.Since(start))
	return err
}

// Call is Do for functions that return a value.
func Call[T any](ctx context.Context, b *Breaker, fn func(context.Context) (T, error)) (T, error) {
	var out T
	err := b.Do(ctx, func(ctx context.Context) error {
		var e error
		out, e = fn(ctx)
		return e
	})
	return out, err
}

func (b *Breaker) finish(ctx context.Context, p *Permit, err error, latency time.Duration) {
	switch {
	case err == nil:
		p.Success(latency)
	case errors.Is(err, context.Canceled) && ctx.Err() != nil:
		p.Abandon()
	case b.cfg.IsFailure(err):
		p.Failure()
	default:
		// The dependency answered; the error is the caller's business.
		p.Success(latency)
	}
}
