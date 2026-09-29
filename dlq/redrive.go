package dlq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/obs"
	"github.com/justinclev/GoGuardLib/retry"
	"github.com/justinclev/GoGuardLib/secure"
)

// Item is a leased record handed to a RedriveHandler.
type Item struct {
	Record Record
	store  Store
	token  string
}

// Checkpoint durably saves progress so a later attempt resumes from here, and
// updates Record.Checkpoint. A multi-step handler calls it after each step.
func (it *Item) Checkpoint(ctx context.Context, checkpoint []byte) error {
	if err := it.store.Checkpoint(ctx, it.Record.ID, it.token, checkpoint); err != nil {
		return err
	}
	it.Record.Checkpoint = cloneBytes(checkpoint)
	return nil
}

// RedriveHandler reprocesses one stored record. Return:
//
//   - nil when the work is done (the record is removed);
//   - a *BlockedError, or an error matching breaker.ErrOpen, when a dependency is
//     unavailable (the record waits for that dependency);
//   - an error wrapped by retry.Permanent when retrying cannot help (the record is
//     parked for a human);
//   - any other error for a transient failure (retried with backoff, parked after
//     MaxAttempts).
//
// The context ends before the lease does. Work must be safe to see twice.
type RedriveHandler func(ctx context.Context, item *Item) error

// RedriveConfig configures a Redriver.
type RedriveConfig struct {
	// Store holds the records. Required.
	Store Store
	// Handler reprocesses a record. Required.
	Handler RedriveHandler
	// Breakers maps dependency names (a record's BlockedOn) to their circuit
	// breakers. Records blocked on a dependency whose circuit is open are not
	// leased at all, so a long outage costs no attempts and no churn.
	Breakers map[string]*breaker.Breaker

	// Workers is how many records are handled at once. Default 4.
	Workers int
	// LeaseTTL is how long a worker may hold a record. The handler's context ends
	// at 90% of it. Choose it longer than your slowest handler. Default 1 minute.
	LeaseTTL time.Duration
	// PollInterval is how often the store is checked for due records when nothing
	// wakes the redriver. Default 500ms.
	PollInterval time.Duration

	// MaxAttempts is the poison-pill limit: a record whose handling has failed
	// this many times, or whose lease has been taken this many times without
	// finishing (a worker that keeps crashing), is parked. Default 10.
	MaxAttempts int
	// Backoff is the delay before a failed record may be tried again. Default
	// exponential from 1s to 5m with jitter.
	Backoff retry.Backoff

	// Rate limits how many records are started per second across all
	// dependencies, so a recovering dependency is not hit by the whole backlog at
	// once. Zero means unlimited.
	Rate float64
	// Burst is the most records that may start together. Default max(1, Rate).
	Burst int
	// RampUp starts at 10% of Rate whenever a dependency recovers and rises to
	// the full rate over this period.
	RampUp time.Duration

	// Redactor scrubs error text before it is stored. Default secure.NewRedactor().
	Redactor *secure.Redactor
	// Events receives an obs.Redrive event per attempt. It must not block.
	Events obs.Sink
}

// RedriveStats counts what a Redriver has done.
type RedriveStats struct {
	Leased      uint64
	Succeeded   uint64
	Retried     uint64
	Blocked     uint64
	Parked      uint64
	LeaseLost   uint64
	StoreErrors uint64
	InFlight    int
}

// Redriver replays stored records when their dependencies are usable again. It
// only leases records whose dependency is available, spreads the replay over time
// with a rate limit and ramp, hands each to a handler under a lease, and settles
// the result: remove, retry with backoff, hold for a dependency, or park.
type Redriver struct {
	cfg RedriveConfig
	lim *limiter

	wake     chan struct{}
	inflight atomic.Int32

	leased, succeeded, retried, blocked, parked, leaseLost, storeErrors atomic.Uint64

	active sync.Map // IDs of records a worker is handling right now

	mu             sync.Mutex
	blockedUntil   map[string]time.Time
	wasUnavailable map[string]bool
}

// NewRedriver validates cfg and applies defaults.
func NewRedriver(cfg RedriveConfig) (*Redriver, error) {
	if cfg.Store == nil || cfg.Handler == nil {
		return nil, errors.New("dlq: RedriveConfig needs a Store and a Handler")
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = time.Minute
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 10
	}
	if cfg.Backoff == nil {
		cfg.Backoff = retry.Jitter(retry.Exponential(time.Second, 5*time.Minute), 0.5)
	}
	if cfg.Burst <= 0 {
		cfg.Burst = int(math.Max(1, math.Ceil(cfg.Rate)))
	}
	if cfg.Redactor == nil {
		cfg.Redactor = secure.NewRedactor()
	}
	return &Redriver{
		cfg:            cfg,
		lim:            newLimiter(cfg.Rate, cfg.Burst, cfg.RampUp),
		wake:           make(chan struct{}, 1),
		blockedUntil:   map[string]time.Time{},
		wasUnavailable: map[string]bool{},
	}, nil
}

// Wake makes the redriver look for work now instead of at the next poll.
func (r *Redriver) Wake() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Sink returns an event sink that wakes the redriver when a circuit stops being
// open. Add it to the breakers' Events so recovery is acted on at once.
func (r *Redriver) Sink() obs.Sink {
	return obs.SinkFunc(func(e obs.Event) {
		if sc, ok := e.(obs.StateChanged); ok && sc.To != obs.StateOpen {
			r.Wake()
		}
	})
}

// Stats returns the counters.
func (r *Redriver) Stats() RedriveStats {
	return RedriveStats{
		Leased: r.leased.Load(), Succeeded: r.succeeded.Load(), Retried: r.retried.Load(),
		Blocked: r.blocked.Load(), Parked: r.parked.Load(), LeaseLost: r.leaseLost.Load(),
		StoreErrors: r.storeErrors.Load(), InFlight: int(r.inflight.Load()),
	}
}

// Run redrives records until ctx ends, then finishes the records in hand and
// returns nil. It returns an error only if the store fails. Records that were
// leased but not finished when ctx ended are released without penalty.
func (r *Redriver) Run(ctx context.Context) error {
	work := make(chan Lease, r.capacity())
	var wg sync.WaitGroup
	for i := 0; i < r.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for l := range work {
				r.process(ctx, l)
			}
		}()
	}
	err := r.dispatch(ctx, work)
	close(work)
	wg.Wait()
	return err
}

func (r *Redriver) capacity() int { return r.cfg.Workers * 2 }

func (r *Redriver) dispatch(ctx context.Context, work chan<- Lease) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	for {
		if err := r.fill(ctx, work); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-r.wake:
		}
	}
}

// fill leases work until nothing is due, the workers are full, or the rate limit
// says wait.
func (r *Redriver) fill(ctx context.Context, work chan<- Lease) error {
	for ctx.Err() == nil {
		free := r.capacity() - int(r.inflight.Load())
		if free <= 0 {
			return nil // process() wakes us when a worker frees up
		}
		now := time.Now()
		skip := r.unavailable(now)
		granted, wait := r.lim.reserve(now, free)
		if granted == 0 {
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				return nil
			case <-t.C:
			case <-r.wake:
				t.Stop()
			}
			continue
		}
		leases, err := r.cfg.Store.Lease(ctx, LeaseRequest{Max: granted, TTL: r.cfg.LeaseTTL, Skip: skip})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			r.storeErrors.Add(1)
			return fmt.Errorf("dlq: redriver lease: %w", err)
		}
		r.lim.give(granted - len(leases))
		if len(leases) == 0 {
			return nil
		}
		queued := 0
		for _, l := range leases {
			if _, busy := r.active.LoadOrStore(l.Record.ID, struct{}{}); busy {
				// A handler that outlived its lease is still working on this record
				// and the lease was offered again. Never run one record twice at the
				// same time: hand it back untouched.
				_ = r.cfg.Store.Release(context.WithoutCancel(ctx), l.Record.ID, l.Token)
				continue
			}
			queued++
			r.inflight.Add(1)
			r.leased.Add(1)
			work <- l // never blocks: inflight is bounded by the channel's capacity
		}
		if queued == 0 {
			return nil // only stale re-offers: wait for the next poll rather than spin
		}
	}
	return nil
}

// unavailable lists the dependencies not to lease now: circuits that are open and
// dependencies that just reported themselves blocked. It also notices recoveries
// so the ramp can restart.
func (r *Redriver) unavailable(now time.Time) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var skip []string
	current := map[string]bool{}
	for name, br := range r.cfg.Breakers {
		if br.State() == obs.StateOpen {
			current[name] = true
			skip = append(skip, name)
		}
	}
	for name := range r.wasUnavailable {
		if !current[name] {
			r.lim.recovered(now)
		}
	}
	r.wasUnavailable = current
	for name, until := range r.blockedUntil {
		if !until.After(now) {
			delete(r.blockedUntil, name)
		} else if !current[name] {
			skip = append(skip, name)
		}
	}
	return skip
}

func (r *Redriver) holdDependency(name string, d time.Duration) {
	r.mu.Lock()
	r.blockedUntil[name] = time.Now().Add(d)
	r.mu.Unlock()
}

func (r *Redriver) handlerTimeout() time.Duration {
	return r.cfg.LeaseTTL - r.cfg.LeaseTTL/10
}

func (r *Redriver) emit(l Lease, dep string, o obs.RedriveOutcome, start time.Time) {
	obs.Emit(r.cfg.Events, obs.Redrive{
		Dependency: dep, RecordID: l.Record.ID, Outcome: o, Attempts: l.Record.Attempts,
		Latency: time.Since(start), At: time.Now(),
	})
}

func (r *Redriver) process(ctx context.Context, l Lease) {
	defer func() {
		r.active.Delete(l.Record.ID)
		r.inflight.Add(-1)
		r.Wake()
	}()
	start := time.Now()
	rec := l.Record
	st := r.cfg.Store

	if ctx.Err() != nil { // shutting down: hand it back untouched
		_ = st.Release(context.WithoutCancel(ctx), rec.ID, l.Token)
		return
	}
	if rec.Attempts > r.cfg.MaxAttempts {
		// Leased more times than allowed without ever finishing: whatever handles
		// it keeps dying. Stop feeding it to workers.
		r.park(ctx, l, fmt.Sprintf("leased %d times without finishing (limit %d): the handler may be crashing on it",
			rec.Attempts, r.cfg.MaxAttempts), start)
		return
	}

	hctx, cancel := context.WithTimeout(ctx, r.handlerTimeout())
	err := r.call(hctx, &Item{Record: rec, store: st, token: l.Token})
	cancel()
	r.settle(ctx, l, err, start)
}

func (r *Redriver) call(ctx context.Context, item *Item) (err error) {
	defer func() {
		if p := recover(); p != nil {
			// The panic value can contain payload data, so it is not kept.
			err = errors.New("handler panicked")
		}
	}()
	return r.cfg.Handler(ctx, item)
}

func (r *Redriver) settle(ctx context.Context, l Lease, err error, start time.Time) {
	rec := l.Record
	st := r.cfg.Store
	bg := context.WithoutCancel(ctx) // settling must complete even while shutting down
	dep := rec.BlockedOn

	var be *BlockedError
	var oe *breaker.OpenError
	switch {
	case err == nil:
		if aerr := st.Ack(bg, rec.ID, l.Token); aerr != nil {
			r.storeFailure(aerr, l, dep, start)
			return
		}
		r.succeeded.Add(1)
		r.emit(l, dep, obs.RedriveSucceeded, start)

	case errors.As(err, &be):
		if be.Dependency != "" {
			dep = be.Dependency
		}
		r.block(bg, l, dep, be.Refund, err, start)

	case errors.As(err, &oe):
		if oe.Name != "" {
			dep = oe.Name
		}
		r.block(bg, l, dep, true, err, start)

	case retry.IsPermanent(err):
		r.park(bg, l, "permanent failure: "+r.cfg.Redactor.Error(err), start)

	case ctx.Err() != nil:
		// Interrupted by shutdown, not the record's fault.
		if rerr := st.Release(bg, rec.ID, l.Token); rerr != nil {
			r.storeFailure(rerr, l, dep, start)
		}

	default:
		text := r.cfg.Redactor.Error(err)
		if rec.Attempts >= r.cfg.MaxAttempts {
			r.park(bg, l, fmt.Sprintf("gave up after %d attempts: %s", rec.Attempts, text), start)
			return
		}
		nerr := st.Nack(bg, rec.ID, l.Token, NackOptions{Delay: r.cfg.Backoff(rec.Attempts), Err: text})
		if nerr != nil {
			r.storeFailure(nerr, l, dep, start)
			return
		}
		r.retried.Add(1)
		r.emit(l, dep, obs.RedriveRetry, start)
	}
}

// block returns a record to the queue tagged with the dependency it is waiting on.
func (r *Redriver) block(ctx context.Context, l Lease, dep string, refund bool, cause error, start time.Time) {
	if !refund && l.Record.Attempts >= r.cfg.MaxAttempts {
		r.park(ctx, l, fmt.Sprintf("gave up after %d attempts: %s", l.Record.Attempts, r.cfg.Redactor.Error(cause)), start)
		return
	}
	hold := r.cfg.PollInterval * 2
	err := r.cfg.Store.Nack(ctx, l.Record.ID, l.Token, NackOptions{
		Delay: hold, Err: r.cfg.Redactor.Error(cause), BlockedOn: dep, Refund: refund,
	})
	if err != nil {
		r.storeFailure(err, l, dep, start)
		return
	}
	r.holdDependency(dep, hold) // stop leasing its other records for a moment
	r.blocked.Add(1)
	r.emit(l, dep, obs.RedriveBlocked, start)
}

func (r *Redriver) park(ctx context.Context, l Lease, reason string, start time.Time) {
	if err := r.cfg.Store.Park(context.WithoutCancel(ctx), l.Record.ID, l.Token, reason); err != nil {
		r.storeFailure(err, l, l.Record.BlockedOn, start)
		return
	}
	r.parked.Add(1)
	r.emit(l, l.Record.BlockedOn, obs.RedriveParked, start)
}

func (r *Redriver) storeFailure(err error, l Lease, dep string, start time.Time) {
	if errors.Is(err, ErrLeaseLost) || errors.Is(err, ErrNotFound) {
		r.leaseLost.Add(1)
		r.emit(l, dep, obs.RedriveLeaseLost, start)
		return
	}
	r.storeErrors.Add(1)
}

// limiter is a token bucket whose rate ramps up after a recovery.
type limiter struct {
	mu        sync.Mutex
	rate      float64
	burst     float64
	rampUp    time.Duration
	tokens    float64
	last      time.Time
	rampStart time.Time
}

func newLimiter(rate float64, burst int, rampUp time.Duration) *limiter {
	return &limiter{rate: rate, burst: float64(burst), rampUp: rampUp, tokens: float64(burst)}
}

func (l *limiter) factor(now time.Time) float64 {
	if l.rampUp <= 0 || l.rampStart.IsZero() {
		return 1
	}
	e := now.Sub(l.rampStart)
	if e >= l.rampUp {
		return 1
	}
	return 0.1 + 0.9*float64(e)/float64(l.rampUp)
}

func (l *limiter) recovered(now time.Time) {
	l.mu.Lock()
	l.rampStart = now
	l.tokens = 0 // the backlog must not burst into a dependency that just came back
	l.mu.Unlock()
}

// reserve grants up to max tokens, or says how long to wait for the next one.
func (l *limiter) reserve(now time.Time, max int) (granted int, wait time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rate <= 0 {
		return max, 0
	}
	rate := l.rate * l.factor(now)
	if !l.last.IsZero() {
		l.tokens = math.Min(l.burst, l.tokens+rate*now.Sub(l.last).Seconds())
	}
	l.last = now
	if l.tokens >= 1 {
		granted = int(math.Min(float64(max), math.Floor(l.tokens)))
		l.tokens -= float64(granted)
		return granted, 0
	}
	return 0, time.Duration((1 - l.tokens) / rate * float64(time.Second))
}

// give returns tokens that were reserved but not used.
func (l *limiter) give(n int) {
	if n <= 0 || l.rate <= 0 {
		return
	}
	l.mu.Lock()
	l.tokens = math.Min(l.burst, l.tokens+float64(n))
	l.mu.Unlock()
}
