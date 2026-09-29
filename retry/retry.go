// Package retry runs an operation again after transient failures, with
// backoff, jitter and an optional budget that stops retries from amplifying an
// outage.
package retry

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

// Backoff returns the delay before retry number attempt (1 is the first retry).
type Backoff func(attempt int) time.Duration

// Constant waits d before every retry.
func Constant(d time.Duration) Backoff {
	return func(int) time.Duration { return d }
}

// Exponential doubles the delay on each retry, starting at base and never
// exceeding max (max <= 0 means uncapped).
func Exponential(base, max time.Duration) Backoff {
	return func(attempt int) time.Duration {
		if attempt < 1 || base <= 0 {
			return 0
		}
		d := base
		for i := 1; i < attempt; i++ {
			if max > 0 && d >= max {
				break
			}
			if d > time.Duration(1<<62)/2 {
				break // avoid overflow
			}
			d *= 2
		}
		if max > 0 && d > max {
			d = max
		}
		return d
	}
}

// Jitter randomises b so that many clients retrying together do not
// synchronise. fraction is clamped to [0, 1]: 0 leaves the delay unchanged and
// 1 picks uniformly from [0, delay] ("full jitter").
func Jitter(b Backoff, fraction float64) Backoff {
	if fraction <= 0 {
		return b
	}
	if fraction > 1 {
		fraction = 1
	}
	return func(attempt int) time.Duration {
		d := b(attempt)
		if d <= 0 {
			return d
		}
		spread := time.Duration(float64(d) * fraction)
		return d - spread + time.Duration(rand.Int64N(int64(spread)+1))
	}
}

// Budget limits retries to a fraction of request volume: every request adds
// ratio tokens and every retry spends one, up to a burst cap. During an outage
// the budget drains and retries stop, so retries cannot multiply the load on a
// struggling dependency. A nil *Budget allows every retry.
type Budget struct {
	mu     sync.Mutex
	tokens float64
	max    float64
	ratio  float64
}

// NewBudget returns a budget that starts full at burst tokens and earns ratio
// tokens per request (ratio 0.1 allows roughly one retry per ten requests).
func NewBudget(ratio float64, burst int) *Budget {
	if burst < 1 {
		burst = 1
	}
	return &Budget{tokens: float64(burst), max: float64(burst), ratio: ratio}
}

// Deposit records a request.
func (b *Budget) Deposit() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.tokens += b.ratio
	if b.tokens > b.max {
		b.tokens = b.max
	}
	b.mu.Unlock()
}

// TryWithdraw spends one token, reporting whether a retry is allowed.
func (b *Budget) TryWithdraw() bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

type permanentError struct{ err error }

func (p *permanentError) Error() string { return p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Permanent marks err as not worth retrying. Do returns the original err.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// nonRetryable is implemented by errors that should never be retried, such as a
// rejection by an open circuit breaker.
type nonRetryable interface{ NonRetryable() bool }

// DefaultRetryable retries everything except permanent errors, cancellation
// and errors that declare themselves non-retryable.
func DefaultRetryable(err error) bool {
	if err == nil || IsPermanent(err) || errors.Is(err, context.Canceled) {
		return false
	}
	var nr nonRetryable
	if errors.As(err, &nr) && nr.NonRetryable() {
		return false
	}
	return true
}

// Policy describes how to retry.
type Policy struct {
	// MaxRetries is the number of retries after the first attempt (0 disables retrying).
	MaxRetries int
	// Backoff gives the delay before each retry; nil retries immediately.
	Backoff Backoff
	// Budget optionally caps retries relative to request volume.
	Budget *Budget
	// Retryable decides whether an error is worth retrying; nil uses DefaultRetryable.
	Retryable func(error) bool
	// OnRetry is called before waiting to retry. It must not block.
	OnRetry func(attempt int, delay time.Duration, err error)
}

// Do calls fn until it succeeds, returns a non-retryable error, exhausts the
// retries or budget, or ctx ends. It returns the last error from fn.
func Do(ctx context.Context, p Policy, fn func(context.Context) error) error {
	p.Budget.Deposit()
	retryable := p.Retryable
	if retryable == nil {
		retryable = DefaultRetryable
	}

	for attempt := 0; ; attempt++ {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil || attempt >= p.MaxRetries || !retryable(err) || !p.Budget.TryWithdraw() {
			return unwrapPermanent(err)
		}

		var delay time.Duration
		if p.Backoff != nil {
			delay = p.Backoff(attempt + 1)
		}
		if p.OnRetry != nil {
			p.OnRetry(attempt+1, delay, err)
		}
		if delay > 0 {
			t := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				t.Stop()
				return unwrapPermanent(err)
			case <-t.C:
			}
		}
	}
}

// unwrapPermanent strips a Permanent marker applied directly to err. A marker
// buried inside a wrapping chain is left alone so outer context is not lost.
func unwrapPermanent(err error) error {
	if p, ok := err.(*permanentError); ok { //nolint:errorlint // top-level only, by design
		return p.err
	}
	return err
}
