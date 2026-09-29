package goguard

import (
	"net/http"
	"time"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/retry"
)

// Policy describes how requests to an endpoint are protected. The zero value
// of every field selects a default.
type Policy struct {
	// FailureThreshold is the failure rate (0 to 1) that opens the circuit. Default 0.5.
	FailureThreshold float64
	// MinSamples is the number of requests required in the window before the
	// circuit may open. Default 10.
	MinSamples int64
	// SamplingWindow is the period failures are measured over. Default 10s.
	SamplingWindow time.Duration
	// BucketDuration is the resolution of the window. Default SamplingWindow/10.
	BucketDuration time.Duration
	// SleepWindow is how long the circuit stays open before probing. Default 30s.
	SleepWindow time.Duration

	// MaxLatency treats slower responses as failures.
	MaxLatency time.Duration
	// OutlierFactor, when above 0, treats a response slower than this multiple
	// of the recent average latency as a failure (2 flags anything over twice
	// the average). Off by default: ordinary latency variance can otherwise
	// trip a healthy endpoint.
	OutlierFactor float64
	// RequestTimeout bounds each attempt.
	RequestTimeout time.Duration

	// MaxInflight caps concurrent requests (bulkhead); 0 is unlimited.
	MaxInflight int
	// BulkheadWaitTimeout is how long a request waits for a slot.
	BulkheadWaitTimeout time.Duration

	// MaxRetries is the number of retries for idempotent requests. 0 disables retries.
	MaxRetries int
	// RetryBudget (0 to 1) is the share of recent traffic that may be retries.
	RetryBudget float64
	// RetryBackoff gives the delay before each retry.
	RetryBackoff retry.Backoff

	// IsFailure decides whether a response counts against the endpoint.
	// Default: a transport error or a status of 500 or above.
	IsFailure func(*http.Response, error) bool

	// HeartbeatInterval enables active probing while the circuit is open.
	HeartbeatInterval time.Duration
	// HeartbeatFunc probes an endpoint; default is HEAD http://host.
	HeartbeatFunc func(host string) error

	// DryRun tracks state and emits events but never rejects a request.
	DryRun bool
}

// PolicyOption adjusts a Policy.
type PolicyOption func(*Policy)

func WithTimeout(d time.Duration) PolicyOption { return func(p *Policy) { p.RequestTimeout = d } }
func WithRetries(n int) PolicyOption           { return func(p *Policy) { p.MaxRetries = n } }
func WithRetryBudget(f float64) PolicyOption   { return func(p *Policy) { p.RetryBudget = f } }
func WithBulkhead(max int, wait time.Duration) PolicyOption {
	return func(p *Policy) {
		p.MaxInflight = max
		p.BulkheadWaitTimeout = wait
	}
}

func (p Policy) with(opts []PolicyOption) Policy {
	for _, o := range opts {
		o(&p)
	}
	return p
}

func defaultIsFailure(resp *http.Response, err error) bool {
	return err != nil || (resp != nil && resp.StatusCode >= 500)
}

func (p *Policy) applyDefaults() {
	if p.IsFailure == nil {
		p.IsFailure = defaultIsFailure
	}
}

func (p Policy) breakerConfig(name string) breaker.Config {
	return breaker.Config{
		Name:             name,
		FailureThreshold: p.FailureThreshold,
		MinSamples:       p.MinSamples,
		SamplingWindow:   p.SamplingWindow,
		BucketDuration:   p.BucketDuration,
		SleepWindow:      p.SleepWindow,
		MaxInflight:      p.MaxInflight,
		WaitTimeout:      p.BulkheadWaitTimeout,
		RetryBudget:      p.RetryBudget,
		DryRun:           p.DryRun,
	}
}

// Endpoint binds a Policy to the requests a Matcher selects.
type Endpoint struct {
	// Name identifies the endpoint in events, stats and Use. Required, unique.
	Name string
	// Match selects the requests this endpoint protects. Required.
	Match Matcher
	// Policy configures protection.
	Policy Policy
	// PerHost gives every distinct host its own circuit. By default all
	// requests matched by the endpoint share one circuit, named after it.
	PerHost bool
}

// guardAllName is the reserved name of the endpoint created by GuardAll.
const guardAllName = "*"
