package goguard

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/health"
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

	// Health makes recovery depend on the endpoint's own health check instead of
	// a timer: while the circuit is open it is probed with backoff, and only after
	// enough healthy checks does real traffic resume as a canary. If Check is nil,
	// HealthPath supplies it. See package health.
	Health *health.Config
	// HealthPath (for example "/health") builds an HTTP health check against the
	// same scheme and host as the traffic, so one policy works for many hosts
	// (GuardAll, PerHost). Probes use the underlying transport, never the guarded
	// one, and follow no redirects. A circuit shared by several hosts (an endpoint
	// without PerHost) probes the host of its first request; give such an endpoint
	// PerHost, or a Health.Check that knows what to ask.
	HealthPath string
	// HealthHeader is sent with HealthPath probes, for an auth token the health
	// endpoint needs. Nothing else from the request is ever copied. It goes to every
	// host the endpoint's circuits probe, so use it only on endpoints whose Match
	// selects hosts you trust with that credential; GuardAll refuses it.
	HealthHeader http.Header

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

func (p *Policy) validate(name string) error {
	if p.Health != nil && p.Health.Check == nil && p.HealthPath == "" {
		return fmt.Errorf("goguard: endpoint %q sets Health without a Check or HealthPath", name)
	}
	if p.HealthPath != "" && !strings.HasPrefix(p.HealthPath, "/") {
		return fmt.Errorf("goguard: endpoint %q HealthPath must start with \"/\"", name)
	}
	return nil
}

func (p *Policy) applyDefaults() {
	if p.IsFailure == nil {
		p.IsFailure = defaultIsFailure
	}
}

func (p Policy) breakerConfig(name string) breaker.Config { // Health is attached per circuit

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
