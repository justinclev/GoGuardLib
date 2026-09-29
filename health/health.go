// Package health decides when a failed dependency has recovered by asking the
// dependency itself, instead of spending real requests as probes.
//
// A circuit breaker configured with a health check probes only while it is open:
// a healthy dependency sees no probe traffic. Probes back off with jitter while
// the dependency is down so a struggling service is not hammered and many
// instances do not probe in lockstep. After enough consecutive healthy checks the
// breaker lets real traffic through as a canary before closing.
//
// The package never logs. Each probe is reported as an obs.ProbeResult carrying
// only its outcome, status and latency: never the URL or error text.
package health

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/justinclev/GoGuardLib/obs"
	"github.com/justinclev/GoGuardLib/retry"
)

// Check reports whether the dependency is healthy: nil means healthy.
type Check func(ctx context.Context) error

// Config configures probing. The zero value of every field selects a default.
type Config struct {
	// Check probes the dependency. Required (see HTTP).
	Check Check
	// Interval is the pause between probes. It is also the start of the backoff
	// while probes fail. Default 5s.
	Interval time.Duration
	// MaxInterval caps the backoff. Default 1 minute.
	MaxInterval time.Duration
	// Timeout bounds each probe. Default 3s.
	Timeout time.Duration
	// SuccessThreshold is how many healthy probes in a row are needed before the
	// dependency is considered recovered. Default 2.
	SuccessThreshold int
	// Jitter randomises each pause by up to this fraction (0 to 1) so instances do
	// not probe together. Default 0.2; a negative value disables jitter.
	Jitter float64
	// MaxOpen is a safety valve for a health check that lies: after the circuit
	// has been open this long the breaker resumes timer-based canaries whatever the
	// check says, so a wrong /health can never block traffic forever. Default 5
	// minutes; a negative value disables it.
	MaxOpen time.Duration
	// TrustHealth closes the circuit as soon as the threshold is met, without a
	// real canary request. Off by default: a dependency can answer /health and
	// still fail real work.
	TrustHealth bool
}

// WithDefaults returns c with defaults filled in.
func (c Config) WithDefaults() Config {
	if c.Interval <= 0 {
		c.Interval = 5 * time.Second
	}
	if c.MaxInterval <= 0 {
		c.MaxInterval = time.Minute
	}
	if c.MaxInterval < c.Interval {
		c.MaxInterval = c.Interval
	}
	if c.Timeout <= 0 {
		c.Timeout = 3 * time.Second
	}
	if c.SuccessThreshold <= 0 {
		c.SuccessThreshold = 2
	}
	if c.Jitter == 0 {
		c.Jitter = 0.2
	}
	if c.MaxOpen == 0 {
		c.MaxOpen = 5 * time.Minute
	}
	return c
}

// Target is what is being probed: a circuit breaker, or anything with the same
// needs.
type Target interface {
	// Active reports whether probing is still needed. Run returns once it is not.
	Active() bool
	// Generation changes each time the target fails again, so a run of healthy
	// probes from before a relapse is not counted toward the next recovery.
	Generation() uint64
	// Result delivers one probe outcome and the current run of healthy probes.
	Result(healthy bool, consecutive int)
}

// delay is the pause before the next probe: a steady interval while probes pass
// (to reach the threshold quickly), an exponential backoff while they fail.
func delay(cfg Config, lastOK bool, failures int) time.Duration {
	var b retry.Backoff
	if lastOK || failures == 0 {
		b = retry.Constant(cfg.Interval)
	} else {
		b = retry.Exponential(cfg.Interval, cfg.MaxInterval)
	}
	return retry.Jitter(b, cfg.Jitter)(max(failures, 1))
}

// Run probes until ctx ends or the target no longer needs probing. name labels
// the events. cfg must have had WithDefaults applied. It blocks; run it in a
// goroutine.
func Run(ctx context.Context, cfg Config, name string, t Target, sink obs.Sink) {
	var consecutive, failures int
	gen := t.Generation()
	lastOK := false
	for {
		timer := time.NewTimer(delay(cfg, lastOK, failures))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if !t.Active() {
			return
		}
		if g := t.Generation(); g != gen { // the target failed again: start counting afresh
			gen, consecutive, failures = g, 0, 0
		}

		ok, status, latency := probe(ctx, cfg)
		if ctx.Err() != nil {
			return
		}
		if ok {
			consecutive++
			failures = 0
		} else {
			consecutive = 0
			failures++
		}
		lastOK = ok
		obs.Emit(sink, obs.ProbeResult{Dependency: name, OK: ok, Status: status, Latency: latency, Consecutive: consecutive, At: time.Now()})
		t.Result(ok, consecutive)
	}
}

// probe runs one check. A panicking or timed-out check counts as unhealthy.
func probe(ctx context.Context, cfg Config) (ok bool, status int, latency time.Duration) {
	cctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	start := time.Now()
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("health check panicked: %v", r)
			}
		}()
		err = cfg.Check(cctx)
	}()
	latency = time.Since(start)
	if err == nil {
		return true, 0, latency
	}
	var se *StatusError
	if errors.As(err, &se) {
		status = se.Code
	}
	return false, status, latency
}

// StatusError is returned by an HTTP check when the status is not healthy.
type StatusError struct{ Code int }

func (e *StatusError) Error() string { return fmt.Sprintf("health check returned status %d", e.Code) }

type httpCheck struct {
	method  string
	header  http.Header
	rt      http.RoundTripper
	healthy func(status int) bool
}

// HTTPOption customises an HTTP check.
type HTTPOption func(*httpCheck)

// WithMethod sets the request method. Default GET.
func WithMethod(m string) HTTPOption { return func(c *httpCheck) { c.method = m } }

// WithHeader adds a request header, for example an auth token the health
// endpoint requires. Only headers set here are ever sent.
func WithHeader(key, value string) HTTPOption {
	return func(c *httpCheck) { c.header.Add(key, value) }
}

// WithRoundTripper sets the transport used for probes. Give it the transport
// underneath your guarded client, never the guarded one: probes must not count
// against the circuit they are checking.
func WithRoundTripper(rt http.RoundTripper) HTTPOption { return func(c *httpCheck) { c.rt = rt } }

// WithHealthyStatus decides which statuses are healthy. Default 200 to 299.
func WithHealthyStatus(f func(status int) bool) HTTPOption {
	return func(c *httpCheck) { c.healthy = f }
}

// HTTP returns a Check that requests rawURL. It sends no body and only the
// headers set with WithHeader, follows no redirects and reads at most a few KiB
// of the response.
func HTTP(rawURL string, opts ...HTTPOption) (Check, error) {
	u, err := url.Parse(rawURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("health: URL must be an absolute http or https URL")
	}
	c := &httpCheck{method: http.MethodGet, header: http.Header{}, healthy: func(s int) bool { return s >= 200 && s < 300 }}
	for _, o := range opts {
		o(c)
	}
	if c.rt == nil {
		c.rt = &http.Transport{Proxy: http.ProxyFromEnvironment, MaxIdleConns: 1, IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: 5 * time.Second}
	}
	client := &http.Client{
		Transport:     c.rt,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, c.method, rawURL, nil)
		if err != nil {
			return err
		}
		for k, vs := range c.header {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		if !c.healthy(resp.StatusCode) {
			return &StatusError{Code: resp.StatusCode}
		}
		return nil
	}, nil
}
