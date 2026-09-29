// Package goguard protects outbound HTTP calls with circuit breakers, bulkheads,
// timeouts and safe retries.
//
// Protection is opt-in. A ResilientTransport guards only the endpoints you
// register, each with its own Policy; every other request passes straight
// through to the underlying transport with no breaker, no retry and no state:
//
//	rt, err := goguard.New(goguard.Config{},
//		goguard.WithEndpoint("payments", goguard.Host("payments.internal"),
//			goguard.Policy{FailureThreshold: 0.5, MaxRetries: 2}),
//	)
//
// The library never logs. Observe it through Config.OnStateChange, Config.Events
// and Stats.
package goguard

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/health"
	"github.com/justinclev/GoGuardLib/obs"
)

// State is the state of a circuit breaker.
type State = obs.State

const (
	StateClosed   = obs.StateClosed
	StateOpen     = obs.StateOpen
	StateHalfOpen = obs.StateHalfOpen
)

// ErrCircuitOpen matches errors returned when a circuit rejects a request.
var ErrCircuitOpen = breaker.ErrOpen

// CircuitError describes a request that was rejected or that failed while
// protected. Host is the name of the circuit: the endpoint name, or the host for
// per-host circuits.
type CircuitError struct {
	Host      string
	State     State
	Err       error
	Retryable bool
}

func (e *CircuitError) Error() string {
	return fmt.Sprintf("goguard: %v for host %s (state: %v, retryable: %v)", e.Err, e.Host, e.State, e.Retryable)
}

func (e *CircuitError) Unwrap() error { return e.Err }

// Config configures the transport itself. Protection is configured per endpoint.
type Config struct {
	// Transport is the underlying transport. Default: a pooled http.Transport.
	Transport http.RoundTripper

	MaxIdleConns        int
	MaxIdleConnsPerHost int
	IdleConnTimeout     time.Duration

	// Endpoints lists the protected endpoints. The first whose Match accepts a
	// request wins.
	Endpoints []Endpoint
	// GuardAll, when set, protects every request no endpoint matched, with one
	// circuit per host. Leave nil to let unmatched requests pass through.
	GuardAll *Policy

	// MaxBreakers caps live circuits per shard; the least recently used is
	// evicted. Default 1000.
	MaxBreakers int
	// MaxIdleTime evicts circuits unused for this long; 0 disables pruning.
	MaxIdleTime time.Duration
	// ShardCount is the number of lock shards (a power of two). Default 64.
	ShardCount int

	// OnStateChange is called for every circuit transition, in order, on a
	// dedicated goroutine, so it may be slow without affecting requests. Events
	// are dropped, and counted in Stats, if it falls EventBuffer events behind.
	OnStateChange func(circuit string, from, to State)
	// Events receives every event. Emit runs on the request path and must not
	// block; wrap slow consumers in an obs.Dispatcher.
	Events obs.Sink
	// EventBuffer sizes the OnStateChange queue. Default 1024.
	EventBuffer int
}

// Option adjusts a Config.
type Option func(*Config)

// WithEndpoint registers a protected endpoint. Options refine the policy.
func WithEndpoint(name string, match Matcher, p Policy, opts ...PolicyOption) Option {
	return func(c *Config) {
		c.Endpoints = append(c.Endpoints, Endpoint{Name: name, Match: match, Policy: p.with(opts)})
	}
}

// GuardAll protects every request not matched by an endpoint, with one circuit
// per host. It restores the "protect everything" behaviour explicitly.
func GuardAll(p Policy, opts ...PolicyOption) Option {
	return func(c *Config) {
		pp := p.with(opts)
		c.GuardAll = &pp
	}
}

// WithEvents sets the event sink.
func WithEvents(s obs.Sink) Option { return func(c *Config) { c.Events = s } }

type endpoint struct {
	Endpoint
	idx     int
	perHost bool
	policy  Policy
	bcfg    breaker.Config
}

type key struct {
	idx  int
	host string // empty for a shared circuit
}

// touchInterval bounds how often a cache hit refreshes LRU position, so the
// hot path stays on the shard read lock.
const touchInterval = int64(time.Second)

type entry struct {
	key        key
	name       string
	ep         *endpoint
	breaker    *breaker.Breaker
	lastAccess int64 // unix nanos, atomic
}

type shard struct {
	mu      sync.RWMutex
	entries map[key]*list.Element
	lru     *list.List
}

// ResilientTransport is an http.RoundTripper that protects registered endpoints.
type ResilientTransport struct {
	shards     []*shard
	ctx        context.Context
	cancel     context.CancelFunc
	underlying http.RoundTripper
	config     Config
	hashPool   sync.Pool
	shardMask  uint64

	endpoints []*endpoint
	byName    map[string]*endpoint
	guardAll  *endpoint

	sink       obs.Sink
	dispatcher *obs.Dispatcher
	passthru   atomic.Uint64

	closeMu sync.Mutex
	closed  bool
	wg      sync.WaitGroup
}

// New creates a transport, rejecting invalid configuration.
func New(cfg Config, opts ...Option) (*ResilientTransport, error) {
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.ShardCount <= 0 || (cfg.ShardCount&(cfg.ShardCount-1)) != 0 {
		cfg.ShardCount = 64
	}
	if cfg.MaxBreakers == 0 {
		cfg.MaxBreakers = 1000
	}

	ctx, cancel := context.WithCancel(context.Background())
	rt := &ResilientTransport{
		config:    cfg,
		ctx:       ctx,
		cancel:    cancel,
		shards:    make([]*shard, cfg.ShardCount),
		shardMask: uint64(cfg.ShardCount - 1),
		byName:    make(map[string]*endpoint),
		hashPool:  sync.Pool{New: func() interface{} { return fnv.New64a() }},
	}

	for i, e := range cfg.Endpoints {
		if e.Name == "" || e.Name == guardAllName {
			cancel()
			return nil, fmt.Errorf("goguard: endpoint %d needs a unique name other than %q", i, guardAllName)
		}
		if e.Match == nil {
			cancel()
			return nil, fmt.Errorf("goguard: endpoint %q has no Match", e.Name)
		}
		if _, dup := rt.byName[e.Name]; dup {
			cancel()
			return nil, fmt.Errorf("goguard: duplicate endpoint name %q", e.Name)
		}
		if err := e.Policy.validate(e.Name); err != nil {
			cancel()
			return nil, err
		}
		ep := newEndpoint(len(rt.endpoints), e)
		rt.endpoints = append(rt.endpoints, ep)
		rt.byName[e.Name] = ep
	}
	if cfg.GuardAll != nil {
		if err := cfg.GuardAll.validate(guardAllName); err != nil {
			cancel()
			return nil, err
		}
		rt.guardAll = newEndpoint(len(rt.endpoints), Endpoint{
			Name: guardAllName, Match: func(*http.Request) bool { return true },
			Policy: *cfg.GuardAll, PerHost: true,
		})
		rt.byName[guardAllName] = rt.guardAll
	}

	rt.underlying = cfg.Transport
	if rt.underlying == nil {
		rt.underlying = &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			DialContext:         (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConns:        cfg.MaxIdleConns,
			MaxIdleConnsPerHost: cfg.MaxIdleConnsPerHost,
			IdleConnTimeout:     cfg.IdleConnTimeout,
			TLSHandshakeTimeout: 10 * time.Second,
		}
	}

	if cfg.OnStateChange != nil {
		rt.dispatcher = obs.NewDispatcher(func(e obs.Event) {
			if sc, ok := e.(obs.StateChanged); ok {
				cfg.OnStateChange(sc.Dependency, sc.From, sc.To)
			}
		}, cfg.EventBuffer)
	}
	if rt.dispatcher != nil {
		rt.sink = obs.Multi(rt.dispatcher, cfg.Events)
	} else {
		rt.sink = cfg.Events
	}

	for i := range rt.shards {
		rt.shards[i] = &shard{entries: make(map[key]*list.Element), lru: list.New()}
	}

	if cfg.MaxIdleTime > 0 && rt.addWorker() {
		go rt.janitor()
	}
	return rt, nil
}

// NewResilientTransport is New for static configuration: it panics if the
// configuration is invalid.
func NewResilientTransport(cfg Config, opts ...Option) *ResilientTransport {
	rt, err := New(cfg, opts...)
	if err != nil {
		panic(err)
	}
	return rt
}

func newEndpoint(idx int, e Endpoint) *endpoint {
	ep := &endpoint{Endpoint: e, idx: idx, perHost: e.PerHost, policy: e.Policy}
	ep.policy.applyDefaults()
	ep.bcfg = ep.policy.breakerConfig(e.Name)
	return ep
}

// Close stops background workers and waits for them, then delivers any queued
// events. It is safe to call more than once. A custom health Check that ignores
// its context, or an OnStateChange that never returns, can delay Close.
func (t *ResilientTransport) Close() error {
	t.closeMu.Lock()
	if t.closed {
		t.closeMu.Unlock()
		return nil
	}
	t.closed = true
	t.closeMu.Unlock()

	t.cancel()
	for _, s := range t.shards {
		s.mu.RLock()
		entries := make([]*entry, 0, len(s.entries))
		for _, el := range s.entries {
			entries = append(entries, el.Value.(*entry))
		}
		s.mu.RUnlock()
		for _, e := range entries {
			e.breaker.Close() // stops its health prober
		}
	}
	t.wg.Wait()
	if t.dispatcher != nil {
		t.dispatcher.Close()
	}
	return nil
}

// retire stops the background work of a circuit that has been evicted or pruned.
// It does not wait inline, because it is called with a shard lock held and a
// health check may take a while to notice it was cancelled.
func (t *ResilientTransport) retire(e *entry) {
	if !t.addWorker() {
		e.breaker.Close()
		return
	}
	go func() {
		defer t.wg.Done()
		e.breaker.Close()
	}()
}

// addWorker registers a background goroutine, refusing once Close has begun.
func (t *ResilientTransport) addWorker() bool {
	t.closeMu.Lock()
	defer t.closeMu.Unlock()
	if t.closed {
		return false
	}
	t.wg.Add(1)
	return true
}

// Stats is a point-in-time view of the transport.
type Stats struct {
	// Breakers has one element per live circuit.
	Breakers []breaker.Stats
	// Passthrough counts requests that matched no endpoint and were not guarded.
	Passthrough uint64
	// DroppedEvents counts OnStateChange events discarded because the queue was full.
	DroppedEvents uint64
}

// Stats returns a snapshot.
func (t *ResilientTransport) Stats() Stats {
	st := Stats{Passthrough: t.passthru.Load()}
	if t.dispatcher != nil {
		st.DroppedEvents = t.dispatcher.Dropped()
	}
	for _, s := range t.shards {
		s.mu.RLock()
		for _, el := range s.entries {
			st.Breakers = append(st.Breakers, el.Value.(*entry).breaker.Stats())
		}
		s.mu.RUnlock()
	}
	return st
}

func (t *ResilientTransport) janitor() {
	defer t.wg.Done()
	ticker := time.NewTicker(t.config.MaxIdleTime / 2)
	defer ticker.Stop()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.pruneIdle()
		}
	}
}

func (t *ResilientTransport) pruneIdle() {
	now := time.Now().UnixNano()
	for _, s := range t.shards {
		s.mu.Lock()
		for el := s.lru.Back(); el != nil; {
			e := el.Value.(*entry)
			if now-atomic.LoadInt64(&e.lastAccess) > int64(t.config.MaxIdleTime) {
				prev := el.Prev()
				s.lru.Remove(el)
				delete(s.entries, e.key)
				t.retire(e)
				el = prev
				continue
			}
			break
		}
		s.mu.Unlock()
	}
}

func (t *ResilientTransport) shardFor(name string) *shard {
	h := t.hashPool.Get().(interface {
		Write([]byte) (int, error)
		Sum64() uint64
		Reset()
	})
	defer t.hashPool.Put(h)
	h.Reset()
	_, _ = h.Write([]byte(name))
	return t.shards[h.Sum64()&t.shardMask]
}

// getEntry returns the circuit for a request to host under ep, creating it on
// first use.
func (t *ResilientTransport) getEntry(ep *endpoint, scheme, host string) *entry {
	k := key{idx: ep.idx}
	shardKey := ep.Name
	if ep.perHost {
		k.host = host
		shardKey = host
	}
	s := t.shardFor(shardKey)
	now := time.Now().UnixNano()

	s.mu.RLock()
	el, ok := s.entries[k]
	s.mu.RUnlock()
	if ok {
		e := el.Value.(*entry)
		if now-atomic.LoadInt64(&e.lastAccess) > touchInterval {
			s.mu.Lock()
			atomic.StoreInt64(&e.lastAccess, now)
			s.lru.MoveToFront(el) // no-op if the element was evicted meanwhile
			s.mu.Unlock()
		}
		return e
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok = s.entries[k]; ok {
		e := el.Value.(*entry)
		atomic.StoreInt64(&e.lastAccess, now)
		s.lru.MoveToFront(el)
		return e
	}

	if s.lru.Len() >= t.config.MaxBreakers {
		if back := s.lru.Back(); back != nil {
			evicted := back.Value.(*entry)
			s.lru.Remove(back)
			delete(s.entries, evicted.key)
			t.retire(evicted)
		}
	}

	cfg := ep.bcfg
	name := circuitName(ep, host)
	cfg.Name = name
	cfg.Events = t.sink
	cfg.Health = t.healthFor(ep, scheme, host)
	e := &entry{key: k, name: name, ep: ep, breaker: breaker.New(cfg), lastAccess: now}

	s.entries[k] = s.lru.PushFront(e)
	return e
}

func circuitName(ep *endpoint, host string) string {
	switch {
	case !ep.perHost:
		return ep.Name
	case ep.idx >= 0 && ep.Name == guardAllName:
		return host
	default:
		return ep.Name + "@" + host
	}
}

// healthFor builds the health configuration for a circuit, or nil when the
// endpoint has none.
func (t *ResilientTransport) healthFor(ep *endpoint, scheme, host string) *health.Config {
	pol := ep.policy
	if pol.Health == nil && pol.HealthPath == "" {
		return nil
	}
	var hc health.Config
	if pol.Health != nil {
		hc = *pol.Health
	}
	if hc.Check == nil {
		var opts []health.HTTPOption
		for k, vs := range pol.HealthHeader {
			for _, v := range vs {
				opts = append(opts, health.WithHeader(k, v))
			}
		}
		// Probes go through the underlying transport: they must not count against
		// the circuit they are checking.
		opts = append(opts, health.WithRoundTripper(t.underlying))
		check, err := health.HTTP(scheme+"://"+host+pol.HealthPath, opts...)
		if err != nil {
			return nil // a host that is not a valid URL cannot be probed; the timer applies
		}
		hc.Check = check
	}
	return &hc
}

func isRetryable(req *http.Request, err error, resp *http.Response) bool {
	switch req.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
	default:
		return false
	}
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return req.Body == nil || req.GetBody != nil
		}
		return false
	}
	if resp != nil && (resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout) {
		return req.Body == nil || req.GetBody != nil
	}
	return false
}

// resolve picks the endpoint that protects req, or nil to pass it through.
func (t *ResilientTransport) resolve(req *http.Request) (*endpoint, error) {
	ctx := req.Context()
	if skip, _ := ctx.Value(skipKey).(bool); skip {
		return nil, nil
	}
	if name, ok := ctx.Value(useKey).(string); ok {
		if ep := t.byName[name]; ep != nil {
			return ep, nil
		}
		return nil, fmt.Errorf("%w: %q", ErrUnknownEndpoint, name)
	}
	for _, ep := range t.endpoints {
		if ep.Match(req) {
			return ep, nil
		}
	}
	return t.guardAll, nil
}

// RoundTrip implements http.RoundTripper.
func (t *ResilientTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	select {
	case <-t.ctx.Done():
		return nil, t.ctx.Err()
	default:
	}

	ep, err := t.resolve(req)
	if err != nil {
		return nil, err
	}
	if ep == nil {
		t.passthru.Add(1)
		return t.underlying.RoundTrip(req)
	}
	return t.guarded(ep, req)
}

// attemptResult is the outcome of one round trip attempt.
type attemptResult struct {
	resp      *http.Response
	err       error
	latency   time.Duration
	retry     bool // attempt failed but will be retried; resp and err are discarded
	failed    bool // counts as a failure for the circuit
	abandoned bool // the caller cancelled; not the endpoint's fault
}

func (t *ResilientTransport) guarded(ep *endpoint, req *http.Request) (*http.Response, error) {
	ent := t.getEntry(ep, req.URL.Scheme, req.URL.Host)
	br := ent.breaker
	pol := ep.policy

	isVIP, _ := req.Context().Value(PriorityKey).(bool)
	permit, err := br.Acquire(req.Context(), isVIP)
	if err != nil {
		cerr := &CircuitError{Host: ent.name, State: br.State(), Err: ErrCircuitOpen}
		if errors.Is(err, breaker.ErrBulkhead) {
			cerr.Err, cerr.Retryable = breaker.ErrBulkhead, true
		}
		return nil, cerr
	}

	defer func() {
		if r := recover(); r != nil {
			permit.Failure()
			panic(r)
		}
	}()

	attempts := 0
	for {
		out := func() attemptResult {
			ctx := req.Context()
			var cancel context.CancelFunc
			if pol.RequestTimeout > 0 {
				ctx, cancel = context.WithTimeout(ctx, pol.RequestTimeout)
			}

			start := time.Now()
			currReq := req
			if cancel != nil {
				currReq = req.WithContext(ctx)
			}

			r, e := t.underlying.RoundTrip(currReq)
			duration := time.Since(start)
			// The timeout context must outlive RoundTrip so the caller can still
			// read the body; it is released when the body is closed.
			if cancel != nil {
				if r != nil && r.Body != nil {
					r.Body = &cancelBody{ReadCloser: r.Body, cancel: cancel}
				} else {
					cancel()
				}
			}

			if e != nil && errors.Is(e, context.Canceled) && req.Context().Err() != nil {
				return attemptResult{err: e, abandoned: true}
			}

			isFail := pol.IsFailure(r, e)
			if !isFail && pol.MaxLatency > 0 && duration > pol.MaxLatency {
				isFail = true
			}
			if f := pol.OutlierFactor; !isFail && f > 0 {
				if avg := br.AvgLatency(); avg > 0 && float64(duration) > float64(avg)*f {
					isFail = true
				}
			}

			if !isFail {
				return attemptResult{resp: r, err: e, latency: duration}
			}

			if attempts < pol.MaxRetries && isRetryable(req, e, r) && br.CanRetry() {
				attempts++
				br.RecordRetry()
				var backoff time.Duration
				if pol.RetryBackoff != nil {
					backoff = pol.RetryBackoff(attempts)
				}
				obs.Emit(t.sink, obs.Retried{Dependency: ent.name, Attempt: attempts, Delay: backoff, At: time.Now()})
				if backoff > 0 {
					select {
					case <-req.Context().Done():
						return attemptResult{resp: r, err: e, failed: true}
					case <-time.After(backoff):
					}
				}
				if req.GetBody != nil {
					newBody, bodyErr := req.GetBody()
					if bodyErr == nil {
						closeResponse(r)
						req = req.Clone(req.Context())
						req.Body = newBody
						return attemptResult{retry: true}
					}
				} else if req.Body == nil {
					closeResponse(r)
					return attemptResult{retry: true}
				}
			}
			return attemptResult{resp: r, err: e, failed: true}
		}()

		if out.retry {
			continue
		}
		switch {
		case out.abandoned:
			permit.Abandon()
			return nil, &CircuitError{Host: ent.name, State: br.State(), Err: out.err}
		case out.failed:
			permit.Failure()
			if out.err != nil {
				return nil, &CircuitError{Host: ent.name, State: br.State(), Err: out.err, Retryable: isRetryable(req, out.err, out.resp)}
			}
		default:
			permit.Success(out.latency)
		}
		return out.resp, out.err
	}
}

// cancelBody releases the request-timeout context when the body is closed.
type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelBody) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}

func closeResponse(r *http.Response) {
	if r != nil && r.Body != nil {
		_ = r.Body.Close()
	}
}
