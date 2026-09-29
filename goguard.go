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

	"github.com/justinclev/GoGuardLib/internal/breaker"
	"github.com/justinclev/GoGuardLib/internal/engine"
)

type contextKey string

const (
	PriorityKey contextKey = "goguard-priority"
)

// State is the state of a circuit breaker.
type State = engine.BreakerState

const (
	StateClosed   = engine.StateClosed
	StateOpen     = engine.StateOpen
	StateHalfOpen = engine.StateHalfOpen
)

var (
	ErrCircuitOpen = errors.New("circuit breaker is open")
)

type CircuitError struct {
	Host      string
	State     State
	Err       error
	Retryable bool
}

func (e *CircuitError) Error() string {
	return fmt.Sprintf("goguard: %v for host %s (state: %v, retryable: %v)", e.Err, e.Host, e.State, e.Retryable)
}

func (e *CircuitError) Unwrap() error {
	return e.Err
}

type Config struct {
	SleepWindow         time.Duration
	SamplingWindow      time.Duration
	BucketDuration      time.Duration
	MaxLatency          time.Duration
	RequestTimeout      time.Duration
	BulkheadWaitTimeout time.Duration
	IdleConnTimeout     time.Duration
	MaxIdleTime         time.Duration
	HeartbeatInterval   time.Duration
	IsFailure           func(*http.Response, error) bool
	OnStateChange       func(host string, from, to State) // must not block: it runs on the request path
	HeartbeatFunc       func(host string) error
	RetryBackoff        func(attempt int) time.Duration
	Transport           http.RoundTripper
	FailureThreshold    float64
	RetryBudget         float64 // 0.0 to 1.0
	MinSamples          int64
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	MaxBreakers         int
	MaxInflight         int
	MaxRetries          int
	ShardCount          int
	DryRun              bool
}

type Option func(*Config)

func WithTimeout(d time.Duration) Option { return func(c *Config) { c.RequestTimeout = d } }
func WithRetries(n int) Option           { return func(c *Config) { c.MaxRetries = n } }
func WithRetryBudget(f float64) Option   { return func(c *Config) { c.RetryBudget = f } }
func WithBulkhead(max int, wait time.Duration) Option {
	return func(c *Config) {
		c.MaxInflight = max
		c.BulkheadWaitTimeout = wait
	}
}

// touchInterval bounds how often a cache hit refreshes LRU position, so the
// hot path stays on the shard read lock.
const touchInterval = int64(time.Second)

type lruEntry struct {
	host       string
	breaker    *breaker.Breaker
	ctx        context.Context // cancelled when the entry is evicted or the transport closes
	cancel     context.CancelFunc
	lastAccess int64 // unix nanos, atomic
	hbRunning  int32 // atomic
}

type shard struct {
	mu       sync.RWMutex
	breakers map[string]*list.Element
	lruList  *list.List
	seed     uint64
}

type ResilientTransport struct {
	shards     []*shard
	ctx        context.Context
	cancel     context.CancelFunc
	underlying http.RoundTripper
	config     Config
	hashPool   sync.Pool
	shardMask  uint64

	closeMu sync.Mutex
	closed  bool
	wg      sync.WaitGroup
}

func NewResilientTransport(cfg Config, opts ...Option) *ResilientTransport {
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.ShardCount <= 0 {
		cfg.ShardCount = 64
	}
	if (cfg.ShardCount & (cfg.ShardCount - 1)) != 0 {
		cfg.ShardCount = 64
	}

	if cfg.SamplingWindow == 0 {
		cfg.SamplingWindow = 10 * time.Second
	}
	// Default bucket duration to 1/10th of window. If window < 10ns, fall back to 1s floor to prevent divide-by-zero.
	if cfg.BucketDuration == 0 {
		cfg.BucketDuration = cfg.SamplingWindow / 10
	}
	if cfg.BucketDuration == 0 {
		cfg.BucketDuration = 1 * time.Second
	}
	if cfg.SleepWindow == 0 {
		cfg.SleepWindow = 30 * time.Second
	}
	if cfg.MaxBreakers == 0 {
		cfg.MaxBreakers = 1000
	}
	if cfg.IsFailure == nil {
		cfg.IsFailure = func(resp *http.Response, err error) bool {
			return err != nil || (resp != nil && resp.StatusCode >= 500)
		}
	}

	underlying := cfg.Transport
	if underlying == nil {
		underlying = &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			DialContext:         (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConns:        cfg.MaxIdleConns,
			MaxIdleConnsPerHost: cfg.MaxIdleConnsPerHost,
			IdleConnTimeout:     cfg.IdleConnTimeout,
			TLSHandshakeTimeout: 10 * time.Second,
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	rt := &ResilientTransport{
		config:     cfg,
		underlying: underlying,
		ctx:        ctx,
		cancel:     cancel,
		shards:     make([]*shard, cfg.ShardCount),
		shardMask:  uint64(cfg.ShardCount - 1),
		hashPool: sync.Pool{
			New: func() interface{} { return fnv.New64a() },
		},
	}

	for i := 0; i < cfg.ShardCount; i++ {
		rt.shards[i] = &shard{
			breakers: make(map[string]*list.Element),
			lruList:  list.New(),
			seed:     uint64(time.Now().UnixNano()),
		}
	}

	if cfg.MaxIdleTime > 0 && rt.addWorker() {
		go rt.janitor()
	}

	return rt
}

// Close stops background workers and waits for them to exit. It is safe to
// call more than once. A custom HeartbeatFunc that ignores its transport
// context can delay Close.
func (t *ResilientTransport) Close() error {
	t.closeMu.Lock()
	if t.closed {
		t.closeMu.Unlock()
		return nil
	}
	t.closed = true
	t.closeMu.Unlock()

	t.cancel()
	t.wg.Wait()
	return nil
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
	for i := 0; i < len(t.shards); i++ {
		s := t.shards[i]
		s.mu.Lock()
		for el := s.lruList.Back(); el != nil; {
			entry := el.Value.(*lruEntry)
			if now-atomic.LoadInt64(&entry.lastAccess) > int64(t.config.MaxIdleTime) {
				prev := el.Prev()
				s.lruList.Remove(el)
				delete(s.breakers, entry.host)
				entry.cancel()
				el = prev
				continue
			}
			break
		}
		s.mu.Unlock()
	}
}

func (t *ResilientTransport) getShard(host string) *shard {
	h := t.hashPool.Get().(interface {
		Write([]byte) (int, error)
		Sum64() uint64
		Reset()
	})
	defer t.hashPool.Put(h)
	h.Reset()
	_, _ = h.Write([]byte(host))
	return t.shards[h.Sum64()&t.shardMask]
}

func (t *ResilientTransport) getBreaker(host string) (*breaker.Breaker, *shard) {
	s := t.getShard(host)
	now := time.Now().UnixNano()

	s.mu.RLock()
	el, ok := s.breakers[host]
	s.mu.RUnlock()
	if ok {
		entry := el.Value.(*lruEntry)
		if now-atomic.LoadInt64(&entry.lastAccess) > touchInterval {
			s.mu.Lock()
			atomic.StoreInt64(&entry.lastAccess, now)
			s.lruList.MoveToFront(el) // no-op if the element was evicted meanwhile
			s.mu.Unlock()
		}
		return entry.breaker, s
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok = s.breakers[host]; ok {
		entry := el.Value.(*lruEntry)
		atomic.StoreInt64(&entry.lastAccess, now)
		s.lruList.MoveToFront(el)
		return entry.breaker, s
	}

	if s.lruList.Len() >= t.config.MaxBreakers {
		if back := s.lruList.Back(); back != nil {
			evicted := back.Value.(*lruEntry)
			s.lruList.Remove(back)
			delete(s.breakers, evicted.host)
			evicted.cancel()
		}
	}

	br := breaker.NewBreaker(
		t.config.FailureThreshold,
		t.config.SleepWindow,
		t.config.SamplingWindow,
		t.config.BucketDuration,
		t.config.MaxInflight,
		t.config.MinSamples,
		t.config.DryRun,
		t.config.RetryBudget,
	)
	ectx, ecancel := context.WithCancel(t.ctx)
	entry := &lruEntry{host: host, breaker: br, ctx: ectx, cancel: ecancel, lastAccess: now}
	if t.config.OnStateChange != nil || t.config.HeartbeatInterval > 0 {
		br.OnStateChange = func(from, to State) {
			if t.config.OnStateChange != nil {
				t.config.OnStateChange(host, from, to)
			}
			if to == StateOpen && t.config.HeartbeatInterval > 0 {
				t.startHeartbeat(entry)
			}
		}
	}

	s.breakers[host] = s.lruList.PushFront(entry)
	return br, s
}

// startHeartbeat launches at most one heartbeat goroutine per breaker.
func (t *ResilientTransport) startHeartbeat(entry *lruEntry) {
	if !atomic.CompareAndSwapInt32(&entry.hbRunning, 0, 1) {
		return
	}
	if !t.addWorker() {
		atomic.StoreInt32(&entry.hbRunning, 0)
		return
	}
	go t.heartbeat(entry)
}

func (t *ResilientTransport) heartbeat(entry *lruEntry) {
	defer t.wg.Done()
	ticker := time.NewTicker(t.config.HeartbeatInterval)
	defer ticker.Stop()
	br := entry.breaker
	probe := t.config.HeartbeatFunc
	if probe == nil {
		probe = func(h string) error {
			req, err := http.NewRequestWithContext(entry.ctx, http.MethodHead, "http://"+h, nil)
			if err != nil {
				return err
			}
			resp, err := t.underlying.RoundTrip(req)
			if err != nil {
				return err
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode >= 500 {
				return fmt.Errorf("status %d", resp.StatusCode)
			}
			return nil
		}
	}
	for {
		select {
		case <-entry.ctx.Done():
			return
		case <-ticker.C:
			if st := br.State(); st != StateOpen && st != StateHalfOpen {
				// Release the slot, then re-check: the breaker may have reopened
				// between the state read and the release, when startHeartbeat
				// would have been refused.
				atomic.StoreInt32(&entry.hbRunning, 0)
				if st = br.State(); (st == StateOpen || st == StateHalfOpen) &&
					atomic.CompareAndSwapInt32(&entry.hbRunning, 0, 1) {
					continue
				}
				return
			}
			if err := probe(entry.host); err == nil {
				br.ProbeSuccess()
			} else {
				br.ProbeFailure()
			}
		}
	}
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

func (t *ResilientTransport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	select {
	case <-t.ctx.Done():
		return nil, t.ctx.Err()
	default:
	}

	br, sh := t.getBreaker(req.URL.Host)
	isVIP, _ := req.Context().Value(PriorityKey).(bool)
	if !br.Allow(req.Context(), t.config.BulkheadWaitTimeout, &sh.seed, isVIP) {
		return nil, &CircuitError{Host: req.URL.Host, State: br.State(), Err: ErrCircuitOpen, Retryable: false}
	}

	defer func() {
		if r := recover(); r != nil {
			br.MarkFailure()
			panic(r)
		}
	}()

	attempts := 0
	for {
		out := func() attemptResult {
			ctx := req.Context()
			var cancel context.CancelFunc
			if t.config.RequestTimeout > 0 {
				ctx, cancel = context.WithTimeout(ctx, t.config.RequestTimeout)
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

			isFail := t.config.IsFailure(r, e)
			if !isFail && t.config.MaxLatency > 0 && duration > t.config.MaxLatency {
				isFail = true
			}
			if !isFail && duration > br.AvgLatency()*2 && br.AvgLatency() > 0 {
				isFail = true
			}

			if !isFail {
				br.MarkSuccess(duration)
				return attemptResult{resp: r, err: e}
			}

			if attempts < t.config.MaxRetries && isRetryable(req, e, r) && br.CanRetry() {
				attempts++
				br.RecordRetry()
				backoff := time.Duration(0)
				if t.config.RetryBackoff != nil {
					backoff = t.config.RetryBackoff(attempts)
				}
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
						return attemptResult{retry: true, failed: true}
					}
				} else if req.Body == nil {
					closeResponse(r)
					return attemptResult{retry: true, failed: true}
				}
			}
			return attemptResult{resp: r, err: e, failed: true}
		}()

		if out.retry {
			continue
		}
		if out.failed {
			br.MarkFailure()
			if out.err != nil {
				return nil, &CircuitError{Host: req.URL.Host, State: br.State(), Err: out.err, Retryable: isRetryable(req, out.err, out.resp)}
			}
		}
		return out.resp, out.err
	}
}

// attemptResult is the outcome of one round trip attempt.
type attemptResult struct {
	resp   *http.Response
	err    error
	retry  bool // attempt failed but will be retried; resp and err are discarded
	failed bool // counts as a failure for the breaker
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
