package goguard

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/justinclev/GoGuardLib/internal/breaker"
	"github.com/justinclev/GoGuardLib/internal/engine"
)

type contextKey string

const (
	PriorityKey contextKey = "goguard-priority"
)

var (
	ErrCircuitOpen = errors.New("circuit breaker is open")
)

type CircuitError struct {
	Host      string
	State     engine.BreakerState
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
	OnStateChange       func(host string, from, to engine.BreakerState)
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
func WithRetries(n int) Option            { return func(c *Config) { c.MaxRetries = n } }
func WithRetryBudget(f float64) Option   { return func(c *Config) { c.RetryBudget = f } }
func WithBulkhead(max int, wait time.Duration) Option {
	return func(c *Config) {
		c.MaxInflight = max
		c.BulkheadWaitTimeout = wait
	}
}

type lruEntry struct {
	host       string
	breaker    *breaker.Breaker
	lastAccess time.Time
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
}

func NewResilientTransport(cfg Config, opts ...Option) *ResilientTransport {
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.ShardCount <= 0 { cfg.ShardCount = 64 }
	if (cfg.ShardCount & (cfg.ShardCount - 1)) != 0 { cfg.ShardCount = 64 }

	if cfg.SamplingWindow == 0 { cfg.SamplingWindow = 10 * time.Second }
	// Default bucket duration to 1/10th of window. If window < 10ns, fall back to 1s floor to prevent divide-by-zero.
	if cfg.BucketDuration == 0 { cfg.BucketDuration = cfg.SamplingWindow / 10 }
	if cfg.BucketDuration == 0 { cfg.BucketDuration = 1 * time.Second }
	if cfg.SleepWindow == 0 { cfg.SleepWindow = 30 * time.Second }
	if cfg.MaxBreakers == 0 { cfg.MaxBreakers = 1000 }
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

	if cfg.MaxIdleTime > 0 { go rt.janitor() }

	return rt
}

func (t *ResilientTransport) Close() error {
	t.cancel()
	return nil
}

func (t *ResilientTransport) janitor() {
	ticker := time.NewTicker(t.config.MaxIdleTime / 2)
	defer ticker.Stop()
	for {
		select {
		case <-t.ctx.Done(): return
		case <-ticker.C: t.pruneIdle()
		}
	}
}

func (t *ResilientTransport) pruneIdle() {
	now := time.Now()
	for i := 0; i < len(t.shards); i++ {
		s := t.shards[i]
		s.mu.Lock()
		for el := s.lruList.Back(); el != nil; {
			entry := el.Value.(*lruEntry)
			if now.Sub(entry.lastAccess) > t.config.MaxIdleTime {
				prev := el.Prev()
				s.lruList.Remove(el)
				delete(s.breakers, entry.host)
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
	now := time.Now()

	s.mu.RLock()
	el, ok := s.breakers[host]
	s.mu.RUnlock()
	if ok {
		s.mu.Lock()
		entry := el.Value.(*lruEntry)
		entry.lastAccess = now
		s.lruList.MoveToFront(el)
		s.mu.Unlock()
		return entry.breaker, s
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok = s.breakers[host]; ok {
		entry := el.Value.(*lruEntry)
		entry.lastAccess = now
		s.lruList.MoveToFront(el)
		return entry.breaker, s
	}

	if s.lruList.Len() >= t.config.MaxBreakers {
		back := s.lruList.Back()
		if back != nil {
			s.lruList.Remove(back)
			delete(s.breakers, back.Value.(*lruEntry).host)
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
	if t.config.OnStateChange != nil {
		br.OnStateChange = func(from, to engine.BreakerState) {
			t.config.OnStateChange(host, from, to)
			if to == engine.StateOpen && t.config.HeartbeatInterval > 0 {
				go t.heartbeat(host, br)
			}
		}
	}

	el = s.lruList.PushFront(&lruEntry{host: host, breaker: br, lastAccess: now})
	s.breakers[host] = el
	return br, s
}

func (t *ResilientTransport) heartbeat(host string, br *breaker.Breaker) {
	ticker := time.NewTicker(t.config.HeartbeatInterval)
	defer ticker.Stop()
	probe := t.config.HeartbeatFunc
	if probe == nil {
		probe = func(h string) error {
			req, _ := http.NewRequestWithContext(t.ctx, "HEAD", "http://"+h, nil)
			resp, err := t.underlying.RoundTrip(req)
			if err != nil { return err }
			defer resp.Body.Close()
			if resp.StatusCode >= 500 { return fmt.Errorf("status %d", resp.StatusCode) }
			return nil
		}
	}
	for {
		select {
		case <-t.ctx.Done(): return
		case <-ticker.C:
			if br.State() != engine.StateOpen && br.State() != engine.StateHalfOpen { return }
			if err := probe(host); err == nil {
				br.MarkSuccess(1 * time.Millisecond)
			} else {
				br.MarkFailure()
			}
		}
	}
}

func isRetryable(req *http.Request, err error, resp *http.Response) bool {
	switch req.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
	default: return false
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
	case <-t.ctx.Done(): return nil, t.ctx.Err()
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
		resp, err, retry := func() (*http.Response, error, bool) {
			ctx := req.Context()
			var cancel context.CancelFunc
			if t.config.RequestTimeout > 0 {
				ctx, cancel = context.WithTimeout(ctx, t.config.RequestTimeout)
			}

			start := time.Now()
			currReq := req
			if cancel != nil { currReq = req.WithContext(ctx) }

			r, e := t.underlying.RoundTrip(currReq)
			duration := time.Since(start)
			if cancel != nil { cancel() }

			isFail := t.config.IsFailure(r, e)
			if !isFail && t.config.MaxLatency > 0 && duration > t.config.MaxLatency { isFail = true }
			if !isFail && duration > br.AvgLatency()*2 && br.AvgLatency() > 0 { isFail = true }

			if !isFail {
				br.MarkSuccess(duration)
				return r, e, false
			}

			if attempts < t.config.MaxRetries && isRetryable(req, e, r) && br.CanRetry() {
				attempts++
				br.RecordRetry()
				backoff := time.Duration(0)
				if t.config.RetryBackoff != nil { backoff = t.config.RetryBackoff(attempts) }
				if backoff > 0 {
					select {
					case <-req.Context().Done(): return r, e, false
					case <-time.After(backoff):
					}
				}
				if req.GetBody != nil {
					newBody, bodyErr := req.GetBody()
					if bodyErr == nil {
						req = req.Clone(req.Context())
						req.Body = newBody
						return r, e, true
					}
				} else if req.Body == nil {
					return r, e, true
				}
			}
			return r, e, false
		}()

		if !retry {
			if err != nil || t.config.IsFailure(resp, err) {
				br.MarkFailure()
				if err != nil {
					return nil, &CircuitError{Host: req.URL.Host, State: br.State(), Err: err, Retryable: isRetryable(req, err, resp)}
				}
				return resp, nil
			}
			return resp, nil
		}
	}
}
