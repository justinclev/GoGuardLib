package goguard

import (
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/justinclev/GoGuardLib/internal/breaker"
)

var (
	ErrCircuitOpen = errors.New("circuit breaker is open: request short-circuited")
)

const shardCount = 64

type Config struct {
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	IdleConnTimeout     time.Duration
	FailureThreshold    float64
	SleepWindow         time.Duration
	SamplingWindow      time.Duration
	BucketDuration      time.Duration
	// IsFailure determines if a response should count as a failure.
	IsFailure func(*http.Response, error) bool
}

type shard struct {
	mu       sync.RWMutex
	breakers map[string]*breaker.Breaker
}

type ResilientTransport struct {
	shards     [shardCount]*shard
	config     Config
	underlying *http.Transport
}

func NewResilientTransport(cfg Config) *ResilientTransport {
	if cfg.BucketDuration == 0 {
		cfg.BucketDuration = cfg.SamplingWindow / 10
	}
	if cfg.IsFailure == nil {
		cfg.IsFailure = func(resp *http.Response, err error) bool {
			return err != nil || (resp != nil && resp.StatusCode >= 500)
		}
	}

	rt := &ResilientTransport{
		config: cfg,
		underlying: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			DialContext:         (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConns:        cfg.MaxIdleConns,
			MaxIdleConnsPerHost: cfg.MaxIdleConnsPerHost,
			IdleConnTimeout:     cfg.IdleConnTimeout,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}

	for i := 0; i < shardCount; i++ {
		rt.shards[i] = &shard{breakers: make(map[string]*breaker.Breaker)}
	}

	return rt
}

func (t *ResilientTransport) getShard(host string) *shard {
	h := fnv.New64a()
	_, _ = h.Write([]byte(host))
	return t.shards[h.Sum64()%shardCount]
}

func (t *ResilientTransport) getBreaker(host string) *breaker.Breaker {
	s := t.getShard(host)
	
	s.mu.RLock()
	br, ok := s.breakers[host]
	s.mu.RUnlock()
	if ok {
		return br
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// Double-check
	if br, ok = s.breakers[host]; ok {
		return br
	}

	br = breaker.NewBreaker(
		t.config.FailureThreshold,
		t.config.SleepWindow,
		t.config.SamplingWindow,
		t.config.BucketDuration,
	)
	s.breakers[host] = br
	return br
}

func (t *ResilientTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Respect context early
	if err := req.Context().Err(); err != nil {
		return nil, err
	}

	br := t.getBreaker(req.URL.Host)
	if !br.Allow() {
		return nil, fmt.Errorf("%w for host %s", ErrCircuitOpen, req.URL.Host)
	}

	resp, err := t.underlying.RoundTrip(req)
	if t.config.IsFailure(resp, err) {
		br.MarkFailure()
	} else {
		br.MarkSuccess()
	}

	return resp, err
}
