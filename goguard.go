package goguard

import (
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/justinclev/GoGuardLib/internal/breaker"
)

var (
	ErrCircuitOpen = errors.New("circuit breaker is open: request short-circuited")
)

type Config struct {
	MaxIdleConns        int
	MaxIdleConnsPerHost int
	IdleConnTimeout     time.Duration
	FailureThreshold    float64       // e.g., 0.50 for 50%
	SleepWindow         time.Duration // Time to remain in OPEN state before trailing traffic
	SamplingWindow      time.Duration // Rolling window duration for error assessment
}

type ResilientTransport struct {
	mu         sync.RWMutex
	breakers   map[string]*breaker.Breaker
	config     Config
	underlying *http.Transport
}

func NewResilientTransport(cfg Config) *ResilientTransport {
	return &ResilientTransport{
		breakers: make(map[string]*breaker.Breaker),
		config:   cfg,
		underlying: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			DialContext:         (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConns:        cfg.MaxIdleConns,
			MaxIdleConnsPerHost: cfg.MaxIdleConnsPerHost,
			IdleConnTimeout:     cfg.IdleConnTimeout,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}
}

func (t *ResilientTransport) getBreaker(host string) *breaker.Breaker {
	t.mu.RLock()
	br, ok := t.breakers[host]
	t.mu.RUnlock()
	if ok {
		return br
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if br, ok := t.breakers[host]; ok {
		return br
	}

	br = breaker.NewBreaker(t.config.FailureThreshold, t.config.SleepWindow, t.config.SamplingWindow)
	t.breakers[host] = br
	return br
}

func (t *ResilientTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	br := t.getBreaker(req.URL.Host)
	if !br.Allow() {
		return nil, ErrCircuitOpen
	}

	resp, err := t.underlying.RoundTrip(req)
	if err != nil {
		br.MarkFailure()
		return nil, err
	}

	if resp.StatusCode >= 500 {
		br.MarkFailure()
		return resp, nil
	}

	br.MarkSuccess()
	return resp, nil
}
