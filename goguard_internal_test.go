package goguard

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCircuitError(t *testing.T) {
	err := &CircuitError{Host: "example.com", State: StateOpen, Err: ErrCircuitOpen}
	want := "goguard: circuit breaker is open for host example.com (state: open, retryable: false)"
	if err.Error() != want {
		t.Errorf("got %q, want %q", err.Error(), want)
	}
	if !errors.Is(err, ErrCircuitOpen) {
		t.Error("CircuitError must wrap ErrCircuitOpen")
	}
}

func TestPolicyOptions(t *testing.T) {
	p := Policy{}.with([]PolicyOption{
		WithTimeout(5 * time.Second), WithRetries(3), WithRetryBudget(0.2), WithBulkhead(10, time.Second),
	})
	if p.RequestTimeout != 5*time.Second || p.MaxRetries != 3 || p.RetryBudget != 0.2 ||
		p.MaxInflight != 10 || p.BulkheadWaitTimeout != time.Second {
		t.Fatalf("policy = %+v", p)
	}
}

func TestMatchers(t *testing.T) {
	req := func(url string) *http.Request { r, _ := http.NewRequest(http.MethodGet, url, nil); return r }
	if !Host("A.test", "b.test:8080")(req("http://a.test/x")) || !Host("b.test:8080")(req("http://B.test:8080/")) {
		t.Error("Host should match case-insensitively, port included")
	}
	if Host("a.test")(req("http://a.test:81/")) {
		t.Error("Host must not ignore the port")
	}
	m := HostPath("a.test", "/api/")
	if !m(req("http://a.test/api/x")) || m(req("http://a.test/other")) || m(req("http://b.test/api/x")) {
		t.Error("HostPath mismatch")
	}
}

func TestShardCountNormalisation(t *testing.T) {
	for in, want := range map[int]int{0: 64, 7: 64, 128: 128, -1: 64} {
		rt := NewResilientTransport(Config{ShardCount: in})
		if len(rt.shards) != want {
			t.Errorf("ShardCount %d -> %d shards, want %d", in, len(rt.shards), want)
		}
		rt.Close()
	}
}

func perHostTransport(t *testing.T, cfg Config) (*ResilientTransport, *endpoint) {
	t.Helper()
	cfg.Endpoints = []Endpoint{{Name: "svc", Match: func(*http.Request) bool { return true }, PerHost: true}}
	rt, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return rt, rt.endpoints[0]
}

func TestJanitorPrunesIdleCircuits(t *testing.T) {
	rt, ep := perHostTransport(t, Config{MaxIdleTime: 100 * time.Millisecond, MaxBreakers: 2, ShardCount: 1})
	defer rt.Close()

	rt.getEntry(ep, "host1")
	rt.getEntry(ep, "host2")
	s := rt.shards[0]
	s.mu.RLock()
	n := len(s.entries)
	s.mu.RUnlock()
	if n != 2 {
		t.Fatalf("got %d circuits, want 2", n)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.RLock()
		n = len(s.entries)
		s.mu.RUnlock()
		if n == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%d circuits left after idle timeout", n)
}

func TestLRUEvictionCancelsEvictedCircuit(t *testing.T) {
	rt, ep := perHostTransport(t, Config{MaxBreakers: 2, ShardCount: 1})
	defer rt.Close()

	first := rt.getEntry(ep, "host1")
	rt.getEntry(ep, "host2")
	rt.getEntry(ep, "host3") // evicts host1

	s := rt.shards[0]
	s.mu.RLock()
	_, has1 := s.entries[key{idx: ep.idx, host: "host1"}]
	_, has3 := s.entries[key{idx: ep.idx, host: "host3"}]
	s.mu.RUnlock()
	if has1 || !has3 {
		t.Fatalf("has1=%v has3=%v, want host1 evicted and host3 present", has1, has3)
	}
	select {
	case <-first.ctx.Done():
	default:
		t.Fatal("evicted circuit's context was not cancelled")
	}
}

func TestGetEntryReusesAndRefreshesCircuit(t *testing.T) {
	rt, ep := perHostTransport(t, Config{ShardCount: 1})
	defer rt.Close()
	a := rt.getEntry(ep, "h")
	atomic.StoreInt64(&a.lastAccess, time.Now().Add(-time.Minute).UnixNano()) // force an LRU touch
	if b := rt.getEntry(ep, "h"); a != b {
		t.Fatal("same host must reuse its circuit")
	}
	if time.Since(time.Unix(0, atomic.LoadInt64(&a.lastAccess))) > time.Second {
		t.Fatal("lastAccess not refreshed")
	}
}

func TestConcurrentEntryCreationAndEviction(t *testing.T) {
	rt, ep := perHostTransport(t, Config{MaxBreakers: 10, ShardCount: 4})
	defer rt.Close()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			host := fmt.Sprintf("host-%d.com", id)
			rt.getEntry(ep, host)
			rt.getEntry(ep, host)
		}(i)
	}
	wg.Wait()
	if n := len(rt.Stats().Breakers); n > 4*10 {
		t.Fatalf("%d circuits alive, cap is 10 per shard", n)
	}
}

func TestIsRetryable(t *testing.T) {
	timeout := &net_timeout{}
	tests := []struct {
		method string
		err    error
		status int
		want   bool
	}{
		{"GET", nil, 200, false},
		{"GET", errors.New("any"), 0, false},
		{"GET", timeout, 0, true},
		{"POST", timeout, 0, false},
		{"GET", nil, 503, true},
		{"GET", nil, 504, true},
		{"POST", nil, 503, false},
		{"HEAD", nil, 503, true},
		{"OPTIONS", nil, 503, true},
		{"TRACE", nil, 503, true},
	}
	for _, tt := range tests {
		req, _ := http.NewRequest(tt.method, "http://example.com", nil)
		var resp *http.Response
		if tt.status != 0 {
			resp = &http.Response{StatusCode: tt.status}
		}
		if got := isRetryable(req, tt.err, resp); got != tt.want {
			t.Errorf("isRetryable(%s, %v, %d) = %v, want %v", tt.method, tt.err, tt.status, got, tt.want)
		}
	}
	// A body that cannot be replayed makes a request unsafe to retry.
	req, _ := http.NewRequest("GET", "http://example.com", http.NoBody)
	req.GetBody = nil
	req.Body = http.NoBody
	if isRetryable(req, nil, &http.Response{StatusCode: 503}) {
		t.Error("request with a non-replayable body must not be retried")
	}
}

type net_timeout struct{}

func (*net_timeout) Error() string   { return "timeout" }
func (*net_timeout) Timeout() bool   { return true }
func (*net_timeout) Temporary() bool { return true }

func TestShardedStress(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer ts.Close()
	rt := NewResilientTransport(Config{MaxBreakers: 10}, GuardAll(Policy{}))
	defer rt.Close()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				if resp, err := get(t, rt, ts.URL); err == nil {
					resp.Body.Close()
				}
			}
		}()
	}
	wg.Wait()
}

func TestEnforcedTimeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	rt := NewResilientTransport(Config{}, WithEndpoint("slow", Host(hostOf(ts)), Policy{}, WithTimeout(50*time.Millisecond)))
	defer rt.Close()
	client := &http.Client{Transport: rt}
	if _, err := client.Get(ts.URL); err == nil {
		t.Fatal("expected a timeout error")
	}
}
