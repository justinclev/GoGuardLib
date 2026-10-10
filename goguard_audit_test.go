package goguard

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/breaker"
)

// auditBody records whether it was closed.
type auditBody struct {
	strings.Reader
	closed atomic.Bool
}

func (b *auditBody) Close() error { b.closed.Store(true); return nil }

func newAuditBody() *auditBody {
	return &auditBody{Reader: *strings.NewReader("payload")}
}

func auditPost(t *testing.T, rt http.RoundTripper, ctx context.Context, url string, body *auditBody) error {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := rt.RoundTrip(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	return err
}

// A RoundTripper must close the request body on every path, including the ones that
// send nothing. A rejected request that leaves its body open leaks a file
// descriptor or a buffer for every call made while a circuit is open.
func TestRejectedRequestsCloseTheirBody(t *testing.T) {
	under := &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 500, Body: http.NoBody}, nil
	}}
	rt, err := New(Config{Transport: under}, WithEndpoint("svc", Host("svc.test"), tripOnFirstFailure()))
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	ctx := context.Background()

	_ = auditPost(t, rt, ctx, "http://svc.test/", newAuditBody()) // a 500 opens the circuit
	b := newAuditBody()
	if err := auditPost(t, rt, ctx, "http://svc.test/", b); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("got %v, want an open circuit", err)
	}
	if !b.closed.Load() {
		t.Error("the body of a request rejected by an open circuit was not closed")
	}

	b = newAuditBody()
	if err := auditPost(t, rt, Use(ctx, "nope"), "http://svc.test/", b); !errors.Is(err, ErrUnknownEndpoint) {
		t.Fatalf("got %v, want ErrUnknownEndpoint", err)
	}
	if !b.closed.Load() {
		t.Error("the body of a request for an unknown endpoint was not closed")
	}

	_ = rt.Close()
	b = newAuditBody()
	if err := auditPost(t, rt, ctx, "http://svc.test/", b); err == nil {
		t.Fatal("a closed transport accepted a request")
	}
	if !b.closed.Load() {
		t.Error("the body of a request to a closed transport was not closed")
	}
}

func TestCloseIdleConnectionsReachesTheUnderlyingTransport(t *testing.T) {
	var called atomic.Bool
	rt, err := New(Config{Transport: idleCloser{&mockTransport{roundTrip: func(*http.Request) (*http.Response, error) { return okResp(), nil }}, &called}})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	(&http.Client{Transport: rt}).CloseIdleConnections()
	if !called.Load() {
		t.Fatal("http.Client.CloseIdleConnections did not reach the underlying transport")
	}
}

type idleCloser struct {
	http.RoundTripper
	called *atomic.Bool
}

func (i idleCloser) CloseIdleConnections() { i.called.Store(true) }

func TestDefaultTransportEnablesHTTP2AndSensibleIdleLimits(t *testing.T) {
	rt, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	tr, ok := rt.underlying.(*http.Transport)
	if !ok {
		t.Fatalf("underlying is %T", rt.underlying)
	}
	if !tr.ForceAttemptHTTP2 {
		t.Error("a custom dialer turns HTTP/2 off unless ForceAttemptHTTP2 is set")
	}
	if tr.MaxIdleConnsPerHost < 10 || tr.MaxIdleConns < 50 || tr.IdleConnTimeout <= 0 {
		t.Errorf("idle limits %d/%d/%v are too tight or unbounded", tr.MaxIdleConnsPerHost, tr.MaxIdleConns, tr.IdleConnTimeout)
	}
}

func TestHostsAreMatchedAndKeyedCanonically(t *testing.T) {
	m := Host("Example.com")
	for _, h := range []string{"example.com", "EXAMPLE.com:443", "example.com:80", "example.com."} {
		req, _ := http.NewRequest(http.MethodGet, "https://"+h+"/", nil)
		if !m(req) {
			t.Errorf("Host(%q) did not match %q", "Example.com", h)
		}
	}
	req, _ := http.NewRequest(http.MethodGet, "https://example.com:8443/", nil)
	if m(req) {
		t.Error("a non-default port is a different service and must not match")
	}
	if !HostPath("svc.test", "/api")(mustReq("https://SVC.test:443/api/x")) {
		t.Error("HostPath must canonicalise the host too")
	}

	rt, ep := perHostTransport(t, Config{ShardCount: 1})
	defer rt.Close()
	a := rt.getEntry(ep, "https", "Svc.test:443")
	b := rt.getEntry(ep, "https", "svc.test")
	if a != b {
		t.Error("two spellings of one host got separate circuits")
	}
}

func mustReq(url string) *http.Request {
	r, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		panic(err)
	}
	return r
}

func TestGuardAllRefusesAHealthHeader(t *testing.T) {
	_, err := New(Config{}, GuardAll(Policy{HealthPath: "/health", HealthHeader: http.Header{"Authorization": {"Bearer secret"}}}))
	if err == nil || !strings.Contains(err.Error(), "HealthHeader") {
		t.Fatalf("got %v, want GuardAll to refuse a HealthHeader", err)
	}
}

func TestSharedCircuitIsOneObjectAndIsListed(t *testing.T) {
	rt, err := New(Config{Transport: &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) { return okResp(), nil }}},
		WithEndpoint("svc", func(*http.Request) bool { return true }, Policy{}))
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	ep := rt.endpoints[0]
	first := rt.getEntry(ep, "http", "a.test")
	if again := rt.getEntry(ep, "http", "b.test"); again != first {
		t.Fatal("a shared endpoint must have exactly one circuit")
	}
	if n := len(rt.Stats().Breakers); n != 1 {
		t.Fatalf("Stats lists %d circuits, want the shared one", n)
	}
}

func TestEvictionPrefersClosedCircuits(t *testing.T) {
	rt, err := New(Config{MaxBreakers: 2, ShardCount: 1, Transport: &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) { return okResp(), nil }}},
		WithEndpoint("svc", func(*http.Request) bool { return true }, tripOnFirstFailure(), func(p *Policy) {}))
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	ep := rt.endpoints[0]
	ep.perHost = true

	down := rt.getEntry(ep, "http", "down.test")
	up := rt.getEntry(ep, "http", "up.test")
	p, err := down.breaker.Acquire(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	p.Failure() // opens down.test's circuit; it is now the least recently used
	if down.breaker.State() != breaker.StateOpen {
		t.Fatalf("state %v, want open", down.breaker.State())
	}

	rt.getEntry(ep, "http", "third.test") // over the cap: something must go

	s := rt.shards[0]
	s.mu.RLock()
	_, hasDown := s.entries[key{idx: ep.idx, host: "down.test"}]
	_, hasUp := s.entries[key{idx: ep.idx, host: "up.test"}]
	s.mu.RUnlock()
	if !hasDown {
		t.Error("the open circuit was evicted: a burst of new hosts could hide an outage")
	}
	if hasUp {
		t.Error("the closed circuit should have been the one to go")
	}
	_ = up
	time.Sleep(10 * time.Millisecond)
}
