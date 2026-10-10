package goguard

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/obs"
	"github.com/justinclev/GoGuardLib/retry"
)

func TestGuardedEndpointTripsAndRecovers(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	ts, _ := countingServer(func() int {
		if fail.Load() {
			return http.StatusInternalServerError
		}
		return http.StatusOK
	})
	defer ts.Close()

	rt := NewResilientTransport(Config{}, WithEndpoint("api", Host(hostOf(ts)), Policy{
		FailureThreshold: 0.1, MinSamples: 1, SleepWindow: 300 * time.Millisecond, SamplingWindow: time.Second,
	}))
	defer rt.Close()
	client := &http.Client{Transport: rt}

	resp, err := client.Get(ts.URL)
	if err != nil || resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("first request: resp=%v err=%v, want the 500 passed through", resp, err)
	}
	resp.Body.Close()

	if _, err = client.Get(ts.URL); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("second request err = %v, want ErrCircuitOpen", err)
	}

	time.Sleep(450 * time.Millisecond)
	fail.Store(false)
	resp, err = client.Get(ts.URL)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("recovery: resp=%v err=%v", resp, err)
	}
	resp.Body.Close()
}

// Nothing is guarded unless registered: unmatched traffic reaches the server
// untouched and allocates no circuit.
func TestUnmatchedRequestsPassThroughUnguarded(t *testing.T) {
	ts, hits := countingServer(func() int { return http.StatusInternalServerError })
	defer ts.Close()

	rt := NewResilientTransport(Config{}, WithEndpoint("other", Host("some.other.host"), tripOnFirstFailure()))
	defer rt.Close()

	for i := 0; i < 20; i++ {
		resp, err := get(t, rt, ts.URL)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()
	}
	if got := atomic.LoadInt32(hits); got != 20 {
		t.Fatalf("server saw %d requests, want all 20", got)
	}
	st := rt.Stats()
	if st.Passthrough != 20 || len(st.Breakers) != 0 {
		t.Fatalf("stats = %+v, want 20 passthrough and no circuits", st)
	}
}

func TestOnlyRegisteredEndpointIsGuarded(t *testing.T) {
	bad, _ := countingServer(func() int { return http.StatusInternalServerError })
	other, otherHits := countingServer(func() int { return http.StatusInternalServerError })
	defer bad.Close()
	defer other.Close()

	rt := NewResilientTransport(Config{}, WithEndpoint("bad", Host(hostOf(bad)), tripOnFirstFailure()))
	defer rt.Close()

	for i := 0; i < 3; i++ {
		if resp, err := get(t, rt, bad.URL); err == nil {
			resp.Body.Close()
		}
	}
	if _, err := get(t, rt, bad.URL); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("guarded endpoint err = %v, want open circuit", err)
	}
	resp, err := get(t, rt, other.URL)
	if err != nil {
		t.Fatalf("unguarded endpoint was blocked: %v", err)
	}
	resp.Body.Close()
	if atomic.LoadInt32(otherHits) != 1 {
		t.Fatal("unguarded request did not reach its server")
	}
}

func TestFirstMatchingEndpointWins(t *testing.T) {
	rt := NewResilientTransport(Config{},
		WithEndpoint("specific", HostPath("svc.test", "/v2/"), Policy{}),
		WithEndpoint("broad", Host("svc.test"), Policy{}),
		WithEndpoint("catch", func(*http.Request) bool { return true }, Policy{}),
	)
	defer rt.Close()

	cases := map[string]string{
		"http://svc.test/v2/items": "specific",
		"http://SVC.test/v1/items": "broad",
		"http://elsewhere.test/":   "catch",
	}
	for url, want := range cases {
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		ep, err := rt.resolve(req)
		if err != nil || ep == nil || ep.Name != want {
			t.Errorf("%s resolved to %v (err %v), want %s", url, ep, err, want)
		}
	}
}

func TestUseAndSkipOverrideMatching(t *testing.T) {
	ts, hits := countingServer(func() int { return http.StatusInternalServerError })
	defer ts.Close()
	host := hostOf(ts)

	rt := NewResilientTransport(Config{},
		WithEndpoint("guarded", Host(host), tripOnFirstFailure()),
		WithEndpoint("strict", Host("never.matches"), tripOnFirstFailure()),
	)
	defer rt.Close()

	// Trip "guarded".
	for i := 0; i < 2; i++ {
		if resp, err := get(t, rt, ts.URL); err == nil {
			resp.Body.Close()
		}
	}
	if _, err := get(t, rt, ts.URL); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected open circuit, got %v", err)
	}
	before := atomic.LoadInt32(hits)

	// Skip bypasses the open circuit entirely.
	req, _ := http.NewRequestWithContext(Skip(context.Background()), http.MethodGet, ts.URL, nil)
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("Skip did not bypass the circuit: %v", err)
	}
	resp.Body.Close()
	if atomic.LoadInt32(hits) != before+1 {
		t.Fatal("skipped request did not reach the server")
	}

	// Use routes to another registered endpoint whose circuit is still closed.
	req, _ = http.NewRequestWithContext(Use(context.Background(), "strict"), http.MethodGet, ts.URL, nil)
	if resp, err = rt.RoundTrip(req); err != nil {
		t.Fatalf("Use(strict) should hit a fresh circuit: %v", err)
	}
	resp.Body.Close()

	// Unknown names fail closed.
	req, _ = http.NewRequestWithContext(Use(context.Background(), "typo"), http.MethodGet, ts.URL, nil)
	if _, err = rt.RoundTrip(req); !errors.Is(err, ErrUnknownEndpoint) {
		t.Fatalf("err = %v, want ErrUnknownEndpoint", err)
	}
}

func TestGuardAllGivesEachHostItsOwnCircuit(t *testing.T) {
	bad, _ := countingServer(func() int { return http.StatusInternalServerError })
	good, _ := countingServer(func() int { return http.StatusOK })
	defer bad.Close()
	defer good.Close()

	rt := NewResilientTransport(Config{}, GuardAll(tripOnFirstFailure()))
	defer rt.Close()

	for i := 0; i < 2; i++ {
		if resp, err := get(t, rt, bad.URL); err == nil {
			resp.Body.Close()
		}
	}
	if _, err := get(t, rt, bad.URL); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("bad host err = %v", err)
	}
	resp, err := get(t, rt, good.URL)
	if err != nil {
		t.Fatalf("good host affected by bad host's circuit: %v", err)
	}
	resp.Body.Close()

	names := map[string]bool{}
	for _, b := range rt.Stats().Breakers {
		names[b.Name] = true
	}
	if !names[hostOf(bad)] || !names[hostOf(good)] {
		t.Fatalf("circuits named %v, want the hosts", names)
	}
}

func TestSharedVersusPerHostCircuits(t *testing.T) {
	flaky := &mockTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
		if r.URL.Host == "a.test" {
			return nil, errors.New("down")
		}
		return okResp(), nil
	}}
	match := Host("a.test", "b.test")

	shared := NewResilientTransport(Config{Transport: flaky}, WithEndpoint("svc", match, tripOnFirstFailure()))
	defer shared.Close()
	_, _ = get(t, shared, "http://a.test/")
	if _, err := get(t, shared, "http://b.test/"); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("shared circuit: b.test err = %v, want it rejected because a.test failed", err)
	}

	ph, err := New(Config{Transport: flaky, Endpoints: []Endpoint{{Name: "svc", Match: match, Policy: tripOnFirstFailure(), PerHost: true}}})
	if err != nil {
		t.Fatal(err)
	}
	defer ph.Close()
	_, _ = get(t, ph, "http://a.test/")
	resp, err := get(t, ph, "http://b.test/")
	if err != nil {
		t.Fatalf("per-host circuit: b.test rejected: %v", err)
	}
	resp.Body.Close()
	names := map[string]bool{}
	for _, b := range ph.Stats().Breakers {
		names[b.Name] = true
	}
	if !names["svc@a.test"] || !names["svc@b.test"] {
		t.Fatalf("per-host names = %v", names)
	}
}

func TestNewValidatesConfiguration(t *testing.T) {
	m := Host("x")
	bad := map[string][]Endpoint{
		"empty name":    {{Match: m}},
		"reserved name": {{Name: "*", Match: m}},
		"nil matcher":   {{Name: "a"}},
		"duplicate":     {{Name: "a", Match: m}, {Name: "a", Match: m}},
	}
	for name, eps := range bad {
		if _, err := New(Config{Endpoints: eps}); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	defer func() {
		if recover() == nil {
			t.Fatal("NewResilientTransport must panic on invalid configuration")
		}
	}()
	NewResilientTransport(Config{Endpoints: bad["duplicate"]})
}

func TestOnStateChangeIsAsyncOrderedAndDrainedOnClose(t *testing.T) {
	release := make(chan struct{})
	var mu sync.Mutex
	var got []string
	rt := NewResilientTransport(Config{
		Transport: &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) { return nil, errors.New("down") }},
		OnStateChange: func(circuit string, from, to State) {
			<-release // a slow callback
			mu.Lock()
			got = append(got, circuit+":"+from.String()+">"+to.String())
			mu.Unlock()
		},
	}, WithEndpoint("svc", Host("x.test"), tripOnFirstFailure()))

	done := make(chan struct{})
	go func() {
		_, _ = get(t, rt, "http://x.test/") // trips the circuit
		_, err := get(t, rt, "http://x.test/")
		if !errors.Is(err, ErrCircuitOpen) {
			t.Errorf("err = %v", err)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("requests were blocked by a slow OnStateChange callback")
	}

	close(release)
	_ = rt.Close()
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != "svc:closed>open" {
		t.Fatalf("callback saw %v, want [svc:closed>open]", got)
	}
}

func TestEventsSinkSeesRetries(t *testing.T) {
	var calls int32
	var mu sync.Mutex
	var retries []obs.Retried
	rt := NewResilientTransport(Config{
		Transport: &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) {
			if atomic.AddInt32(&calls, 1) <= 2 {
				return &http.Response{StatusCode: 503, Body: http.NoBody}, nil
			}
			return okResp(), nil
		}},
		Events: obs.SinkFunc(func(e obs.Event) {
			if r, ok := e.(obs.Retried); ok {
				mu.Lock()
				retries = append(retries, r)
				mu.Unlock()
			}
		}),
	}, WithEndpoint("svc", Host("r.test"), Policy{MinSamples: 100, RetryBackoff: retry.Constant(time.Millisecond)}, WithRetries(2)))
	defer rt.Close()

	resp, err := get(t, rt, "http://r.test/")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("resp=%v err=%v", resp, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(retries) != 2 || retries[0].Attempt != 1 || retries[1].Attempt != 2 || retries[0].Dependency != "svc" || retries[0].Delay != time.Millisecond {
		t.Fatalf("retry events = %+v", retries)
	}
}

func TestNonIdempotentRequestsAreNotRetried(t *testing.T) {
	var calls int32
	rt := NewResilientTransport(Config{
		Transport: &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) {
			atomic.AddInt32(&calls, 1)
			return &http.Response{StatusCode: 503, Body: http.NoBody}, nil
		}},
	}, WithEndpoint("svc", Host("p.test"), Policy{MinSamples: 100}, WithRetries(3)))
	defer rt.Close()

	req, _ := http.NewRequest(http.MethodPost, "http://p.test/", nil)
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if atomic.LoadInt32(&calls) != 1 {
		t.Fatalf("POST attempted %d times, want 1", calls)
	}
}

func TestBulkheadRejectionIsDistinguishedFromOpenCircuit(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	rt := NewResilientTransport(Config{
		Transport: &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return okResp(), nil
		}},
	}, WithEndpoint("svc", Host("b.test"), Policy{MinSamples: 100}, WithBulkhead(1, 0)))
	defer rt.Close()

	go func() { _, _ = get(t, rt, "http://b.test/") }()
	<-started

	_, err := get(t, rt, "http://b.test/")
	var ce *CircuitError
	if !errors.As(err, &ce) || !errors.Is(err, breaker.ErrBulkhead) || errors.Is(err, ErrCircuitOpen) || !ce.Retryable {
		t.Fatalf("err = %v, want a retryable bulkhead rejection", err)
	}

	// VIP requests bypass the bulkhead.
	vipCtx := context.WithValue(context.Background(), PriorityKey, true)
	req, _ := http.NewRequestWithContext(vipCtx, http.MethodGet, "http://b.test/", nil)
	go func() { time.Sleep(20 * time.Millisecond); close(release) }()
	if _, err := rt.RoundTrip(req); err != nil {
		t.Fatalf("VIP request rejected: %v", err)
	}
}

func TestCallerCancellationIsNotCountedAgainstEndpoint(t *testing.T) {
	rt := NewResilientTransport(Config{
		Transport: &mockTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
			<-r.Context().Done()
			return nil, r.Context().Err()
		}},
	}, WithEndpoint("svc", Host("c.test"), tripOnFirstFailure()))
	defer rt.Close()

	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://c.test/", nil)
		go func() { time.Sleep(5 * time.Millisecond); cancel() }()
		_, err := rt.RoundTrip(req)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	}
	st := rt.Stats().Breakers[0]
	if st.State != StateClosed || st.Failure != 0 || st.Inflight != 0 {
		t.Fatalf("cancellations counted against the endpoint: %+v", st)
	}
}

func TestOutlierFactorIsOptIn(t *testing.T) {
	var slow atomic.Bool
	tr := &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) {
		if slow.Load() {
			time.Sleep(60 * time.Millisecond)
		} else {
			time.Sleep(time.Millisecond)
		}
		return okResp(), nil
	}}

	run := func(factor float64) int64 {
		rt := NewResilientTransport(Config{Transport: tr},
			WithEndpoint("svc", Host("o.test"), Policy{MinSamples: 1000, OutlierFactor: factor}))
		defer rt.Close()
		slow.Store(false)
		for i := 0; i < 10; i++ {
			if resp, err := get(t, rt, "http://o.test/"); err == nil {
				resp.Body.Close()
			}
		}
		slow.Store(true)
		before := rt.Stats().Breakers[0].Failure
		if resp, err := get(t, rt, "http://o.test/"); err == nil {
			resp.Body.Close()
		}
		return rt.Stats().Breakers[0].Failure - before
	}

	if got := run(0); got != 0 {
		t.Fatalf("outlier detection fired while disabled: %d failures", got)
	}
	if got := run(3); got < 1 {
		t.Fatal("slow response was not flagged with OutlierFactor 3")
	}
}

func TestStatsReportsCircuits(t *testing.T) {
	rt := NewResilientTransport(Config{
		Transport: &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) { return okResp(), nil }},
	}, WithEndpoint("svc", Host("s.test"), Policy{}))
	defer rt.Close()
	for i := 0; i < 3; i++ {
		resp, _ := get(t, rt, "http://s.test/")
		resp.Body.Close()
	}
	st := rt.Stats()
	if len(st.Breakers) != 1 || st.Breakers[0].Name != "svc" || st.Breakers[0].Success != 3 {
		t.Fatalf("stats = %+v", st)
	}
}
