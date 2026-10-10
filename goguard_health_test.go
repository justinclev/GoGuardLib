package goguard

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/health"
)

// service is a fake dependency with switchable health and traffic behaviour.
type service struct {
	*httptest.Server
	up           atomic.Bool // traffic succeeds
	healthy      atomic.Bool // /health succeeds
	healthHits   atomic.Int32
	trafficHits  atomic.Int32
	lastAuth     atomic.Value
	healthMethod atomic.Value
}

func newService(t *testing.T) *service {
	s := &service{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			s.healthHits.Add(1)
			s.lastAuth.Store(r.Header.Get("X-Probe-Token"))
			s.healthMethod.Store(r.Method)
			if s.healthy.Load() {
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		s.trafficHits.Add(1)
		if !s.up.Load() {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(s.Close)
	return s
}

func healthPolicy() Policy {
	return Policy{
		FailureThreshold: 0.1, MinSamples: 1,
		SleepWindow: 20 * time.Millisecond, // far shorter than the probes: only health may end the outage
		HealthPath:  "/health",
		Health:      &health.Config{Interval: 10 * time.Millisecond, MaxInterval: 20 * time.Millisecond, Jitter: -1, SuccessThreshold: 2},
	}
}

func doGet(t *testing.T, rt http.RoundTripper, url string) error {
	t.Helper()
	resp, err := get(t, rt, url)
	if resp != nil {
		_ = resp.Body.Close()
	}
	return err
}

func TestHealthEndpointDrivesRecovery(t *testing.T) {
	svc := newService(t)
	svc.up.Store(true)
	rt := NewResilientTransport(Config{}, WithEndpoint("svc", Host(hostOf(svc.Server)), healthPolicy()))
	defer rt.Close()

	// While the dependency is fine nobody probes it.
	for i := 0; i < 5; i++ {
		if err := doGet(t, rt, svc.URL); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(60 * time.Millisecond)
	if svc.healthHits.Load() != 0 {
		t.Fatalf("%d health checks hit a healthy dependency", svc.healthHits.Load())
	}

	// It goes down: the circuit opens and probing starts.
	svc.up.Store(false)
	_ = doGet(t, rt, svc.URL)
	if err := doGet(t, rt, svc.URL); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("err = %v, want the circuit open", err)
	}

	// /health still says down: many sleep windows pass and no real request is spent.
	trafficBefore := svc.trafficHits.Load()
	time.Sleep(150 * time.Millisecond)
	if err := doGet(t, rt, svc.URL); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("err = %v, want the circuit held open while /health fails", err)
	}
	if svc.trafficHits.Load() != trafficBefore {
		t.Fatal("real requests were sent to a dependency whose /health is failing")
	}
	if svc.healthHits.Load() == 0 {
		t.Fatal("no health checks ran while the circuit was open")
	}

	// It recovers: /health passes, then a real request closes the circuit.
	svc.up.Store(true)
	svc.healthy.Store(true)
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := doGet(t, rt, svc.URL)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never recovered: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if st := rt.Stats().Breakers[0]; st.State != StateClosed {
		t.Fatalf("state = %v after a successful canary", st.State)
	}
}

func TestProbesBypassTheGuardedTransportAndAreNotTraffic(t *testing.T) {
	var probes, traffic atomic.Int32
	under := &mockTransport{roundTrip: func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/health" {
			probes.Add(1)
			return okResp(), nil
		}
		traffic.Add(1)
		return &http.Response{StatusCode: 500, Body: http.NoBody}, nil
	}}
	rt := NewResilientTransport(Config{Transport: under}, WithEndpoint("svc", Host("svc.test"), healthPolicy()))
	defer rt.Close()

	_ = doGet(t, rt, "http://svc.test/work") // fails and opens the circuit
	deadline := time.Now().Add(3 * time.Second)
	for probes.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if probes.Load() < 3 {
		t.Fatal("probes never went through the underlying transport")
	}
	st := rt.Stats().Breakers[0]
	if st.Failure != 1 || st.Success != 0 {
		t.Fatalf("stats = %+v: only the real request may count, never the probes", st)
	}
}

func TestPerHostCircuitsProbeTheirOwnHost(t *testing.T) {
	a, b := newService(t), newService(t)
	a.up.Store(false)
	b.up.Store(true)
	rt := NewResilientTransport(Config{}, GuardAll(healthPolicy()))
	defer rt.Close()

	_ = doGet(t, rt, a.URL)
	_ = doGet(t, rt, a.URL)
	if err := doGet(t, rt, b.URL); err != nil {
		t.Fatalf("host B affected by host A's outage: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for a.healthHits.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if a.healthHits.Load() == 0 {
		t.Fatal("host A was never probed")
	}
	if b.healthHits.Load() != 0 {
		t.Fatal("healthy host B was probed")
	}
}

func TestHealthHeaderAndCustomCheck(t *testing.T) {
	svc := newService(t)
	pol := healthPolicy()
	pol.HealthHeader = http.Header{"X-Probe-Token": {"s3cret"}}
	rt := NewResilientTransport(Config{}, WithEndpoint("svc", Host(hostOf(svc.Server)), pol))
	_ = doGet(t, rt, svc.URL)
	deadline := time.Now().Add(3 * time.Second)
	for svc.healthHits.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if svc.lastAuth.Load() != "s3cret" || svc.healthMethod.Load() != http.MethodGet {
		t.Fatalf("probe sent %v %v", svc.healthMethod.Load(), svc.lastAuth.Load())
	}
	rt.Close()

	// A custom, non-HTTP check.
	var calls atomic.Int32
	pol = healthPolicy()
	pol.HealthPath = ""
	pol.Health.Check = func(context.Context) error { calls.Add(1); return errors.New("down") }
	rt2 := NewResilientTransport(Config{Transport: downTransport()}, WithEndpoint("svc", Host("x.test"), pol))
	defer rt2.Close()
	_ = doGet(t, rt2, "http://x.test/")
	deadline = time.Now().Add(3 * time.Second)
	for calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if calls.Load() == 0 {
		t.Fatal("custom check never ran")
	}
}

func TestHealthConfigurationIsValidated(t *testing.T) {
	m := Host("x")
	for name, pol := range map[string]Policy{
		"health without check": {Health: &health.Config{}},
		"path without slash":   {HealthPath: "health"},
	} {
		if _, err := New(Config{Endpoints: []Endpoint{{Name: "a", Match: m, Policy: pol}}}); err == nil {
			t.Errorf("%s: expected an error", name)
		}
		if _, err := New(Config{GuardAll: &pol}); err == nil {
			t.Errorf("%s (GuardAll): expected an error", name)
		}
	}
}

func TestEvictedCircuitsStopProbing(t *testing.T) {
	var calls atomic.Int32
	pol := healthPolicy()
	pol.HealthPath = ""
	pol.Health.Check = func(context.Context) error { calls.Add(1); return errors.New("down") }
	rt, err := New(Config{MaxBreakers: 2, ShardCount: 1, Endpoints: []Endpoint{{Name: "svc", Match: func(*http.Request) bool { return true }, PerHost: true, Policy: pol}}})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	ep := rt.endpoints[0]

	first := rt.getEntry(ep, "http", "host1")
	rt.getEntry(ep, "http", "host2")
	rt.getEntry(ep, "http", "host3") // evicts host1, which stops its background work
	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 3; i++ { // the evicted circuit opens: nobody may start probing for it
		permit, err := first.breaker.Acquire(context.Background(), false)
		if err != nil {
			break
		}
		permit.Failure()
	}
	time.Sleep(80 * time.Millisecond)
	if n := calls.Load(); n != 0 {
		t.Fatalf("an evicted circuit ran %d health checks", n)
	}
}

func TestTransportCloseStopsAllProbers(t *testing.T) {
	svc := newService(t)
	rt := NewResilientTransport(Config{}, WithEndpoint("svc", Host(hostOf(svc.Server)), healthPolicy()))
	_ = doGet(t, rt, svc.URL) // opens the circuit; probing starts
	deadline := time.Now().Add(3 * time.Second)
	for svc.healthHits.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}
	n := svc.healthHits.Load()
	time.Sleep(60 * time.Millisecond)
	if svc.healthHits.Load() != n {
		t.Fatal("probes continued after Close returned")
	}
}
