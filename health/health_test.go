package health

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/obs"
)

type fakeTarget struct {
	mu        sync.Mutex
	active    bool
	gen       uint64
	results   []bool
	consec    []int
	times     []time.Time
	stopAfter int
}

func newTarget(stopAfter int) *fakeTarget { return &fakeTarget{active: true, stopAfter: stopAfter} }

func (f *fakeTarget) Active() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active
}
func (f *fakeTarget) Generation() uint64 { f.mu.Lock(); defer f.mu.Unlock(); return f.gen }
func (f *fakeTarget) bump()              { f.mu.Lock(); f.gen++; f.mu.Unlock() }
func (f *fakeTarget) Result(ok bool, consecutive int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.results = append(f.results, ok)
	f.consec = append(f.consec, consecutive)
	f.times = append(f.times, time.Now())
	if f.stopAfter > 0 && len(f.results) >= f.stopAfter {
		f.active = false
	}
}

func fast(check Check) Config {
	return Config{Check: check, Interval: 5 * time.Millisecond, MaxInterval: 40 * time.Millisecond, Timeout: 200 * time.Millisecond, Jitter: -1}.WithDefaults()
}

func runToEnd(t *testing.T, cfg Config, tg Target, sink obs.Sink) {
	t.Helper()
	done := make(chan struct{})
	go func() { Run(context.Background(), cfg, "dep", tg, sink); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestRunCountsConsecutiveHealthyProbesAndStopsWhenInactive(t *testing.T) {
	tg := newTarget(3)
	runToEnd(t, fast(func(context.Context) error { return nil }), tg, nil)
	if fmt.Sprint(tg.consec) != "[1 2 3]" {
		t.Fatalf("consecutive = %v, want [1 2 3]", tg.consec)
	}
}

func TestUnhealthyProbesBackOffAndResetTheRun(t *testing.T) {
	var n atomic.Int32
	check := func(context.Context) error {
		if n.Add(1) == 4 {
			return nil // one healthy blip
		}
		return errors.New("down")
	}
	tg := newTarget(9)
	runToEnd(t, fast(check), tg, nil)

	gaps := func(i int) time.Duration { return tg.times[i].Sub(tg.times[i-1]) }
	if gaps(3) <= gaps(1) { // 5ms, 10ms, 20ms, 40ms(cap)
		t.Fatalf("no backoff: gaps %v %v %v", gaps(1), gaps(2), gaps(3))
	}
	if tg.consec[3] != 1 || tg.consec[4] != 0 {
		t.Fatalf("consecutive = %v; a failure must reset the run", tg.consec)
	}
	if gaps(8) > 250*time.Millisecond { // the cap is 40ms; allow generous scheduling slack
		t.Fatalf("backoff not capped: %v", gaps(8))
	}
}

func TestRelapseStartsTheRunAfresh(t *testing.T) {
	tg := newTarget(0)
	var seen atomic.Int32
	cfg := fast(func(context.Context) error {
		if seen.Add(1) == 3 {
			tg.bump() // the target failed again while we were probing
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { Run(ctx, cfg, "dep", tg, nil); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		tg.mu.Lock()
		n := len(tg.consec)
		tg.mu.Unlock()
		if n >= 5 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	tg.mu.Lock()
	defer tg.mu.Unlock()
	if fmt.Sprint(tg.consec[:5]) != "[1 2 3 1 2]" {
		t.Fatalf("consecutive = %v, want the count to restart after the relapse: [1 2 3 1 2]", tg.consec[:5])
	}
}

func TestPanickingAndSlowChecksCountAsUnhealthy(t *testing.T) {
	var n atomic.Int32
	cfg := Config{
		Interval: 2 * time.Millisecond, MaxInterval: 4 * time.Millisecond, Timeout: 30 * time.Millisecond, Jitter: -1,
		Check: func(ctx context.Context) error {
			switch n.Add(1) {
			case 1:
				panic("bug in the check")
			case 2:
				<-ctx.Done() // hangs until the timeout
				return ctx.Err()
			}
			return nil
		},
	}.WithDefaults()
	tg := newTarget(3)
	start := time.Now()
	runToEnd(t, cfg, tg, nil)
	if fmt.Sprint(tg.results) != "[false false true]" {
		t.Fatalf("results = %v", tg.results)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("the timeout was not enforced")
	}
}

func TestRunStopsWhenContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := Config{Check: func(context.Context) error { return nil }, Interval: time.Hour, Jitter: -1}.WithDefaults()
	done := make(chan struct{})
	go func() { Run(ctx, cfg, "dep", newTarget(0), nil); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run ignored cancellation")
	}
}

func TestProbeEventsCarryOutcomeNotErrorText(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer srv.Close()
	check, err := HTTP(srv.URL + "/health?token=SECRET-IN-URL")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var events []obs.ProbeResult
	sink := obs.SinkFunc(func(e obs.Event) {
		mu.Lock()
		events = append(events, e.(obs.ProbeResult))
		mu.Unlock()
	})
	runToEnd(t, fast(check), newTarget(2), sink)

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 {
		t.Fatalf("got %d events", len(events))
	}
	e := events[0]
	if e.OK || e.Status != 503 || e.Dependency != "dep" || e.Latency <= 0 || e.At.IsZero() {
		t.Fatalf("event = %+v", e)
	}
	if strings.Contains(fmt.Sprintf("%+v", e), "SECRET") || strings.Contains(fmt.Sprintf("%+v", e), srv.URL) {
		t.Fatal("the event leaked the probed URL")
	}
}

func TestHTTPCheck(t *testing.T) {
	var gotMethod, gotAuth atomic.Value
	var status atomic.Int32
	status.Store(200)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod.Store(r.Method)
		gotAuth.Store(r.Header.Get("Authorization"))
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/health", http.StatusFound)
			return
		}
		if r.URL.Path == "/huge" {
			w.Write(make([]byte, 50<<20)) // a misbehaving health endpoint
			return
		}
		w.WriteHeader(int(status.Load()))
	}))
	defer srv.Close()
	ctx := context.Background()

	check, err := HTTP(srv.URL+"/health", WithMethod(http.MethodHead), WithHeader("Authorization", "Bearer probe-token"))
	if err != nil {
		t.Fatal(err)
	}
	if err := check(ctx); err != nil {
		t.Fatalf("healthy: %v", err)
	}
	if gotMethod.Load() != "HEAD" || gotAuth.Load() != "Bearer probe-token" {
		t.Fatalf("sent %v %v", gotMethod.Load(), gotAuth.Load())
	}

	status.Store(500)
	var se *StatusError
	if err := check(ctx); !errors.As(err, &se) || se.Code != 500 || !strings.Contains(err.Error(), "500") {
		t.Fatalf("err = %v, want a 500 StatusError", err)
	}

	redirect, _ := HTTP(srv.URL + "/redirect")
	if err := redirect(ctx); !errors.As(err, &se) || se.Code != 302 {
		t.Fatalf("a redirect must not be followed: %v", err)
	}

	status.Store(204)
	custom, _ := HTTP(srv.URL+"/health", WithHealthyStatus(func(s int) bool { return s == 204 }))
	if err := custom(ctx); err != nil {
		t.Fatalf("custom healthy status: %v", err)
	}

	huge, _ := HTTP(srv.URL + "/huge")
	start := time.Now()
	if err := huge(ctx); err != nil {
		t.Fatalf("huge: %v", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("the response body was not bounded")
	}
}

func TestHTTPCheckUsesGivenTransportAndValidatesURL(t *testing.T) {
	var used atomic.Int32
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		used.Add(1)
		return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
	})
	check, err := HTTP("http://svc.internal/health", WithRoundTripper(rt))
	if err != nil {
		t.Fatal(err)
	}
	if err := check(context.Background()); err != nil || used.Load() != 1 {
		t.Fatalf("err=%v used=%d", err, used.Load())
	}
	for _, bad := range []string{"", "ftp://x/y", "/health", "http://", "::"} {
		if _, err := HTTP(bad); err == nil {
			t.Errorf("HTTP(%q) accepted", bad)
		}
	}
	// A failing transport surfaces as an error, not a panic.
	down, _ := HTTP("http://svc.internal/health", WithRoundTripper(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("connection refused")
	})))
	if down(context.Background()) == nil {
		t.Fatal("transport error not reported")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestDelayAndDefaults(t *testing.T) {
	cfg := Config{Interval: 100 * time.Millisecond, MaxInterval: 800 * time.Millisecond, Jitter: -1}.WithDefaults()
	if delay(cfg, true, 0) != 100*time.Millisecond || delay(cfg, true, 5) != 100*time.Millisecond {
		t.Fatal("healthy probes poll at the steady interval")
	}
	for f, want := range map[int]time.Duration{1: 100, 2: 200, 3: 400, 4: 800, 9: 800} {
		if got := delay(cfg, false, f); got != want*time.Millisecond {
			t.Errorf("after %d failures: %v, want %vms", f, got, want)
		}
	}
	jit := Config{Interval: 100 * time.Millisecond, Jitter: 0.5}.WithDefaults()
	varied := false
	for i := 0; i < 200; i++ {
		d := delay(jit, false, 1)
		if d < 50*time.Millisecond || d > 100*time.Millisecond {
			t.Fatalf("jittered delay %v out of range", d)
		}
		varied = varied || d != 100*time.Millisecond
	}
	if !varied {
		t.Fatal("no jitter applied")
	}
	d := Config{}.WithDefaults()
	if d.Interval != 5*time.Second || d.Timeout != 3*time.Second || d.SuccessThreshold != 2 || d.Jitter != 0.2 ||
		d.MaxOpen != 5*time.Minute || d.MaxInterval != time.Minute || d.TrustHealth {
		t.Fatalf("defaults = %+v", d)
	}
	if c := (Config{Interval: time.Minute, MaxInterval: time.Second}).WithDefaults(); c.MaxInterval != time.Minute {
		t.Fatal("MaxInterval must not be below Interval")
	}
}

func TestMustURLBuildsAConfigAndPanicsOnABadURL(t *testing.T) {
	if cfg := MustURL("http://payments.internal/health"); cfg == nil || cfg.Check == nil {
		t.Fatal("MustURL must return a Config with a Check")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("a relative URL must panic")
		}
	}()
	MustURL("/health")
}
