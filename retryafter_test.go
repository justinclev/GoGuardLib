package goguard

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryAfterParsing(t *testing.T) {
	now := time.Date(2026, 1, 2, 15, 0, 0, 0, time.UTC)
	hdr := func(v string) *http.Response { return &http.Response{Header: http.Header{"Retry-After": {v}}} }
	cases := []struct {
		name string
		resp *http.Response
		want time.Duration
	}{
		{"seconds", hdr("3"), 3 * time.Second},
		{"date", hdr(now.Add(7 * time.Second).Format(http.TimeFormat)), 7 * time.Second},
		{"past date", hdr(now.Add(-time.Minute).Format(http.TimeFormat)), 0},
		{"garbage", hdr("soon"), 0},
		{"negative", hdr("-5"), 0},
		{"huge is capped", hdr("86400"), maxRetryAfter},
		{"absent", &http.Response{Header: http.Header{}}, 0},
		{"nil response", nil, 0},
	}
	for _, c := range cases {
		if got := retryAfter(c.resp, now); got != c.want {
			t.Errorf("%s: retryAfter = %v, want %v", c.name, got, c.want)
		}
	}
}

// A server that says when to come back must not be hit again before then.
func TestRetryWaitsForRetryAfter(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	rt, err := New(Config{}, WithEndpoint("svc", func(*http.Request) bool { return true }, Policy{MaxRetries: 1, MinSamples: 100, RetryBudget: 1}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Close() }()
	start := time.Now()
	resp, err := (&http.Client{Transport: rt}).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || calls.Load() != 2 {
		t.Fatalf("status %d after %d calls, want 200 after 2", resp.StatusCode, calls.Load())
	}
	if d := time.Since(start); d < 900*time.Millisecond {
		t.Fatalf("retried after %v, ignoring Retry-After: 1", d)
	}
}

// A server that accepts the connection and then goes silent must not hold the
// caller for ever.
func TestDefaultTransportBoundsTheWaitForResponseHeaders(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-release }))
	defer srv.Close()
	defer close(release)
	rt, err := New(Config{ResponseHeaderTimeout: 100 * time.Millisecond}, WithEndpoint("svc", func(*http.Request) bool { return true }, Policy{MinSamples: 100}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rt.Close() }()
	start := time.Now()
	resp, err := (&http.Client{Transport: rt}).Get(srv.URL)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected a timeout")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("waited %v for headers", d)
	}
	if got := responseHeaderTimeout(0); got != 30*time.Second {
		t.Fatalf("default = %v, want 30s", got)
	}
}
