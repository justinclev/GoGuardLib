package goguard

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type trackedBody struct {
	io.Reader
	closed int32
}

func (b *trackedBody) Close() error {
	atomic.StoreInt32(&b.closed, 1)
	return nil
}

func TestRetriedResponseBodyIsClosed(t *testing.T) {
	first := &trackedBody{Reader: strings.NewReader("busy")}
	var calls int32
	rt := NewResilientTransport(Config{
		Transport: &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) {
			if atomic.AddInt32(&calls, 1) == 1 {
				return &http.Response{StatusCode: 503, Body: first}, nil
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("ok"))}, nil
		}},
		MaxRetries: 1,
	})
	defer rt.Close()

	req, _ := http.NewRequest(http.MethodGet, "http://retry.test/", nil)
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 after retry", resp.StatusCode)
	}
	if atomic.LoadInt32(&first.closed) != 1 {
		t.Fatal("body of the retried 503 response was leaked")
	}
}

// RequestTimeout must not cancel the request before the caller has read the
// response body.
func TestBodyReadableAfterRoundTripWithTimeout(t *testing.T) {
	rt := NewResilientTransport(Config{
		RequestTimeout: time.Minute,
		Transport: &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
			body := &ctxBody{req: req, data: strings.NewReader("payload")}
			return &http.Response{StatusCode: 200, Body: body}, nil
		}},
	})
	defer rt.Close()

	req, _ := http.NewRequest(http.MethodGet, "http://body.test/", nil)
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil || string(got) != "payload" {
		t.Fatalf("read = %q, %v; want payload", got, err)
	}
	resp.Body.Close()
}

type ctxBody struct {
	req  *http.Request
	data io.Reader
}

func (b *ctxBody) Read(p []byte) (int, error) {
	if err := b.req.Context().Err(); err != nil {
		return 0, err
	}
	return b.data.Read(p)
}
func (b *ctxBody) Close() error { return nil }

// A latency-outlier failure on a 2xx used to skip MarkFailure and leak its
// in-flight slot, eventually starving the bulkhead.
func TestOutlierFailureReleasesSlot(t *testing.T) {
	rt := NewResilientTransport(Config{
		MaxLatency:       5 * time.Millisecond,
		MaxInflight:      1,
		FailureThreshold: 1.0,
		MinSamples:       1000, // never trips; only slot accounting is under test
		Transport: &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) {
			time.Sleep(20 * time.Millisecond)
			return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
		}},
	})
	defer rt.Close()

	for i := 0; i < 3; i++ {
		req, _ := http.NewRequest(http.MethodGet, "http://slow.test/", nil)
		resp, err := rt.RoundTrip(req)
		if err != nil {
			t.Fatalf("request %d rejected: %v", i, err)
		}
		resp.Body.Close()
	}
}

func TestCloseIsIdempotentAndBlocksNewRequests(t *testing.T) {
	rt := NewResilientTransport(Config{MaxIdleTime: 10 * time.Millisecond, Transport: &mockTransport{
		roundTrip: func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
		},
	}})
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, "http://closed.test/", nil)
	if _, err := rt.RoundTrip(req); err == nil {
		t.Fatal("RoundTrip succeeded after Close")
	}
}

// The heartbeat used to start only when OnStateChange was configured.
func TestHeartbeatRunsWithoutOnStateChange(t *testing.T) {
	var probes int32
	rt := NewResilientTransport(Config{
		FailureThreshold:  0.5,
		SleepWindow:       time.Hour,
		HeartbeatInterval: 5 * time.Millisecond,
		HeartbeatFunc: func(string) error {
			atomic.AddInt32(&probes, 1)
			return errors.New("still down")
		},
		Transport: &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) {
			return nil, errors.New("down")
		}},
	})

	req, _ := http.NewRequest(http.MethodGet, "http://hb.test/", nil)
	_, _ = rt.RoundTrip(req) // trips the breaker

	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&probes) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if atomic.LoadInt32(&probes) == 0 {
		t.Fatal("heartbeat never probed an open breaker")
	}

	rt.Close() // must wait for the heartbeat goroutine
	n := atomic.LoadInt32(&probes)
	time.Sleep(30 * time.Millisecond)
	if atomic.LoadInt32(&probes) != n {
		t.Fatal("heartbeat kept probing after Close returned")
	}
}

func TestStateStringAndAliases(t *testing.T) {
	cases := map[State]string{StateClosed: "closed", StateOpen: "open", StateHalfOpen: "half-open", State(9): "unknown"}
	for s, want := range cases {
		if s.String() != want {
			t.Errorf("State(%d).String() = %q, want %q", int(s), s.String(), want)
		}
	}
}
