package goguard

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/obs"
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
	}, WithEndpoint("svc", Host("retry.test"), Policy{MinSamples: 100}, WithRetries(1)))
	defer rt.Close()

	resp, err := get(t, rt, "http://retry.test/")
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

// RequestTimeout must not cancel the request before the caller has read the body.
func TestBodyReadableAfterRoundTripWithTimeout(t *testing.T) {
	rt := NewResilientTransport(Config{
		Transport: &mockTransport{roundTrip: func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: &ctxBody{req: req, data: strings.NewReader("payload")}}, nil
		}},
	}, WithEndpoint("svc", Host("body.test"), Policy{}, WithTimeout(time.Minute)))
	defer rt.Close()

	resp, err := get(t, rt, "http://body.test/")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil || string(got) != "payload" {
		t.Fatalf("read = %q, %v; want payload", got, err)
	}
	resp.Body.Close()
}

// A latency failure on a 2xx used to skip MarkFailure and leak its in-flight
// slot, eventually starving the bulkhead.
func TestMaxLatencyFailureReleasesSlot(t *testing.T) {
	rt := NewResilientTransport(Config{
		Transport: &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) {
			time.Sleep(20 * time.Millisecond)
			return okResp(), nil
		}},
	}, WithEndpoint("svc", Host("slow.test"), Policy{MaxLatency: 5 * time.Millisecond, MinSamples: 1000}, WithBulkhead(1, 0)))
	defer rt.Close()

	for i := 0; i < 3; i++ {
		resp, err := get(t, rt, "http://slow.test/")
		if err != nil {
			t.Fatalf("request %d rejected: %v", i, err)
		}
		resp.Body.Close()
	}
	if got := rt.Stats().Breakers[0]; got.Failure != 3 || got.Inflight != 0 {
		t.Fatalf("stats = %+v, want 3 failures and no leaked slot", got)
	}
}

func TestCloseIsIdempotentAndBlocksNewRequests(t *testing.T) {
	rt := NewResilientTransport(Config{
		MaxIdleTime: 10 * time.Millisecond,
		Transport:   &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) { return okResp(), nil }},
	})
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rt.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := get(t, rt, "http://closed.test/"); err == nil {
		t.Fatal("RoundTrip succeeded after Close")
	}
}

func downTransport() http.RoundTripper {
	return &mockTransport{roundTrip: func(*http.Request) (*http.Response, error) { return nil, errors.New("down") }}
}

func TestStateStringAndAliases(t *testing.T) {
	cases := map[State]string{StateClosed: "closed", StateOpen: "open", StateHalfOpen: "half-open", State(9): "unknown"}
	for s, want := range cases {
		if s.String() != want {
			t.Errorf("State(%d).String() = %q, want %q", int(s), s.String(), want)
		}
	}
}

type sinkFunc func()

func (f sinkFunc) Emit(obs.Event) { f() }

func TestWithEventsSinkReceivesStateChanges(t *testing.T) {
	var got int32
	rt := NewResilientTransport(Config{Transport: downTransport()},
		WithEvents(sinkFunc(func() { atomic.AddInt32(&got, 1) })),
		WithEndpoint("svc", Host("ev.test"), tripOnFirstFailure()))
	defer rt.Close()

	_, _ = get(t, rt, "http://ev.test/") // trips the circuit
	if atomic.LoadInt32(&got) == 0 {
		t.Fatal("WithEvents sink received no StateChanged event")
	}
}
