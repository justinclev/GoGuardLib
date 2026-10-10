package goguard

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type mockTransport struct {
	roundTrip func(*http.Request) (*http.Response, error)
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTrip(req)
}

func okResp() *http.Response { return &http.Response{StatusCode: 200, Body: http.NoBody} }

func get(t testing.TB, rt http.RoundTripper, url string) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	return rt.RoundTrip(req)
}

func hostOf(ts *httptest.Server) string { return strings.TrimPrefix(ts.URL, "http://") }

// countingServer answers with the status returned by status() and counts hits.
func countingServer(status func() int) (*httptest.Server, *int32) {
	var hits int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(status())
	}))
	return ts, &hits
}

// tripOnFirstFailure is a policy whose circuit opens on the first failure.
func tripOnFirstFailure() Policy {
	return Policy{FailureThreshold: 0.1, MinSamples: 1, SleepWindow: time.Hour}
}
