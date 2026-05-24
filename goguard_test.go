package goguard

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestResilientTransport(t *testing.T) {
	// 1. Setup flaky server
	fail := true
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	cfg := Config{
		MaxIdleConns:     10,
		FailureThreshold: 0.1,
		SleepWindow:      500 * time.Millisecond,
		SamplingWindow:   1 * time.Second,
	}
	transport := NewResilientTransport(cfg)
	client := &http.Client{Transport: transport}

	// 2. Trip the breaker
	// We need enough failures to exceed the threshold.
	// NewBreaker uses samplingWindow/10 for buckets.
	for i := 0; i < 5; i++ {
		client.Get(ts.URL)
	}

	// 3. Verify it's open
	_, err := client.Get(ts.URL)
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("Expected ErrCircuitOpen, got %v", err)
	}

	// 4. Wait for SleepWindow and recover
	time.Sleep(600 * time.Millisecond)
	fail = false // Fix server

	resp, err := client.Get(ts.URL)
	if err != nil {
		t.Fatalf("Expected recovery, got error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", resp.StatusCode)
	}
}
