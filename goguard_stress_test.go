package goguard

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestShardedStress(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	cfg := Config{
		MaxBreakers:    10, // Small LRU to force eviction
		SamplingWindow: 1 * time.Second,
		SleepWindow:    1 * time.Second,
	}
	transport := NewResilientTransport(cfg)
	defer transport.Close()

	var wg sync.WaitGroup
	// Simulate many different hosts
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			host := fmt.Sprintf("host-%d.com", id)
			_, _ = transport.getBreaker(host) 
			_, _ = transport.getBreaker(host)
		}(i)
	}
	wg.Wait()
}

func TestEnforcedTimeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	cfg := Config{
		RequestTimeout: 50 * time.Millisecond,
		SamplingWindow: 1 * time.Second,
	}
	transport := NewResilientTransport(cfg)
	defer transport.Close()

	client := &http.Client{Transport: transport}
	_, err := client.Get(ts.URL)
	if err == nil {
		t.Fatal("Expected timeout error, got nil")
	}
}
