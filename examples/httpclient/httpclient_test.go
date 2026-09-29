// Package httpclient shows how a service protects its outbound HTTP calls. The
// snippets are the ones in the README, kept here so they always compile and run.
package httpclient_test

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/justinclev/GoGuardLib"
	"github.com/justinclev/GoGuardLib/obs"
	"github.com/justinclev/GoGuardLib/retry"
)

// newGuardedClient returns an http.Client whose calls to payments are protected.
// Everything else the client sends passes straight through, untouched.
func newGuardedClient(paymentsHost string, events obs.Sink) (*http.Client, func() error, error) {
	guard, err := goguard.New(goguard.Config{},
		goguard.WithEvents(events),
		goguard.WithEndpoint("payments", onHost(paymentsHost), goguard.Policy{
			FailureThreshold: 0.5,              // open at a 50% failure rate...
			MinSamples:       3,                // ...once this many calls have been seen
			SleepWindow:      30 * time.Second, // how long to stay open without a health check
			RequestTimeout:   2 * time.Second,
			MaxRetries:       2, // idempotent requests only (GET, HEAD, OPTIONS, TRACE)
			RetryBackoff:     retry.Jitter(retry.Exponential(100*time.Millisecond, time.Second), 0.3),
		}),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("configuring the guard: %w", err)
	}
	return &http.Client{Transport: guard}, guard.Close, nil
}

// onHost matches requests to one host and port.
func onHost(host string) goguard.Matcher {
	return func(r *http.Request) bool { return r.URL.Host == host }
}

// A failing dependency is detected, its circuit opens, and later calls are refused
// at once instead of waiting for a timeout.
func Example() {
	payments := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer payments.Close()

	client, closeGuard, err := newGuardedClient(payments.Listener.Addr().String(), nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = closeGuard() }()

	// The first calls reach the service. A 5xx is returned to the caller as usual,
	// and counted against the circuit.
	for i := 0; i < 3; i++ {
		resp, err := client.Post(payments.URL+"/charge", "application/json", nil)
		if err != nil {
			fmt.Println("unexpected:", err)
			return
		}
		_ = resp.Body.Close()
	}

	// The circuit is now open: the call is refused without touching the service.
	_, err = client.Post(payments.URL+"/charge", "application/json", nil)

	var circuit *goguard.CircuitError
	switch {
	case errors.Is(err, goguard.ErrCircuitOpen) && errors.As(err, &circuit):
		fmt.Printf("refused: %s circuit is %s\n", "payments", circuit.State)
	case err != nil:
		fmt.Println("other error:", err)
	}

	// Output: refused: payments circuit is open
}
