// Package httpclient is the HTTP half of the orders service in the README. The code
// here is copied into the README, so it compiles and runs as an example test.
package httpclient_test

import (
	"errors"
	"fmt"
	"io"
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
			MinSamples:       3,                // ...once this many calls have been seen (20 or more in production)
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

// ErrPaymentsUnavailable means payments is failing and the call was not attempted.
var ErrPaymentsUnavailable = errors.New("payments is unavailable, try again shortly")

// getQuote asks payments for a price. When payments is failing, it returns at once
// with ErrPaymentsUnavailable instead of waiting for a timeout.
func getQuote(client *http.Client, paymentsURL string) (string, error) {
	resp, err := client.Get(paymentsURL + "/quote")
	if err != nil {
		if errors.Is(err, goguard.ErrCircuitOpen) {
			return "", ErrPaymentsUnavailable // the call was refused: payments was not contacted
		}
		return "", fmt.Errorf("asking payments for a quote: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("payments answered %d", resp.StatusCode)
	}
	price, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("reading the quote: %w", err)
	}
	return string(price), nil
}

// While payments is failing, the first calls reach it and get its errors. Once enough
// have failed, the circuit opens and further calls return immediately.
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

	for call := 1; call <= 4; call++ {
		_, err := getQuote(client, payments.URL)
		fmt.Printf("call %d: %v\n", call, err)
	}

	// Output:
	// call 1: payments answered 503
	// call 2: payments answered 503
	// call 3: payments answered 503
	// call 4: payments is unavailable, try again shortly
}
