// Package readme_test holds the code shown in the README's first three steps, so
// that it compiles and runs. Change it here and in the README together.
package readme_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/justinclev/GoGuardLib"
	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
)

// chargeCard stands in for a call to your payment service. Here it always fails.
func chargeCard(ctx context.Context, order string) (string, error) {
	return "", errors.New("payments answered 503")
}

func newPayments() *breaker.Breaker {
	return breaker.New(breaker.Config{
		Name:             "payments",
		FailureThreshold: 0.5, // open the circuit when half the recent calls fail...
		MinSamples:       3,   // ...but only after this many calls (use 20 or more in production)
	})
}

func placeOrder(ctx context.Context, payments *breaker.Breaker, order string) (string, error) {
	receipt, err := breaker.Call(ctx, payments, func(ctx context.Context) (string, error) {
		return chargeCard(ctx, order)
	})
	if errors.Is(err, breaker.ErrOpen) {
		return "", errors.New("payments is having trouble, please try again in a minute")
	}
	return receipt, err
}

func Example_protectAFunction() {
	payments := newPayments()
	defer payments.Close()

	for i := 1; i <= 4; i++ {
		_, err := placeOrder(context.Background(), payments, "order-1")
		fmt.Printf("call %d: %v\n", i, err)
	}

	// Output:
	// call 1: payments answered 503
	// call 2: payments answered 503
	// call 3: payments answered 503
	// call 4: payments is having trouble, please try again in a minute
}

func newClient() (*http.Client, error) {
	guard, err := goguard.New(goguard.Config{},
		goguard.WithEndpoint("payments", goguard.Host("payments.internal"), goguard.Policy{
			RequestTimeout: 2 * time.Second,
			MaxRetries:     2, // only for GET and HEAD requests, which are safe to repeat
		}),
	)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: guard}, nil
}

func getQuote(client *http.Client) error {
	resp, err := client.Get("https://payments.internal/quote")
	if errors.Is(err, goguard.ErrCircuitOpen) {
		return errors.New("payments is having trouble, please try again in a minute")
	}
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

func newClientWithHealthCheck() (*http.Client, error) {
	guard, err := goguard.New(goguard.Config{},
		goguard.WithEndpoint("payments", goguard.Host("payments.internal"), goguard.Policy{
			RequestTimeout: 2 * time.Second,
			HealthPath:     "/health", // while the circuit is open, ask payments if it is back
		}),
	)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: guard}, nil
}

// The HTTP examples need a host called payments.internal, so they only have to compile.
func Example_protectHTTPCalls() {
	_ = newClient
	_ = getQuote
	_ = newClientWithHealthCheck
}

// currentToken stands in for however your service gets its API token.
func currentToken() string { return "token" }

func newWebhookClient(store dlq.Store) (*goguard.ResilientTransport, *http.Client, error) {
	guard, err := goguard.New(goguard.Config{},
		goguard.WithEndpoint("partner", goguard.Host("hooks.partner.com"), goguard.Policy{
			HealthPath: "/health",
			Defer:      &goguard.Defer{Store: store}, // save what can't be sent right now
		}),
	)
	if err != nil {
		return nil, nil, err
	}
	return guard, &http.Client{Transport: guard}, nil
}

func sendWebhook(client *http.Client, eventID string, payload []byte) error {
	req, err := http.NewRequest(http.MethodPost, "https://hooks.partner.com/events", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Idempotency-Key", eventID) // a request without one is never saved
	req.Header.Set("Authorization", "Bearer "+currentToken())

	resp, err := client.Do(req)
	if errors.Is(err, goguard.ErrDeferred) {
		return nil // saved: it will be sent when the partner is back
	}
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

func newWebhookRedriver(store dlq.Store, guard *goguard.ResilientTransport, client *http.Client) (*dlq.Redriver, error) {
	replay, err := goguard.ReplayHandler(goguard.ReplayConfig{
		Client: client,
		Prepare: func(ctx context.Context, req *http.Request) error {
			req.Header.Set("Authorization", "Bearer "+currentToken()) // credentials are never saved
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	return dlq.NewRedriver(dlq.RedriveConfig{Store: store, Handler: replay, BreakerSource: guard.Breakers})
}

func Example_saveRequestsForLater() {
	_ = newWebhookClient
	_ = sendWebhook
	_ = newWebhookRedriver
}
