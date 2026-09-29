// Package readme_test holds the code shown in the README's first three steps, so
// that it compiles and runs. Change it here and in the README together.
package readme_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/justinclev/GoGuardLib"
	"github.com/justinclev/GoGuardLib/breaker"
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
