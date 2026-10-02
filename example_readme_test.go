package goguard_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"time"

	goguard "github.com/justinclev/GoGuardLib"
	"github.com/justinclev/GoGuardLib/dlq"
)

// currentToken stands in for your code that returns a credential.
func currentToken() string { return "" }

// These are the README's HTTP examples. They only have to compile.

func newReadmeClient() (*goguard.Client, error) {
	return goguard.NewClient(goguard.Policy{
		RequestTimeout: 2 * time.Second,
		MaxRetries:     2, // only for GET and HEAD requests, which are safe to repeat
	})
}

func readmeGetQuote(client *goguard.Client) error {
	resp, err := client.Get("https://payments.internal/quote")
	if errors.Is(err, goguard.ErrCircuitOpen) {
		return errors.New("payments is having trouble, please try again in a minute")
	}
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

func readmeWebhooks(store dlq.Store) (*goguard.Client, *dlq.Redriver, error) {
	hooks, err := goguard.NewClient(goguard.Policy{
		HealthPath: "/health",
		Defer:      &goguard.Defer{Store: store}, // save what can't be sent right now
	})
	if err != nil {
		return nil, nil, err
	}
	redriver, err := dlq.NewRedriver(dlq.RedriveConfig{
		Store: store,
		Handler: hooks.Replay(func(ctx context.Context, req *http.Request) error {
			req.Header.Set("Authorization", "Bearer "+currentToken()) // credentials are never saved
			return nil
		}),
		BreakerSource: hooks.Breakers,
	})
	return hooks, redriver, err
}

func readmeSendWebhook(client *goguard.Client, eventID string, payload []byte) error {
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

func Example_readmeHTTP() {
	_ = newReadmeClient
	_ = readmeGetQuote
	_ = readmeWebhooks
	_ = readmeSendWebhook
}
