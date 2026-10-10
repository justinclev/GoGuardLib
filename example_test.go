package goguard_test

import (
	"context"
	"net/http"
	"time"

	goguard "github.com/justinclev/GoGuardLib"
	"github.com/justinclev/GoGuardLib/retry"
)

// Only the registered endpoint is protected; other requests pass through.
func Example() {
	rt, err := goguard.New(goguard.Config{},
		goguard.WithEndpoint("payments", goguard.Host("payments.internal"), goguard.Policy{
			FailureThreshold: 0.5,
			MinSamples:       20,
			MaxRetries:       2,
			RetryBackoff:     retry.Jitter(retry.Exponential(100*time.Millisecond, 2*time.Second), 0.5),
			RequestTimeout:   3 * time.Second,
		}),
	)
	if err != nil {
		return
	}
	defer rt.Close()

	client := &http.Client{Transport: rt}
	_ = client
}

func ExampleUse() {
	ctx := goguard.Use(context.Background(), "payments") // route through a named endpoint
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://any.host/", nil)
	_ = req
}
