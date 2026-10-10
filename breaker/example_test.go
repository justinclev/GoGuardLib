package breaker_test

import (
	"context"
	"errors"

	"github.com/justinclev/GoGuardLib/breaker"
)

func Example() {
	b := breaker.New(breaker.Config{Name: "inventory", FailureThreshold: 0.5, MinSamples: 20})

	err := b.Do(context.Background(), func(ctx context.Context) error {
		return nil // call the dependency
	})
	if errors.Is(err, breaker.ErrOpen) {
		// the dependency is down: fail fast or fall back
	}
}
