package breaker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/health"
)

func trip(t *testing.T, b *breaker.Breaker) {
	t.Helper()
	_ = b.Do(context.Background(), func(context.Context) error { return errors.New("down") })
	if b.State() != breaker.StateOpen {
		t.Fatalf("setup: state = %v, want open", b.State())
	}
}

// State stays "open" until a request arrives, so anything waiting on the breaker must ask
// Refusing: it says when a call would now be admitted as a canary.
func TestRefusingEndsWhenTheSleepWindowPassesEvenThoughStateStaysOpen(t *testing.T) {
	b := breaker.New(breaker.Config{Name: "x", FailureThreshold: 0.1, MinSamples: 1, SleepWindow: 100 * time.Millisecond})
	defer b.Close()
	if b.Refusing() {
		t.Fatal("a closed breaker is not refusing")
	}
	trip(t, b)
	if !b.Refusing() {
		t.Fatal("an open breaker inside its sleep window must be refusing")
	}
	time.Sleep(150 * time.Millisecond)
	if b.State() != breaker.StateOpen {
		t.Fatal("State is expected to stay open until a request arrives")
	}
	if b.Refusing() {
		t.Fatal("after the sleep window the next call would be the canary, so the breaker is not refusing")
	}
	if err := b.Do(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("the canary should be admitted: %v", err)
	}
	if b.State() != breaker.StateClosed || b.Refusing() {
		t.Fatalf("after a successful canary: state %v, refusing %v", b.State(), b.Refusing())
	}
}

// A health check owns the way out of the open state, until MaxOpen says it may be lying.
func TestRefusingHonoursAHealthCheckUntilMaxOpen(t *testing.T) {
	never := func(context.Context) error { return errors.New("still down") }
	b := breaker.New(breaker.Config{
		Name: "h", FailureThreshold: 0.1, MinSamples: 1, SleepWindow: 20 * time.Millisecond,
		Health: &health.Config{Check: never, Interval: time.Hour, MaxOpen: 250 * time.Millisecond},
	})
	defer b.Close()
	trip(t, b)
	time.Sleep(80 * time.Millisecond) // past the sleep window, inside MaxOpen
	if !b.Refusing() {
		t.Fatal("while the health check owns recovery the breaker keeps refusing")
	}
	time.Sleep(250 * time.Millisecond)
	if b.Refusing() {
		t.Fatal("after MaxOpen a canary is allowed whatever the health check says")
	}
}

func TestRefusingIgnoresDryRunAndFollowsOverrides(t *testing.T) {
	dry := breaker.New(breaker.Config{Name: "d", FailureThreshold: 0.1, MinSamples: 1, SleepWindow: time.Hour, DryRun: true})
	defer dry.Close()
	_ = dry.Do(context.Background(), func(context.Context) error { return errors.New("down") })
	if dry.Refusing() {
		t.Fatal("a dry-run breaker never refuses a call")
	}
	b := breaker.New(breaker.Config{Name: "o", SleepWindow: time.Hour})
	defer b.Close()
	b.SetOverride(breaker.OverrideForceOpen)
	if !b.Refusing() {
		t.Fatal("a breaker forced open is refusing")
	}
	b.SetOverride(breaker.OverrideForceClosed)
	if b.Refusing() {
		t.Fatal("a breaker forced closed is not")
	}
}
