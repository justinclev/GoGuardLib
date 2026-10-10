package goguard_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/health"
	"github.com/justinclev/GoGuardLib/retry"
)

// NewClient is the short way in: guard every host, save what cannot be sent, replay it later
// through the same client.
func TestClientSavesRequestsAndReplaysThemThroughItself(t *testing.T) {
	svc := newService()
	defer svc.Close()
	svc.down.Store(true)
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})

	client, err := goguard.NewClient(goguard.Policy{
		FailureThreshold: 0.5, MinSamples: 2, SleepWindow: time.Hour,
		HealthPath: "/health",
		Health:     &health.Config{Interval: 20 * time.Millisecond, SuccessThreshold: 1, TrustHealth: true},
		Defer:      &goguard.Defer{Store: store},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	for _, id := range []string{"a", "b", "c"} {
		if err := post(client.Client, svc.URL+"/hook", "body-"+id, map[string]string{"Idempotency-Key": id}); !errors.Is(err, goguard.ErrDeferred) {
			t.Fatalf("post %s = %v, want ErrDeferred", id, err)
		}
	}
	if len(client.Breakers()) == 0 || client.Guard() == nil {
		t.Fatal("the client must expose its circuits and its transport")
	}

	svc.down.Store(false)
	rd, err := dlq.NewRedriver(dlq.RedriveConfig{
		Store: store, Handler: client.Replay(nil), BreakerSource: client.Breakers,
		PollInterval: 10 * time.Millisecond, Backoff: retry.Constant(10 * time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = rd.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	deadline := time.Now().Add(15 * time.Second)
	for len(svc.received()) < 3 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := svc.received(); len(got) != 3 {
		t.Fatalf("service received %d requests, want 3: %+v", len(got), got)
	}
}

func TestClientAppliesOptionsAfterTheDefaultPolicy(t *testing.T) {
	c0, err := goguard.NewClient(goguard.Policy{Defer: &goguard.Defer{}})
	if err != nil {
		t.Fatal("a Defer with no Store must wait for BindStore:", err)
	}
	_ = c0.Close()
	c, err := goguard.NewClient(goguard.Policy{}, goguard.WithEndpoint("pay", goguard.Host("pay.internal"), goguard.Policy{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal("Close must be safe to repeat:", err)
	}
}
