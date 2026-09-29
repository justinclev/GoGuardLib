package dlq_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/health"
	"github.com/justinclev/GoGuardLib/obs"
	"github.com/justinclev/GoGuardLib/retry"
)

func eventually(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func newRedriver(t testing.TB, cfg dlq.RedriveConfig) *dlq.Redriver {
	t.Helper()
	if cfg.Store == nil {
		cfg.Store = dlq.NewMemoryStore(dlq.MemoryOptions{})
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 5 * time.Millisecond
	}
	r, err := dlq.NewRedriver(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// start runs the redriver and returns a function that stops it and waits.
func start(t testing.TB, r *dlq.Redriver) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Run returned %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Error("Run did not stop")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func addBlocked(t testing.TB, s dlq.Store, n int, dep string) {
	t.Helper()
	for i := 0; i < n; i++ {
		must(t, s.Append(context.Background(), dlq.Record{
			ID: fmt.Sprintf("%s-%03d", dep, i), Value: []byte("payload"), BlockedOn: dep,
		}))
	}
}

func TestRedriverProcessesAndRemovesRecords(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 25, "pay")
	var mu sync.Mutex
	seen := map[string]int{}
	var events []obs.Redrive
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store,
		Handler: func(ctx context.Context, it *dlq.Item) error {
			mu.Lock()
			seen[it.Record.ID]++
			mu.Unlock()
			return nil
		},
		Events: obs.SinkFunc(func(e obs.Event) {
			mu.Lock()
			events = append(events, e.(obs.Redrive))
			mu.Unlock()
		}),
	})
	start(t, r)
	eventually(t, "the queue to drain", func() bool { st, _ := store.Stats(context.Background()); return st.Total() == 0 })

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 25 {
		t.Fatalf("handled %d distinct records", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("%s handled %d times", id, n)
		}
	}
	if s := r.Stats(); s.Succeeded != 25 || s.Leased != 25 || s.Parked != 0 {
		t.Fatalf("stats = %+v", s)
	}
	if len(events) != 25 || events[0].Outcome != obs.RedriveSucceeded || events[0].Dependency != "pay" || events[0].RecordID == "" {
		t.Fatalf("events = %+v", events[:1])
	}
}

// The whole point: an outage queues work, and recovery drains it, with no attempts
// wasted while the dependency was down.
func TestRecoveryDrainsTheBacklogThroughACanary(t *testing.T) {
	var healthy atomic.Bool
	pay := breaker.New(breaker.Config{
		Name: "pay", FailureThreshold: 0.1, MinSamples: 1, SleepWindow: time.Hour,
		Health: &health.Config{Check: func(context.Context) error {
			if healthy.Load() {
				return nil
			}
			return errors.New("down")
		}, Interval: 5 * time.Millisecond, MaxInterval: 10 * time.Millisecond, Jitter: -1, SuccessThreshold: 1},
	})
	defer pay.Close()
	_ = pay.Do(context.Background(), func(context.Context) error { return errors.New("outage") })
	if pay.State() != breaker.StateOpen {
		t.Fatal("setup: circuit should be open")
	}

	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 30, "pay")
	var calls, maxAttempts atomic.Int32
	r := newRedriver(t, dlq.RedriveConfig{
		Store:    store,
		Breakers: map[string]*breaker.Breaker{"pay": pay},
		Handler: func(ctx context.Context, it *dlq.Item) error {
			calls.Add(1)
			if a := int32(it.Record.Attempts); a > maxAttempts.Load() {
				maxAttempts.Store(a)
			}
			return pay.Do(ctx, func(context.Context) error { return nil }) // the real dependency call
		},
	})
	start(t, r)

	time.Sleep(150 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("handler ran %d times while the circuit was open", calls.Load())
	}
	if st, _ := store.Stats(context.Background()); st.Pending != 30 {
		t.Fatalf("stats = %+v; the backlog must wait untouched", st)
	}

	healthy.Store(true)
	eventually(t, "the backlog to drain", func() bool { st, _ := store.Stats(context.Background()); return st.Total() == 0 })
	if maxAttempts.Load() != 1 || r.Stats().Parked != 0 {
		t.Fatalf("attempts up to %d, %d parked: the outage must not cost the records any attempts", maxAttempts.Load(), r.Stats().Parked)
	}
	if pay.State() != breaker.StateClosed {
		t.Fatalf("state = %v; a canary should have closed the circuit", pay.State())
	}
}

func TestBlockedRecordsAreHeldWithoutPenaltyAndRetagged(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 1, "pay")
	var calls atomic.Int32
	var mu sync.Mutex
	var deps []string
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, MaxAttempts: 3,
		Handler: func(ctx context.Context, it *dlq.Item) error {
			n := calls.Add(1)
			mu.Lock()
			deps = append(deps, it.Record.BlockedOn)
			mu.Unlock()
			if n <= 6 { // more than MaxAttempts: only a refund keeps it alive
				return &dlq.BlockedError{Dependency: "inventory", Err: errors.New("circuit open"), Refund: true}
			}
			return nil
		},
	})
	start(t, r)
	eventually(t, "the record to be handled", func() bool { st, _ := store.Stats(context.Background()); return st.Total() == 0 })

	if s := r.Stats(); s.Parked != 0 || s.Blocked != 6 || s.Succeeded != 1 {
		t.Fatalf("stats = %+v", s)
	}
	mu.Lock()
	defer mu.Unlock()
	if deps[0] != "pay" || deps[1] != "inventory" {
		t.Fatalf("BlockedOn seen by the handler: %v; it should move from pay to inventory", deps)
	}
}

func TestAnOpenCircuitInsideTheHandlerCountsAsBlockedNotFailed(t *testing.T) {
	b := breaker.New(breaker.Config{Name: "inventory", FailureThreshold: 0.1, MinSamples: 1, SleepWindow: time.Hour})
	_ = b.Do(context.Background(), func(context.Context) error { return errors.New("boom") }) // open

	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 1, "pay")
	var calls atomic.Int32
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, MaxAttempts: 2,
		Handler: func(ctx context.Context, it *dlq.Item) error {
			calls.Add(1)
			return b.Do(ctx, func(context.Context) error { return nil }) // rejected: *breaker.OpenError
		},
	})
	start(t, r)
	eventually(t, "several blocked rounds", func() bool { return r.Stats().Blocked >= 5 })

	if r.Stats().Parked != 0 {
		t.Fatal("a record was parked for hitting an open circuit; that is not its fault")
	}
	ls, _ := store.Lease(context.Background(), dlq.LeaseRequest{Max: 1, TTL: time.Hour})
	if len(ls) == 1 && (ls[0].Record.BlockedOn != "inventory" || ls[0].Record.Attempts > 1) {
		t.Fatalf("record = %+v: it should be tagged with the circuit's name and unpenalised", ls[0].Record)
	}
}

func TestPoisonPillIsParkedAfterMaxAttemptsWithRedactedReason(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 1, "pay")
	var calls atomic.Int32
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, MaxAttempts: 3, Backoff: retry.Constant(time.Millisecond),
		Handler: func(ctx context.Context, it *dlq.Item) error {
			calls.Add(1)
			return errors.New("upstream said 400: password=hunter2hunter2 is invalid")
		},
	})
	start(t, r)
	eventually(t, "the record to be parked", func() bool { return r.Stats().Parked == 1 })

	if calls.Load() != 3 {
		t.Fatalf("handler ran %d times, want exactly MaxAttempts (3)", calls.Load())
	}
	st, _ := store.Stats(context.Background())
	if st.Parked != 1 || st.Pending != 0 {
		t.Fatalf("stats = %+v", st)
	}
	must(t, store.Requeue(context.Background(), "pay-000"))
	ls, _ := store.Lease(context.Background(), dlq.LeaseRequest{Max: 1, TTL: time.Hour})
	reason := ls[0].Record.LastError
	if !strings.Contains(reason, "gave up after 3 attempts") || strings.Contains(reason, "hunter2hunter2") {
		t.Fatalf("reason = %q: it must say why, without leaking the secret", reason)
	}
}

func TestPermanentFailureParksAtOnce(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 1, "pay")
	var calls atomic.Int32
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store,
		Handler: func(ctx context.Context, it *dlq.Item) error {
			calls.Add(1)
			return retry.Permanent(errors.New("schema v9 is not supported"))
		},
	})
	start(t, r)
	eventually(t, "parking", func() bool { return r.Stats().Parked == 1 })
	if calls.Load() != 1 {
		t.Fatalf("a permanent failure was retried: %d calls", calls.Load())
	}
}

// A worker that dies on a record leaves an expired lease each time. After enough of
// those the record must be parked rather than handed to the next worker to kill.
func TestARecordThatKeepsKillingItsWorkerIsParkedWithoutRunningTheHandler(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 1, "pay")
	for crash := 0; crash < 4; crash++ { // four workers took it and never finished
		ls, err := store.Lease(context.Background(), dlq.LeaseRequest{Max: 1, TTL: time.Millisecond})
		must(t, err)
		if len(ls) != 1 {
			t.Fatal("setup: no lease")
		}
		time.Sleep(3 * time.Millisecond)
	}
	var calls atomic.Int32
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, MaxAttempts: 3,
		Handler: func(context.Context, *dlq.Item) error { calls.Add(1); return nil },
	})
	start(t, r)
	eventually(t, "parking", func() bool { return r.Stats().Parked == 1 })
	if calls.Load() != 0 {
		t.Fatal("the handler was given a record that has already crashed workers")
	}
}

func TestFailedRecordsWaitForTheirBackoff(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 1, "pay")
	var mu sync.Mutex
	var times []time.Time
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, MaxAttempts: 100, Backoff: retry.Constant(60 * time.Millisecond),
		Handler: func(ctx context.Context, it *dlq.Item) error {
			mu.Lock()
			times = append(times, time.Now())
			mu.Unlock()
			return errors.New("transient")
		},
	})
	start(t, r)
	eventually(t, "four attempts", func() bool { mu.Lock(); defer mu.Unlock(); return len(times) >= 4 })
	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < 4; i++ {
		if gap := times[i].Sub(times[i-1]); gap < 50*time.Millisecond {
			t.Fatalf("attempt %d came %v after the previous one; the 60ms backoff was ignored", i, gap)
		}
	}
}

func TestRateLimitSpreadsTheBacklog(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 30, "pay")
	var mu sync.Mutex
	var times []time.Time
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, Rate: 100, Burst: 5, Workers: 8,
		Handler: func(ctx context.Context, it *dlq.Item) error {
			mu.Lock()
			times = append(times, time.Now())
			mu.Unlock()
			return nil
		},
	})
	start(t, r)
	eventually(t, "the queue to drain", func() bool { st, _ := store.Stats(context.Background()); return st.Total() == 0 })
	mu.Lock()
	defer mu.Unlock()
	// 30 records at 100/s with a burst of 5: at least (30-5)/100 = 250ms, not an instant flood.
	if span := times[len(times)-1].Sub(times[0]); span < 180*time.Millisecond {
		t.Fatalf("30 records started within %v; the rate limit is not applied", span)
	}
}

func TestLimiterRampAfterRecovery(t *testing.T) {
	// Exercised through the public behaviour: after a recovery the rate starts low.
	var healthy atomic.Bool
	b := breaker.New(breaker.Config{
		Name: "pay", FailureThreshold: 0.1, MinSamples: 1, SleepWindow: time.Hour,
		Health: &health.Config{Check: func(context.Context) error {
			if healthy.Load() {
				return nil
			}
			return errors.New("down")
		}, Interval: 5 * time.Millisecond, Jitter: -1, SuccessThreshold: 1, TrustHealth: true},
	})
	defer b.Close()
	_ = b.Do(context.Background(), func(context.Context) error { return errors.New("x") })

	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 40, "pay")
	var mu sync.Mutex
	var times []time.Time
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, Breakers: map[string]*breaker.Breaker{"pay": b},
		Rate: 200, Burst: 10, RampUp: 400 * time.Millisecond, Workers: 8,
		Handler: func(ctx context.Context, it *dlq.Item) error {
			mu.Lock()
			times = append(times, time.Now())
			mu.Unlock()
			return nil
		},
	})
	start(t, r)
	time.Sleep(50 * time.Millisecond) // the redriver sees the open circuit
	healthy.Store(true)
	eventually(t, "the queue to drain", func() bool { st, _ := store.Stats(context.Background()); return st.Total() == 0 })

	mu.Lock()
	defer mu.Unlock()
	// Without a ramp 40 records at 200/s take ~150ms. Starting at 10% and rising over
	// 400ms, the first ten records alone take far longer than they would at full rate.
	early := times[9].Sub(times[0])
	if early < 100*time.Millisecond {
		t.Fatalf("first 10 records started within %v; the post-recovery ramp is not applied", early)
	}
}

func TestLimiterArithmetic(t *testing.T) {
	// Deterministic check of the token bucket through many fast polls.
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 10, "pay")
	var n atomic.Int32
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, Rate: 20, Burst: 2, Workers: 4,
		Handler: func(context.Context, *dlq.Item) error { n.Add(1); return nil },
	})
	stop := start(t, r)
	time.Sleep(100 * time.Millisecond) // burst 2 + 20/s * 0.1s = about 4
	got := n.Load()
	stop()
	if got < 2 || got > 6 {
		t.Fatalf("%d records in 100ms at 20/s with burst 2; expected about 4", got)
	}
}

func TestShutdownCancelsHandlersAndReleasesWithoutPenalty(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 12, "pay") // more than the workers can hold at once
	var started atomic.Int32
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, Workers: 2, MaxAttempts: 1,
		Handler: func(ctx context.Context, it *dlq.Item) error {
			started.Add(1)
			<-ctx.Done()
			return ctx.Err()
		},
	})
	stop := start(t, r)
	eventually(t, "both workers busy", func() bool { return started.Load() == 2 })
	begin := time.Now()
	stop()
	if time.Since(begin) > 3*time.Second {
		t.Fatal("shutdown was slow")
	}

	// Nothing was lost, parked or penalised: with MaxAttempts 1 any counted attempt
	// would have parked a record.
	st, _ := store.Stats(context.Background())
	if st.Total() != 12 || st.Parked != 0 || st.Leased != 0 {
		t.Fatalf("stats = %+v: records must all be back in the queue", st)
	}
	ls, _ := store.Lease(context.Background(), dlq.LeaseRequest{Max: 12, TTL: time.Hour})
	for _, l := range ls {
		if l.Record.Attempts != 1 { // the lease just taken, and nothing else
			t.Fatalf("%s has %d attempts after an interrupted shutdown", l.Record.ID, l.Record.Attempts)
		}
	}
}

func TestPanickingHandlerDoesNotStopTheRedriverOrLeakThePayload(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 2, "pay")
	var first atomic.Bool
	var done atomic.Int32
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, Backoff: retry.Constant(time.Millisecond), Workers: 1,
		Handler: func(ctx context.Context, it *dlq.Item) error {
			if first.CompareAndSwap(false, true) {
				panic("boom with payload " + string(it.Record.Value))
			}
			done.Add(1)
			return nil
		},
	})
	start(t, r)
	eventually(t, "recovery from the panic", func() bool { return done.Load() == 2 })
	if r.Stats().Retried != 1 {
		t.Fatalf("stats = %+v", r.Stats())
	}
}

func TestItemCheckpointPersistsAcrossAttempts(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 1, "pay")
	var mu sync.Mutex
	var seen []string
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, Backoff: retry.Constant(time.Millisecond),
		Handler: func(ctx context.Context, it *dlq.Item) error {
			mu.Lock()
			seen = append(seen, string(it.Record.Checkpoint))
			n := len(seen)
			mu.Unlock()
			if n == 1 {
				if err := it.Checkpoint(ctx, []byte("step-2-done")); err != nil {
					return err
				}
				return errors.New("step 3 failed")
			}
			return nil
		},
	})
	start(t, r)
	eventually(t, "two attempts", func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) == 2 })
	mu.Lock()
	defer mu.Unlock()
	if seen[0] != "" || seen[1] != "step-2-done" {
		t.Fatalf("checkpoints seen: %q", seen)
	}
}

func TestHandlerOutlivingItsLeaseIsReportedAsLeaseLost(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 1, "pay")
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, LeaseTTL: 80 * time.Millisecond,
		Handler: func(ctx context.Context, it *dlq.Item) error {
			once.Do(func() { close(entered) })
			<-release // ignores ctx: a misbehaving handler
			return nil
		},
	})
	start(t, r)
	<-entered
	time.Sleep(150 * time.Millisecond)
	// Somebody else takes the expired lease. (The redriver keeps being offered the
	// record too, and must hand it straight back rather than run it a second time.)
	var took bool
	eventually(t, "the expired lease to be offered again", func() bool {
		ls, err := store.Lease(context.Background(), dlq.LeaseRequest{Max: 1, TTL: time.Hour})
		must(t, err)
		took = len(ls) == 1
		return took
	})
	close(release)
	eventually(t, "the stale ack to be rejected", func() bool { return r.Stats().LeaseLost == 1 })
	if st, _ := store.Stats(context.Background()); st.Total() != 1 {
		t.Fatalf("stats = %+v: the record must survive the stale worker's ack", st)
	}
}

func TestWakeTriggersImmediateWork(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	var n atomic.Int32
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, PollInterval: time.Hour,
		Handler: func(context.Context, *dlq.Item) error { n.Add(1); return nil },
	})
	start(t, r)
	time.Sleep(20 * time.Millisecond)
	addBlocked(t, store, 1, "pay")
	r.Wake()
	eventually(t, "the woken redriver to process", func() bool { return n.Load() == 1 })
}

func TestSinkWakesOnRecoveryEvents(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	var n atomic.Int32
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, PollInterval: time.Hour,
		Handler: func(context.Context, *dlq.Item) error { n.Add(1); return nil },
	})
	start(t, r)
	time.Sleep(20 * time.Millisecond)
	addBlocked(t, store, 1, "pay")
	r.Sink().Emit(obs.StateChanged{To: obs.StateOpen}) // opening must not wake it
	time.Sleep(30 * time.Millisecond)
	if n.Load() != 0 {
		t.Fatal("woken by an opening circuit")
	}
	r.Sink().Emit(obs.StateChanged{To: obs.StateHalfOpen})
	eventually(t, "the wake", func() bool { return n.Load() == 1 })
	r.Sink().Emit(obs.ProbeResult{}) // unrelated events are ignored
}

func TestOrderedRecordsAreRedrivenInOrderEvenWithManyWorkers(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	for i := 0; i < 20; i++ {
		must(t, store.Append(context.Background(), dlq.Record{ID: fmt.Sprintf("o%02d", i), OrderKey: "customer-1", BlockedOn: "pay"}))
	}
	var mu sync.Mutex
	var order []string
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, Workers: 8,
		Handler: func(ctx context.Context, it *dlq.Item) error {
			mu.Lock()
			order = append(order, it.Record.ID)
			mu.Unlock()
			return nil
		},
	})
	start(t, r)
	eventually(t, "the queue to drain", func() bool { st, _ := store.Stats(context.Background()); return st.Total() == 0 })
	mu.Lock()
	defer mu.Unlock()
	for i, id := range order {
		if id != fmt.Sprintf("o%02d", i) {
			t.Fatalf("processed out of order: %v", order)
		}
	}
}

func TestWorkersRunConcurrentlyUpToTheLimit(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 40, "pay")
	var cur, peak atomic.Int32
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, Workers: 4,
		Handler: func(ctx context.Context, it *dlq.Item) error {
			n := cur.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(3 * time.Millisecond)
			cur.Add(-1)
			return nil
		},
	})
	start(t, r)
	eventually(t, "the queue to drain", func() bool { st, _ := store.Stats(context.Background()); return st.Total() == 0 })
	if p := peak.Load(); p < 2 || p > 4 {
		t.Fatalf("peak concurrency %d, want between 2 and 4", p)
	}
}

func TestRedriverReturnsAStoreFailure(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	r := newRedriver(t, dlq.RedriveConfig{Store: store, Handler: func(context.Context, *dlq.Item) error { return nil }})
	must(t, store.Close())
	if err := r.Run(context.Background()); !errors.Is(err, dlq.ErrClosed) {
		t.Fatalf("Run = %v, want the store's error", err)
	}
	if r.Stats().StoreErrors == 0 {
		t.Fatal("store error not counted")
	}
}

func TestNewRedriverValidates(t *testing.T) {
	if _, err := dlq.NewRedriver(dlq.RedriveConfig{}); err == nil {
		t.Fatal("expected an error without a store and handler")
	}
	if _, err := dlq.NewRedriver(dlq.RedriveConfig{Store: dlq.NewMemoryStore(dlq.MemoryOptions{})}); err == nil {
		t.Fatal("expected an error without a handler")
	}
}

// The redriver works against the durable store, and a refunded nack survives a
// restart (it is a new field in the log).
func TestRedriverOnTheWALAndRefundSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s := openWAL(t, dir, dlq.WALOptions{})
	addBlocked(t, s, 1, "pay")
	ls, _ := s.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Hour})
	must(t, s.Nack(ctx, "pay-000", ls[0].Token, dlq.NackOptions{Refund: true, BlockedOn: "inventory", Err: "circuit open"}))
	must(t, s.Close())

	s = openWAL(t, dir, dlq.WALOptions{})
	t.Cleanup(func() { _ = s.Close() }) // registered before start, so it runs after the redriver stops
	var attempts atomic.Int32
	var dep atomic.Value
	r := newRedriver(t, dlq.RedriveConfig{
		Store: s,
		Handler: func(ctx context.Context, it *dlq.Item) error {
			attempts.Store(int32(it.Record.Attempts))
			dep.Store(it.Record.BlockedOn)
			return nil
		},
	})
	start(t, r)
	eventually(t, "the record to be handled", func() bool { return r.Stats().Succeeded == 1 })
	if attempts.Load() != 1 || dep.Load() != "inventory" {
		t.Fatalf("attempts=%d dep=%v: the refund and the new BlockedOn must survive a restart", attempts.Load(), dep.Load())
	}
}

// A handler that ignores its context and outlives its lease must not be run a
// second time on the same record while it is still busy.
func TestARecordIsNeverHandledTwiceConcurrently(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 1, "pay")
	var cur, peak, calls atomic.Int32
	release := make(chan struct{})
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, LeaseTTL: 40 * time.Millisecond, Workers: 4,
		Handler: func(ctx context.Context, it *dlq.Item) error {
			calls.Add(1)
			n := cur.Add(1)
			if n > peak.Load() {
				peak.Store(n)
			}
			<-release // outlives the lease
			cur.Add(-1)
			return nil
		},
	})
	start(t, r)
	time.Sleep(300 * time.Millisecond) // many lease periods
	close(release)
	if peak.Load() != 1 || calls.Load() != 1 {
		t.Fatalf("peak concurrency %d, %d calls: one record must never be handled twice at once", peak.Load(), calls.Load())
	}
}

// A message that itself makes a dependency fail must not be retried forever just
// because the failure is reported as "blocked": only refunded blocks are free.
func TestABlockThatCountsEventuallyParksTheRecord(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 1, "pay")
	var calls atomic.Int32
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, MaxAttempts: 3, Backoff: retry.Constant(time.Millisecond),
		Handler: func(ctx context.Context, it *dlq.Item) error {
			calls.Add(1)
			return &dlq.BlockedError{Dependency: "pay", Err: errors.New("payments returned 500 for this order")} // Refund: false
		},
	})
	start(t, r)
	eventually(t, "the record to be parked", func() bool { return r.Stats().Parked == 1 })
	if calls.Load() != 3 {
		t.Fatalf("handler ran %d times, want MaxAttempts (3)", calls.Load())
	}
}

func TestOnParkReceivesTheParkedRecordAndFailuresAreOnlyCounted(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 3, "pay")
	var mu sync.Mutex
	got := map[string]string{}
	var calls atomic.Int32
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store,
		Handler: func(ctx context.Context, it *dlq.Item) error {
			return retry.Permanent(errors.New("rejected: " + it.Record.ID))
		},
		OnPark: func(ctx context.Context, rec dlq.Record, reason string) error {
			mu.Lock()
			got[rec.ID] = string(rec.Value) + "|" + reason
			mu.Unlock()
			switch calls.Add(1) {
			case 1:
				return errors.New("kafka is down") // a failing mirror must not un-park
			case 2:
				panic("mirror bug")
			}
			return nil
		},
	})
	start(t, r)
	eventually(t, "three records parked", func() bool { return r.Stats().Parked == 3 && calls.Load() == 3 })

	mu.Lock()
	defer mu.Unlock()
	for id, v := range got {
		if !strings.HasPrefix(v, "payload|permanent failure: rejected: "+id) {
			t.Fatalf("OnPark saw %q for %s", v, id)
		}
	}
	if st, _ := store.Stats(context.Background()); st.Parked != 3 {
		t.Fatalf("stats = %+v: a failing mirror must not affect parking", st)
	}
	if r.Stats().MirrorErrors != 2 {
		t.Fatalf("MirrorErrors = %d, want 2 (one error, one panic)", r.Stats().MirrorErrors)
	}
}

// flakyStore fails Lease a few times with an ordinary error, as a remote store
// would when its connection drops.
type flakyStore struct {
	dlq.Store
	fails atomic.Int32
}

func (f *flakyStore) Lease(ctx context.Context, req dlq.LeaseRequest) ([]dlq.Lease, error) {
	if f.fails.Add(-1) >= 0 {
		return nil, errors.New("connection reset")
	}
	return f.Store.Lease(ctx, req)
}

func TestRedriverSurvivesTransientStoreErrors(t *testing.T) {
	inner := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, inner, 1, "pay")
	fs := &flakyStore{Store: inner}
	fs.fails.Store(2)
	var handled atomic.Int32
	r := newRedriver(t, dlq.RedriveConfig{Store: fs, Handler: func(context.Context, *dlq.Item) error { handled.Add(1); return nil }})
	start(t, r)
	eventually(t, "the record to be handled after the store recovered", func() bool { return handled.Load() == 1 })
	if r.Stats().StoreErrors < 2 {
		t.Fatalf("StoreErrors = %d, want the failures counted", r.Stats().StoreErrors)
	}
}

// A handler that ignores its context must not hold up shutdown for ever: Run returns
// once ShutdownTimeout has passed, and the record's lease simply expires.
func TestRunReturnsEvenIfAHandlerIgnoresItsContext(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 1, "pay")
	release := make(chan struct{})
	defer close(release)
	var started atomic.Bool
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store, ShutdownTimeout: 150 * time.Millisecond,
		Handler: func(ctx context.Context, it *dlq.Item) error { started.Store(true); <-release; return nil },
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	eventually(t, "the handler to start", func() bool { return started.Load() })
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return: a stuck handler held up shutdown")
	}
}

// A mirror that never answers must not hold a redrive worker for ever.
func TestOnParkIsBoundedByItsTimeout(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 2, "pay")
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store,
		Handler: func(ctx context.Context, it *dlq.Item) error {
			return retry.Permanent(errors.New("rejected"))
		},
		OnParkTimeout: 50 * time.Millisecond,
		OnPark: func(ctx context.Context, rec dlq.Record, reason string) error {
			<-ctx.Done() // a hung producer: only the deadline ends this
			return ctx.Err()
		},
	})
	start(t, r)
	eventually(t, "both hung mirrors to time out", func() bool { return r.Stats().MirrorErrors == 2 })
}

// Circuits that appear while the service runs (a guarded HTTP client's) must be
// honoured too, not only the ones known when the redriver was built.
func TestBreakerSourceHoldsRecordsWhileALateCircuitIsOpen(t *testing.T) {
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	addBlocked(t, store, 2, "late")
	br := breaker.New(breaker.Config{Name: "late", FailureThreshold: 0.1, MinSamples: 1, SleepWindow: time.Hour})
	defer br.Close()
	_ = br.Do(context.Background(), func(context.Context) error { return errors.New("down") })
	if br.State() != obs.StateOpen {
		t.Fatal("test setup: the circuit should be open")
	}
	var handled atomic.Int32
	var known atomic.Bool // the circuit is unknown to the redriver at first, as a lazy one is
	r := newRedriver(t, dlq.RedriveConfig{
		Store: store,
		Handler: func(ctx context.Context, it *dlq.Item) error {
			handled.Add(1)
			return nil
		},
		BreakerSource: func() map[string]*breaker.Breaker {
			if known.Load() {
				return map[string]*breaker.Breaker{"late": br}
			}
			return nil
		},
	})
	known.Store(true)
	start(t, r)
	time.Sleep(150 * time.Millisecond)
	if n := handled.Load(); n != 0 {
		t.Fatalf("the handler ran %d times while the circuit was open", n)
	}
}
