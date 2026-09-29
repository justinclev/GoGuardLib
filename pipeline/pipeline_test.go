package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/health"
	"github.com/justinclev/GoGuardLib/pipeline"
	"github.com/justinclev/GoGuardLib/retry"
	"github.com/justinclev/GoGuardLib/secure"
)

// harness records what each step did and lets a test decide how each one behaves.
type harness struct {
	mu    sync.Mutex
	calls map[string]int
	comps map[string]int
	keys  map[string][]string
	order []string
	fail  map[string]func() error
}

func newHarness() *harness {
	return &harness{calls: map[string]int{}, comps: map[string]int{}, keys: map[string][]string{}, fail: map[string]func() error{}}
}

func (h *harness) failWith(step string, f func() error) {
	h.mu.Lock()
	h.fail[step] = f
	h.mu.Unlock()
}

func (h *harness) count(step string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls[step]
}

func (h *harness) run(name string) func(context.Context, *pipeline.Exec) error {
	return func(ctx context.Context, x *pipeline.Exec) error {
		h.mu.Lock()
		h.calls[name]++
		h.keys[name] = append(h.keys[name], x.IdempotencyKey())
		h.order = append(h.order, name)
		f := h.fail[name]
		h.mu.Unlock()
		if name == "a" {
			x.Set("order-id", []byte("o-42")) // a later step needs this
		}
		if name == "d" {
			if v, ok := x.Get("order-id"); !ok || string(v) != "o-42" {
				return retry.Permanent(errors.New("step d lost the data step a produced"))
			}
		}
		if f != nil {
			return f()
		}
		return nil
	}
}

func (h *harness) comp(name string) func(context.Context, *pipeline.Exec) error {
	return func(ctx context.Context, x *pipeline.Exec) error {
		h.mu.Lock()
		h.comps[name]++
		h.order = append(h.order, "undo-"+name)
		f := h.fail["undo-"+name]
		h.mu.Unlock()
		if f != nil {
			return f()
		}
		return nil
	}
}

func (h *harness) steps(withComp bool) []pipeline.Step {
	var out []pipeline.Step
	for _, n := range []string{"a", "b", "c", "d"} {
		s := pipeline.Step{Name: n, Dependency: "dep-" + n, Run: h.run(n)}
		if withComp {
			s.Compensate = h.comp(n)
		}
		out = append(out, s)
	}
	return out
}

func mustPipeline(t testing.TB, steps []pipeline.Step, opts ...pipeline.Option) *pipeline.Pipeline {
	t.Helper()
	p, err := pipeline.New("orders", "v1", steps, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func input(id string) pipeline.Input {
	return pipeline.Input{
		ID: id, Source: dlq.Source{Kind: "kafka", Name: "orders", Partition: 2, Offset: 7},
		Key: []byte("cust-1"), Value: []byte("order body"), OrderKey: "cust-1",
		Headers: []dlq.Header{{Key: "trace", Value: []byte("t1")}},
	}
}

func startRedriver(t testing.TB, store dlq.Store, p *pipeline.Pipeline, breakers map[string]*breaker.Breaker) (*dlq.Redriver, func()) {
	t.Helper()
	r, err := dlq.NewRedriver(dlq.RedriveConfig{
		Store: store, Handler: p.Handler(), Breakers: breakers,
		PollInterval: 5 * time.Millisecond, Backoff: retry.Constant(5 * time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = r.Run(ctx); close(done) }()
	stop := func() { cancel(); <-done }
	t.Cleanup(stop)
	return r, stop
}

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

func total(t testing.TB, s dlq.Store) dlq.StoreStats {
	t.Helper()
	st, err := s.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestAllStepsRunOnceAndNothingIsStored(t *testing.T) {
	h := newHarness()
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	res, err := mustPipeline(t, h.steps(false)).Execute(context.Background(), store, input("m1"))
	if err != nil || res != pipeline.Done {
		t.Fatalf("res=%v err=%v", res, err)
	}
	if strings.Join(h.order, ",") != "a,b,c,d" || total(t, store).Total() != 0 {
		t.Fatalf("order %v, stored %+v", h.order, total(t, store))
	}
}

// The scenario the package exists for: step c of four is down. The message is set
// aside with its progress; when c recovers, a and b are NOT run again.
func TestResumesAtTheFailedStepWithoutRepeatingEarlierOnes(t *testing.T) {
	h := newHarness()
	h.failWith("c", func() error { return errors.New("payments API 503") })
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	p := mustPipeline(t, h.steps(false))

	res, err := p.Execute(context.Background(), store, input("m1"))
	if err != nil || res != pipeline.Deferred {
		t.Fatalf("res=%v err=%v, want Deferred", res, err)
	}
	if h.count("a") != 1 || h.count("b") != 1 || h.count("c") != 1 || h.count("d") != 0 {
		t.Fatalf("calls a=%d b=%d c=%d d=%d", h.count("a"), h.count("b"), h.count("c"), h.count("d"))
	}

	// What was stored: the message, tagged with the dependency that blocked it,
	// with a checkpoint that says a and b are done.
	ls, _ := store.Lease(context.Background(), dlq.LeaseRequest{Max: 1, TTL: time.Hour})
	rec := ls[0].Record
	done, err := pipeline.Completed(rec.Checkpoint)
	if err != nil || strings.Join(done, ",") != "a,b" {
		t.Fatalf("completed = %v (%v)", done, err)
	}
	if rec.BlockedOn != "dep-c" || string(rec.Value) != "order body" || rec.OrderKey != "cust-1" || rec.Source.Offset != 7 ||
		string(rec.Headers[0].Value) != "t1" || !strings.Contains(rec.LastError, "503") {
		t.Fatalf("record = %+v", rec)
	}
	keys, _ := pipeline.StateKeys(rec.Checkpoint)
	if strings.Join(keys, ",") != "order-id" {
		t.Fatalf("state keys = %v", keys)
	}
	must(t, store.Release(context.Background(), rec.ID, ls[0].Token))

	// c comes back. The redriver resumes at c; d still sees the data a produced.
	h.failWith("c", nil)
	startRedriver(t, store, p, nil)
	eventually(t, "the message to finish", func() bool { return total(t, store).Total() == 0 })
	if h.count("a") != 1 || h.count("b") != 1 {
		t.Fatalf("a ran %d times and b %d times: finished steps must not be repeated", h.count("a"), h.count("b"))
	}
	if h.count("c") != 2 || h.count("d") != 1 {
		t.Fatalf("c ran %d times, d %d times", h.count("c"), h.count("d"))
	}
}

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// Progress survives a restart of the whole process because it lives in the store.
func TestResumeSurvivesARestartOnTheDurableStore(t *testing.T) {
	dir := t.TempDir()
	h := newHarness()
	h.failWith("c", func() error { return errors.New("down") })
	store, err := dlq.OpenWAL(dir, dlq.WALOptions{})
	must(t, err)
	p := mustPipeline(t, h.steps(false))
	if res, err := p.Execute(context.Background(), store, input("m1")); err != nil || res != pipeline.Deferred {
		t.Fatalf("res=%v err=%v", res, err)
	}
	must(t, store.Close())

	h.failWith("c", nil)
	store, err = dlq.OpenWAL(dir, dlq.WALOptions{})
	must(t, err)
	defer store.Close()
	startRedriver(t, store, p, nil)
	eventually(t, "completion after restart", func() bool { return total(t, store).Total() == 0 })
	if h.count("a") != 1 || h.count("b") != 1 || h.count("d") != 1 {
		t.Fatalf("a=%d b=%d d=%d", h.count("a"), h.count("b"), h.count("d"))
	}
}

// Each step is checkpointed as it finishes, so a failure later in a resumed run
// does not lose the steps completed in that run.
func TestEachResumedStepIsCheckpointedBeforeTheNext(t *testing.T) {
	h := newHarness()
	h.failWith("b", func() error { return errors.New("b down") })
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	p := mustPipeline(t, h.steps(false))
	if res, _ := p.Execute(context.Background(), store, input("m1")); res != pipeline.Deferred {
		t.Fatalf("res = %v", res)
	}

	// b recovers but d is now down: the redrive completes b and c, then stops at d.
	h.failWith("b", nil)
	h.failWith("d", func() error { return errors.New("d down") })
	startRedriver(t, store, p, nil)
	eventually(t, "d to be attempted", func() bool { return h.count("d") >= 1 })
	time.Sleep(30 * time.Millisecond)

	for _, n := range []string{"a", "b", "c"} {
		if h.count(n) != map[string]int{"a": 1, "b": 2, "c": 1}[n] {
			t.Fatalf("step %s ran %d times; only the failing steps may be repeated", n, h.count(n))
		}
	}
}

func TestIdempotencyKeysAreStablePerMessageAndStep(t *testing.T) {
	first, second := pipeline.IdempotencyKey("m1", "a"), pipeline.IdempotencyKey("m1", "a")
	if first != second {
		t.Fatal("not stable")
	}
	seen := map[string]bool{}
	for _, id := range []string{"m1", "m2"} {
		for _, s := range []string{"a", "b"} {
			k := pipeline.IdempotencyKey(id, s)
			if len(k) != 32 || seen[k] {
				t.Fatalf("key %q is short or collides", k)
			}
			seen[k] = true
		}
	}
	if pipeline.IdempotencyKey("ab", "c") == pipeline.IdempotencyKey("a", "bc") {
		t.Fatal("ambiguous encoding")
	}
}

// If the process dies after a step's work but before anything is stored, the
// message is redelivered and the step runs again, with the SAME key.
func TestACrashBeforeStorageRerunsStepsWithTheSameKeys(t *testing.T) {
	h := newHarness()
	h.failWith("c", func() error { return errors.New("down") })
	full := dlq.NewMemoryStore(dlq.MemoryOptions{MaxRecords: 1})
	must(t, full.Append(context.Background(), dlq.Record{ID: "occupant"})) // so storing the message fails
	p := mustPipeline(t, h.steps(false))

	res, err := p.Execute(context.Background(), full, input("m1"))
	if res != pipeline.Failed || !errors.Is(err, dlq.ErrFull) {
		t.Fatalf("res=%v err=%v: a message that could not be stored must not be reported safe", res, err)
	}
	firstA := h.keys["a"][0]

	res, err = p.Execute(context.Background(), full, input("m1")) // redelivery
	if res != pipeline.Failed || err == nil {
		t.Fatalf("second delivery: res=%v err=%v", res, err)
	}
	if len(h.keys["a"]) != 2 || h.keys["a"][1] != firstA {
		t.Fatalf("keys for step a across deliveries: %v; they must be identical so a repeat is harmless", h.keys["a"])
	}
}

func TestAnOpenCircuitDefersWithoutCallingTheDependency(t *testing.T) {
	var healthy atomic.Bool
	cBreaker := breaker.New(breaker.Config{
		Name: "dep-c", FailureThreshold: 0.1, MinSamples: 1, SleepWindow: time.Hour,
		Health: &health.Config{Check: func(context.Context) error {
			if healthy.Load() {
				return nil
			}
			return errors.New("down")
		}, Interval: 5 * time.Millisecond, Jitter: -1, SuccessThreshold: 1},
	})
	defer cBreaker.Close()
	_ = cBreaker.Do(context.Background(), func(context.Context) error { return errors.New("outage") })

	h := newHarness()
	steps := h.steps(false)
	steps[2].Breaker = cBreaker
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	p := mustPipeline(t, steps)

	res, err := p.Execute(context.Background(), store, input("m1"))
	if err != nil || res != pipeline.Deferred {
		t.Fatalf("res=%v err=%v", res, err)
	}
	if h.count("c") != 0 {
		t.Fatal("step c was called although its circuit was open")
	}

	// The redriver leaves the record alone while the circuit is open, then drains it.
	_, _ = startRedriver(t, store, p, map[string]*breaker.Breaker{"dep-c": cBreaker})
	time.Sleep(80 * time.Millisecond)
	if h.count("c") != 0 || total(t, store).Pending != 1 {
		t.Fatalf("c=%d stats=%+v: the record must wait for the dependency", h.count("c"), total(t, store))
	}
	healthy.Store(true)
	eventually(t, "drain after recovery", func() bool { return total(t, store).Total() == 0 })
	if h.count("a") != 1 || h.count("c") != 1 || h.count("d") != 1 {
		t.Fatalf("a=%d c=%d d=%d", h.count("a"), h.count("c"), h.count("d"))
	}
}

func TestPermanentFailureParksWithProgressRecorded(t *testing.T) {
	h := newHarness()
	h.failWith("c", func() error { return retry.Permanent(errors.New("order references an unknown SKU")) })
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	res, err := mustPipeline(t, h.steps(false)).Execute(context.Background(), store, input("m1"))
	if err != nil || res != pipeline.Parked {
		t.Fatalf("res=%v err=%v", res, err)
	}
	st := total(t, store)
	if st.Parked != 1 || st.Pending != 0 {
		t.Fatalf("stats = %+v", st)
	}
	must(t, store.Requeue(context.Background(), "m1"))
	ls, _ := store.Lease(context.Background(), dlq.LeaseRequest{Max: 1, TTL: time.Hour})
	done, _ := pipeline.Completed(ls[0].Record.Checkpoint)
	if strings.Join(done, ",") != "a,b" || !strings.Contains(ls[0].Record.LastError, "unknown SKU") || !strings.Contains(ls[0].Record.LastError, `"c"`) {
		t.Fatalf("record = %+v done=%v", ls[0].Record, done)
	}
}

func TestPermanentFailureDuringRedriveParksViaTheRedriver(t *testing.T) {
	h := newHarness()
	h.failWith("c", func() error { return errors.New("down") })
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	p := mustPipeline(t, h.steps(false))
	_, _ = p.Execute(context.Background(), store, input("m1"))

	h.failWith("c", func() error { return retry.Permanent(errors.New("rejected for good")) })
	r, _ := startRedriver(t, store, p, nil)
	eventually(t, "parking", func() bool { return r.Stats().Parked == 1 })
	if h.count("c") != 2 {
		t.Fatalf("c ran %d times", h.count("c"))
	}
}

func TestSagaCompensatesCompletedStepsInReverseOrder(t *testing.T) {
	h := newHarness()
	h.failWith("c", func() error { return retry.Permanent(errors.New("declined")) })
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	res, err := mustPipeline(t, h.steps(true), pipeline.Saga()).Execute(context.Background(), store, input("m1"))
	if err != nil || res != pipeline.Parked {
		t.Fatalf("res=%v err=%v", res, err)
	}
	if got := strings.Join(h.order, ","); got != "a,b,c,undo-b,undo-a" {
		t.Fatalf("order = %s, want the completed steps undone newest first (c never completed, so it is not undone)", got)
	}
	must(t, store.Requeue(context.Background(), "m1"))
	ls, _ := store.Lease(context.Background(), dlq.LeaseRequest{Max: 1, TTL: time.Hour})
	if !strings.Contains(ls[0].Record.LastError, "2 step(s) compensated") {
		t.Fatalf("reason = %q", ls[0].Record.LastError)
	}
}

// Compensation survives outages and restarts: it resumes where it stopped and never
// undoes a step twice.
func TestSagaCompensationResumesAfterAnOutage(t *testing.T) {
	h := newHarness()
	h.failWith("c", func() error { return errors.New("down") })
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	p := mustPipeline(t, h.steps(true), pipeline.Saga())
	_, _ = p.Execute(context.Background(), store, input("m1")) // a,b done; c blocked

	// On redrive c fails for good, and undoing b hits an outage.
	h.failWith("c", func() error { return retry.Permanent(errors.New("declined")) })
	h.failWith("undo-b", func() error { return errors.New("b's undo API is down") })
	r, _ := startRedriver(t, store, p, nil)
	eventually(t, "the undo of b to be attempted", func() bool { h.mu.Lock(); defer h.mu.Unlock(); return h.comps["b"] >= 2 })

	h.failWith("undo-b", nil)
	eventually(t, "parking after compensation", func() bool { return r.Stats().Parked == 1 })
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.comps["a"] != 1 {
		t.Fatalf("undo-a ran %d times", h.comps["a"])
	}
	if h.calls["c"] != 2 { // once live, once on redrive: the failure was not retried once permanent
		t.Fatalf("c ran %d times", h.calls["c"])
	}
	// undo-b failed during the outage (at least once) and then succeeded; undo-a
	// ran exactly once and only after that.
	if h.comps["b"] < 2 {
		t.Fatalf("undo-b ran %d times, want at least 2 (an outage, then a success)", h.comps["b"])
	}
	if n := len(h.order); h.order[n-2] != "undo-b" || h.order[n-1] != "undo-a" {
		t.Fatalf("order = %v: undo-a must follow the successful undo-b", h.order)
	}
}

func TestACompensationThatFailsForGoodNeedsAHuman(t *testing.T) {
	h := newHarness()
	h.failWith("c", func() error { return retry.Permanent(errors.New("declined")) })
	h.failWith("undo-b", func() error { return retry.Permanent(errors.New("cannot be undone")) })
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	res, err := mustPipeline(t, h.steps(true), pipeline.Saga()).Execute(context.Background(), store, input("m1"))
	if err != nil || res != pipeline.Parked {
		t.Fatalf("res=%v err=%v", res, err)
	}
	must(t, store.Requeue(context.Background(), "m1"))
	ls, _ := store.Lease(context.Background(), dlq.LeaseRequest{Max: 1, TTL: time.Hour})
	if !strings.Contains(ls[0].Record.LastError, "manual intervention") {
		t.Fatalf("reason = %q", ls[0].Record.LastError)
	}
}

func TestWithoutSagaNothingIsCompensated(t *testing.T) {
	h := newHarness()
	h.failWith("c", func() error { return retry.Permanent(errors.New("declined")) })
	_, _ = mustPipeline(t, h.steps(true)).Execute(context.Background(), dlq.NewMemoryStore(dlq.MemoryOptions{}), input("m1"))
	if len(h.comps) != 0 {
		t.Fatalf("compensations ran without Saga: %v", h.comps)
	}
}

// A checkpoint from a different shape of pipeline must never be resumed: it could
// skip or repeat the wrong work.
func TestMismatchedPipelineParksInsteadOfResuming(t *testing.T) {
	h := newHarness()
	h.failWith("c", func() error { return errors.New("down") })
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	_, _ = mustPipeline(t, h.steps(false)).Execute(context.Background(), store, input("m1")) // a,b done

	cases := map[string]func() *pipeline.Pipeline{
		"step renamed": func() *pipeline.Pipeline {
			s := h.steps(false)
			s[1].Name = "b2"
			return mustPipeline(t, s)
		},
		"step removed": func() *pipeline.Pipeline { return mustPipeline(t, h.steps(false)[:1]) },
		"step reordered": func() *pipeline.Pipeline {
			s := h.steps(false)
			s[0], s[1] = s[1], s[0]
			return mustPipeline(t, s)
		},
		"other pipeline": func() *pipeline.Pipeline {
			p, err := pipeline.New("refunds", "v1", h.steps(false))
			must(t, err)
			return p
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			ls, _ := store.Lease(context.Background(), dlq.LeaseRequest{Max: 1, TTL: time.Hour})
			item := itemFor(t, store, ls[0])
			before := totalCalls(h)
			err := build().Handler()(context.Background(), item)
			if !errors.Is(err, pipeline.ErrPipelineMismatch) || !retry.IsPermanent(err) {
				t.Fatalf("err = %v, want a permanent ErrPipelineMismatch", err)
			}
			if totalCalls(h) != before {
				t.Fatal("a step ran against a checkpoint that does not fit")
			}
			must(t, store.Release(context.Background(), ls[0].Record.ID, ls[0].Token))
		})
	}
}

func totalCalls(h *harness) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, c := range h.calls {
		n += c
	}
	return n
}

// itemFor obtains a dlq.Item the way the redriver does, by running one through it.
func itemFor(t *testing.T, store dlq.Store, l dlq.Lease) *dlq.Item {
	t.Helper()
	// Item's fields are private to package dlq; capture one via a real redrive round.
	var got *dlq.Item
	sub := dlq.NewMemoryStore(dlq.MemoryOptions{})
	rec := l.Record
	rec.State = dlq.Pending
	must(t, sub.Append(context.Background(), rec))
	r, err := dlq.NewRedriver(dlq.RedriveConfig{
		Store: sub, PollInterval: time.Millisecond,
		Handler: func(ctx context.Context, it *dlq.Item) error {
			got = it
			return retry.Permanent(errors.New("captured"))
		},
	})
	must(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = r.Run(ctx); close(done) }()
	eventually(t, "capture", func() bool { return r.Stats().Parked == 1 })
	cancel()
	<-done
	return got
}

func TestCompatibleChangesResume(t *testing.T) {
	h := newHarness()
	h.failWith("c", func() error { return errors.New("down") })
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	_, _ = mustPipeline(t, h.steps(false)).Execute(context.Background(), store, input("m1"))

	// A new version that adds a step at the end, and changes the version label.
	h.failWith("c", nil)
	steps := append(h.steps(false), pipeline.Step{Name: "e", Run: h.run("e")})
	p2, err := pipeline.New("orders", "v2", steps)
	must(t, err)
	startRedriver(t, store, p2, nil)
	eventually(t, "completion", func() bool { return total(t, store).Total() == 0 })
	if h.count("e") != 1 || h.count("a") != 1 {
		t.Fatalf("e=%d a=%d", h.count("e"), h.count("a"))
	}
}

func TestGarbageCheckpointIsAMismatchNotAPanic(t *testing.T) {
	h := newHarness()
	p := mustPipeline(t, h.steps(false))
	for _, cp := range []string{"not json", `{"v":99}`, `{"v":1,"pipeline":"orders","phase":"sideways"}`, `{"v":1,"pipeline":"orders","phase":"forward","completed":[{"step":"a"},{"step":"b"},{"step":"c"},{"step":"d"},{"step":"e"}]}`} {
		store := dlq.NewMemoryStore(dlq.MemoryOptions{})
		must(t, store.Append(context.Background(), dlq.Record{ID: "x", Checkpoint: []byte(cp)}))
		ls, _ := store.Lease(context.Background(), dlq.LeaseRequest{Max: 1, TTL: time.Hour})
		err := p.Handler()(context.Background(), itemFor(t, store, ls[0]))
		if !errors.Is(err, pipeline.ErrPipelineMismatch) {
			t.Errorf("checkpoint %q: err = %v", cp, err)
		}
	}
	if totalCalls(h) != 0 {
		t.Fatal("steps ran against garbage")
	}
}

func TestRecordWithoutACheckpointStartsFromTheTop(t *testing.T) {
	h := newHarness()
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	must(t, store.Append(context.Background(), dlq.Record{ID: "raw", Value: []byte("v")})) // stored by something else
	startRedriver(t, store, mustPipeline(t, h.steps(false)), nil)
	eventually(t, "completion", func() bool { return total(t, store).Total() == 0 })
	if strings.Join(h.order, ",") != "a,b,c,d" {
		t.Fatalf("order = %v", h.order)
	}
}

func TestStepTimeoutRetryPanicAndRedaction(t *testing.T) {
	var flaky atomic.Int32
	var secretStore = dlq.NewMemoryStore(dlq.MemoryOptions{})
	steps := []pipeline.Step{
		{Name: "flaky", Retry: retry.Policy{MaxRetries: 2}, Run: func(context.Context, *pipeline.Exec) error {
			if flaky.Add(1) < 3 {
				return errors.New("blip")
			}
			return nil
		}},
		{Name: "slow", Timeout: 20 * time.Millisecond, Run: func(ctx context.Context, x *pipeline.Exec) error {
			<-ctx.Done()
			return ctx.Err()
		}},
	}
	p := mustPipeline(t, steps)
	start := time.Now()
	res, err := p.Execute(context.Background(), secretStore, input("m1"))
	if err != nil || res != pipeline.Deferred || flaky.Load() != 3 || time.Since(start) > 3*time.Second {
		t.Fatalf("res=%v err=%v flaky=%d: in-process retries then the timeout should defer", res, err, flaky.Load())
	}

	panicky := mustPipeline(t, []pipeline.Step{{Name: "boom", Run: func(_ context.Context, x *pipeline.Exec) error {
		panic("crashed with " + string(x.Value))
	}}})
	st := dlq.NewMemoryStore(dlq.MemoryOptions{})
	if res, err := panicky.Execute(context.Background(), st, input("m2")); err != nil || res != pipeline.Deferred {
		t.Fatalf("a panicking step: res=%v err=%v", res, err)
	}
	ls, _ := st.Lease(context.Background(), dlq.LeaseRequest{Max: 1, TTL: time.Hour})
	if strings.Contains(ls[0].Record.LastError, "order body") {
		t.Fatalf("the panic leaked the message into the stored error: %q", ls[0].Record.LastError)
	}

	leaky := mustPipeline(t, []pipeline.Step{{Name: "auth", Run: func(context.Context, *pipeline.Exec) error {
		return errors.New("401 from upstream: password=hunter2hunter2")
	}}})
	st2 := dlq.NewMemoryStore(dlq.MemoryOptions{})
	_, _ = leaky.Execute(context.Background(), st2, input("m3"))
	ls, _ = st2.Lease(context.Background(), dlq.LeaseRequest{Max: 1, TTL: time.Hour})
	if strings.Contains(ls[0].Record.LastError, "hunter2hunter2") {
		t.Fatalf("stored error is not redacted: %q", ls[0].Record.LastError)
	}
}

func TestCancelledContextStoresNothingAndFails(t *testing.T) {
	h := newHarness()
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	h.failWith("b", func() error { return context.Canceled })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := mustPipeline(t, h.steps(false)).Execute(ctx, store, input("m1"))
	if res != pipeline.Failed || err == nil || total(t, store).Total() != 0 {
		t.Fatalf("res=%v err=%v stored=%+v: a cancelled run says nothing about the message", res, err, total(t, store))
	}
}

func TestValidation(t *testing.T) {
	ok := pipeline.Step{Name: "a", Run: func(context.Context, *pipeline.Exec) error { return nil }}
	for name, fn := range map[string]func() error{
		"no name":      func() error { _, e := pipeline.New("", "v", []pipeline.Step{ok}); return e },
		"no steps":     func() error { _, e := pipeline.New("p", "v", nil); return e },
		"unnamed step": func() error { _, e := pipeline.New("p", "v", []pipeline.Step{{Run: ok.Run}}); return e },
		"no run":       func() error { _, e := pipeline.New("p", "v", []pipeline.Step{{Name: "x"}}); return e },
		"duplicate":    func() error { _, e := pipeline.New("p", "v", []pipeline.Step{ok, ok}); return e },
	} {
		if fn() == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	p := mustPipeline(t, []pipeline.Step{ok})
	if _, err := p.Execute(context.Background(), dlq.NewMemoryStore(dlq.MemoryOptions{}), pipeline.Input{}); err == nil {
		t.Fatal("Execute without an ID must fail")
	}
	if strings.Join(p.StepNames(), ",") != "a" {
		t.Fatal("StepNames")
	}
	for r, want := range map[pipeline.Result]string{pipeline.Done: "done", pipeline.Deferred: "deferred", pipeline.Parked: "parked", pipeline.Failed: "failed"} {
		if r.String() != want {
			t.Errorf("%d -> %q", int(r), r.String())
		}
	}
	if _, err := pipeline.Completed([]byte("x")); err == nil {
		t.Fatal("Completed accepted garbage")
	}
	if _, err := pipeline.StateKeys([]byte("x")); err == nil {
		t.Fatal("StateKeys accepted garbage")
	}
}

func TestDependencyDefaults(t *testing.T) {
	b := breaker.New(breaker.Config{Name: "from-breaker", FailureThreshold: 0.1, MinSamples: 1})
	steps := []pipeline.Step{
		{Name: "plain", Run: func(context.Context, *pipeline.Exec) error { return errors.New("x") }},
	}
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	_, _ = mustPipeline(t, steps).Execute(context.Background(), store, input("m1"))
	ls, _ := store.Lease(context.Background(), dlq.LeaseRequest{Max: 1, TTL: time.Hour})
	if ls[0].Record.BlockedOn != "plain" {
		t.Fatalf("BlockedOn = %q, want the step name", ls[0].Record.BlockedOn)
	}

	steps[0].Breaker = b
	store2 := dlq.NewMemoryStore(dlq.MemoryOptions{})
	_, _ = mustPipeline(t, steps).Execute(context.Background(), store2, input("m2"))
	ls, _ = store2.Lease(context.Background(), dlq.LeaseRequest{Max: 1, TTL: time.Hour})
	if ls[0].Record.BlockedOn != "from-breaker" {
		t.Fatalf("BlockedOn = %q, want the breaker's name", ls[0].Record.BlockedOn)
	}
	_ = fmt.Sprint
}

func TestWithRedactorScrubsStoredErrors(t *testing.T) {
	re := regexp.MustCompile(`ORDER-\d+`)
	steps := []pipeline.Step{{Name: "a", Run: func(context.Context, *pipeline.Exec) error {
		return errors.New("could not charge ORDER-12345")
	}}}
	p, err := pipeline.New("orders", "v1", steps, pipeline.WithRedactor(secure.NewRedactor(secure.WithPatterns(re))))
	must(t, err)
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	_, _ = p.Execute(context.Background(), store, input("m1"))
	ls, _ := store.Lease(context.Background(), dlq.LeaseRequest{Max: 1, TTL: time.Hour})
	if strings.Contains(ls[0].Record.LastError, "ORDER-12345") {
		t.Fatalf("custom pattern not applied: %q", ls[0].Record.LastError)
	}
}

func TestBreakersListsEachGuardingBreakerOnce(t *testing.T) {
	b1 := breaker.New(breaker.Config{Name: "one"})
	b2 := breaker.New(breaker.Config{Name: "two"})
	run := func(context.Context, *pipeline.Exec) error { return nil }
	p := mustPipeline(t, []pipeline.Step{
		{Name: "a", Breaker: b1, Run: run}, {Name: "b", Run: run}, {Name: "c", Breaker: b2, Run: run}, {Name: "d", Breaker: b1, Run: run},
	})
	got := p.Breakers()
	if len(got) != 2 || got[0] != b1 || got[1] != b2 {
		t.Fatalf("Breakers = %v", got)
	}
	if len(mustPipeline(t, []pipeline.Step{{Name: "x", Run: run}}).Breakers()) != 0 {
		t.Fatal("an unguarded pipeline has no breakers")
	}
}
