package kafka_test

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
	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/pipeline"
	"github.com/justinclev/GoGuardLib/retry"
)

const topic = "orders"

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

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// rig wires a consumer to a simulated broker with a one-step pipeline whose
// behaviour each test controls.
type rig struct {
	t      *testing.T
	broker *fakeBroker
	client *fakeClient
	store  dlq.Store

	mu   sync.Mutex
	ok   []string // values the step completed, in order
	fail func(value string) error
	br   *breaker.Breaker
}

func newRig(t *testing.T) *rig {
	b := newFakeBroker()
	return &rig{t: t, broker: b, client: newFakeClient(b), store: dlq.NewMemoryStore(dlq.MemoryOptions{})}
}

func (r *rig) setFail(f func(string) error) { r.mu.Lock(); r.fail = f; r.mu.Unlock() }

func (r *rig) done() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ok...)
}

func (r *rig) doneCount() int { return len(r.done()) }

func (r *rig) pipeline() *pipeline.Pipeline {
	step := pipeline.Step{Name: "handle", Breaker: r.br, Run: func(ctx context.Context, x *pipeline.Exec) error {
		r.mu.Lock()
		f := r.fail
		r.mu.Unlock()
		if f != nil {
			if err := f(string(x.Value)); err != nil {
				return err
			}
		}
		r.mu.Lock()
		r.ok = append(r.ok, string(x.Value))
		r.mu.Unlock()
		return nil
	}}
	p, err := pipeline.New("orders", "v1", []pipeline.Step{step})
	must(r.t, err)
	return p
}

func (r *rig) config(mod ...func(*kafka.Config)) kafka.Config {
	cfg := kafka.Config{
		Client: r.client, Store: r.store, Bindings: []kafka.Binding{{Topic: topic, Pipeline: r.pipeline()}},
		PollTimeout: 5 * time.Millisecond, CommitInterval: 10 * time.Millisecond,
		RetryBackoff: retry.Constant(15 * time.Millisecond),
	}
	for _, m := range mod {
		m(&cfg)
	}
	return cfg
}

func (r *rig) assign(parts ...int32) {
	var tps []kafka.TopicPartition
	for _, p := range parts {
		tps = append(tps, tp(topic, p))
	}
	r.client.inject(assignedEvent{Partitions: tps})
}

// run starts the consumer and returns a function that stops it and returns Run's error.
func (r *rig) run(c *kafka.Consumer) (stop func() error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	var once sync.Once
	var result error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case result = <-done:
			case <-time.After(10 * time.Second):
				r.t.Error("Run did not stop")
			}
		})
		return result
	}
	r.t.Cleanup(func() { _ = stop() })
	return stop
}

// runAsync starts the consumer and returns a channel that delivers Run's result
// when it exits on its own. It is cancelled at the end of the test.
func (r *rig) runAsync(c *kafka.Consumer) <-chan error {
	ctx, cancel := context.WithCancel(context.Background())
	r.t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	out := make(chan error, 1)
	go func() {
		select {
		case err := <-done:
			out <- err
		case <-time.After(10 * time.Second):
			out <- errors.New("Run did not exit")
		}
	}()
	return out
}

func (r *rig) newConsumer(mod ...func(*kafka.Config)) *kafka.Consumer {
	c, err := kafka.NewConsumer(r.config(mod...))
	must(r.t, err)
	return c
}

func (r *rig) storeStats() dlq.StoreStats {
	st, err := r.store.Stats(context.Background())
	must(r.t, err)
	return st
}

func (r *rig) produceN(n int, prefix string) {
	for i := 0; i < n; i++ {
		r.broker.produce(topic, 0, fmt.Sprintf("k%d", i), fmt.Sprintf("%s%d", prefix, i))
	}
}

func (r *rig) startRedriver(p *pipeline.Pipeline) (*dlq.Redriver, func()) {
	rd, err := dlq.NewRedriver(dlq.RedriveConfig{
		Store: r.store, Handler: p.Handler(), PollInterval: 5 * time.Millisecond, Backoff: retry.Constant(5 * time.Millisecond),
	})
	must(r.t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = rd.Run(ctx); close(done) }()
	stop := func() { cancel(); <-done }
	r.t.Cleanup(stop)
	return rd, stop
}

func TestConsumesEverythingAndCommitsWhatIsSafe(t *testing.T) {
	r := newRig(t)
	r.produceN(100, "m")
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.CommitBatch = 10 })
	r.assign(0)
	stop := r.run(c)
	eventually(t, "all messages", func() bool { return r.doneCount() == 100 })
	must(t, stop())

	if got := r.broker.committedOffset(topic, 0); got != 100 {
		t.Fatalf("committed %d, want 100", got)
	}
	if st := c.Stats(); st.Done != 100 || st.Received != 100 || st.Deferred != 0 || st.Commits < 2 {
		t.Fatalf("stats = %+v", st)
	}
	if r.storeStats().Total() != 0 {
		t.Fatal("nothing should be stored when nothing failed")
	}
	if strings.Join(r.done()[:3], ",") != "m0,m1,m2" {
		t.Fatalf("processed out of order: %v", r.done()[:3])
	}
}

// A dependency failing for some messages must not stop the topic: they are stored
// with their progress, everything else flows, offsets move past them, and the
// redriver finishes them later. Nothing is processed twice or lost.
func TestFailuresAreStoredNotBlockingAndLaterRedriven(t *testing.T) {
	r := newRig(t)
	r.produceN(20, "m")
	r.setFail(func(v string) error {
		if v == "m3" || v == "m8" || v == "m13" {
			return errors.New("payments 503")
		}
		return nil
	})
	p := r.pipeline()
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.Bindings[0].Pipeline = p })
	r.assign(0)
	stop := r.run(c)
	eventually(t, "the topic to be consumed", func() bool { return c.Stats().Received == 20 })
	eventually(t, "offsets to reach the end", func() bool { return r.broker.committedOffset(topic, 0) == 20 })

	if st := c.Stats(); st.Done != 17 || st.Deferred != 3 || st.Retried != 0 {
		t.Fatalf("stats = %+v", st)
	}
	if st := r.storeStats(); st.Pending != 3 {
		t.Fatalf("store = %+v, want the 3 failed messages", st)
	}

	// The dependency recovers; the redriver completes the three, each once.
	r.setFail(nil)
	_, stopRD := r.startRedriver(p)
	eventually(t, "the store to drain", func() bool { return r.storeStats().Total() == 0 })
	stopRD()
	must(t, stop())

	counts := map[string]int{}
	for _, v := range r.done() {
		counts[v]++
	}
	if len(counts) != 20 {
		t.Fatalf("%d distinct messages completed, want 20", len(counts))
	}
	for v, n := range counts {
		if n != 1 {
			t.Errorf("message %s completed %d times", v, n)
		}
	}
}

// While a dependency's circuit is open the topic is paused: messages stay in Kafka
// (not copied into the local store), the consumer keeps polling, and when the
// circuit half-opens everything flows again with a real message as the canary.
func TestTopicPausesWhileACircuitIsOpenAndResumesOnRecovery(t *testing.T) {
	var healthy atomic.Bool
	r := newRig(t)
	r.br = breaker.New(breaker.Config{
		Name: "payments", FailureThreshold: 0.1, MinSamples: 1, SleepWindow: time.Hour,
		Health: &health.Config{Check: func(context.Context) error {
			if healthy.Load() {
				return nil
			}
			return errors.New("down")
		}, Interval: 5 * time.Millisecond, Jitter: -1, SuccessThreshold: 1},
	})
	defer r.br.Close()
	_ = r.br.Do(context.Background(), func(context.Context) error { return errors.New("outage") })
	if r.br.State() != breaker.StateOpen {
		t.Fatal("setup: circuit should be open")
	}

	r.produceN(30, "m")
	c := r.newConsumer()
	r.assign(0)
	r.run(c)

	eventually(t, "the partition to be paused", func() bool { return r.client.isPaused(tp(topic, 0)) })
	polls := r.client.pollCount()
	time.Sleep(100 * time.Millisecond)
	if c.Stats().Received != 0 || r.storeStats().Total() != 0 || r.broker.committedOffset(topic, 0) != 0 {
		t.Fatalf("stats=%+v store=%+v: during an outage messages must stay in Kafka, untouched", c.Stats(), r.storeStats())
	}
	if r.client.pollCount() <= polls {
		t.Fatal("the consumer stopped polling while paused; the group would consider it dead")
	}
	if c.Stats().Paused != 1 {
		t.Fatalf("Paused = %d", c.Stats().Paused)
	}

	healthy.Store(true)
	eventually(t, "the backlog to be processed", func() bool { return r.doneCount() == 30 })
	eventually(t, "offsets to reach the end", func() bool { return r.broker.committedOffset(topic, 0) == 30 })
	if st := c.Stats(); st.Deferred != 0 || st.Done != 30 || st.Paused != 0 {
		t.Fatalf("stats = %+v: recovery should drain the topic directly, without going through the store", st)
	}
	if r.storeStats().Total() != 0 {
		t.Fatal("the store should never have been used")
	}
}

// A later message with the same key must never overtake an earlier one that is
// waiting; other keys are unaffected.
func TestSameKeyMessagesStayInOrderBehindADeferredOne(t *testing.T) {
	r := newRig(t)
	r.broker.produce(topic, 0, "cust-1", "first")
	r.broker.produce(topic, 0, "cust-1", "second")
	r.broker.produce(topic, 0, "cust-2", "other")
	r.setFail(func(v string) error {
		if v == "first" {
			return errors.New("down")
		}
		return nil
	})
	p := r.pipeline()
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.Bindings[0].Pipeline = p })
	r.assign(0)
	stop := r.run(c)
	eventually(t, "offsets to reach the end", func() bool { return r.broker.committedOffset(topic, 0) == 3 })

	if st := c.Stats(); st.Deferred != 2 || st.OrderHeld != 1 || st.Done != 1 {
		t.Fatalf("stats = %+v: 'first' deferred, 'second' held behind it, 'other' done", st)
	}
	if got := r.done(); len(got) != 1 || got[0] != "other" {
		t.Fatalf("completed %v: 'second' must not run ahead of 'first'", got)
	}

	r.setFail(nil)
	_, stopRD := r.startRedriver(p)
	eventually(t, "the store to drain", func() bool { return r.storeStats().Total() == 0 })
	stopRD()
	must(t, stop())
	if got := strings.Join(r.done(), ","); got != "other,first,second" {
		t.Fatalf("completion order %s, want other,first,second", got)
	}
}

func TestIgnoreKeyOrderLetsLaterMessagesProceed(t *testing.T) {
	r := newRig(t)
	r.broker.produce(topic, 0, "cust-1", "first")
	r.broker.produce(topic, 0, "cust-1", "second")
	r.setFail(func(v string) error {
		if v == "first" {
			return errors.New("down")
		}
		return nil
	})
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.IgnoreKeyOrder = true })
	r.assign(0)
	r.run(c)
	eventually(t, "second to complete", func() bool { return r.doneCount() == 1 })
	if c.Stats().OrderHeld != 0 || r.done()[0] != "second" {
		t.Fatalf("stats = %+v done = %v", c.Stats(), r.done())
	}
}

// If a message can neither be processed nor stored, it is not skipped: the
// partition goes back to it and waits, and nothing behind it is processed first.
func TestAFullStoreAppliesBackpressureInsteadOfSkipping(t *testing.T) {
	r := newRig(t)
	r.store = dlq.NewMemoryStore(dlq.MemoryOptions{MaxRecords: 1})
	must(t, r.store.Append(context.Background(), dlq.Record{ID: "occupant", Value: []byte("x")}))
	r.broker.produce(topic, 0, "a", "bad")
	r.broker.produce(topic, 0, "b", "good")
	r.setFail(func(v string) error {
		if v == "bad" {
			return errors.New("down")
		}
		return nil
	})
	c := r.newConsumer()
	r.assign(0)
	r.run(c)

	eventually(t, "backpressure", func() bool { return c.Stats().Backpressure >= 1 })
	time.Sleep(60 * time.Millisecond)
	if r.broker.committedOffset(topic, 0) != 0 {
		t.Fatalf("committed %d while a message could not be protected", r.broker.committedOffset(topic, 0))
	}
	if got := r.done(); len(got) != 0 {
		t.Fatalf("completed %v: nothing may run ahead of the message that is waiting", got)
	}

	// Space appears (the occupant is finished); the message is now stored and the
	// partition moves on.
	ls, _ := r.store.Lease(context.Background(), dlq.LeaseRequest{Max: 1, TTL: time.Hour})
	must(t, r.store.Ack(context.Background(), ls[0].Record.ID, ls[0].Token))
	eventually(t, "the partition to advance", func() bool { return r.broker.committedOffset(topic, 0) == 2 })
	if c.Stats().Deferred != 1 || len(r.done()) != 1 || r.done()[0] != "good" {
		t.Fatalf("stats = %+v done = %v", c.Stats(), r.done())
	}
	if !strings.Contains(strings.Join(r.client.callLog(), "|"), "seek orders[0] 0") {
		t.Fatalf("the consumer never went back to the failed message: %v", r.client.callLog())
	}
}

// If commits fail the consumer keeps working; after a crash the messages are
// redelivered. Storing is idempotent, so nothing is duplicated in the store and
// nothing is lost.
func TestCrashBeforeCommitRedeliversWithoutLossOrDuplicateStorage(t *testing.T) {
	r := newRig(t)
	r.produceN(10, "m")
	r.setFail(func(v string) error {
		if v == "m2" || v == "m5" {
			return errors.New("down")
		}
		return nil
	})
	p := r.pipeline()
	r.client.commitErr = func() error { return errBoom } // nothing can be committed: a crash before any commit
	c1 := r.newConsumer(func(cfg *kafka.Config) { cfg.Bindings[0].Pipeline = p; cfg.ShutdownTimeout = 40 * time.Millisecond })
	r.assign(0)
	stop := r.run(c1)
	eventually(t, "the first run to process everything", func() bool { return c1.Stats().Received == 10 })
	err := stop()
	if err == nil || !strings.Contains(err.Error(), "committing offsets") {
		t.Fatalf("Run = %v; a failed final commit must be reported", err)
	}
	if r.broker.committedOffset(topic, 0) != 0 {
		t.Fatal("setup: nothing should have been committed")
	}
	if c1.Stats().CommitErrors == 0 {
		t.Fatal("commit errors not counted")
	}
	storedBefore := r.storeStats().Total()

	// A new instance takes over and reads everything again.
	client2 := newFakeClient(r.broker)
	c2 := r.newConsumer(func(cfg *kafka.Config) { cfg.Client = client2; cfg.Bindings[0].Pipeline = p })
	client2.inject(assignedEvent{Partitions: []kafka.TopicPartition{tp(topic, 0)}})
	stop2 := r.run(c2)
	eventually(t, "the second run to finish", func() bool { return r.broker.committedOffset(topic, 0) == 10 })
	must(t, stop2())

	if r.storeStats().Total() != storedBefore || storedBefore != 2 {
		t.Fatalf("stored %d before and %d after: redelivery must not duplicate stored messages", storedBefore, r.storeStats().Total())
	}
	seen := map[string]bool{}
	for _, v := range r.done() {
		seen[v] = true
	}
	for i := 0; i < 10; i++ {
		v := fmt.Sprintf("m%d", i)
		if v != "m2" && v != "m5" && !seen[v] {
			t.Fatalf("message %s was lost", v)
		}
	}
}

func TestRebalanceCommitsBeforeGivingUpPartitionsAndReappliesPauses(t *testing.T) {
	var healthy atomic.Bool
	r := newRig(t)
	r.br = breaker.New(breaker.Config{
		Name: "payments", FailureThreshold: 0.1, MinSamples: 1, SleepWindow: time.Hour,
		Health: &health.Config{Check: func(context.Context) error {
			if healthy.Load() {
				return nil
			}
			return errors.New("down")
		}, Interval: 5 * time.Millisecond, Jitter: -1, SuccessThreshold: 1},
	})
	defer r.br.Close()
	for i := 0; i < 5; i++ {
		r.broker.produce(topic, 0, "", fmt.Sprintf("a%d", i))
		r.broker.produce(topic, 1, "", fmt.Sprintf("b%d", i))
	}
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.CommitInterval = time.Hour; cfg.CommitBatch = 1000 })
	r.assign(0, 1)
	r.run(c)
	eventually(t, "both partitions to be read", func() bool { return r.doneCount() == 10 })

	// Losing partition 1 must commit its progress first, and only then let go.
	r.client.inject(revokedEvent{Partitions: []kafka.TopicPartition{tp(topic, 1)}})
	eventually(t, "the revocation", func() bool { return strings.Contains(strings.Join(r.client.callLog(), "|"), "unassign orders[1]") })
	log := r.client.callLog()
	commitAt, unassignAt := -1, -1
	for i, e := range log {
		if e == "commit orders[1] 5" {
			commitAt = i
		}
		if e == "unassign orders[1]" {
			unassignAt = i
		}
	}
	if commitAt < 0 || unassignAt < 0 || commitAt > unassignAt {
		t.Fatalf("calls %v: progress on a revoked partition must be committed before it is released", log)
	}
	if r.broker.committedOffset(topic, 0) != 0 {
		t.Fatal("partition 0 was not revoked and must not have been committed yet")
	}

	// The circuit opens, then partition 1 comes back: the pause must be re-applied.
	for i := 0; i < 40; i++ { // enough failures to outweigh the successes so far
		_ = r.br.Do(context.Background(), func(context.Context) error { return errors.New("outage") })
	}
	if r.br.State() != breaker.StateOpen {
		t.Fatal("setup: the circuit should be open")
	}
	r.client.inject(assignedEvent{Partitions: []kafka.TopicPartition{tp(topic, 1)}})
	eventually(t, "the pause to be re-applied to the returned partition", func() bool { return r.client.isPaused(tp(topic, 1)) })
	healthy.Store(true)
	eventually(t, "resume after recovery", func() bool { return !r.client.isPaused(tp(topic, 1)) })
	if c.Stats().Rebalances < 3 {
		t.Fatalf("Rebalances = %d", c.Stats().Rebalances)
	}
}

func TestAFailedStoreStopsTheConsumerWithoutCommittingPastTheMessage(t *testing.T) {
	r := newRig(t)
	r.produceN(6, "m")
	r.setFail(func(v string) error {
		if v == "m3" {
			_ = r.store.Close() // the store dies just as this message needs it
			return errors.New("down")
		}
		return nil
	})
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.CommitInterval = time.Hour })
	r.assign(0)
	err := <-r.runAsync(c)
	if !errors.Is(err, dlq.ErrClosed) {
		t.Fatalf("Run = %v, want the store failure", err)
	}
	if got := r.broker.committedOffset(topic, 0); got != 3 {
		t.Fatalf("committed %d: the messages before the failure are safe (3), the failing one and later are not", got)
	}
}

// If a message arrives from the future (a gap), the consumer must not accept it:
// it points the partition back at the missing offset.
func TestAGapInDeliveryIsNeverAcceptedAsProgress(t *testing.T) {
	r := newRig(t)
	r.produceN(12, "m")
	r.client.skipAhead = 5
	c := r.newConsumer()
	r.assign(0)
	stop := r.run(c)
	eventually(t, "all messages", func() bool { return r.doneCount() == 12 })
	must(t, stop())
	for i, v := range r.done() {
		if v != fmt.Sprintf("m%d", i) {
			t.Fatalf("order %v: message %d was skipped or reordered", r.done(), i)
		}
	}
	if r.broker.committedOffset(topic, 0) != 12 {
		t.Fatalf("committed %d", r.broker.committedOffset(topic, 0))
	}
}

func TestClientFailureStopsTheConsumerAfterCommittingWhatWasSafe(t *testing.T) {
	r := newRig(t)
	r.produceN(5, "m")
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.CommitInterval = time.Hour })
	r.assign(0)
	errc := r.runAsync(c)
	eventually(t, "processing", func() bool { return r.doneCount() == 5 })
	r.client.mu.Lock()
	r.client.pollErr = errBoom
	r.client.mu.Unlock()
	if err := <-errc; !errors.Is(err, errBoom) {
		t.Fatalf("Run = %v", err)
	}
	if r.broker.committedOffset(topic, 0) != 5 {
		t.Fatalf("committed %d; the work that was safe must be committed on the way out", r.broker.committedOffset(topic, 0))
	}
}

func TestMessagesFromUnboundTopicsAreAnError(t *testing.T) {
	r := newRig(t)
	r.broker.produce("other", 0, "k", "v")
	c := r.newConsumer()
	r.client.inject(assignedEvent{Partitions: []kafka.TopicPartition{tp("other", 0)}})
	if err := <-r.runAsync(c); !errors.Is(err, kafka.ErrUnboundTopic) {
		t.Fatalf("Run = %v", err)
	}
}

func TestParkedMessagesAreMirroredToTheDeadLetterTopic(t *testing.T) {
	r := newRig(t)
	prod := &fakeProducer{}
	pub, err := kafka.NewDLQPublisher(kafka.PublisherConfig{Producer: prod})
	must(t, err)
	r.broker.produce(topic, 2, "cust-9", "bad-order")
	r.broker.produce(topic, 2, "cust-8", "fine")
	r.setFail(func(v string) error {
		if v == "bad-order" {
			return retry.Permanent(errors.New("unknown SKU"))
		}
		return nil
	})
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.Mirror = pub })
	r.client.inject(assignedEvent{Partitions: []kafka.TopicPartition{tp(topic, 2)}})
	r.run(c)
	eventually(t, "the mirror", func() bool { return len(prod.messages()) == 1 })

	m := prod.messages()[0]
	if m.Topic != "orders.dlq" || string(m.Key) != "cust-9" || string(m.Value) != "bad-order" {
		t.Fatalf("mirrored %+v", m)
	}
	if m.Headers[kafka.HeaderOriginalTopic] != "orders" || m.Headers[kafka.HeaderOriginalPartition] != "2" ||
		m.Headers[kafka.HeaderOriginalOffset] != "0" || m.Headers["trace"] != "t0" || m.Headers[kafka.HeaderRecordID] == "" {
		t.Fatalf("headers = %v", m.Headers)
	}
	if r.storeStats().Parked != 1 {
		t.Fatalf("store = %+v: the message must be parked in the store as well", r.storeStats())
	}
	if c.Stats().Parked != 1 || c.Stats().Done != 1 {
		t.Fatalf("stats = %+v", c.Stats())
	}
}

func TestAFailingMirrorDoesNotAffectConsumption(t *testing.T) {
	r := newRig(t)
	prod := &fakeProducer{err: errBoom}
	pub, _ := kafka.NewDLQPublisher(kafka.PublisherConfig{Producer: prod})
	r.broker.produce(topic, 0, "k", "bad")
	r.broker.produce(topic, 0, "k2", "fine")
	r.setFail(func(v string) error {
		if v == "bad" {
			return retry.Permanent(errors.New("nope"))
		}
		return nil
	})
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.Mirror = pub })
	r.assign(0)
	r.run(c)
	eventually(t, "offsets to reach the end", func() bool { return r.broker.committedOffset(topic, 0) == 2 })
	if c.Stats().MirrorErrors != 1 || r.storeStats().Parked != 1 {
		t.Fatalf("stats = %+v store = %+v", c.Stats(), r.storeStats())
	}
}

func TestCommitsAreBatched(t *testing.T) {
	r := newRig(t)
	r.produceN(25, "m")
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.CommitBatch = 10; cfg.CommitInterval = time.Hour })
	r.assign(0)
	stop := r.run(c)
	eventually(t, "two batch commits", func() bool { return r.broker.committedOffset(topic, 0) >= 20 })
	if got := r.broker.committedOffset(topic, 0); got%10 != 0 && got != 25 {
		t.Fatalf("committed %d; commits should fall on batch boundaries", got)
	}
	must(t, stop())
	if r.broker.committedOffset(topic, 0) != 25 {
		t.Fatalf("final commit %d, want 25", r.broker.committedOffset(topic, 0))
	}
}

// A message that outlives ProcessTimeout is stored with its progress and the
// offset moves on, so one message that hangs its dependency cannot pin the
// partition for ever. The redriver then owns it, counts its attempts and parks it
// if it keeps hanging.
func TestAMessageThatTimesOutIsStoredNotRetriedForEver(t *testing.T) {
	r := newRig(t)
	var stuck atomic.Bool
	stuck.Store(true)
	p, err := pipeline.New("orders", "v1", []pipeline.Step{{Name: "slow", Run: func(ctx context.Context, x *pipeline.Exec) error {
		if stuck.Load() {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}}})
	must(t, err)
	r.broker.produce(topic, 0, "k", "v")
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.Bindings[0].Pipeline = p; cfg.ProcessTimeout = 30 * time.Millisecond })
	r.assign(0)
	r.run(c)
	eventually(t, "the timed-out message to be stored and committed", func() bool {
		return r.broker.committedOffset(topic, 0) == 1 && r.storeStats().Total() == 1
	})
	if c.Stats().Deferred != 1 || c.Stats().Retried != 0 {
		t.Fatalf("stats %+v: the timeout must defer the message, not seek back to it", c.Stats())
	}
	stuck.Store(false)
	_, stop := r.startRedriver(p)
	defer stop()
	eventually(t, "the redriver to finish it", func() bool { return r.storeStats().Total() == 0 })
}

func TestConfigurationIsValidated(t *testing.T) {
	r := newRig(t)
	good := r.config()
	if _, err := kafka.NewConsumer(good); err != nil {
		t.Fatal(err)
	}
	p := good.Bindings[0].Pipeline
	for name, mod := range map[string]func(*kafka.Config){
		"no client":   func(c *kafka.Config) { c.Client = nil },
		"no store":    func(c *kafka.Config) { c.Store = nil },
		"no bindings": func(c *kafka.Config) { c.Bindings = nil },
		"empty topic": func(c *kafka.Config) { c.Bindings = []kafka.Binding{{Pipeline: p}} },
		"no pipeline": func(c *kafka.Config) { c.Bindings = []kafka.Binding{{Topic: "t"}} },
		"duplicate topic": func(c *kafka.Config) {
			c.Bindings = []kafka.Binding{{Topic: "t", Pipeline: p}, {Topic: "t", Pipeline: p}}
		},
	} {
		cfg := r.config()
		mod(&cfg)
		if _, err := kafka.NewConsumer(cfg); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	cfg := r.config(func(c *kafka.Config) {
		c.Bindings = []kafka.Binding{{Topic: "a", Pipeline: p}, {Topic: "b", Pipeline: p}}
	})
	c, _ := kafka.NewConsumer(cfg)
	if got := strings.Join(c.Topics(), ","); got != "a,b" {
		t.Fatalf("Topics = %s", got)
	}
}

func TestSeekFailureIsFatalBecauseTheMessageCannotBeProtected(t *testing.T) {
	r := newRig(t)
	r.store = dlq.NewMemoryStore(dlq.MemoryOptions{MaxRecords: 1})
	must(t, r.store.Append(context.Background(), dlq.Record{ID: "occupant"}))
	r.setFail(func(string) error { return errors.New("down") })
	r.broker.produce(topic, 0, "k", "v")
	r.client.seekErr = errBoom
	c := r.newConsumer()
	r.assign(0)
	stop := r.run(c)
	eventually(t, "the failure to surface", func() bool { return c.Stats().Received == 1 })
	if err := stop(); !errors.Is(err, errBoom) {
		t.Fatalf("Run = %v; if the consumer cannot go back to a message it must stop, not continue past it", err)
	}
}

// A commit that fails because a rebalance is in progress must be retried while the
// consumer keeps polling: only polling lets the rebalance finish.
func TestShutdownCommitRetriesWhileStillPolling(t *testing.T) {
	r := newRig(t)
	r.produceN(5, "m")
	var refusals atomic.Int32
	r.client.commitErr = func() error {
		if refusals.Add(1) <= 3 {
			return errors.New("rebalance in progress")
		}
		return nil
	}
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.CommitInterval = time.Hour; cfg.ShutdownTimeout = 5 * time.Second })
	r.assign(0)
	stop := r.run(c)
	eventually(t, "processing", func() bool { return r.doneCount() == 5 })
	pollsBefore := r.client.pollCount()
	if err := stop(); err != nil {
		t.Fatalf("Run = %v; a transient commit failure at shutdown should be retried", err)
	}
	if r.broker.committedOffset(topic, 0) != 5 {
		t.Fatalf("committed %d, want 5", r.broker.committedOffset(topic, 0))
	}
	if r.client.pollCount() <= pollsBefore {
		t.Fatal("the consumer stopped polling while retrying its final commit")
	}
	if c.Stats().CommitErrors != 3 {
		t.Fatalf("CommitErrors = %d, want the 3 refusals", c.Stats().CommitErrors)
	}
}

// A breaker with no health check reopens only when a request arrives. The consumer pauses the
// topic while the circuit is open, so once the sleep window has passed it must resume and let
// a message through as the canary; otherwise the topic stays paused for ever.
func TestTopicResumesThroughATimerOnlyBreaker(t *testing.T) {
	var healthy atomic.Bool
	r := newRig(t)
	r.br = breaker.New(breaker.Config{Name: "payments", FailureThreshold: 0.1, MinSamples: 1, SleepWindow: 100 * time.Millisecond})
	defer r.br.Close()
	r.produceN(3, "m")
	r.setFail(func(string) error {
		if healthy.Load() {
			return nil
		}
		return errors.New("payments 503")
	})
	c := r.newConsumer()
	r.assign(0)
	stop := r.run(c)

	eventually(t, "the circuit to open and the topic to pause", func() bool { return c.Stats().Paused > 0 })
	healthy.Store(true) // the dependency recovers; nothing tells the breaker
	eventually(t, "every message to be processed", func() bool { return r.broker.committedOffset(topic, 0) == 3 })
	must(t, stop())
}
