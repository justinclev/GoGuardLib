package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/pipeline"
)

// gatedStore fails Append with ErrFull while its gate is closed, which is how a
// message ends up neither processed nor stored (the consumer's Failed outcome).
type gatedStore struct {
	dlq.Store
	closed atomic.Bool
}

func (g *gatedStore) Append(ctx context.Context, r dlq.Record) error {
	if g.closed.Load() {
		return dlq.ErrFull
	}
	return g.Store.Append(ctx, r)
}

// step is what a test's pipeline does with one message.
type step func(ctx context.Context, key, value string) error

func stepPipeline(t *testing.T, fn step) *pipeline.Pipeline {
	t.Helper()
	p, err := pipeline.New("orders", "v1", []pipeline.Step{{Name: "handle", Run: func(ctx context.Context, x *pipeline.Exec) error {
		return fn(ctx, string(x.Key), string(x.Value))
	}}})
	must(t, err)
	return p
}

// concRig is a rig whose consumer uses a worker pool and a test-defined step.
func concRig(t *testing.T, workers int, fn step, mod ...func(*kafka.Config)) (*rig, *kafka.Consumer, *gatedStore) {
	r := newRig(t)
	gs := &gatedStore{Store: dlq.NewMemoryStore(dlq.MemoryOptions{})}
	r.store = gs
	p := stepPipeline(t, fn)
	c := r.newConsumer(append([]func(*kafka.Config){func(cfg *kafka.Config) {
		cfg.Bindings[0].Pipeline = p
		cfg.Workers = workers
	}}, mod...)...)
	return r, c, gs
}

func (r *rig) produceKeyed(partition int32, key string, n int) {
	for i := 0; i < n; i++ {
		r.broker.produce(topic, partition, key, fmt.Sprintf("%s-%d", key, i))
	}
}

func TestWorkersRunMessagesConcurrently(t *testing.T) {
	var cur, peak atomic.Int32
	var mu sync.Mutex
	ran := map[string]int{}
	r, c, _ := concRig(t, 4, func(ctx context.Context, key, value string) error {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		cur.Add(-1)
		mu.Lock()
		ran[value]++
		mu.Unlock()
		return nil
	})
	for p := int32(0); p < 2; p++ {
		for i := 0; i < 20; i++ {
			r.broker.produce(topic, p, fmt.Sprintf("k%d-%d", p, i), fmt.Sprintf("v%d-%d", p, i))
		}
	}
	r.assign(0, 1)
	start := time.Now()
	r.run(c)
	eventually(t, "everything committed", func() bool {
		return r.broker.committedOffset(topic, 0) == 20 && r.broker.committedOffset(topic, 1) == 20
	})
	elapsed := time.Since(start)
	if peak.Load() < 3 {
		t.Fatalf("peak concurrency %d with 4 workers", peak.Load())
	}
	if elapsed > 600*time.Millisecond { // 40 messages of 20ms is 800ms one at a time
		t.Fatalf("took %v: the pool did not speed things up", elapsed)
	}
	for v, n := range ran {
		if n != 1 {
			t.Errorf("%s ran %d times", v, n)
		}
	}
	if len(ran) != 40 || c.Stats().Done != 40 {
		t.Fatalf("ran %d, stats %+v", len(ran), c.Stats())
	}
}

func TestSameKeyMessagesRunInOrderAcrossWorkers(t *testing.T) {
	var mu sync.Mutex
	order := map[string][]int{}
	rng := rand.New(rand.NewSource(1))
	var rmu sync.Mutex
	r, c, _ := concRig(t, 4, func(ctx context.Context, key, value string) error {
		rmu.Lock()
		d := time.Duration(rng.Intn(6)) * time.Millisecond
		rmu.Unlock()
		time.Sleep(d)
		var idx int
		_, _ = fmt.Sscanf(value[strings.LastIndex(value, "-")+1:], "%d", &idx)
		mu.Lock()
		order[key] = append(order[key], idx)
		mu.Unlock()
		return nil
	})
	keys := []string{"a", "b", "c", "d", "e"}
	for i := 0; i < 12; i++ { // interleave keys in the log
		for _, k := range keys {
			r.broker.produce(topic, 0, k, fmt.Sprintf("%s-%d", k, i))
		}
	}
	r.assign(0)
	r.run(c)
	eventually(t, "all committed", func() bool { return r.broker.committedOffset(topic, 0) == 60 })
	for _, k := range keys {
		got := order[k]
		if len(got) != 12 {
			t.Fatalf("key %s ran %d messages", k, len(got))
		}
		for i, v := range got {
			if v != i {
				t.Fatalf("key %s ran out of order: %v", k, got)
			}
		}
	}
}

// The commit point must not pass a message that is still running, however many
// later ones have finished.
func TestCommitsStayContiguousWhileASlowMessageRuns(t *testing.T) {
	release := make(chan struct{})
	var finished atomic.Int32
	r, c, _ := concRig(t, 4, func(ctx context.Context, key, value string) error {
		if value == "slow" {
			<-release
		}
		finished.Add(1)
		return nil
	})
	r.broker.produce(topic, 0, "k-slow", "slow")
	for i := 1; i < 30; i++ {
		r.broker.produce(topic, 0, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	r.assign(0)
	r.run(c)
	eventually(t, "the fast messages to finish", func() bool { return finished.Load() == 29 })
	time.Sleep(60 * time.Millisecond) // several commit intervals
	if got := r.broker.committedOffset(topic, 0); got != 0 {
		t.Fatalf("committed %d while message 0 was still running", got)
	}
	for _, call := range r.client.callLog() {
		if strings.HasPrefix(call, "commit ") && !strings.HasSuffix(call, " 0") {
			t.Fatalf("a commit went past the running message: %s", call)
		}
	}
	close(release)
	eventually(t, "the commit point to jump over everything", func() bool { return r.broker.committedOffset(topic, 0) == 30 })
}

// A message that fails is retried; the ones after it that already finished are not
// run a second time when the partition is read again from the failed one.
func TestAFailedMessageIsRetriedWithoutRerunningFinishedOnes(t *testing.T) {
	var mu sync.Mutex
	runs := map[string]int{}
	var gs *gatedStore
	r, c, g := concRig(t, 4, func(ctx context.Context, key, value string) error {
		mu.Lock()
		runs[value]++
		mu.Unlock()
		if value == "v3" {
			return errors.New("down") // and while the store is full it cannot even be set aside
		}
		return nil
	})
	gs = g
	gs.closed.Store(true)
	for i := 0; i < 10; i++ {
		r.broker.produce(topic, 0, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	r.assign(0)
	r.run(c)

	eventually(t, "the failure to be noticed", func() bool { return c.Stats().Retried >= 1 })
	eventually(t, "the others to finish", func() bool { return c.Stats().Done == 9 })
	time.Sleep(80 * time.Millisecond) // several retries, each re-reading from the failed message
	if got := r.broker.committedOffset(topic, 0); got > 3 {
		t.Fatalf("committed %d past the message that could not be protected (offset 3)", got)
	}
	gs.closed.Store(false) // room again: the message can be set aside
	eventually(t, "the partition to be committed to the end", func() bool { return r.broker.committedOffset(topic, 0) == 10 })

	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < 10; i++ {
		v := fmt.Sprintf("v%d", i)
		if v != "v3" && runs[v] != 1 {
			t.Errorf("%s ran %d times: a message that finished must not run again when its neighbour is retried", v, runs[v])
		}
	}
	if st := c.Stats(); st.Done != 9 || st.Deferred != 1 {
		t.Fatalf("stats %+v, want 9 done and the failed one set aside", st)
	}
}

// Nothing may run ahead of an earlier message with the same key, not even when that
// message failed after the later ones had already been handed to the pool.
func TestALaterMessageWithTheSameKeyNeverOvertakesAFailedOne(t *testing.T) {
	var mu sync.Mutex
	var ran []string
	var gs *gatedStore
	r, c, g := concRig(t, 4, func(ctx context.Context, key, value string) error {
		if value == "a-0" {
			time.Sleep(20 * time.Millisecond) // let a-1 and a-2 queue up behind it
			return errors.New("down")
		}
		mu.Lock()
		ran = append(ran, value)
		mu.Unlock()
		return nil
	})
	gs = g
	gs.closed.Store(true)
	r.produceKeyed(0, "a", 3)
	r.produceKeyed(0, "b", 3)
	r.assign(0)
	r.run(c)

	eventually(t, "the b messages to finish", func() bool { return c.Stats().Done == 3 })
	time.Sleep(80 * time.Millisecond)
	mu.Lock()
	for _, v := range ran {
		if strings.HasPrefix(v, "a-") {
			t.Fatalf("%s ran while a-0 was failing: it overtook an earlier message with its key (ran %v)", v, ran)
		}
	}
	mu.Unlock()
	if n := r.storeStats().Total(); n != 0 {
		t.Fatalf("%d records stored while the store was refusing them", n)
	}

	gs.closed.Store(false)
	eventually(t, "the partition to finish", func() bool { return r.broker.committedOffset(topic, 0) == 6 })
	// a-0 was set aside; a-1 and a-2 were held behind it, in order.
	var ids []string
	for i := 0; i < 3; i++ {
		rec, err := r.store.Get(context.Background(), dlq.KafkaID(topic, 0, int64(i)))
		must(t, err)
		ids = append(ids, string(rec.Value))
	}
	if strings.Join(ids, ",") != "a-0,a-1,a-2" {
		t.Fatalf("stored %v, want a-0, a-1, a-2 in order", ids)
	}
}

func TestRevokingAPartitionWithWorkInFlightLosesNothing(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	release := make(chan struct{})
	var once sync.Once
	r, c, _ := concRig(t, 3, func(ctx context.Context, key, value string) error {
		if value == "v2" {
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		mu.Lock()
		seen[value]++
		mu.Unlock()
		return nil
	})
	for i := 0; i < 8; i++ {
		r.broker.produce(topic, 0, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	r.assign(0)
	r.run(c)
	eventually(t, "the others to finish", func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) == 7 })

	r.client.inject(revokedEvent{Partitions: []kafka.TopicPartition{tp(topic, 0)}})
	eventually(t, "the revocation", func() bool { return c.Stats().Rebalances >= 2 })
	if got := r.broker.committedOffset(topic, 0); got > 2 {
		t.Fatalf("committed %d for a partition given up with message 2 unfinished", got)
	}
	once.Do(func() { close(release) })

	r.assign(0) // it comes back, and is read again from the committed offset
	eventually(t, "everything committed", func() bool { return r.broker.committedOffset(topic, 0) == 8 })
	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < 8; i++ {
		if seen[fmt.Sprintf("v%d", i)] == 0 {
			t.Fatalf("v%d was never processed: %v", i, seen)
		}
	}
}

func TestShuttingDownWithMessagesInFlightKeepsOnlyWhatFinished(t *testing.T) {
	before := runtime.NumGoroutine()
	release := make(chan struct{})
	var started atomic.Int32
	r, c, _ := concRig(t, 4, func(ctx context.Context, key, value string) error {
		if value == "hold" {
			started.Add(1)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	r.broker.produce(topic, 0, "k0", "v0")
	r.broker.produce(topic, 0, "k1", "hold")
	for i := 2; i < 6; i++ {
		r.broker.produce(topic, 0, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	r.assign(0)
	stop := r.run(c)
	eventually(t, "the held message to start", func() bool { return started.Load() == 1 })
	eventually(t, "the others to finish", func() bool { return c.Stats().Done == 5 })
	if err := stop(); err != nil {
		t.Fatalf("Run = %v", err)
	}
	// Offsets 0 is contiguous and safe; 1 was interrupted, so nothing past it may be committed.
	if got := r.broker.committedOffset(topic, 0); got != 1 {
		t.Fatalf("committed %d, want 1 (the interrupted message must be redelivered)", got)
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before+2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before+2 {
		t.Fatalf("%d goroutines after Run returned, %d before: workers leaked", n, before)
	}
}

func TestMaxInFlightThrottlesFetching(t *testing.T) {
	release := make(chan struct{})
	r, c, _ := concRig(t, 2, func(ctx context.Context, key, value string) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}, func(cfg *kafka.Config) { cfg.MaxInFlight = 4 })
	for i := 0; i < 200; i++ {
		r.broker.produce(topic, 0, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	r.assign(0)
	r.run(c)
	eventually(t, "the pool to fill", func() bool { return c.Stats().InFlight == 4 })
	time.Sleep(60 * time.Millisecond)
	r.client.mu.Lock()
	delivered := r.client.delivered
	r.client.mu.Unlock()
	if delivered > 4+3 {
		t.Fatalf("the client delivered %d messages with the pool full: fetching must pause", delivered)
	}
	if !r.client.isPaused(tp(topic, 0)) {
		t.Fatal("the partition was not paused while the pool was full")
	}
	close(release)
	eventually(t, "everything committed", func() bool { return r.broker.committedOffset(topic, 0) == 200 })
	if c.Stats().InFlight != 0 {
		t.Fatalf("in flight %d at the end", c.Stats().InFlight)
	}
}

func TestWorkerSettingsAreValidated(t *testing.T) {
	r := newRig(t)
	if _, err := kafka.NewConsumer(r.config(func(c *kafka.Config) { c.Workers = -1 })); err == nil {
		t.Fatal("negative Workers accepted")
	}
	if _, err := kafka.NewConsumer(r.config(func(c *kafka.Config) { c.MaxInFlight = -1 })); err == nil {
		t.Fatal("negative MaxInFlight accepted")
	}
}

// Randomised: failures and delays everywhere, several partitions and keys. Whatever
// happens, every message ends up done or stored, the commit point reaches the end,
// and for each key the messages that ran are a prefix of its messages (the rest
// were held behind them, in order).
func TestRandomFailuresNeverLoseOrReorder(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			var rmu sync.Mutex
			roll := func(n int) int { rmu.Lock(); defer rmu.Unlock(); return rng.Intn(n) }
			var mu sync.Mutex
			ranBy := map[string][]int{}
			var gs *gatedStore
			r, c, g := concRig(t, 4, func(ctx context.Context, key, value string) error {
				time.Sleep(time.Duration(roll(4)) * time.Millisecond)
				if roll(6) == 0 {
					return errors.New("transient") // deferred to the store, or failed if the store is refusing
				}
				var idx int
				_, _ = fmt.Sscanf(value[strings.LastIndex(value, "-")+1:], "%d", &idx)
				mu.Lock()
				ranBy[key] = append(ranBy[key], idx)
				mu.Unlock()
				return nil
			})
			gs = g
			keys := []string{"a", "b", "c", "d"}
			const per = 15
			for i := 0; i < per; i++ {
				for ki, k := range keys {
					r.broker.produce(topic, int32(ki%2), k, fmt.Sprintf("%s-%d", k, i)) // a key stays in one partition
				}
			}
			r.assign(0, 1)
			r.run(c)
			stopFlap := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() { // the store keeps refusing and accepting
				defer wg.Done()
				for {
					select {
					case <-stopFlap:
						gs.closed.Store(false)
						return
					default:
					}
					gs.closed.Store(roll(3) == 0)
					time.Sleep(time.Duration(2+roll(8)) * time.Millisecond)
				}
			}()
			total := int64(len(keys) * per)
			eventually(t, "both partitions committed to the end", func() bool {
				return r.broker.committedOffset(topic, 0)+r.broker.committedOffset(topic, 1) == total
			})
			close(stopFlap)
			wg.Wait()

			stored := map[string]bool{}
			ls, err := r.store.Parked(context.Background(), dlq.ParkedQuery{Limit: 1000})
			must(t, err)
			_ = ls
			for p := int32(0); p < 2; p++ {
				n := int64(0)
				r.broker.mu.Lock()
				n = int64(len(r.broker.logs[tp(topic, p)]))
				r.broker.mu.Unlock()
				for off := int64(0); off < n; off++ {
					if rec, err := r.store.Get(context.Background(), dlq.KafkaID(topic, p, off)); err == nil {
						stored[string(rec.Value)] = true
					}
				}
			}
			mu.Lock()
			defer mu.Unlock()
			for _, k := range keys {
				ran := map[int]bool{}
				for _, idx := range ranBy[k] {
					ran[idx] = true
				}
				heldFrom := -1
				for i := 0; i < per; i++ {
					v := fmt.Sprintf("%s-%d", k, i)
					switch {
					case stored[v]:
						if heldFrom < 0 {
							heldFrom = i
						}
					case ran[i]:
						if heldFrom >= 0 {
							t.Fatalf("key %s: %s ran after %s-%d was set aside: it overtook an earlier message", k, v, k, heldFrom)
						}
					default:
						t.Fatalf("%s neither ran nor was stored: a message was lost", v)
					}
				}
			}
		})
	}
}
