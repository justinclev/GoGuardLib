//go:build integration

// These tests need a real Kafka broker. Point KAFKA_BROKERS at one (default
// localhost:9092) and run:
//
//	go test -tags integration -count=1 ./confluent/
package confluent_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/health"
	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/kafka/confluent"
	"github.com/justinclev/GoGuardLib/pipeline"
	"github.com/justinclev/GoGuardLib/retry"
)

func brokers() string {
	if b := os.Getenv("KAFKA_BROKERS"); b != "" {
		return b
	}
	return "localhost:9092"
}

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func eventually(t testing.TB, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", within, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func unique(prefix string) string { return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()) }

func createTopic(t *testing.T, name string, partitions int) {
	t.Helper()
	admin, err := ck.NewAdminClient(&ck.ConfigMap{"bootstrap.servers": brokers()})
	must(t, err)
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	res, err := admin.CreateTopics(ctx, []ck.TopicSpecification{{Topic: name, NumPartitions: partitions, ReplicationFactor: 1}})
	must(t, err)
	for _, r := range res {
		if r.Error.Code() != ck.ErrNoError && r.Error.Code() != ck.ErrTopicAlreadyExists {
			t.Fatalf("creating %s: %v", name, r.Error)
		}
	}
}

func newProducer(t *testing.T) *confluent.Producer {
	t.Helper()
	p, err := confluent.NewProducer(ck.ConfigMap{"bootstrap.servers": brokers()})
	must(t, err)
	t.Cleanup(p.Close)
	return p
}

func produce(t *testing.T, p *confluent.Producer, topic string, n int, keyFor func(i int) string, valueFor func(i int) string) {
	t.Helper()
	for i := 0; i < n; i++ {
		must(t, p.Produce(context.Background(), topic, []byte(keyFor(i)), []byte(valueFor(i)), nil))
	}
}

func clientFor(t *testing.T, group, topic string, extra ck.ConfigMap) *confluent.Client {
	t.Helper()
	cfg := ck.ConfigMap{
		"bootstrap.servers":    brokers(),
		"group.id":             group,
		"auto.offset.reset":    "earliest",
		"session.timeout.ms":   6000,
		"max.poll.interval.ms": 6000,
	}
	for k, v := range extra {
		cfg[k] = v
	}
	c, err := confluent.NewClient(cfg, []string{topic})
	must(t, err)
	return c
}

// committedTotal and endTotal read the group's progress independently of the
// consumer under test.
func committedTotal(t *testing.T, group, topic string, partitions int) int64 {
	t.Helper()
	c, err := ck.NewConsumer(&ck.ConfigMap{"bootstrap.servers": brokers(), "group.id": group})
	must(t, err)
	defer c.Close()
	var tps []ck.TopicPartition
	for p := 0; p < partitions; p++ {
		tt := topic
		tps = append(tps, ck.TopicPartition{Topic: &tt, Partition: int32(p)})
	}
	got, err := c.Committed(tps, 10000)
	must(t, err)
	var sum int64
	for _, tp := range got {
		if tp.Offset >= 0 {
			sum += int64(tp.Offset)
		}
	}
	return sum
}

func endTotal(t *testing.T, topic string, partitions int) int64 {
	t.Helper()
	c, err := ck.NewConsumer(&ck.ConfigMap{"bootstrap.servers": brokers(), "group.id": unique("watermarks")})
	must(t, err)
	defer c.Close()
	var sum int64
	for p := 0; p < partitions; p++ {
		_, hi, err := c.QueryWatermarkOffsets(topic, int32(p), 10000)
		must(t, err)
		sum += hi
	}
	return sum
}

// tracker records what the pipeline completed.
type tracker struct {
	mu     sync.Mutex
	seen   map[string]int
	seq    []string
	byPart map[int32]int
}

func newTracker() *tracker { return &tracker{seen: map[string]int{}, byPart: map[int32]int{}} }

func (tr *tracker) add(partition int32, v string) {
	tr.mu.Lock()
	tr.seen[v]++
	tr.seq = append(tr.seq, v)
	tr.byPart[partition]++
	tr.mu.Unlock()
}

// partitionsWithAtLeast counts partitions that have had at least n messages processed.
func (tr *tracker) partitionsWithAtLeast(n int) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	c := 0
	for _, k := range tr.byPart {
		if k >= n {
			c++
		}
	}
	return c
}

func (tr *tracker) distinct() int { tr.mu.Lock(); defer tr.mu.Unlock(); return len(tr.seen) }

func (tr *tracker) has(v string) bool { tr.mu.Lock(); defer tr.mu.Unlock(); return tr.seen[v] > 0 }

func (tr *tracker) order() []string {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]string(nil), tr.seq...)
}

func buildPipeline(t *testing.T, tr *tracker, br *breaker.Breaker, fail func(string) error) *pipeline.Pipeline {
	t.Helper()
	p, err := pipeline.New("orders", "v1", []pipeline.Step{{Name: "handle", Breaker: br, Run: func(ctx context.Context, x *pipeline.Exec) error {
		if fail != nil {
			if err := fail(string(x.Value)); err != nil {
				return err
			}
		}
		tr.add(x.Source.Partition, string(x.Value))
		return nil
	}}})
	must(t, err)
	return p
}

type running struct {
	c      *kafka.Consumer
	client *confluent.Client
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
	err    error
}

func (r *running) stop(t testing.TB) error {
	r.once.Do(func() {
		r.cancel()
		select {
		case r.err = <-r.done:
		case <-time.After(20 * time.Second):
			t.Error("consumer did not stop")
		}
		_ = r.client.Close()
	})
	return r.err
}

func start(t *testing.T, client *confluent.Client, store dlq.Store, topic string, p *pipeline.Pipeline, mod ...func(*kafka.Config)) *running {
	t.Helper()
	cfg := kafka.Config{
		Client: client, Store: store, Bindings: []kafka.Binding{{Topic: topic, Pipeline: p}},
		PollTimeout: 50 * time.Millisecond, CommitInterval: 200 * time.Millisecond,
		RetryBackoff: retry.Constant(100 * time.Millisecond),
	}
	for _, m := range mod {
		m(&cfg)
	}
	c, err := kafka.NewConsumer(cfg)
	must(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{c: c, client: client, cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- c.Run(ctx) }()
	t.Cleanup(func() { _ = r.stop(t) })
	return r
}

func TestRealBrokerProcessesEverythingAndCommits(t *testing.T) {
	topic, group := unique("it-basic"), unique("g")
	createTopic(t, topic, 3)
	prod := newProducer(t)
	produce(t, prod, topic, 60, func(i int) string { return fmt.Sprintf("k%d", i) }, func(i int) string { return fmt.Sprintf("m%d", i) })

	tr := newTracker()
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	r := start(t, clientFor(t, group, topic, nil), store, topic, buildPipeline(t, tr, nil, nil))
	eventually(t, 60*time.Second, "all 60 messages", func() bool { return tr.distinct() == 60 })
	must(t, r.stop(t))

	if got := committedTotal(t, group, topic, 3); got != 60 {
		t.Fatalf("the group committed %d offsets in total, want 60", got)
	}
	if st, _ := store.Stats(context.Background()); st.Total() != 0 {
		t.Fatalf("store = %+v", st)
	}
}

// The heart of it, against a real broker: while a circuit is open the consumer
// pauses its topic and leaves the messages in Kafka, and it stays a live member of
// the group for longer than the session and poll timeouts. When the dependency
// recovers everything is processed, with no rebalance and nothing copied into the
// dead-letter store.
func TestRealBrokerPausesThroughAnOutageAndStaysInTheGroup(t *testing.T) {
	topic, group := unique("it-pause"), unique("g")
	createTopic(t, topic, 2)
	var healthy atomic.Bool
	br := breaker.New(breaker.Config{
		Name: "payments", FailureThreshold: 0.1, MinSamples: 1, SleepWindow: time.Hour,
		Health: &health.Config{Check: func(context.Context) error {
			if healthy.Load() {
				return nil
			}
			return errors.New("down")
		}, Interval: 100 * time.Millisecond, Jitter: -1, SuccessThreshold: 2},
	})
	defer br.Close()
	_ = br.Do(context.Background(), func(context.Context) error { return errors.New("outage") })
	if br.State() != breaker.StateOpen {
		t.Fatal("setup: circuit should be open")
	}

	prod := newProducer(t)
	produce(t, prod, topic, 40, func(i int) string { return fmt.Sprintf("k%d", i) }, func(i int) string { return fmt.Sprintf("m%d", i) })

	tr := newTracker()
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	r := start(t, clientFor(t, group, topic, nil), store, topic, buildPipeline(t, tr, br, nil))
	eventually(t, 30*time.Second, "the topic to be paused", func() bool { return r.c.Stats().Paused == 2 })

	// Longer than session.timeout.ms and max.poll.interval.ms (both 6s): a consumer
	// that stopped polling would be expelled and the group would rebalance.
	time.Sleep(9 * time.Second)
	st := r.c.Stats()
	if tr.distinct() != 0 || st.Received != 0 || st.Deferred != 0 {
		t.Fatalf("stats = %+v processed=%d: messages must stay in Kafka during an outage", st, tr.distinct())
	}
	if s, _ := store.Stats(context.Background()); s.Total() != 0 {
		t.Fatalf("the store holds %+v; nothing should have been copied out of Kafka", s)
	}
	if got := committedTotal(t, group, topic, 2); got != 0 {
		t.Fatalf("committed %d while nothing was processed", got)
	}
	if st.Rebalances != 1 {
		t.Fatalf("Rebalances = %d: the paused consumer was expelled from the group", st.Rebalances)
	}
	if end := endTotal(t, topic, 2); end != 40 {
		t.Fatalf("topic holds %d messages", end)
	}

	healthy.Store(true)
	eventually(t, 60*time.Second, "the backlog to be processed after recovery", func() bool { return tr.distinct() == 40 })
	must(t, r.stop(t))
	if got := committedTotal(t, group, topic, 2); got != 40 {
		t.Fatalf("committed %d, want 40", got)
	}
	if r.c.Stats().Deferred != 0 {
		t.Fatalf("stats = %+v", r.c.Stats())
	}
}

// Failures are stored (durably) and redriven; permanent failures are parked and
// mirrored to a dead-letter topic; same-key messages keep their order.
func TestRealBrokerDefersRedrivesAndMirrorsParkedMessages(t *testing.T) {
	topic, group := unique("it-dlq"), unique("g")
	createTopic(t, topic, 1)
	createTopic(t, topic+".dlq", 1)
	prod := newProducer(t)

	// cust-1: "first" fails transiently, so "second" must wait behind it.
	msgs := [][2]string{{"cust-1", "first"}, {"cust-1", "second"}, {"cust-2", "fine"}, {"cust-3", "poison"}, {"cust-4", "also-fine"}}
	produce(t, prod, topic, len(msgs), func(i int) string { return msgs[i][0] }, func(i int) string { return msgs[i][1] })

	var heal atomic.Bool
	fail := func(v string) error {
		switch {
		case v == "first" && !heal.Load():
			return errors.New("payments 503")
		case v == "poison":
			return retry.Permanent(errors.New("schema not supported"))
		}
		return nil
	}
	tr := newTracker()
	p := buildPipeline(t, tr, nil, fail)

	store, err := dlq.OpenWAL(t.TempDir(), dlq.WALOptions{})
	must(t, err)
	defer store.Close()
	pub, err := kafka.NewDLQPublisher(kafka.PublisherConfig{Producer: newProducer(t)})
	must(t, err)

	r := start(t, clientFor(t, group, topic, nil), store, topic, p, func(c *kafka.Config) { c.Mirror = pub })
	eventually(t, 60*time.Second, "the topic to be consumed", func() bool { return committedTotal(t, group, topic, 1) == int64(len(msgs)) })

	if st := r.c.Stats(); st.Deferred != 2 || st.OrderHeld != 1 || st.Parked != 1 || st.Done != 2 {
		t.Fatalf("stats = %+v", st)
	}
	if tr.has("second") || tr.has("first") {
		t.Fatal("cust-1's messages must wait: 'first' failed and 'second' may not overtake it")
	}
	if s, _ := store.Stats(context.Background()); s.Pending != 2 || s.Parked != 1 {
		t.Fatalf("store = %+v", s)
	}

	// The parked message reaches the dead-letter topic with its metadata.
	dc, err := ck.NewConsumer(&ck.ConfigMap{"bootstrap.servers": brokers(), "group.id": unique("dlq-reader"), "auto.offset.reset": "earliest"})
	must(t, err)
	defer dc.Close()
	must(t, dc.Subscribe(topic+".dlq", nil))
	var dm *ck.Message
	eventually(t, 30*time.Second, "the dead-letter message", func() bool {
		if m, ok := dc.Poll(200).(*ck.Message); ok {
			dm = m
		}
		return dm != nil
	})
	hdr := map[string]string{}
	for _, h := range dm.Headers {
		hdr[h.Key] = string(h.Value)
	}
	if string(dm.Key) != "cust-3" || string(dm.Value) != "poison" || hdr[kafka.HeaderOriginalTopic] != topic ||
		hdr[kafka.HeaderOriginalOffset] != "3" || hdr[kafka.HeaderRecordID] == "" {
		t.Fatalf("dead-letter message %s=%s headers %v", dm.Key, dm.Value, hdr)
	}

	// The dependency recovers: a redriver finishes both, in key order.
	heal.Store(true)
	rd, err := dlq.NewRedriver(dlq.RedriveConfig{Store: store, Handler: p.Handler(), PollInterval: 20 * time.Millisecond, Backoff: retry.Constant(20 * time.Millisecond)})
	must(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = rd.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	eventually(t, 30*time.Second, "the redrive", func() bool { return tr.has("second") })
	order := tr.order()
	pos := func(v string) int {
		for i, x := range order {
			if x == v {
				return i
			}
		}
		return -1
	}
	if pos("first") < 0 || pos("first") > pos("second") {
		t.Fatalf("completion order %v: 'first' must precede 'second'", order)
	}
	must(t, r.stop(t))
}

// Stopping and starting a consumer resumes exactly where the group left off.
func TestRealBrokerRestartResumesFromCommittedOffsets(t *testing.T) {
	topic, group := unique("it-restart"), unique("g")
	createTopic(t, topic, 2)
	prod := newProducer(t)
	const total = 120
	produce(t, prod, topic, total, func(i int) string { return fmt.Sprintf("k%d", i) }, func(i int) string { return fmt.Sprintf("m%03d", i) })

	tr := newTracker()
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	slow := func(string) error { time.Sleep(15 * time.Millisecond); return nil }
	p := buildPipeline(t, tr, nil, slow)

	r1 := start(t, clientFor(t, group, topic, nil), store, topic, p)
	eventually(t, 60*time.Second, "some progress", func() bool { return tr.distinct() >= 30 })
	must(t, r1.stop(t))
	firstRun := tr.distinct()
	committed := committedTotal(t, group, topic, 2)
	if committed == 0 || committed > int64(firstRun)+1 {
		t.Fatalf("committed %d after processing %d; it must be close and never ahead", committed, firstRun)
	}

	r2 := start(t, clientFor(t, group, topic, nil), store, topic, buildPipeline(t, tr, nil, nil))
	eventually(t, 60*time.Second, "the rest", func() bool { return tr.distinct() == total })
	must(t, r2.stop(t))

	if got := committedTotal(t, group, topic, 2); got != total {
		t.Fatalf("committed %d, want %d", got, total)
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	redelivered := 0
	for _, n := range tr.seen {
		if n > 1 {
			redelivered++
		}
	}
	if redelivered > 5 { // at most what was in flight or uncommitted at the stop
		t.Fatalf("%d messages were processed twice; a clean restart should redeliver almost nothing", redelivered)
	}
}

// A second consumer joining the group triggers a real rebalance. The first must
// commit what it finished for the partitions it gives up, and together they must
// process every message.
func TestRealBrokerRebalanceLosesNothing(t *testing.T) {
	topic, group := unique("it-rebalance"), unique("g")
	createTopic(t, topic, 4)
	prod := newProducer(t)
	const total = 240
	produce(t, prod, topic, total, func(i int) string { return fmt.Sprintf("k%d", i) }, func(i int) string { return fmt.Sprintf("m%03d", i) })

	tr := newTracker()
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	slow := func(string) error { time.Sleep(30 * time.Millisecond); return nil }
	// Small fetches make the broker interleave the partitions, so A is partway
	// through every partition (and so has real progress to hand over) when B joins.
	coop := ck.ConfigMap{"partition.assignment.strategy": "cooperative-sticky", "max.partition.fetch.bytes": 256}

	// A's periodic commit is slow on purpose: apart from the commit it makes when a
	// partition is revoked, almost nothing it has done would be committed by the
	// time B joins.
	a := start(t, clientFor(t, group, topic, coop), store, topic, buildPipeline(t, tr, nil, slow),
		func(c *kafka.Config) { c.CommitInterval = time.Minute; c.CommitBatch = 100000 })
	eventually(t, 60*time.Second, "A to make progress on every partition", func() bool { return tr.partitionsWithAtLeast(3) == 4 })

	b := start(t, clientFor(t, group, topic, coop), store, topic, buildPipeline(t, tr, nil, slow))
	eventually(t, 60*time.Second, "B to receive partitions", func() bool { return b.c.Stats().Rebalances >= 1 })
	eventually(t, 90*time.Second, "B to do real work", func() bool { return b.c.Stats().Received > 0 })
	eventually(t, 90*time.Second, "everything to be processed", func() bool { return tr.distinct() == total })

	t.Logf("A %+v", a.c.Stats())
	t.Logf("B %+v", b.c.Stats())
	must(t, a.stop(t))
	must(t, b.stop(t))
	if got := committedTotal(t, group, topic, 4); got != total {
		t.Fatalf("committed %d, want %d", got, total)
	}
	if a.c.Stats().Rebalances < 2 {
		t.Fatalf("A saw %d rebalance events; expected its assignment to be shrunk when B joined", a.c.Stats().Rebalances)
	}
	var all []string
	dups := 0
	tr.mu.Lock()
	for v, n := range tr.seen {
		all = append(all, v)
		if n > 1 {
			dups++
		}
	}
	tr.mu.Unlock()
	// A commits what it finished for the partitions it hands over before it lets go,
	// so B starts where A stopped. Without that, everything A did since its last
	// periodic commit (about 20 messages per partition here) would be redone.
	if a.c.Stats().Commits == 0 {
		t.Fatal("A never committed: the test did not exercise the hand-over of in-progress partitions")
	}
	if dups > 8 {
		t.Fatalf("%d messages were processed twice across the rebalance; progress on revoked partitions was not committed first", dups)
	}
	sort.Strings(all)
	if len(all) != total || !strings.HasPrefix(all[0], "m000") || !strings.HasPrefix(all[total-1], fmt.Sprintf("m%03d", total-1)) {
		t.Fatalf("processed %d distinct messages, %s .. %s", len(all), all[0], all[len(all)-1])
	}
}
