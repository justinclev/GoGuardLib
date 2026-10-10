package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/obs"
	"github.com/justinclev/GoGuardLib/pipeline"
	"github.com/justinclev/GoGuardLib/retry"
)

// ErrUnboundTopic is returned when a message arrives from a topic that has no
// Binding. The consumer only subscribes to bound topics, so this means the Client
// was configured with a different topic list.
var ErrUnboundTopic = errors.New("kafka: message from a topic with no binding")

// ErrUnstorable is returned by Run when a message can be neither processed nor
// stored (for example, it is larger than the store accepts) and OnUnstorable is
// UnstorableHalt. Retrying cannot help, so the consumer stops instead of leaving
// the partition blocked while looking healthy. The error names the topic,
// partition and offset, never the payload.
var ErrUnstorable = errors.New("kafka: message can be neither processed nor stored")

// ErrStoreNotDurable is returned by NewConsumer when RequireDurableStore is set and
// the Store does not report DurabilityDurable.
var ErrStoreNotDurable = errors.New("kafka: the store is not durable")

// ErrBrokersDown is returned by Run when the Client has reported that no broker is
// reachable for longer than Config.BrokersDownTimeout.
var ErrBrokersDown = errors.New("kafka: no broker has been reachable for longer than BrokersDownTimeout")

// UnstorablePolicy says what to do with a message that fails and that the store
// will never accept.
type UnstorablePolicy int

const (
	// UnstorableHalt stops Run with ErrUnstorable, committing only what is safe.
	// Nothing is lost; an operator must deal with the message (raise the store's
	// limit, fix the producer, or restart with UnstorableSkip after deciding it may
	// go). This is the default.
	UnstorableHalt UnstorablePolicy = iota
	// UnstorableSkip commits past the message. It is lost from this consumer: it is
	// counted in Stats.Unstorable and copied to Config.Mirror if one is set (best
	// effort, and that copy can fail for the same reason). Choose it only when
	// availability matters more than any single message.
	UnstorableSkip
)

// Binding guards one topic: every message on it runs through Pipeline. Topics with
// no binding are not consumed by this Consumer.
type Binding struct {
	Topic    string
	Pipeline *pipeline.Pipeline
}

// Config configures a Consumer.
type Config struct {
	// Client reads from Kafka. Required; must have auto-commit and offset storing off.
	Client Client
	// Store holds messages that could not be finished, with their progress. It must
	// be durable if you cannot afford to lose them on a crash (see dlq.OpenWAL).
	// Required.
	Store dlq.Store
	// Bindings lists the topics to consume and the pipeline for each. Required.
	Bindings []Binding

	// Workers is how many messages are processed at once. The default, 1, processes
	// one message at a time on the goroutine that polls, exactly in offset order. With
	// more, messages are handled by a pool while the offsets committed stay
	// contiguous: an offset is committed only when it and every offset before it in
	// its partition are safe, so a slow or failing message holds back the commit, and
	// is retried, without holding back the others. Messages with the same key always
	// run one after another in offset order (on the same worker), and a message that
	// fails stops later messages with its key from running until it has been retried.
	// Your pipeline steps run concurrently, so they must be safe for that; breakers,
	// stores and the mirror already are.
	Workers int
	// MaxInFlight bounds the messages taken from the client but not yet finished
	// (running or queued for a worker). Beyond it the consumer stops fetching, by
	// pausing its partitions, until workers catch up. Default 4 times Workers.
	MaxInFlight int

	// IgnoreKeyOrder turns off per-key ordering. By default, while a message with a
	// key is waiting in the store, later messages with the same key are stored
	// behind it instead of being processed ahead of it.
	IgnoreKeyOrder bool

	// PollTimeout bounds each poll, and so how quickly the consumer reacts to a
	// breaker changing state. Default 100ms.
	PollTimeout time.Duration
	// CommitInterval and CommitBatch decide when safe offsets are committed: after
	// this long or this many messages, whichever comes first, and always on
	// rebalance and shutdown. A crash between commits redelivers messages that were
	// already safe; that is harmless because storing them is idempotent and steps
	// carry idempotency keys. Defaults 1s and 1000.
	CommitInterval time.Duration
	CommitBatch    int
	// ProcessTimeout bounds the processing of one message, which must stay well
	// under the client's max.poll.interval.ms. Default 2 minutes.
	ProcessTimeout time.Duration
	// ShutdownTimeout bounds how long Run waits for running messages on the way out
	// (with Workers above one) and then how long it keeps trying to commit. A
	// commit is refused while a rebalance is in progress and succeeds once the
	// consumer has polled again, so Run keeps polling and retrying until this
	// passes. Default 5 seconds.
	ShutdownTimeout time.Duration
	// RetryBackoff is how long a partition waits before a message that could not
	// be processed or stored is tried again. Default exponential from 500ms to 30s
	// with jitter.
	RetryBackoff retry.Backoff

	// BrokersDownTimeout stops Run with ErrBrokersDown once the Client (if it is a
	// HealthReporter) has had no reachable broker for this long, so that a
	// supervisor restarts the process and an alert fires. Without it a consumer
	// that has lost the cluster keeps polling and looks idle. Zero (the default)
	// never stops; it will default to 5 minutes at v1.0.
	BrokersDownTimeout time.Duration
	// RequireDurableStore makes NewConsumer refuse a Store that cannot promise an
	// acknowledged Append survives a crash and a power failure (see
	// dlq.StoreDurability): a MemoryStore, or a WAL with SyncInterval or SyncNone.
	// The consumer commits an offset once a message is in the store, so a store
	// that forgets makes that commit a data-loss event. A custom Store must
	// implement dlq.DurabilityReporter to pass. It will default to true at v1.0.
	RequireDurableStore bool
	// OnUnstorable is what happens to a message that fails and cannot be stored
	// (see UnstorablePolicy). Default UnstorableHalt.
	OnUnstorable UnstorablePolicy
	// IDNamespace names the Kafka cluster this consumer reads (see dlq.KafkaIDIn).
	// Set it whenever topics can be recreated or the consumer can fail over to
	// another cluster, and keep it stable for the life of the store: changing it
	// makes messages already stored look new. Default empty.
	IDNamespace string
	// MirrorTimeout bounds one publish to the dead-letter topic. A producer that
	// hangs (broker down, full queue) would otherwise hold a worker, and with it the
	// partition, indefinitely. Default 10 seconds.
	MirrorTimeout time.Duration

	// Mirror, if set, receives a copy of every message the pipeline parks, for a
	// Kafka dead-letter topic. It is best effort: the message is already parked in
	// the Store.
	Mirror *DLQPublisher
	// Events receives an obs.ConsumerEvent for what the consumer does that an
	// operator would alert on: messages deferred, parked or held for ordering,
	// retries and backpressure, partitions paused and resumed, failed commits and
	// mirror publishes, rebalances. Events carry positions and fixed reasons, never
	// message data. It may be called from several goroutines at once and must not block; wrap a slow sink in an obs.Dispatcher.
	// Breakers and the redriver emit their own events to their own sinks.
	Events obs.Sink
}

// Stats counts what a Consumer has done.
type Stats struct {
	Received     uint64 // messages taken from Kafka
	Done         uint64 // fully processed
	Deferred     uint64 // stored with progress, waiting for a dependency
	Parked       uint64 // stored for a human
	OrderHeld    uint64 // stored behind an earlier message with the same key
	Retried      uint64 // seeked back and retried after a failure
	Backpressure uint64 // of those, because the store was full
	Commits      uint64
	CommitErrors uint64
	Rebalances   uint64
	MirrorErrors uint64
	Unstorable   uint64 // skipped because they could not be stored (UnstorableSkip)
	// Health is what the Client reports about the cluster connection; the zero
	// value if the Client does not report (see HealthReporter).
	Health   ClientHealth
	Paused   int // partitions currently paused
	InFlight int // messages taken from the client and not yet finished
}

type partition struct {
	paused      bool
	pauseReason string
	retryAt     time.Time
	failures    int
	gen         uint64 // identifies this assignment; results from an older one are discarded

	next      int64              // the next offset to accept from the client; -1 until the first message
	frontier  int64              // every offset below it is safe (done or durably stored)
	completed map[int64]struct{} // finished offsets at or above frontier
	inflight  int                // accepted and dispatched, result not yet applied
	failedAt  int64              // lowest offset that failed and must be retried; -1 if none
}

// task is one message on its way to a worker.
type task struct {
	m   Message
	in  pipeline.Input
	b   *binding
	gen uint64
}

// result is what a worker reports back to the goroutine that owns the state.
type result struct {
	task
	res pipeline.Result
	err error
}

// Consumer runs bindings against a Client. Create one with NewConsumer and call Run.
type Consumer struct {
	cfg      Config
	bindings map[string]*binding

	// Everything below is owned by the goroutine running Run.
	parts       map[TopicPartition]*partition
	safe        map[TopicPartition]int64 // next offset to commit per partition
	committed   map[TopicPartition]int64
	sinceCommit int
	lastCommit  time.Time
	gen         uint64
	backlog     []task // accepted, waiting for room in the pool
	inflight    int    // dispatched and not yet applied
	maxInFlight int
	throttled   bool
	stopping    bool
	work        chan task // shared by the workers; nil when messages run inline
	results     chan result

	// keyQueues holds, for every key with a message running, the later messages
	// with that key waiting for it, so that they run one at a time in offset order
	// on whichever worker is free.
	keyQueues map[failedKey][]task

	received, done, deferred, parked, orderHeld, retried, backpressure atomic.Uint64
	commits, commitErrors, rebalances, mirrorErrors, unstorable        atomic.Uint64
	pausedNow, inflightNow                                             atomic.Int64
}

// emit reports a consumer event. offset is -1 when the event is not about one message.
func (c *Consumer) emit(code obs.ConsumerCode, tp TopicPartition, offset int64, reason string) {
	obs.Emit(c.cfg.Events, obs.ConsumerEvent{Code: code, Topic: tp.Topic, Partition: tp.Partition, Offset: offset, Reason: reason, At: time.Now()})
}

// keyID identifies a key within a partition: Kafka orders messages only there.
type failedKey struct {
	tp  TopicPartition
	key string
}

type binding struct {
	pipeline *pipeline.Pipeline
	breakers []*breaker.Breaker
}

// NewConsumer validates the configuration.
func NewConsumer(cfg Config) (*Consumer, error) {
	if cfg.Client == nil || cfg.Store == nil {
		return nil, errors.New("kafka: Config needs a Client and a Store")
	}
	if cfg.RequireDurableStore {
		if d := dlq.StoreDurability(cfg.Store); d != dlq.DurabilityDurable {
			return nil, fmt.Errorf("%w: it reports %s; use a dlq.WALStore with SyncAlways, or implement dlq.DurabilityReporter", ErrStoreNotDurable, d)
		}
	}
	if len(cfg.Bindings) == 0 {
		return nil, errors.New("kafka: at least one Binding is required")
	}
	if cfg.PollTimeout <= 0 {
		cfg.PollTimeout = 100 * time.Millisecond
	}
	if cfg.CommitInterval <= 0 {
		cfg.CommitInterval = time.Second
	}
	if cfg.CommitBatch <= 0 {
		cfg.CommitBatch = 1000
	}
	if cfg.ProcessTimeout <= 0 {
		cfg.ProcessTimeout = 2 * time.Minute
	}
	if cfg.MirrorTimeout <= 0 {
		cfg.MirrorTimeout = 10 * time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 5 * time.Second
	}
	if cfg.Workers < 0 || cfg.MaxInFlight < 0 {
		return nil, errors.New("kafka: Workers and MaxInFlight cannot be negative")
	}
	if cfg.Workers == 0 {
		cfg.Workers = 1
	}
	if cfg.MaxInFlight == 0 {
		cfg.MaxInFlight = 4 * cfg.Workers
	}
	if cfg.MaxInFlight < cfg.Workers {
		cfg.MaxInFlight = cfg.Workers
	}
	if cfg.RetryBackoff == nil {
		cfg.RetryBackoff = retry.Jitter(retry.Exponential(500*time.Millisecond, 30*time.Second), 0.3)
	}
	c := &Consumer{
		cfg: cfg, bindings: map[string]*binding{}, maxInFlight: cfg.MaxInFlight, keyQueues: map[failedKey][]task{},
		parts: map[TopicPartition]*partition{}, safe: map[TopicPartition]int64{}, committed: map[TopicPartition]int64{},
	}
	for _, b := range cfg.Bindings {
		if b.Topic == "" || b.Pipeline == nil {
			return nil, errors.New("kafka: every Binding needs a Topic and a Pipeline")
		}
		if _, dup := c.bindings[b.Topic]; dup {
			return nil, fmt.Errorf("kafka: topic %q is bound twice", b.Topic)
		}
		c.bindings[b.Topic] = &binding{pipeline: b.Pipeline, breakers: b.Pipeline.Breakers()}
	}
	return c, nil
}

// Topics lists the bound topics, for subscribing the Client.
func (c *Consumer) Topics() []string {
	out := make([]string, 0, len(c.cfg.Bindings))
	for _, b := range c.cfg.Bindings {
		out = append(out, b.Topic)
	}
	return out
}

// Stats returns the counters.
func (c *Consumer) Stats() Stats {
	var health ClientHealth
	if hr, ok := c.cfg.Client.(HealthReporter); ok {
		health = hr.Health()
	}
	return Stats{
		Health:   health,
		Received: c.received.Load(), Done: c.done.Load(), Deferred: c.deferred.Load(), Parked: c.parked.Load(),
		OrderHeld: c.orderHeld.Load(), Retried: c.retried.Load(), Backpressure: c.backpressure.Load(),
		Commits: c.commits.Load(), CommitErrors: c.commitErrors.Load(), Rebalances: c.rebalances.Load(),
		MirrorErrors: c.mirrorErrors.Load(), Unstorable: c.unstorable.Load(), Paused: int(c.pausedNow.Load()), InFlight: int(c.inflightNow.Load()),
	}
}

// Run consumes until ctx ends or something goes wrong that cannot be recovered:
// the Client failing, or the Store failing closed. It commits what is safe before
// returning. It returns nil when stopped by ctx. Run does not close the Client.
//
// A message is never skipped: an offset is committed only after its message is
// done or durable in the Store, and a message that cannot be handled is seeked
// back to and retried.
func (c *Consumer) Run(ctx context.Context) error {
	c.lastCommit = time.Now()
	c.cfg.Client.SetRebalanceHandler(rebalanceHandler{c})

	wctx, cancelWorkers := context.WithCancel(ctx)
	var wg sync.WaitGroup
	c.startWorkers(wctx, &wg)

	var fatal error
loop:
	for ctx.Err() == nil {
		if err := c.drain(ctx, false); err != nil {
			fatal = err
			break
		}
		c.updateThrottle()
		c.reconcile()
		c.maybeCommit(false)

		timeout := c.cfg.PollTimeout
		if c.inflight > 0 && timeout > 10*time.Millisecond {
			timeout = 10 * time.Millisecond // results are waiting on workers: come back soon
		}
		if err := c.checkBrokers(); err != nil {
			fatal = err
			break
		}
		ev, err := c.cfg.Client.Poll(ctx, timeout)
		if err != nil {
			fatal = fmt.Errorf("kafka: polling: %w", err)
			break
		}
		switch e := ev.(type) {
		case nil:
		case Message:
			if err := c.accept(ctx, e); err != nil {
				fatal = err
				break loop
			}
		}
		if err := c.dispatchBacklog(ctx); err != nil {
			fatal = err
			break
		}
	}

	// Stop the pool, wait for what is running, and keep what finished: those
	// messages are safe even though we are leaving.
	c.stopping = true
	cancelWorkers()
	stopped := c.stopWorkers(&wg)
	// If a handler ignored its context and is still running, its message is simply not
	// finished: its offset is not committed and it is delivered again.
	_ = c.drain(ctx, stopped)
	return errors.Join(fatal, c.finalCommit())
}

// finalCommit commits everything that is safe. A commit fails while a rebalance is
// in progress and works again after the consumer polls, so it keeps polling and
// retrying until ShutdownTimeout. Messages that arrive meanwhile are ignored: they
// were never marked safe, so they will be redelivered. A rebalance that completes
// in the meantime also drops the offsets of partitions this consumer no longer
// owns, so the retry commits only what it still may.
func (c *Consumer) finalCommit() error {
	deadline := time.Now().Add(c.cfg.ShutdownTimeout)
	for {
		err := c.commit()
		if err == nil || !time.Now().Before(deadline) {
			return err
		}
		if _, perr := c.cfg.Client.Poll(context.Background(), c.cfg.PollTimeout); perr != nil {
			return errors.Join(err, fmt.Errorf("kafka: polling during shutdown: %w", perr))
		}
	}
}

// ---- partitions, pausing ----

func (c *Consumer) breakerOpen(topic string) bool {
	if b := c.bindings[topic]; b != nil {
		for _, br := range b.breakers {
			if br.Refusing() {
				return true
			}
		}
	}
	return false
}

// reconcile pauses partitions whose topic has an open circuit, that are backing
// off after a failure, or (while the pool is full) all of them, and resumes the
// rest. Pausing leaves messages in Kafka;
// the client keeps being polled so the group does not think this consumer died.
func (c *Consumer) reconcile() {
	now := time.Now()
	var pause, resume []TopicPartition
	for tp, p := range c.parts {
		reason := ""
		switch {
		case c.breakerOpen(tp.Topic):
			reason = "breaker_open"
		case p.retryAt.After(now):
			reason = "retry_backoff"
		case c.throttled:
			reason = "backlog"
		}
		want := reason != ""
		switch {
		case want && !p.paused:
			pause = append(pause, tp)
			p.pauseReason = reason
		case !want && p.paused:
			resume = append(resume, tp)
		}
	}
	if len(pause) > 0 && c.cfg.Client.Pause(pause) == nil {
		for _, tp := range pause {
			c.parts[tp].paused = true
			c.emit(obs.ConsumerPaused, tp, -1, c.parts[tp].pauseReason)
		}
	}
	if len(resume) > 0 && c.cfg.Client.Resume(resume) == nil {
		for _, tp := range resume {
			c.parts[tp].paused = false
			c.emit(obs.ConsumerResumed, tp, -1, "")
		}
	}
	var n int64
	for _, p := range c.parts {
		if p.paused {
			n++
		}
	}
	c.pausedNow.Store(n)
}

// rebalanceHandler runs inside Client.Poll, on Run's goroutine, so it may touch
// the consumer's state without locking.
type rebalanceHandler struct{ c *Consumer }

func (h rebalanceHandler) OnAssigned(parts []TopicPartition) error { return h.c.onAssigned(parts) }
func (h rebalanceHandler) OnRevoked(parts []TopicPartition) error  { return h.c.onRevoked(parts) }

func (c *Consumer) newPartition() *partition {
	c.gen++
	return &partition{next: -1, frontier: -1, failedAt: -1, gen: c.gen, completed: map[int64]struct{}{}}
}

func (c *Consumer) onAssigned(parts []TopicPartition) error {
	c.rebalances.Add(1)
	for _, tp := range parts {
		c.emit(obs.ConsumerRebalance, tp, -1, "assigned")
	}
	for _, tp := range parts {
		c.parts[tp] = c.newPartition() // a new assignment starts unpaused; reconcile re-applies pauses
		delete(c.committed, tp)
	}
	return nil
}

func (c *Consumer) onRevoked(parts []TopicPartition) error {
	// Commit what is safe for these partitions first: whoever gets them next
	// resumes from there. Messages still running here are simply redelivered to the
	// new owner; storing them is idempotent and steps carry idempotency keys.
	c.commitPartitions(parts)
	revoked := map[TopicPartition]bool{}
	for _, tp := range parts {
		revoked[tp] = true
		delete(c.parts, tp)
		delete(c.safe, tp)
		delete(c.committed, tp)
		c.dropQueued(tp)
	}
	kept := c.backlog[:0]
	for _, t := range c.backlog {
		if !revoked[t.m.TopicPartition] {
			kept = append(kept, t)
		}
	}
	c.backlog = kept
	c.rebalances.Add(1)
	for _, tp := range parts {
		c.emit(obs.ConsumerRebalance, tp, -1, "revoked")
	}
	return nil
}

// ---- committing ----

// markDone records that a message is safe (done, or durable in the store) and
// moves the partition's commit point over every offset that is now contiguous.
func (c *Consumer) markDone(tp TopicPartition, p *partition, offset int64) {
	p.completed[offset] = struct{}{}
	advanced := false
	for {
		if _, ok := p.completed[p.frontier]; !ok {
			break
		}
		delete(p.completed, p.frontier)
		p.frontier++
		advanced = true
	}
	if advanced {
		c.safe[tp] = p.frontier
		// Only progress at the commit point ends a run of failures. A message that keeps
		// failing must keep lengthening its backoff even while others finish around it.
		p.failures = 0
	}
	c.sinceCommit++
}

func (c *Consumer) maybeCommit(force bool) {
	if c.sinceCommit == 0 {
		return
	}
	if force || c.sinceCommit >= c.cfg.CommitBatch || time.Since(c.lastCommit) >= c.cfg.CommitInterval {
		_ = c.commit()
	}
}

func (c *Consumer) pending(only map[TopicPartition]bool) []TopicPartitionOffset {
	var out []TopicPartitionOffset
	for tp, off := range c.safe {
		if only != nil && !only[tp] {
			continue
		}
		if last, ok := c.committed[tp]; !ok || off > last {
			out = append(out, TopicPartitionOffset{TopicPartition: tp, Offset: off})
		}
	}
	return out
}

func (c *Consumer) commitOffsets(offs []TopicPartitionOffset) error {
	if len(offs) == 0 {
		return nil
	}
	if err := c.cfg.Client.Commit(offs); err != nil {
		c.commitErrors.Add(1)
		c.emit(obs.ConsumerCommitFailed, offs[0].TopicPartition, offs[0].Offset, "")
		return fmt.Errorf("kafka: committing offsets: %w", err)
	}
	for _, o := range offs {
		c.committed[o.TopicPartition] = o.Offset
	}
	c.commits.Add(1)
	return nil
}

// commit stores every safe offset. A failure only means some messages may be
// redelivered after a restart, so it is counted and retried, not fatal.
func (c *Consumer) commit() error {
	err := c.commitOffsets(c.pending(nil))
	if err == nil {
		c.sinceCommit = 0
		c.lastCommit = time.Now()
	}
	return err
}

func (c *Consumer) commitPartitions(parts []TopicPartition) {
	only := map[TopicPartition]bool{}
	for _, tp := range parts {
		only[tp] = true
	}
	_ = c.commitOffsets(c.pending(only))
}

// ---- messages ----

func orderKeyFor(topic string, key []byte) string {
	if len(key) == 0 {
		return ""
	}
	return topic + "\x00" + string(key)
}

// accept takes a message from the client. It is never skipped and never repeated
// within an assignment: the next message must be exactly the one after the last
// accepted. Anything else (a stale message delivered after a seek, a gap) is
// dropped and the partition re-pointed.
func (c *Consumer) accept(ctx context.Context, m Message) error {
	b := c.bindings[m.Topic]
	if b == nil {
		return fmt.Errorf("%w: %s", ErrUnboundTopic, m.Topic)
	}
	p := c.parts[m.TopicPartition]
	if p == nil {
		return nil // a message for a partition we no longer own: its new owner will read it
	}
	if p.failedAt >= 0 {
		return nil // draining to retry a failed message: this one will be redelivered
	}
	if p.next >= 0 && m.Offset != p.next {
		if m.Offset > p.next {
			if err := c.cfg.Client.Seek(m.TopicPartition, p.next); err != nil {
				return fmt.Errorf("kafka: re-seeking %s: %w", m.TopicPartition, err)
			}
		}
		return nil
	}
	if p.next < 0 {
		p.frontier = m.Offset // the first message of an assignment is where progress starts
	}
	p.next = m.Offset + 1
	if _, finished := p.completed[m.Offset]; finished {
		return nil // redelivered after a retry, but it already finished
	}
	c.received.Add(1)

	in := pipeline.Input{
		ID:      dlq.KafkaIDIn(c.cfg.IDNamespace, m.Topic, m.Partition, m.Offset),
		Source:  dlq.Source{Kind: "kafka", Name: m.Topic, Partition: m.Partition, Offset: m.Offset},
		Key:     m.Key,
		Value:   m.Value,
		Headers: m.Headers,
	}
	if !c.cfg.IgnoreKeyOrder {
		in.OrderKey = orderKeyFor(m.Topic, m.Key)
	}
	c.backlog = append(c.backlog, task{m: m, in: in, b: b, gen: p.gen})
	return nil
}

// dispatchBacklog hands accepted messages to the pool while there is room. With one
// worker it runs them right here, in order.
func (c *Consumer) dispatchBacklog(ctx context.Context) error {
	for len(c.backlog) > 0 && c.inflight < c.maxInFlight {
		t := c.backlog[0]
		c.backlog[0] = task{}
		c.backlog = c.backlog[1:]
		p := c.parts[t.m.TopicPartition]
		if p == nil || p.gen != t.gen || p.failedAt >= 0 {
			continue // revoked, or waiting to be retried: it will be redelivered
		}
		p.inflight++
		c.inflight++
		c.inflightNow.Store(int64(c.inflight))
		if c.work == nil {
			if err := c.complete(ctx, c.exec(ctx, t)); err != nil {
				return err
			}
			continue
		}
		c.send(t)
	}
	return nil
}

func keyOf(t task) failedKey { return failedKey{tp: t.m.TopicPartition, key: t.in.OrderKey} }

// send gives a message to the pool, unless an earlier message with its key is still
// running: then it waits in line behind it. Messages with different keys never wait
// for each other.
func (c *Consumer) send(t task) {
	if t.in.OrderKey != "" {
		k := keyOf(t)
		if q, busy := c.keyQueues[k]; busy {
			c.keyQueues[k] = append(q, t)
			return
		}
		c.keyQueues[k] = nil // busy, nobody waiting yet
	}
	c.work <- t // has room: everything in flight fits
}

// released starts the next message waiting behind a finished one, if any.
func (c *Consumer) released(t task) {
	if t.in.OrderKey == "" || c.work == nil {
		return
	}
	k := keyOf(t)
	q := c.keyQueues[k]
	if len(q) == 0 {
		delete(c.keyQueues, k)
		return
	}
	next := q[0]
	q[0] = task{}
	c.keyQueues[k] = q[1:]
	c.work <- next
}

// dropQueued discards the messages waiting behind a message that failed (for one
// key) or a partition that was taken away (tp only). They never started, and the
// partition is read again from its first unfinished message.
func (c *Consumer) dropQueued(tp TopicPartition, only ...string) {
	for k, q := range c.keyQueues {
		if k.tp != tp || (len(only) > 0 && k.key != only[0]) {
			continue
		}
		for range q {
			c.inflight--
			if p := c.parts[tp]; p != nil {
				p.inflight--
			}
		}
		c.inflightNow.Store(int64(c.inflight))
		if len(only) > 0 {
			c.keyQueues[k] = nil // the failed message itself is still running or has just finished
		} else {
			delete(c.keyQueues, k)
		}
	}
}

func (c *Consumer) startWorkers(ctx context.Context, wg *sync.WaitGroup) {
	c.results = make(chan result, c.maxInFlight+c.cfg.Workers)
	if c.cfg.Workers <= 1 {
		return // run inline on the polling goroutine
	}
	c.work = make(chan task, c.maxInFlight) // total in flight is bounded, so sending never blocks
	for i := 0; i < c.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range c.work {
				c.results <- c.exec(ctx, t) // results has room for everything in flight
			}
		}()
	}
}

// stopWorkers waits for the pool to finish, for at most ShutdownTimeout. It reports
// whether every worker stopped. A handler that ignores its context cannot hold up
// shutdown (a rolling deploy waiting on one stuck message is worse than replaying it).
func (c *Consumer) stopWorkers(wg *sync.WaitGroup) bool {
	if c.work != nil {
		close(c.work)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	t := time.NewTimer(c.cfg.ShutdownTimeout)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}

// drain applies the results workers have produced. With wait, it waits for every
// message still in flight.
func (c *Consumer) drain(ctx context.Context, wait bool) error {
	for {
		if wait && c.inflight == 0 {
			return nil
		}
		select {
		case r := <-c.results:
			if err := c.complete(ctx, r); err != nil {
				return err
			}
		default:
			if !wait {
				return nil
			}
			r := <-c.results
			if err := c.complete(ctx, r); err != nil {
				return err
			}
		}
	}
}

// exec runs one message. It may run on a worker, so it touches no state owned by
// the polling goroutine.
func (c *Consumer) exec(ctx context.Context, t task) result {
	r := result{task: t}
	pctx, cancel := context.WithTimeout(ctx, c.cfg.ProcessTimeout)
	r.res, r.err = c.process(pctx, t.b, t.in)
	cancel()
	if r.res == pipeline.Parked {
		c.mirror(ctx, t.in)
	}
	return r
}

// complete applies one result on the polling goroutine.
func (c *Consumer) complete(ctx context.Context, r result) error {
	c.inflight--
	c.inflightNow.Store(int64(c.inflight))
	tp := r.m.TopicPartition
	p := c.parts[tp]
	if p == nil || p.gen != r.gen {
		return nil // the partition was revoked meanwhile: its new owner starts from the committed offset
	}
	p.inflight--
	if r.res != pipeline.Done && r.res != pipeline.Deferred && r.res != pipeline.Parked && errors.Is(r.err, dlq.ErrInvalidRecord) {
		// The store will never accept this message, so retrying is pointless.
		if c.cfg.OnUnstorable != UnstorableSkip {
			return fmt.Errorf("%w: %s offset %d: %w", ErrUnstorable, tp, r.m.Offset, r.err)
		}
		c.unstorable.Add(1)
		c.emit(obs.ConsumerUnstorable, tp, r.m.Offset, "")
		c.mirror(ctx, r.in)
		c.markDone(tp, p, r.m.Offset)
		c.released(r.task)
		if p.failedAt >= 0 && p.inflight == 0 && !c.stopping {
			return c.rewind(tp, p)
		}
		return nil
	}
	switch r.res {
	case pipeline.Done, pipeline.Deferred, pipeline.Parked:
		switch r.res {
		case pipeline.Done:
			c.done.Add(1)
		case pipeline.Deferred:
			c.deferred.Add(1)
			c.emit(obs.ConsumerDeferred, tp, r.m.Offset, "")
		default:
			c.parked.Add(1)
			c.emit(obs.ConsumerParked, tp, r.m.Offset, "")
		}
		c.markDone(tp, p, r.m.Offset)
		c.released(r.task)
	default:
		if r.in.OrderKey != "" {
			// Later messages with this key must not overtake it: they never start.
			c.dropQueued(tp, r.in.OrderKey)
			delete(c.keyQueues, keyOf(r.task))
		}
		if err := c.noteFailure(ctx, p, r); err != nil {
			return err
		}
	}
	if p.failedAt >= 0 && p.inflight == 0 && !c.stopping {
		return c.rewind(tp, p)
	}
	return nil
}

// noteFailure records a message that is neither done nor stored. It must not be
// skipped, so the partition stops taking new work and, once what is already
// running has finished, is pointed back at it.
func (c *Consumer) noteFailure(ctx context.Context, p *partition, r result) error {
	err := r.err
	switch {
	case errors.Is(err, dlq.ErrStoreFailed), errors.Is(err, dlq.ErrClosed), errors.Is(err, dlq.ErrCorrupt):
		// The store can no longer be trusted to hold what we give it. Stop rather
		// than commit past messages we could not protect.
		return fmt.Errorf("kafka: the dead-letter store failed: %w", err)
	case ctx.Err() != nil || c.stopping:
		return nil // shutting down: leave the message uncommitted
	case errors.Is(err, dlq.ErrFull):
		c.backpressure.Add(1)
		c.emit(obs.ConsumerBackpressure, r.m.TopicPartition, r.m.Offset, "")
	}
	if p.failedAt < 0 || r.m.Offset < p.failedAt {
		p.failedAt = r.m.Offset
	}
	c.retried.Add(1)
	c.emit(obs.ConsumerRetry, r.m.TopicPartition, r.m.Offset, "")
	return nil
}

// rewind points a drained partition back at its first unfinished message and backs
// off before it is tried again. Messages beyond it that already finished are
// remembered and skipped when they are delivered again.
func (c *Consumer) rewind(tp TopicPartition, p *partition) error {
	kept := c.backlog[:0]
	for _, t := range c.backlog {
		if t.m.TopicPartition != tp {
			kept = append(kept, t)
		}
	}
	c.backlog = kept
	if err := c.cfg.Client.Seek(tp, p.frontier); err != nil {
		return fmt.Errorf("kafka: seeking %s back to %d: %w", tp, p.frontier, err)
	}
	p.next = p.frontier
	p.failedAt = -1
	p.failures++
	p.retryAt = time.Now().Add(c.cfg.RetryBackoff(p.failures))
	return nil
}

// updateThrottle stops fetching while the pool is full and resumes once it has
// half emptied, so the partitions are not paused and resumed on every message.
func (c *Consumer) updateThrottle() {
	held := c.inflight + len(c.backlog)
	switch {
	case held >= c.maxInFlight:
		c.throttled = true
	case held <= c.maxInFlight/2:
		c.throttled = false
	}
}

// process runs the message, or stores it behind an earlier message with the same
// key so that it cannot overtake it.
func (c *Consumer) process(ctx context.Context, b *binding, in pipeline.Input) (pipeline.Result, error) {
	if in.OrderKey != "" {
		held, err := c.cfg.Store.HasOrderKey(ctx, in.OrderKey)
		if err != nil {
			return pipeline.Failed, err
		}
		if held {
			err := c.cfg.Store.Append(context.WithoutCancel(ctx), dlq.Record{
				ID: in.ID, Source: in.Source, Key: in.Key, Value: in.Value, Headers: in.Headers, OrderKey: in.OrderKey,
				LastError: "stored behind an earlier message with the same key",
			})
			if err != nil {
				return pipeline.Failed, err
			}
			c.orderHeld.Add(1)
			c.emit(obs.ConsumerOrderHeld, TopicPartition{Topic: in.Source.Name, Partition: in.Source.Partition}, in.Source.Offset, "")
			return pipeline.Deferred, nil
		}
	}
	return b.pipeline.Execute(ctx, c.cfg.Store, in)
}

func (c *Consumer) mirror(ctx context.Context, in pipeline.Input) {
	if c.cfg.Mirror == nil {
		return
	}
	rec := dlq.Record{
		ID: in.ID, State: dlq.Parked, Source: in.Source, Key: in.Key, Value: in.Value, Headers: in.Headers, OrderKey: in.OrderKey,
	}
	mctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.MirrorTimeout)
	defer cancel()
	if err := c.cfg.Mirror.Publish(mctx, rec, "parked by the pipeline: see the dead-letter store"); err != nil {
		c.mirrorErrors.Add(1)
		c.emit(obs.ConsumerMirrorFailed, TopicPartition{Topic: in.Source.Name, Partition: in.Source.Partition}, in.Source.Offset, "")
	}
}

// checkBrokers applies BrokersDownTimeout.
func (c *Consumer) checkBrokers() error {
	if c.cfg.BrokersDownTimeout <= 0 {
		return nil
	}
	hr, ok := c.cfg.Client.(HealthReporter)
	if !ok {
		return nil
	}
	if h := hr.Health(); h.AllBrokersDown && !h.Since.IsZero() && time.Since(h.Since) > c.cfg.BrokersDownTimeout {
		return fmt.Errorf("%w (down since %s)", ErrBrokersDown, h.Since.UTC().Format(time.RFC3339))
	}
	return nil
}
