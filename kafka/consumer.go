package kafka

import (
	"context"
	"errors"
	"fmt"
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
	// ShutdownTimeout bounds how long Run keeps trying to commit on the way out. A
	// commit is refused while a rebalance is in progress and succeeds once the
	// consumer has polled again, so Run keeps polling and retrying until this
	// passes. Default 5 seconds.
	ShutdownTimeout time.Duration
	// RetryBackoff is how long a partition waits before a message that could not
	// be processed or stored is tried again. Default exponential from 500ms to 30s
	// with jitter.
	RetryBackoff retry.Backoff

	// Mirror, if set, receives a copy of every message the pipeline parks, for a
	// Kafka dead-letter topic. It is best effort: the message is already parked in
	// the Store.
	Mirror *DLQPublisher
	// Events receives obs events. Currently unused by the consumer itself and
	// reserved; breakers and the redriver emit their own.
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
	Paused       int // partitions currently paused
}

type partition struct {
	paused   bool
	retryAt  time.Time
	failures int
	expect   int64 // the next offset that must arrive; -1 until the first message
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

	received, done, deferred, parked, orderHeld, retried, backpressure atomic.Uint64
	commits, commitErrors, rebalances, mirrorErrors                    atomic.Uint64
	pausedNow                                                          atomic.Int64
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
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 5 * time.Second
	}
	if cfg.RetryBackoff == nil {
		cfg.RetryBackoff = retry.Jitter(retry.Exponential(500*time.Millisecond, 30*time.Second), 0.3)
	}
	c := &Consumer{
		cfg: cfg, bindings: map[string]*binding{},
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
	return Stats{
		Received: c.received.Load(), Done: c.done.Load(), Deferred: c.deferred.Load(), Parked: c.parked.Load(),
		OrderHeld: c.orderHeld.Load(), Retried: c.retried.Load(), Backpressure: c.backpressure.Load(),
		Commits: c.commits.Load(), CommitErrors: c.commitErrors.Load(), Rebalances: c.rebalances.Load(),
		MirrorErrors: c.mirrorErrors.Load(), Paused: int(c.pausedNow.Load()),
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
	var fatal error
loop:
	for ctx.Err() == nil {
		c.reconcile()
		c.maybeCommit(false)

		ev, err := c.cfg.Client.Poll(ctx, c.cfg.PollTimeout)
		if err != nil {
			fatal = fmt.Errorf("kafka: polling: %w", err)
			break
		}
		switch e := ev.(type) {
		case nil:
		case Message:
			if err := c.handle(ctx, e); err != nil {
				fatal = err
				break loop
			}
		}
	}
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
			if br.State() == obs.StateOpen {
				return true
			}
		}
	}
	return false
}

// reconcile pauses partitions whose topic has an open circuit or that are backing
// off after a failure, and resumes the rest. Pausing leaves messages in Kafka;
// the client keeps being polled so the group does not think this consumer died.
func (c *Consumer) reconcile() {
	now := time.Now()
	var pause, resume []TopicPartition
	for tp, p := range c.parts {
		want := p.retryAt.After(now) || c.breakerOpen(tp.Topic)
		switch {
		case want && !p.paused:
			pause = append(pause, tp)
		case !want && p.paused:
			resume = append(resume, tp)
		}
	}
	if len(pause) > 0 && c.cfg.Client.Pause(pause) == nil {
		for _, tp := range pause {
			c.parts[tp].paused = true
		}
	}
	if len(resume) > 0 && c.cfg.Client.Resume(resume) == nil {
		for _, tp := range resume {
			c.parts[tp].paused = false
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

func (c *Consumer) onAssigned(parts []TopicPartition) error {
	c.rebalances.Add(1)
	for _, tp := range parts {
		c.parts[tp] = &partition{expect: -1} // a new assignment starts unpaused; reconcile re-applies pauses
		delete(c.committed, tp)
	}
	return nil
}

func (c *Consumer) onRevoked(parts []TopicPartition) error {
	// Commit what is safe for these partitions first: whoever gets them next
	// resumes from there. The client releases them only after this returns.
	c.commitPartitions(parts)
	for _, tp := range parts {
		if p := c.parts[tp]; p != nil && p.paused {
			p.paused = false
		}
		delete(c.parts, tp)
		delete(c.safe, tp)
		delete(c.committed, tp)
	}
	c.rebalances.Add(1)
	return nil
}

// ---- committing ----

func (c *Consumer) markSafe(m Message) {
	tp := m.TopicPartition
	c.safe[tp] = m.Offset + 1
	if p := c.parts[tp]; p != nil {
		p.expect = m.Offset + 1
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

func (c *Consumer) handle(ctx context.Context, m Message) error {
	b := c.bindings[m.Topic]
	if b == nil {
		return fmt.Errorf("%w: %s", ErrUnboundTopic, m.Topic)
	}
	p := c.parts[m.TopicPartition]
	if p == nil {
		return nil // a message for a partition we no longer own: its new owner will read it
	}
	// Never skip and never repeat within an assignment: the next message must be
	// exactly the one after the last safe one. Anything else (a stale message
	// delivered after a seek, a gap) is dropped and the partition re-pointed.
	if p.expect >= 0 && m.Offset != p.expect {
		if m.Offset > p.expect {
			if err := c.cfg.Client.Seek(m.TopicPartition, p.expect); err != nil {
				return fmt.Errorf("kafka: re-seeking %s: %w", m.TopicPartition, err)
			}
		}
		return nil
	}
	c.received.Add(1)

	in := pipeline.Input{
		ID:      dlq.KafkaID(m.Topic, m.Partition, m.Offset),
		Source:  dlq.Source{Kind: "kafka", Name: m.Topic, Partition: m.Partition, Offset: m.Offset},
		Key:     m.Key,
		Value:   m.Value,
		Headers: m.Headers,
	}
	if !c.cfg.IgnoreKeyOrder {
		in.OrderKey = orderKeyFor(m.Topic, m.Key)
	}

	pctx, cancel := context.WithTimeout(ctx, c.cfg.ProcessTimeout)
	res, err := c.process(pctx, b, in)
	cancel()

	switch res {
	case pipeline.Done:
		c.done.Add(1)
	case pipeline.Deferred:
		c.deferred.Add(1)
	case pipeline.Parked:
		c.parked.Add(1)
		c.mirror(ctx, in)
	default:
		return c.failed(ctx, m, err)
	}
	c.markSafe(m)
	return nil
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
			return pipeline.Deferred, nil
		}
	}
	return b.pipeline.Execute(ctx, c.cfg.Store, in)
}

// failed handles a message that is neither done nor stored. It must not be
// skipped, so the partition is pointed back at it and paused for a backoff.
func (c *Consumer) failed(ctx context.Context, m Message, err error) error {
	switch {
	case errors.Is(err, dlq.ErrStoreFailed), errors.Is(err, dlq.ErrClosed), errors.Is(err, dlq.ErrCorrupt):
		// The store can no longer be trusted to hold what we give it. Stop rather
		// than commit past messages we could not protect.
		return fmt.Errorf("kafka: the dead-letter store failed: %w", err)
	case ctx.Err() != nil:
		return nil // shutting down: leave the message uncommitted
	case errors.Is(err, dlq.ErrFull):
		c.backpressure.Add(1)
	}
	p := c.parts[m.TopicPartition]
	if p == nil {
		return nil
	}
	if serr := c.cfg.Client.Seek(m.TopicPartition, m.Offset); serr != nil {
		return fmt.Errorf("kafka: seeking %s back to %d: %w", m.TopicPartition, m.Offset, serr)
	}
	p.expect = m.Offset
	p.failures++
	p.retryAt = time.Now().Add(c.cfg.RetryBackoff(p.failures))
	c.retried.Add(1)
	return nil
}

func (c *Consumer) mirror(ctx context.Context, in pipeline.Input) {
	if c.cfg.Mirror == nil {
		return
	}
	rec := dlq.Record{
		ID: in.ID, State: dlq.Parked, Source: in.Source, Key: in.Key, Value: in.Value, Headers: in.Headers, OrderKey: in.OrderKey,
	}
	if err := c.cfg.Mirror.Publish(context.WithoutCancel(ctx), rec, "parked by the pipeline: see the dead-letter store"); err != nil {
		c.mirrorErrors.Add(1)
	}
}
