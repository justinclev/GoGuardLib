package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/kafka"
)

// fakeBroker is an in-memory Kafka cluster with one consumer group. Unlike a
// scripted stub it behaves like a broker: per-partition logs, read positions,
// pause, seek and committed offsets survive a consumer restarting.
type fakeBroker struct {
	mu        sync.Mutex
	logs      map[kafka.TopicPartition][]kafka.Message
	committed map[kafka.TopicPartition]int64
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{logs: map[kafka.TopicPartition][]kafka.Message{}, committed: map[kafka.TopicPartition]int64{}}
}

func (b *fakeBroker) produce(topic string, partition int32, key, value string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	tp := kafka.TopicPartition{Topic: topic, Partition: partition}
	off := int64(len(b.logs[tp]))
	b.logs[tp] = append(b.logs[tp], kafka.Message{
		TopicPartition: tp, Offset: off, Key: []byte(key), Value: []byte(value),
		Headers: []dlq.Header{{Key: "trace", Value: []byte(fmt.Sprintf("t%d", off))}}, Timestamp: time.Unix(1700000000+off, 0),
	})
}

func (b *fakeBroker) committedOffset(topic string, partition int32) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.committed[kafka.TopicPartition{Topic: topic, Partition: partition}]
}

// fakeClient is one consumer's session with the broker.
type fakeClient struct {
	b *fakeBroker

	mu       sync.Mutex
	assigned map[kafka.TopicPartition]bool
	paused   map[kafka.TopicPartition]bool
	pos      map[kafka.TopicPartition]int64
	injected []kafka.Event
	handler  kafka.RebalanceHandler
	calls    []string
	polls    int

	commitErr func() error
	pollErr   error
	seekErr   error
	skipAhead int // if > 0, the Nth delivered message is skipped once (a gap)
	delivered int
}

type assignedEvent struct{ Partitions []kafka.TopicPartition }
type revokedEvent struct{ Partitions []kafka.TopicPartition }

func (c *fakeClient) SetRebalanceHandler(h kafka.RebalanceHandler) {
	c.mu.Lock()
	c.handler = h
	c.mu.Unlock()
}

func newFakeClient(b *fakeBroker) *fakeClient {
	return &fakeClient{b: b, assigned: map[kafka.TopicPartition]bool{}, paused: map[kafka.TopicPartition]bool{}, pos: map[kafka.TopicPartition]int64{}}
}

func tp(topic string, p int32) kafka.TopicPartition {
	return kafka.TopicPartition{Topic: topic, Partition: p}
}

// inject queues a rebalance event for the next Poll.
func (c *fakeClient) inject(e kafka.Event) {
	c.mu.Lock()
	c.injected = append(c.injected, e)
	c.mu.Unlock()
}

func (c *fakeClient) record(s string) { c.calls = append(c.calls, s) }

func (c *fakeClient) callLog() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

func (c *fakeClient) pollCount() int { c.mu.Lock(); defer c.mu.Unlock(); return c.polls }

func (c *fakeClient) isPaused(t kafka.TopicPartition) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paused[t]
}

func (c *fakeClient) Poll(ctx context.Context, timeout time.Duration) (kafka.Event, error) {
	c.mu.Lock()
	c.polls++
	if c.pollErr != nil {
		err := c.pollErr
		c.mu.Unlock()
		return nil, err
	}
	if len(c.injected) > 0 {
		e := c.injected[0]
		c.injected = c.injected[1:]
		h := c.handler
		c.mu.Unlock()
		// Like the real client: the handler runs inside Poll, and the assignment or
		// revocation is completed only after it returns.
		switch ev := e.(type) {
		case assignedEvent:
			if err := h.OnAssigned(ev.Partitions); err != nil {
				return nil, err
			}
			c.assign(ev.Partitions)
		case revokedEvent:
			if err := h.OnRevoked(ev.Partitions); err != nil {
				return nil, err
			}
			c.unassign(ev.Partitions)
		}
		return nil, nil
	}
	var parts []kafka.TopicPartition
	for t := range c.assigned {
		parts = append(parts, t)
	}
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].Topic != parts[j].Topic {
			return parts[i].Topic < parts[j].Topic
		}
		return parts[i].Partition < parts[j].Partition
	})
	for _, t := range parts {
		if c.paused[t] {
			continue
		}
		c.b.mu.Lock()
		log := c.b.logs[t]
		c.b.mu.Unlock()
		p := c.pos[t]
		if p >= int64(len(log)) {
			continue
		}
		c.delivered++
		if c.skipAhead > 0 && c.delivered == c.skipAhead && p+1 < int64(len(log)) {
			p++ // deliver a message from the future: a gap the consumer must not accept
		}
		c.pos[t] = p + 1
		m := log[p]
		c.mu.Unlock()
		return m, nil
	}
	c.mu.Unlock()
	d := timeout
	if d > 2*time.Millisecond {
		d = 2 * time.Millisecond
	}
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
	return nil, nil
}

func (c *fakeClient) assign(parts []kafka.TopicPartition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range parts {
		c.assigned[t] = true
		c.b.mu.Lock()
		c.pos[t] = c.b.committed[t]
		c.b.mu.Unlock()
		c.paused[t] = false // a fresh assignment starts unpaused
		c.record("assign " + t.String())
	}
}

func (c *fakeClient) unassign(parts []kafka.TopicPartition) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range parts {
		delete(c.assigned, t)
		delete(c.paused, t)
		c.record("unassign " + t.String())
	}
}

func (c *fakeClient) Pause(parts []kafka.TopicPartition) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range parts {
		c.paused[t] = true
		c.record("pause " + t.String())
	}
	return nil
}

func (c *fakeClient) Resume(parts []kafka.TopicPartition) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range parts {
		c.paused[t] = false
		c.record("resume " + t.String())
	}
	return nil
}

func (c *fakeClient) Seek(t kafka.TopicPartition, offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seekErr != nil {
		return c.seekErr
	}
	c.pos[t] = offset
	c.record(fmt.Sprintf("seek %s %d", t, offset))
	return nil
}

func (c *fakeClient) Commit(offs []kafka.TopicPartitionOffset) error {
	c.mu.Lock()
	fn := c.commitErr
	c.mu.Unlock()
	if fn != nil {
		if err := fn(); err != nil {
			return err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.b.mu.Lock()
	defer c.b.mu.Unlock()
	for _, o := range offs {
		c.b.committed[o.TopicPartition] = o.Offset
		c.record(fmt.Sprintf("commit %s %d", o.TopicPartition, o.Offset))
	}
	return nil
}

// fakeProducer captures dead-letter messages.
type fakeProducer struct {
	mu   sync.Mutex
	sent []sentMessage
	err  error
}

type sentMessage struct {
	Topic      string
	Key, Value []byte
	Headers    map[string]string
}

func (p *fakeProducer) Produce(ctx context.Context, topic string, key, value []byte, headers []dlq.Header) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	h := map[string]string{}
	for _, x := range headers {
		h[x.Key] = string(x.Value)
	}
	p.sent = append(p.sent, sentMessage{Topic: topic, Key: key, Value: value, Headers: h})
	return nil
}

func (p *fakeProducer) messages() []sentMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]sentMessage(nil), p.sent...)
}

var errBoom = errors.New("boom")
