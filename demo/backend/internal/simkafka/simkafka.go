// Package simkafka is a tiny in-memory stand-in for Kafka, used when the demo runs
// without a broker (KAFKA_BROKERS unset). It implements the same Client and Producer
// interfaces the confluent adapter does, so the consumer under test is the real one;
// only the broker is simulated.
package simkafka

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/kafka"
)

// Broker holds the partitions of one or more topics.
type Broker struct {
	mu        sync.Mutex
	parts     []kafka.TopicPartition // every partition, in a fixed order
	index     map[kafka.TopicPartition]int
	perTopic  map[string]int
	logs      [][]kafka.Message
	committed []int64
}

// New creates a broker where every topic has n partitions.
func New(topics []string, partitions int) *Broker {
	b := &Broker{index: map[kafka.TopicPartition]int{}, perTopic: map[string]int{}}
	for _, t := range topics {
		b.perTopic[t] = partitions
		for p := 0; p < partitions; p++ {
			tp := kafka.TopicPartition{Topic: t, Partition: int32(p)}
			b.index[tp] = len(b.parts)
			b.parts = append(b.parts, tp)
		}
	}
	b.logs = make([][]kafka.Message, len(b.parts))
	b.committed = make([]int64, len(b.parts))
	return b
}

var _ kafka.Producer = (*Broker)(nil)

// Produce appends a message to the partition of topic its key hashes to.
func (b *Broker) Produce(_ context.Context, topic string, key, value []byte, headers []dlq.Header) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, ok := b.perTopic[topic]
	if !ok {
		return fmt.Errorf("simkafka: unknown topic %q", topic)
	}
	h := fnv.New32a()
	_, _ = h.Write(key)
	p := int(h.Sum32()) % n
	if p < 0 {
		p = -p
	}
	i := b.index[kafka.TopicPartition{Topic: topic, Partition: int32(p)}]
	off := int64(len(b.logs[i]))
	b.logs[i] = append(b.logs[i], kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: topic, Partition: int32(p)},
		Offset:         off, Key: key, Value: value, Headers: headers, Timestamp: time.Now(),
	})
	return nil
}

// Client returns a consumer session on the broker.
func (b *Broker) Client() *Client {
	return &Client{b: b, paused: map[kafka.TopicPartition]bool{}, pos: make([]int64, len(b.parts))}
}

// Client is one consumer's session. It is owned by a single goroutine, like the
// real client.
type Client struct {
	b        *Broker
	handler  kafka.RebalanceHandler
	assigned bool
	paused   map[kafka.TopicPartition]bool
	pos      []int64
	next     int
}

var _ kafka.Client = (*Client)(nil)

func (c *Client) SetRebalanceHandler(h kafka.RebalanceHandler) { c.handler = h }

func (c *Client) Poll(ctx context.Context, timeout time.Duration) (kafka.Event, error) {
	if !c.assigned {
		c.assigned = true
		c.b.mu.Lock()
		parts := append([]kafka.TopicPartition(nil), c.b.parts...)
		for i := range parts {
			c.pos[i] = c.b.committed[i]
		}
		c.b.mu.Unlock()
		if c.handler != nil {
			if err := c.handler.OnAssigned(parts); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	c.b.mu.Lock()
	n := len(c.pos)
	for i := 0; i < n; i++ {
		p := (c.next + i) % n
		if c.paused[c.b.parts[p]] || c.pos[p] >= int64(len(c.b.logs[p])) {
			continue
		}
		m := c.b.logs[p][c.pos[p]]
		c.pos[p]++
		c.next = p + 1
		c.b.mu.Unlock()
		return m, nil
	}
	c.b.mu.Unlock()
	d := timeout
	if d > 5*time.Millisecond {
		d = 5 * time.Millisecond
	}
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
	return nil, nil
}

func (c *Client) Pause(parts []kafka.TopicPartition) error {
	for _, p := range parts {
		c.paused[p] = true
	}
	return nil
}

func (c *Client) Resume(parts []kafka.TopicPartition) error {
	for _, p := range parts {
		delete(c.paused, p)
	}
	return nil
}

func (c *Client) Seek(tp kafka.TopicPartition, offset int64) error {
	if i, ok := c.b.index[tp]; ok {
		c.pos[i] = offset
	}
	return nil
}

func (c *Client) Commit(offs []kafka.TopicPartitionOffset) error {
	c.b.mu.Lock()
	defer c.b.mu.Unlock()
	for _, o := range offs {
		if i, ok := c.b.index[o.TopicPartition]; ok {
			c.b.committed[i] = o.Offset
		}
	}
	return nil
}
