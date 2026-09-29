// Package simkafka is a tiny in-memory stand-in for a Kafka topic, used when the
// demo runs without a broker (KAFKA_BROKERS unset). It implements the same Client
// and Producer interfaces the confluent adapter does, so the consumer under test
// is the real one; only the broker is simulated.
package simkafka

import (
	"context"
	"hash/fnv"
	"sync"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/kafka"
)

// Broker holds one topic's partitions.
type Broker struct {
	topic string

	mu        sync.Mutex
	logs      [][]kafka.Message
	committed []int64
}

// New creates a broker with one topic of n partitions.
func New(topic string, partitions int) *Broker {
	return &Broker{topic: topic, logs: make([][]kafka.Message, partitions), committed: make([]int64, partitions)}
}

var _ kafka.Producer = (*Broker)(nil)

// Produce appends a message to the partition its key hashes to.
func (b *Broker) Produce(_ context.Context, topic string, key, value []byte, headers []dlq.Header) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	h := fnv.New32a()
	_, _ = h.Write(key)
	p := int(h.Sum32()) % len(b.logs)
	if p < 0 {
		p = -p
	}
	off := int64(len(b.logs[p]))
	b.logs[p] = append(b.logs[p], kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: topic, Partition: int32(p)},
		Offset:         off, Key: key, Value: value, Headers: headers, Timestamp: time.Now(),
	})
	return nil
}

// Client returns a consumer session on the broker.
func (b *Broker) Client() *Client {
	return &Client{b: b, paused: map[int32]bool{}, pos: make([]int64, len(b.logs))}
}

// Client is one consumer's session. It is owned by a single goroutine, like the
// real client.
type Client struct {
	b        *Broker
	handler  kafka.RebalanceHandler
	assigned bool
	paused   map[int32]bool
	pos      []int64
	next     int
}

var _ kafka.Client = (*Client)(nil)

func (c *Client) SetRebalanceHandler(h kafka.RebalanceHandler) { c.handler = h }

func (c *Client) Poll(ctx context.Context, timeout time.Duration) (kafka.Event, error) {
	if !c.assigned {
		c.assigned = true
		parts := make([]kafka.TopicPartition, len(c.pos))
		c.b.mu.Lock()
		for i := range parts {
			parts[i] = kafka.TopicPartition{Topic: c.b.topic, Partition: int32(i)}
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
		if c.paused[int32(p)] || c.pos[p] >= int64(len(c.b.logs[p])) {
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
		c.paused[p.Partition] = true
	}
	return nil
}

func (c *Client) Resume(parts []kafka.TopicPartition) error {
	for _, p := range parts {
		delete(c.paused, p.Partition)
	}
	return nil
}

func (c *Client) Seek(tp kafka.TopicPartition, offset int64) error {
	c.pos[tp.Partition] = offset
	return nil
}

func (c *Client) Commit(offs []kafka.TopicPartitionOffset) error {
	c.b.mu.Lock()
	defer c.b.mu.Unlock()
	for _, o := range offs {
		c.b.committed[o.Partition] = o.Offset
	}
	return nil
}
