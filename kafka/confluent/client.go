// Package confluent adapts confluent-kafka-go to the kafka package's Client and
// Producer interfaces. It needs cgo; confluent-kafka-go bundles librdkafka for the
// common platforms.
package confluent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/kafka"
)

// Client is a kafka.Client backed by a confluent-kafka-go consumer.
type Client struct {
	c       *ck.Consumer
	handler atomic.Pointer[kafka.RebalanceHandler]
	failure atomic.Pointer[error] // an error from the handler, reported by the next Poll
	onError func(error)

	requireTLS bool
}

// Option customises NewClient.
type Option func(*Client)

// WithErrorHandler receives the errors librdkafka retries by itself (a broker
// down, a connection lost, an authentication problem). The consumer keeps running
// through them, so without a handler an unreachable cluster looks like an idle
// topic. The handler is called on the polling goroutine and must not block; the
// error text can name broker addresses but never message data.
func WithErrorHandler(f func(error)) Option { return func(c *Client) { c.onError = f } }

// ErrInsecureTransport is returned when WithRequireTLS or WithProducerRequireTLS is
// set and security.protocol is not ssl or sasl_ssl. librdkafka defaults to
// plaintext, so a configuration that forgets the setting would otherwise send
// messages, and SASL credentials, in the clear without any warning.
var ErrInsecureTransport = errors.New("confluent: security.protocol must be ssl or sasl_ssl")

// WithRequireTLS makes NewClient fail with ErrInsecureTransport unless
// security.protocol is ssl or sasl_ssl.
func WithRequireTLS() Option { return func(c *Client) { c.requireTLS = true } }

// checkTLS enforces the requirement on a configuration.
func checkTLS(cfg ck.ConfigMap) error {
	v, _ := cfg.Get("security.protocol", "plaintext")
	p, _ := v.(string)
	switch strings.ToLower(p) {
	case "ssl", "sasl_ssl":
		return nil
	}
	return fmt.Errorf("%w (got %q)", ErrInsecureTransport, p)
}

var _ kafka.Client = (*Client)(nil)

// NewClient creates a consumer subscribed to topics.
//
// cfg is your usual configuration (bootstrap.servers, group.id, security.protocol,
// sasl.*, ssl.*, and so on: TLS and SASL are entirely yours to configure, and
// nothing here logs it). Two settings are forced because the kafka package must
// decide when an offset is safe: enable.auto.commit=false and
// enable.auto.offset.store=false. Set auto.offset.reset (earliest is usual)
// yourself. Both the eager and the cooperative rebalance protocols work.
func NewClient(cfg ck.ConfigMap, topics []string, opts ...Option) (*Client, error) {
	if _, err := cfg.Get("group.id", nil); err != nil {
		return nil, err
	}
	if g, _ := cfg.Get("group.id", ""); g == "" {
		return nil, errors.New("confluent: group.id is required")
	}
	cl := &Client{}
	for _, o := range opts {
		o(cl)
	}
	if cl.requireTLS {
		if err := checkTLS(cfg); err != nil {
			return nil, err
		}
	}
	cm := ck.ConfigMap{}
	for k, v := range cfg {
		cm[k] = v
	}
	for k, v := range map[string]any{
		"enable.auto.commit":              false,
		"enable.auto.offset.store":        false,
		"go.application.rebalance.enable": true,
		"go.logs.channel.enable":          false,
	} {
		if err := cm.SetKey(k, v); err != nil {
			return nil, err
		}
	}
	c, err := ck.NewConsumer(&cm)
	if err != nil {
		return nil, fmt.Errorf("confluent: creating consumer: %w", err)
	}
	cl.c = c
	if err := c.SubscribeTopics(topics, cl.rebalance); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("confluent: subscribing: %w", err)
	}
	return cl, nil
}

// SetRebalanceHandler implements kafka.Client.
func (c *Client) SetRebalanceHandler(h kafka.RebalanceHandler) { c.handler.Store(&h) }

// rebalance is confluent-kafka-go's rebalance callback. It runs inside Poll. It
// tells the handler and returns nil, which makes the client itself complete the
// assignment or revocation (incrementally under the cooperative protocol) once we
// return; the handler has therefore had the chance to commit first.
func (c *Client) rebalance(_ *ck.Consumer, ev ck.Event) error {
	hp := c.handler.Load()
	if hp == nil {
		return nil
	}
	var err error
	switch e := ev.(type) {
	case ck.AssignedPartitions:
		err = (*hp).OnAssigned(toTPs(e.Partitions))
	case ck.RevokedPartitions:
		err = (*hp).OnRevoked(toTPs(e.Partitions))
	}
	if err != nil {
		c.failure.CompareAndSwap(nil, &err) // Poll reports it; a callback cannot
	}
	return nil
}

// Close leaves the group and releases the consumer.
func (c *Client) Close() error { return c.c.Close() }

func toTP(p ck.TopicPartition) kafka.TopicPartition {
	t := ""
	if p.Topic != nil {
		t = *p.Topic
	}
	return kafka.TopicPartition{Topic: t, Partition: p.Partition}
}

func toCK(parts []kafka.TopicPartition) []ck.TopicPartition {
	out := make([]ck.TopicPartition, len(parts))
	for i, p := range parts {
		t := p.Topic
		out[i] = ck.TopicPartition{Topic: &t, Partition: p.Partition, Offset: ck.OffsetInvalid}
	}
	return out
}

func toTPs(parts []ck.TopicPartition) []kafka.TopicPartition {
	out := make([]kafka.TopicPartition, len(parts))
	for i, p := range parts {
		out[i] = toTP(p)
	}
	return out
}

// Poll implements kafka.Client.
func (c *Client) Poll(ctx context.Context, timeout time.Duration) (kafka.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil
	}
	if f := c.failure.Load(); f != nil {
		return nil, *f
	}
	if timeout > 100*time.Millisecond {
		timeout = 100 * time.Millisecond // stay responsive to ctx; a poll cannot be interrupted
	}
	ev := c.c.Poll(int(timeout / time.Millisecond)) // a rebalance callback may run in here
	if f := c.failure.Load(); f != nil {
		return nil, *f
	}
	switch e := ev.(type) {
	case *ck.Message:
		if e.TopicPartition.Error != nil {
			// A message that could not be delivered must not be skipped: stop and say why.
			return nil, fmt.Errorf("confluent: message error on %s[%d]: %w", toTP(e.TopicPartition).Topic, e.TopicPartition.Partition, e.TopicPartition.Error)
		}
		m := kafka.Message{
			TopicPartition: toTP(e.TopicPartition), Offset: int64(e.TopicPartition.Offset),
			Key: e.Key, Value: e.Value, Timestamp: e.Timestamp,
		}
		for _, h := range e.Headers {
			m.Headers = append(m.Headers, dlq.Header{Key: h.Key, Value: h.Value})
		}
		return m, nil
	case ck.Error:
		if e.IsFatal() {
			return nil, e
		}
		if c.onError != nil {
			c.onError(e)
		}
		return nil, nil // librdkafka retries transient errors (a broker down, a timeout) itself
	default:
		return nil, nil // stats, committed-offset notifications, and so on
	}
}

// Pause implements kafka.Client.
func (c *Client) Pause(parts []kafka.TopicPartition) error { return c.c.Pause(toCK(parts)) }

// Resume implements kafka.Client.
func (c *Client) Resume(parts []kafka.TopicPartition) error { return c.c.Resume(toCK(parts)) }

// Seek implements kafka.Client.
func (c *Client) Seek(tp kafka.TopicPartition, offset int64) error {
	t := tp.Topic
	return c.c.Seek(ck.TopicPartition{Topic: &t, Partition: tp.Partition, Offset: ck.Offset(offset)}, 5000)
}

// Commit implements kafka.Client with a synchronous commit.
func (c *Client) Commit(offsets []kafka.TopicPartitionOffset) error {
	cko := make([]ck.TopicPartition, len(offsets))
	for i, o := range offsets {
		t := o.Topic
		cko[i] = ck.TopicPartition{Topic: &t, Partition: o.Partition, Offset: ck.Offset(o.Offset)}
	}
	_, err := c.c.CommitOffsets(cko)
	return err
}

type producerOptions struct{ requireTLS bool }

// ProducerOption customises NewProducer.
type ProducerOption func(*producerOptions)

// WithProducerRequireTLS makes NewProducer fail with ErrInsecureTransport unless
// security.protocol is ssl or sasl_ssl.
func WithProducerRequireTLS() ProducerOption { return func(o *producerOptions) { o.requireTLS = true } }

// Producer is a kafka.Producer backed by an idempotent confluent-kafka-go producer.
type Producer struct{ p *ck.Producer }

var _ kafka.Producer = (*Producer)(nil)

// NewProducer creates a producer for dead-letter topics. It forces
// enable.idempotence=true and acks=all so a retried publish cannot duplicate or be
// silently dropped. Connection, TLS and SASL settings are yours.
func NewProducer(cfg ck.ConfigMap, opts ...ProducerOption) (*Producer, error) {
	var po producerOptions
	for _, o := range opts {
		o(&po)
	}
	if po.requireTLS {
		if err := checkTLS(cfg); err != nil {
			return nil, err
		}
	}
	cm := ck.ConfigMap{}
	for k, v := range cfg {
		cm[k] = v
	}
	for k, v := range map[string]any{"enable.idempotence": true, "acks": "all", "go.logs.channel.enable": false} {
		if err := cm.SetKey(k, v); err != nil {
			return nil, err
		}
	}
	p, err := ck.NewProducer(&cm)
	if err != nil {
		return nil, fmt.Errorf("confluent: creating producer: %w", err)
	}
	return &Producer{p: p}, nil
}

// Produce implements kafka.Producer: it returns once the broker has acknowledged
// the message, or with the delivery error.
func (p *Producer) Produce(ctx context.Context, topic string, key, value []byte, headers []dlq.Header) error {
	hs := make([]ck.Header, len(headers))
	for i, h := range headers {
		hs[i] = ck.Header{Key: h.Key, Value: h.Value}
	}
	delivery := make(chan ck.Event, 1)
	msg := &ck.Message{TopicPartition: ck.TopicPartition{Topic: &topic, Partition: ck.PartitionAny}, Key: key, Value: value, Headers: hs}
	if err := p.p.Produce(msg, delivery); err != nil {
		return err
	}
	select {
	case ev := <-delivery:
		if m, ok := ev.(*ck.Message); ok && m.TopicPartition.Error != nil {
			return m.TopicPartition.Error
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flush waits for outstanding messages, up to timeout.
func (p *Producer) Flush(timeout time.Duration) int {
	return p.p.Flush(int(timeout / time.Millisecond))
}

// Close flushes briefly and releases the producer.
func (p *Producer) Close() {
	p.p.Flush(5000)
	p.p.Close()
}
