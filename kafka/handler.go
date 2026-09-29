package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/pipeline"
	"github.com/justinclev/GoGuardLib/retry"
)

// HandlerConfig describes a topic handled by one function, for services that have a
// single "process this message" function and no multi-step workflow.
type HandlerConfig struct {
	// Topic is required.
	Topic string
	// Name identifies the handler in checkpoints and in the dependency name that
	// releases deferred messages. It must not change while messages may still be
	// stored. Default: Topic.
	Name string
	// Handle processes one message. Required. Return nil when the message is fully
	// handled. Return retry.Permanent(err) for a failure that retrying cannot fix (the
	// message is then parked for a person). Any other error is transient: the message
	// is stored durably and retried later, and later messages with the same key wait
	// behind it, so per-key order holds.
	//
	// Handle may run again for a message it already partly handled (after a crash, or
	// a timeout that fired after the work was done), so it must be idempotent. Use
	// Message.ID or pipeline.IdempotencyKey to deduplicate downstream calls.
	Handle func(ctx context.Context, m HandledMessage) error
	// Breaker guards Handle. While it is open the handler is not called, the topic
	// pauses, and messages wait in Kafka. Optional but recommended.
	Breaker *breaker.Breaker
	// Timeout bounds one call of Handle. Default 30s.
	Timeout time.Duration
	// Retry retries Handle in-process before the message is deferred. Optional. Keep
	// it short: the partition waits while it runs.
	Retry retry.Policy
}

// HandledMessage is the message passed to a HandlerConfig's Handle. Treat it as read-only.
type HandledMessage struct {
	// ID is stable across redeliveries and redrives of the same Kafka message
	// (topic, partition, offset). Use it for idempotency.
	ID      string
	Source  dlq.Source
	Key     []byte
	Value   []byte
	Headers []dlq.Header
	// Attempt is 0 on the first live run and counts redrives after that.
	Attempt int
}

// HandlerBinding builds a Binding for a single handler function. It gives an
// existing "poll, then process" service the consumer's guarantees (contiguous
// commits, a durable store for messages that could not be handled, per-key order,
// pause while a dependency is down, resume after a crash) without writing a
// pipeline. A Store is still required in Config: there is deliberately no default,
// because a message set aside in memory is lost on restart.
func HandlerBinding(c HandlerConfig) (Binding, error) {
	if c.Topic == "" {
		return Binding{}, errors.New("kafka: HandlerConfig.Topic is required")
	}
	if c.Handle == nil {
		return Binding{}, errors.New("kafka: HandlerConfig.Handle is required")
	}
	name := c.Name
	if name == "" {
		name = c.Topic
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	p, err := pipeline.New(name, "v1", []pipeline.Step{{
		Name: "handle", Breaker: c.Breaker, Timeout: timeout, Retry: c.Retry,
		Run: func(ctx context.Context, x *pipeline.Exec) error {
			return c.Handle(ctx, HandledMessage{ID: x.ID, Source: x.Source, Key: x.Key, Value: x.Value, Headers: x.Headers, Attempt: x.Attempt})
		},
	}})
	if err != nil {
		return Binding{}, err
	}
	return Binding{Topic: c.Topic, Pipeline: p}, nil
}
