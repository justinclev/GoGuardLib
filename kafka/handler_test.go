package kafka_test

import (
	"context"
	"errors"
	"testing"

	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/retry"
)

func TestHandlerBindingValidates(t *testing.T) {
	if _, err := kafka.HandlerBinding(kafka.HandlerConfig{Handle: func(context.Context, kafka.HandledMessage) error { return nil }}); err == nil {
		t.Fatal("a topic is required")
	}
	if _, err := kafka.HandlerBinding(kafka.HandlerConfig{Topic: topic}); err == nil {
		t.Fatal("a handler is required")
	}
}

// A handler that fails must not lose the message and must not let later messages for the
// same key overtake it.
func TestHandlerFailureStoresAndKeepsOrder(t *testing.T) {
	r := newRig(t)
	var handled []string
	var failing = true
	b, err := kafka.HandlerBinding(kafka.HandlerConfig{
		Topic: topic,
		Handle: func(_ context.Context, m kafka.HandledMessage) error {
			if failing && string(m.Value) == "a" {
				return errors.New("downstream 503")
			}
			handled = append(handled, string(m.Value))
			return nil
		},
	})
	must(t, err)
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.Bindings = []kafka.Binding{b} })
	r.assign(0)
	for _, v := range []string{"a", "b", "c"} {
		r.broker.produce(topic, 0, "same-key", v)
	}
	stop := r.run(c)
	eventually(t, "the failed message and those behind it to be stored", func() bool { return r.storeStats().Pending == 3 })
	if len(handled) != 0 {
		t.Fatalf("nothing may overtake the failed message: %v", handled)
	}
	must(t, stop())
	if r.broker.committedOffset(topic, 0) != 3 {
		t.Fatalf("stored messages are safe, so the offset may advance: %d", r.broker.committedOffset(topic, 0))
	}
}

func TestHandlerPermanentErrorIsParkedNotRetried(t *testing.T) {
	r := newRig(t)
	calls := 0
	b, err := kafka.HandlerBinding(kafka.HandlerConfig{
		Topic: topic,
		Handle: func(context.Context, kafka.HandledMessage) error {
			calls++
			return retry.Permanent(errors.New("invalid member id"))
		},
	})
	must(t, err)
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.Bindings = []kafka.Binding{b} })
	r.assign(0)
	r.broker.produce(topic, 0, "k", "bad")
	r.run(c)
	eventually(t, "the message to be parked", func() bool { return r.storeStats().Parked == 1 })
	if calls != 1 {
		t.Fatalf("a permanent failure must not be retried: %d calls", calls)
	}
}
