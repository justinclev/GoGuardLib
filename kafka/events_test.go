package kafka_test

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/obs"
	"github.com/justinclev/GoGuardLib/retry"
)

// What the consumer does with a message must be visible without polling Stats,
// and must never carry message data.
func TestConsumerEmitsEventsWithoutMessageData(t *testing.T) {
	r := newRig(t)
	r.broker.produce(topic, 0, "cust-1", "SECRET-payload-1")
	r.broker.produce(topic, 0, "cust-2", "SECRET-bad")
	r.broker.produce(topic, 0, "cust-3", "SECRET-slow")
	r.setFail(func(v string) error {
		switch v {
		case "SECRET-bad":
			return retry.Permanent(errors.New("rejected"))
		case "SECRET-slow":
			return errors.New("payments 503")
		}
		return nil
	})
	var mu sync.Mutex
	var got []obs.ConsumerEvent
	sink := obs.SinkFunc(func(e obs.Event) {
		if ce, ok := e.(obs.ConsumerEvent); ok {
			mu.Lock()
			got = append(got, ce)
			mu.Unlock()
		}
	})
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.Events = sink })
	r.assign(0)
	stop := r.run(c)
	eventually(t, "the partition to advance", func() bool { return r.broker.committedOffset(topic, 0) == 3 })
	must(t, stop())

	mu.Lock()
	defer mu.Unlock()
	seen := map[obs.ConsumerCode]obs.ConsumerEvent{}
	for _, e := range got {
		seen[e.Code] = e
		if strings.Contains(e.Reason, "SECRET") {
			t.Fatalf("event carries message data: %+v", e)
		}
	}
	if e, ok := seen[obs.ConsumerParked]; !ok || e.Topic != topic || e.Offset != 1 {
		t.Errorf("parked event = %+v, %v; want offset 1 of %s", e, ok, topic)
	}
	if e, ok := seen[obs.ConsumerDeferred]; !ok || e.Offset != 2 {
		t.Errorf("deferred event = %+v, %v; want offset 2", e, ok)
	}
	if _, ok := seen[obs.ConsumerRebalance]; !ok {
		t.Error("no rebalance event for the assignment")
	}
}
