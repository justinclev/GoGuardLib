package kafka_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/retry"
	"time"
)

// A message the store can never hold (too large) and that cannot be processed
// must not wedge its partition silently: retrying it forever changes nothing.
func unstorableRig(t *testing.T) *rig {
	r := newRig(t)
	r.store = dlq.NewMemoryStore(dlq.MemoryOptions{MaxRecordBytes: 64})
	r.broker.produce(topic, 0, "a", "before")
	r.broker.produce(topic, 0, "b", strings.Repeat("x", 4096)) // over the store's limit
	r.broker.produce(topic, 0, "c", "after")
	r.setFail(func(v string) error {
		if len(v) > 100 {
			return errors.New("payments 503")
		}
		return nil
	})
	return r
}

func TestUnstorableMessageHaltsTheConsumerByDefault(t *testing.T) {
	r := unstorableRig(t)
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.CommitInterval = 1 })
	r.assign(0)
	err := <-r.runAsync(c)
	if !errors.Is(err, kafka.ErrUnstorable) {
		t.Fatalf("Run = %v, want ErrUnstorable", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "orders[0]") || !strings.Contains(msg, "offset 1") || strings.Contains(msg, "xxxx") {
		t.Fatalf("error must name the message position and never its payload: %q", msg)
	}
	if got := r.broker.committedOffset(topic, 0); got > 1 {
		t.Fatalf("committed %d: the unstorable message (offset 1) must not be committed past", got)
	}
}

func TestUnstorableSkipCountsAndMovesOn(t *testing.T) {
	r := unstorableRig(t)
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.OnUnstorable = kafka.UnstorableSkip })
	r.assign(0)
	stop := r.run(c)
	eventually(t, "the partition to advance past the message", func() bool { return r.broker.committedOffset(topic, 0) == 3 })
	must(t, stop())
	if st := c.Stats(); st.Unstorable != 1 {
		t.Fatalf("Stats.Unstorable = %d, want 1", st.Unstorable)
	}
	if got := strings.Join(r.done(), ","); got != "before,after" {
		t.Fatalf("processed %q, want before,after", got)
	}
}

// hangingProducer never answers until its context ends.
type hangingProducer struct{}

func (hangingProducer) Produce(ctx context.Context, _ string, _, _ []byte, _ []dlq.Header) error {
	<-ctx.Done()
	return ctx.Err()
}

// A dead-letter producer that hangs must not hold the partition.
func TestAHangingMirrorIsBoundedByMirrorTimeout(t *testing.T) {
	r := newRig(t)
	pub, err := kafka.NewDLQPublisher(kafka.PublisherConfig{Producer: hangingProducer{}})
	must(t, err)
	r.broker.produce(topic, 0, "k", "bad")
	r.broker.produce(topic, 0, "k2", "fine")
	r.setFail(func(v string) error {
		if v == "bad" {
			return retry.Permanent(errors.New("nope"))
		}
		return nil
	})
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.Mirror = pub; cfg.MirrorTimeout = 50 * time.Millisecond })
	r.assign(0)
	r.run(c)
	eventually(t, "the partition to advance past the hung mirror", func() bool { return r.broker.committedOffset(topic, 0) == 2 })
	if c.Stats().MirrorErrors != 1 {
		t.Fatalf("MirrorErrors = %d, want 1", c.Stats().MirrorErrors)
	}
}

// Recreating a topic restarts its offsets. A stored record from the old topic
// must not make the same offset on the new one look like a duplicate.
func TestIDNamespaceKeepsRecreatedTopicsApart(t *testing.T) {
	if dlq.KafkaIDIn("cluster-a", "orders", 0, 7) == dlq.KafkaIDIn("cluster-b", "orders", 0, 7) {
		t.Fatal("different namespaces produced the same ID")
	}
	if dlq.KafkaIDIn("", "orders", 0, 7) != dlq.KafkaID("orders", 0, 7) {
		t.Fatal("an empty namespace must equal KafkaID, so existing stores keep matching")
	}
	if dlq.KafkaIDIn("a", "b", 0, 1) == dlq.KafkaIDIn("", "a", 0, 1) {
		t.Fatal("a namespace must not collide with a topic name")
	}
}
