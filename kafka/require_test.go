package kafka_test

import (
	"errors"
	"testing"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/kafka"
)

func TestRequireDurableStoreRefusesAStoreThatForgets(t *testing.T) {
	r := newRig(t) // a MemoryStore
	if _, err := kafka.NewConsumer(r.config(func(c *kafka.Config) { c.RequireDurableStore = true })); !errors.Is(err, kafka.ErrStoreNotDurable) {
		t.Fatalf("NewConsumer with a memory store = %v, want ErrStoreNotDurable", err)
	}

	for _, sync := range []dlq.SyncPolicy{dlq.SyncInterval, dlq.SyncNone} {
		w, err := dlq.OpenWAL(t.TempDir(), dlq.WALOptions{Sync: sync})
		must(t, err)
		r.store = w
		if _, err := kafka.NewConsumer(r.config(func(c *kafka.Config) { c.RequireDurableStore = true })); !errors.Is(err, kafka.ErrStoreNotDurable) {
			t.Errorf("NewConsumer with sync policy %d = %v, want ErrStoreNotDurable", sync, err)
		}
		must(t, w.Close())
	}

	w, err := dlq.OpenWAL(t.TempDir(), dlq.WALOptions{})
	must(t, err)
	defer func() { _ = w.Close() }()
	r.store = w
	if _, err := kafka.NewConsumer(r.config(func(c *kafka.Config) { c.RequireDurableStore = true })); err != nil {
		t.Fatalf("a WAL with SyncAlways must pass: %v", err)
	}
}

func TestRequireEncryptionRefusesAPlaintextMirror(t *testing.T) {
	if _, err := kafka.NewDLQPublisher(kafka.PublisherConfig{Producer: &fakeProducer{}, RequireEncryption: true}); !errors.Is(err, kafka.ErrEncryptionRequired) {
		t.Fatalf("NewDLQPublisher = %v, want ErrEncryptionRequired", err)
	}
	if _, err := kafka.NewDLQPublisher(kafka.PublisherConfig{Producer: &fakeProducer{}}); err != nil {
		t.Fatalf("the default must stay permissive for now: %v", err)
	}
}
