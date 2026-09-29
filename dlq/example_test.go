package dlq_test

import (
	"context"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/secure"
)

// Set a failed message aside, then work through it later.
func Example() {
	ctx := context.Background()
	var key [secure.KeySize]byte // load a real key from your secret store
	enc, _ := secure.NewAESGCM(secure.Key{ID: "2025-06", Material: key[:]})
	store, _ := dlq.Secure(dlq.NewMemoryStore(dlq.MemoryOptions{MaxRecords: 100_000}), dlq.SecureOptions{Encryptor: enc})
	defer store.Close()

	// The ID makes a redelivery after a crash a no-op.
	err := store.Append(ctx, dlq.Record{
		ID:        dlq.KafkaID("orders", 3, 1042),
		Source:    dlq.Source{Kind: "kafka", Name: "orders", Partition: 3, Offset: 1042},
		Value:     []byte(`{"order":"o-1"}`),
		BlockedOn: "payments",
	})
	if err != nil { // dlq.ErrFull: pause consuming instead of dropping the message
		return
	}

	leases, _ := store.Lease(ctx, dlq.LeaseRequest{BlockedOn: "payments", Max: 10, TTL: time.Minute})
	for _, l := range leases {
		if handle(l.Record) == nil {
			_ = store.Ack(ctx, l.Record.ID, l.Token)
		} else {
			_ = store.Nack(ctx, l.Record.ID, l.Token, dlq.NackOptions{Delay: 30 * time.Second, Err: "payments still failing"})
		}
	}
}

func handle(dlq.Record) error { return nil }

// A durable queue that survives crashes and power loss.
func ExampleOpenWAL() {
	store, err := dlq.OpenWAL("/var/lib/myapp/dlq", dlq.WALOptions{MaxBytes: 2 << 30})
	if err != nil {
		// ErrLocked: another process has it. ErrCorrupt: damage recovery will not
		// guess about; see the error for the file and offset.
		return
	}
	defer store.Close()

	rep := store.Recovery() // what was found and repaired at startup
	_ = rep.TruncatedBytes  // alert on this: a torn tail means the last process died mid-write
}
