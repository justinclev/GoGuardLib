package dlq_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/dlq/storetest"
)

func TestMemoryStoreConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T, now func() time.Time) dlq.Store {
		return dlq.NewMemoryStore(dlq.MemoryOptions{Clock: now})
	})
}

func rec(id string, size int) dlq.Record {
	return dlq.Record{ID: id, Value: make([]byte, size)}
}

// A full store must refuse, never drop: the caller has to pause.
func TestMemoryStoreCapacityNeverEvicts(t *testing.T) {
	ctx := context.Background()
	s := dlq.NewMemoryStore(dlq.MemoryOptions{MaxRecords: 2})
	for _, id := range []string{"a", "b"} {
		if err := s.Append(ctx, rec(id, 1)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Append(ctx, rec("c", 1)); !errors.Is(err, dlq.ErrFull) {
		t.Fatalf("err = %v, want ErrFull", err)
	}
	if err := s.Append(ctx, rec("a", 1)); err != nil {
		t.Fatalf("re-appending an existing ID must succeed even when full: %v", err)
	}
	st, _ := s.Stats(ctx)
	if st.Total() != 2 {
		t.Fatalf("total = %d; a full store must keep every record it accepted", st.Total())
	}

	// Space frees up when a record is finished.
	ls, _ := s.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Minute})
	if err := s.Ack(ctx, ls[0].Record.ID, ls[0].Token); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, rec("c", 1)); err != nil {
		t.Fatalf("append after freeing space: %v", err)
	}
}

func TestMemoryStoreByteLimits(t *testing.T) {
	ctx := context.Background()
	s := dlq.NewMemoryStore(dlq.MemoryOptions{MaxBytes: 1000, MaxRecordBytes: 600})
	if err := s.Append(ctx, rec("huge", 700)); !errors.Is(err, dlq.ErrInvalidRecord) {
		t.Fatalf("oversized record: %v", err)
	}
	if err := s.Append(ctx, rec("a", 480)); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, rec("b", 480)); !errors.Is(err, dlq.ErrFull) {
		t.Fatalf("byte cap: err = %v, want ErrFull", err)
	}

	ls, _ := s.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Minute})
	if err := s.Checkpoint(ctx, "a", ls[0].Token, make([]byte, 5000)); !errors.Is(err, dlq.ErrFull) && !errors.Is(err, dlq.ErrInvalidRecord) {
		t.Fatalf("checkpoint beyond limits: %v", err)
	}
	if err := s.Checkpoint(ctx, "a", ls[0].Token, []byte("ok")); err != nil {
		t.Fatalf("small checkpoint: %v", err)
	}
}

func TestMemoryStoreByteAccountingReturnsToZero(t *testing.T) {
	ctx := context.Background()
	s := dlq.NewMemoryStore(dlq.MemoryOptions{})
	r := dlq.Record{ID: "k", OrderKey: "o", Value: []byte("v"), Headers: []dlq.Header{{Key: "h", Value: []byte("x")}}}
	if err := s.Append(ctx, r); err != nil {
		t.Fatal(err)
	}
	ls, _ := s.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Minute})
	_ = s.Checkpoint(ctx, "k", ls[0].Token, []byte("progress"))
	_ = s.Nack(ctx, "k", ls[0].Token, dlq.NackOptions{Err: "some failure text"})
	ls, _ = s.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Minute})
	_ = s.Ack(ctx, "k", ls[0].Token)
	if st, _ := s.Stats(ctx); st.Bytes != 0 || st.Total() != 0 {
		t.Fatalf("stats after draining = %+v, want zero bytes", st)
	}
}
