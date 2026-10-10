package dlq_test

import (
	"context"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
)

// An operator must be able to see that parked records are stopping other work.
func TestStatsShowBlockedKeysOldestParkedAndDependencies(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	s := dlq.NewMemoryStore(dlq.MemoryOptions{Clock: func() time.Time { return now }})
	add := func(r dlq.Record) {
		t.Helper()
		if err := s.Append(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	old := now.Add(-2 * time.Hour)
	add(dlq.Record{ID: "p1", State: dlq.Parked, OrderKey: "alice", FirstFailed: old})
	add(dlq.Record{ID: "p2", State: dlq.Parked, OrderKey: "bob", FirstFailed: now.Add(-time.Minute)})
	add(dlq.Record{ID: "p3", State: dlq.Parked, OrderKey: "bob", FirstFailed: now}) // behind p2: bob counts once
	add(dlq.Record{ID: "w1", OrderKey: "alice", BlockedOn: "payments"})             // stuck behind p1
	add(dlq.Record{ID: "w2", BlockedOn: "payments"})
	add(dlq.Record{ID: "w3", BlockedOn: "shipping"})

	st, err := s.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.BlockedKeys != 2 {
		t.Errorf("BlockedKeys = %d, want 2 (alice and bob)", st.BlockedKeys)
	}
	if st.OldestParked != 2*time.Hour {
		t.Errorf("OldestParked = %v, want 2h", st.OldestParked)
	}
	if st.ByDependency["payments"] != 2 || st.ByDependency["shipping"] != 1 || len(st.ByDependency) != 2 {
		t.Errorf("ByDependency = %v", st.ByDependency)
	}
}
