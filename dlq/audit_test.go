package dlq_test

import (
	"context"
	"testing"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/obs"
)

func TestAuditedRecordsOperatorActionsWithTheActor(t *testing.T) {
	ctx := context.Background()
	inner := dlq.NewMemoryStore(dlq.MemoryOptions{})
	var got []obs.OperatorAction
	st := dlq.Audited(inner, dlq.AuditOptions{Events: obs.SinkFunc(func(e obs.Event) {
		if a, ok := e.(obs.OperatorAction); ok {
			got = append(got, a)
		}
	})})
	for _, id := range []string{"a", "b"} {
		if err := st.Append(ctx, dlq.Record{ID: id, State: dlq.Parked, Value: []byte("secret payload")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Requeue(dlq.WithActor(ctx, "alice"), "a"); err != nil {
		t.Fatal(err)
	}
	if err := st.Discard(dlq.WithActor(ctx, "bob"), "b"); err != nil {
		t.Fatal(err)
	}
	if err := st.Discard(ctx, "missing"); err == nil {
		t.Fatal("discarding a missing record should fail")
	}
	want := []obs.OperatorAction{
		{Action: obs.ActionRequeue, RecordID: "a", Actor: "alice", OK: true},
		{Action: obs.ActionDiscard, RecordID: "b", Actor: "bob", OK: true},
		{Action: obs.ActionDiscard, RecordID: "missing", Actor: "", OK: false},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d events, want %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		g := got[i]
		g.At = w.At
		if g != w {
			t.Errorf("event %d = %+v, want %+v", i, g, w)
		}
	}
}

func TestStoresReportTheirDurability(t *testing.T) {
	if d := dlq.StoreDurability(dlq.NewMemoryStore(dlq.MemoryOptions{})); d != dlq.DurabilityVolatile {
		t.Fatalf("memory store = %v", d)
	}
	w, err := dlq.OpenWAL(t.TempDir(), dlq.WALOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Close() }()
	if d := dlq.StoreDurability(w); d != dlq.DurabilityDurable {
		t.Fatalf("WAL with SyncAlways = %v", d)
	}
	w2, err := dlq.OpenWAL(t.TempDir(), dlq.WALOptions{Sync: dlq.SyncInterval})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w2.Close() }()
	if d := dlq.StoreDurability(dlq.Audited(w2, dlq.AuditOptions{})); d != dlq.DurabilityBuffered {
		t.Fatalf("audited WAL with SyncInterval = %v", d)
	}
}
