// Package storetest is a conformance suite for dlq.Store implementations. Every
// store, in-memory or durable, must pass it: it pins down the contract that the
// redriver and consumers rely on for correctness and for never losing data.
package storetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
)

// Clock is a manually advanced time source.
type Clock struct {
	mu sync.Mutex
	t  time.Time
}

// NewClock starts at a fixed instant.
func NewClock() *Clock { return &Clock{t: time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)} }

// Now returns the current fake time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// Advance moves time forward.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// Factory creates a fresh, empty store that reads time from now. Cleanup is
// registered with t.Cleanup by the factory if needed.
type Factory func(t *testing.T, now func() time.Time) dlq.Store

type env struct {
	t     *testing.T
	s     dlq.Store
	clock *Clock
	ctx   context.Context
}

func newEnv(t *testing.T, f Factory) *env {
	t.Helper()
	c := NewClock()
	return &env{t: t, s: f(t, c.Now), clock: c, ctx: context.Background()}
}

func rec(id string, mut ...func(*dlq.Record)) dlq.Record {
	r := dlq.Record{
		ID:        id,
		Source:    dlq.Source{Kind: "kafka", Name: "orders", Partition: 3, Offset: 42},
		Key:       []byte("key-" + id),
		Value:     []byte("value-" + id),
		Headers:   []dlq.Header{{Key: "trace", Value: []byte("t-" + id)}, {Key: "trace", Value: []byte("dup")}},
		BlockedOn: "payments",
	}
	for _, m := range mut {
		m(&r)
	}
	return r
}

func (e *env) append(r dlq.Record) {
	e.t.Helper()
	if err := e.s.Append(e.ctx, r); err != nil {
		e.t.Fatalf("Append(%s): %v", r.ID, err)
	}
}

func (e *env) lease(max int, blockedOn string) []dlq.Lease {
	e.t.Helper()
	ls, err := e.s.Lease(e.ctx, dlq.LeaseRequest{Max: max, TTL: 10 * time.Second, BlockedOn: blockedOn})
	if err != nil {
		e.t.Fatalf("Lease: %v", err)
	}
	return ls
}

func (e *env) leaseOne() dlq.Lease {
	e.t.Helper()
	ls := e.lease(1, "")
	if len(ls) != 1 {
		e.t.Fatalf("expected 1 lease, got %d", len(ls))
	}
	return ls[0]
}

func (e *env) expectNone() {
	e.t.Helper()
	if ls := e.lease(10, ""); len(ls) != 0 {
		e.t.Fatalf("expected nothing to lease, got %d (first: %s)", len(ls), ls[0].Record.ID)
	}
}

func ids(ls []dlq.Lease) []string {
	out := make([]string, len(ls))
	for i, l := range ls {
		out[i] = l.Record.ID
	}
	return out
}

func (e *env) stats() dlq.StoreStats {
	e.t.Helper()
	st, err := e.s.Stats(e.ctx)
	if err != nil {
		e.t.Fatalf("Stats: %v", err)
	}
	return st
}

func wantErr(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

// Run executes the whole suite against stores made by f.
func Run(t *testing.T, f Factory) {
	tests := []struct {
		name string
		fn   func(*env)
	}{
		{"AppendAndLease", appendAndLease},
		{"AppendIsIdempotent", appendIsIdempotent},
		{"StoreCopiesData", storeCopiesData},
		{"NilVersusEmptyValue", nilVersusEmpty},
		{"AppendValidation", appendValidation},
		{"LeaseRequestValidation", leaseRequestValidation},
		{"NextAttemptIsHonoured", nextAttempt},
		{"LeaseExpiryReoffersRecord", leaseExpiry},
		{"ExpiredButUnreclaimedLeaseCanStillFinish", expiredStillFinishes},
		{"AckRemoves", ackRemoves},
		{"FencingRejectsWrongTokenAndUnknownID", fencing},
		{"NackDelaysAndRecordsFailure", nack},
		{"ReleaseDoesNotCountAttempt", release},
		{"ParkRequeueDiscard", parkRequeueDiscard},
		{"AppendParkedDirectly", appendParked},
		{"CheckpointSurvivesRetry", checkpoint},
		{"BlockedOnFilter", blockedOnFilter},
		{"SkipExcludesDependencies", skipDependencies},
		{"NackRefundDoesNotCountTheAttempt", nackRefund},
		{"OrderKeyIsHeadOfLine", orderKey},
		{"ParkedHeadBlocksOrderedSuccessors", parkedHeadBlocks},
		{"HasOrderKeyReflectsContents", hasOrderKey},
		{"GetReturnsACopyInAnyState", getRecord},
		{"ParkedListsOldestFirstInPages", parkedPages},
		{"RequeueWithReplacesTheCheckpointAtomically", requeueWith},
		{"SeqFollowsAppendOrder", seqOrder},
		{"StatsReflectContents", statsContents},
		{"ClosedStoreRefusesWork", closed},
		{"CancelledContextIsHonoured", cancelled},
		{"ConcurrentWorkersNeverShareARecord", concurrentWorkers},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { tc.fn(newEnv(t, f)) })
	}
}

func appendAndLease(e *env) {
	for _, id := range []string{"a", "b", "c"} {
		e.append(rec(id, func(r *dlq.Record) { r.Checkpoint = []byte("cp-" + id); r.OrderKey = "" }))
	}
	got := e.lease(2, "")
	if fmt.Sprint(ids(got)) != "[a b]" {
		e.t.Fatalf("leased %v, want [a b] (oldest first)", ids(got))
	}
	l := got[0]
	r := l.Record
	if r.Attempts != 1 || r.State != dlq.Leased || l.Token == "" || !l.Until.After(e.clock.Now()) {
		e.t.Fatalf("lease metadata wrong: %+v token=%q until=%v", r, l.Token, l.Until)
	}
	if !bytes.Equal(r.Key, []byte("key-a")) || !bytes.Equal(r.Value, []byte("value-a")) || !bytes.Equal(r.Checkpoint, []byte("cp-a")) ||
		r.Source != (dlq.Source{Kind: "kafka", Name: "orders", Partition: 3, Offset: 42}) || r.BlockedOn != "payments" {
		e.t.Fatalf("record fields not preserved: %+v", r)
	}
	if len(r.Headers) != 2 || r.Headers[0].Key != "trace" || string(r.Headers[0].Value) != "t-a" || string(r.Headers[1].Value) != "dup" {
		e.t.Fatalf("ordered, repeatable headers not preserved: %+v", r.Headers)
	}
	if r.FirstFailed.IsZero() || r.SchemaVersion != dlq.CurrentSchema || got[0].Token == got[1].Token {
		e.t.Fatalf("defaults/tokens wrong: %+v", r)
	}
	if rest := e.lease(5, ""); fmt.Sprint(ids(rest)) != "[c]" {
		e.t.Fatalf("second lease = %v, want [c]", ids(rest))
	}
}

func appendIsIdempotent(e *env) {
	e.append(rec("x", func(r *dlq.Record) { r.Value = []byte("first") }))
	e.append(rec("x", func(r *dlq.Record) { r.Value = []byte("second") }))
	if st := e.stats(); st.Total() != 1 {
		e.t.Fatalf("duplicate append created %d records", st.Total())
	}
	l := e.leaseOne()
	if string(l.Record.Value) != "first" {
		e.t.Fatalf("value = %q; the original record must win", l.Record.Value)
	}
	// Still a no-op while leased and while parked.
	e.append(rec("x", func(r *dlq.Record) { r.Value = []byte("third") }))
	if st := e.stats(); st.Total() != 1 || st.Leased != 1 {
		e.t.Fatalf("append of a leased ID changed the store: %+v", st)
	}
}

func storeCopiesData(e *env) {
	in := rec("copy")
	e.append(in)
	in.Value[0] = 'X'
	in.Headers[0].Value[0] = 'X'
	in.Key[0] = 'X'

	l := e.leaseOne()
	if string(l.Record.Value) != "value-copy" || string(l.Record.Headers[0].Value) != "t-copy" || string(l.Record.Key) != "key-copy" {
		e.t.Fatalf("store aliased the caller's memory: %+v", l.Record)
	}
	l.Record.Value[0] = 'Y'
	l.Record.Headers[0].Value[0] = 'Y'
	wantErr(e.t, e.s.Nack(e.ctx, "copy", l.Token, dlq.NackOptions{}), nil)
	l2 := e.leaseOne()
	if string(l2.Record.Value) != "value-copy" || string(l2.Record.Headers[0].Value) != "t-copy" {
		e.t.Fatalf("mutating a leased record altered the store: %+v", l2.Record)
	}
}

func nilVersusEmpty(e *env) {
	e.append(rec("tomb", func(r *dlq.Record) { r.Value = nil }))
	e.append(rec("empty", func(r *dlq.Record) { r.Value = []byte{} }))
	got := e.lease(2, "")
	if got[0].Record.Value != nil {
		e.t.Fatal("a nil value (tombstone) came back non-nil")
	}
	if got[1].Record.Value == nil || len(got[1].Record.Value) != 0 {
		e.t.Fatal("an empty value came back nil")
	}
}

func appendValidation(e *env) {
	wantErr(e.t, e.s.Append(e.ctx, dlq.Record{}), dlq.ErrInvalidRecord)
	wantErr(e.t, e.s.Append(e.ctx, rec("l", func(r *dlq.Record) { r.State = dlq.Leased })), dlq.ErrInvalidRecord)
	wantErr(e.t, e.s.Append(e.ctx, rec("n", func(r *dlq.Record) { r.Attempts = -1 })), dlq.ErrInvalidRecord)
	if e.stats().Total() != 0 {
		e.t.Fatal("an invalid record was stored")
	}
}

func leaseRequestValidation(e *env) {
	for _, req := range []dlq.LeaseRequest{{Max: 0, TTL: time.Second}, {Max: 1, TTL: 0}, {Max: -1, TTL: time.Second}} {
		if _, err := e.s.Lease(e.ctx, req); !errors.Is(err, dlq.ErrInvalidRequest) {
			e.t.Fatalf("Lease(%+v) err = %v, want ErrInvalidRequest", req, err)
		}
	}
}

func nextAttempt(e *env) {
	e.append(rec("later", func(r *dlq.Record) { r.NextAttempt = e.clock.Now().Add(time.Minute) }))
	e.expectNone()
	e.clock.Advance(59 * time.Second)
	e.expectNone()
	e.clock.Advance(2 * time.Second)
	e.leaseOne()
}

func leaseExpiry(e *env) {
	e.append(rec("r"))
	first := e.leaseOne()
	e.clock.Advance(9 * time.Second)
	e.expectNone() // still held

	e.clock.Advance(2 * time.Second)
	second := e.leaseOne()
	if second.Record.ID != "r" || second.Token == first.Token || second.Record.Attempts != 2 {
		e.t.Fatalf("expired lease not re-offered correctly: %+v", second)
	}
	wantErr(e.t, e.s.Ack(e.ctx, "r", first.Token), dlq.ErrLeaseLost)
	wantErr(e.t, e.s.Nack(e.ctx, "r", first.Token, dlq.NackOptions{}), dlq.ErrLeaseLost)
	wantErr(e.t, e.s.Checkpoint(e.ctx, "r", first.Token, []byte("stale")), dlq.ErrLeaseLost)
	wantErr(e.t, e.s.Ack(e.ctx, "r", second.Token), nil)
}

func expiredStillFinishes(e *env) {
	e.append(rec("r"))
	l := e.leaseOne()
	e.clock.Advance(time.Hour) // expired, but nobody else has taken it
	wantErr(e.t, e.s.Ack(e.ctx, "r", l.Token), nil)
	if e.stats().Total() != 0 {
		e.t.Fatal("record not removed")
	}
}

func ackRemoves(e *env) {
	e.append(rec("r"))
	l := e.leaseOne()
	wantErr(e.t, e.s.Ack(e.ctx, "r", l.Token), nil)
	if e.stats().Total() != 0 {
		e.t.Fatal("record still stored after Ack")
	}
	e.expectNone()
	wantErr(e.t, e.s.Ack(e.ctx, "r", l.Token), dlq.ErrNotFound)
}

func fencing(e *env) {
	e.append(rec("r"))
	l := e.leaseOne()
	for name, err := range map[string]error{
		"ack":        e.s.Ack(e.ctx, "r", "bogus"),
		"nack":       e.s.Nack(e.ctx, "r", "bogus", dlq.NackOptions{}),
		"release":    e.s.Release(e.ctx, "r", "bogus"),
		"park":       e.s.Park(e.ctx, "r", "bogus", "why"),
		"checkpoint": e.s.Checkpoint(e.ctx, "r", "bogus", nil),
		"empty":      e.s.Ack(e.ctx, "r", ""),
	} {
		if !errors.Is(err, dlq.ErrLeaseLost) {
			e.t.Errorf("%s with a wrong token: err = %v, want ErrLeaseLost", name, err)
		}
	}
	wantErr(e.t, e.s.Ack(e.ctx, "missing", l.Token), dlq.ErrNotFound)
	wantErr(e.t, e.s.Requeue(e.ctx, "missing"), dlq.ErrNotFound)
	wantErr(e.t, e.s.Discard(e.ctx, "missing"), dlq.ErrNotFound)

	// A finished lease cannot be reused.
	wantErr(e.t, e.s.Nack(e.ctx, "r", l.Token, dlq.NackOptions{}), nil)
	wantErr(e.t, e.s.Ack(e.ctx, "r", l.Token), dlq.ErrLeaseLost)
}

func nack(e *env) {
	e.append(rec("r"))
	l := e.leaseOne()
	e.clock.Advance(time.Second)
	wantErr(e.t, e.s.Nack(e.ctx, "r", l.Token, dlq.NackOptions{Delay: 30 * time.Second, Err: "boom", BlockedOn: "inventory"}), nil)
	e.expectNone()
	e.clock.Advance(31 * time.Second)

	l2 := e.leaseOne()
	r := l2.Record
	if r.Attempts != 2 || r.LastError != "boom" || r.BlockedOn != "inventory" || !r.LastFailed.After(r.FirstFailed) {
		e.t.Fatalf("failure not recorded: %+v", r)
	}
	// A Nack without BlockedOn keeps the previous value.
	wantErr(e.t, e.s.Nack(e.ctx, "r", l2.Token, dlq.NackOptions{}), nil)
	if l3 := e.leaseOne(); l3.Record.BlockedOn != "inventory" {
		e.t.Fatalf("BlockedOn = %q, want it kept", l3.Record.BlockedOn)
	}
}

func release(e *env) {
	e.append(rec("r"))
	l := e.leaseOne()
	wantErr(e.t, e.s.Release(e.ctx, "r", l.Token), nil)
	l2 := e.leaseOne() // immediately available again
	if l2.Record.Attempts != 1 {
		e.t.Fatalf("Attempts = %d after Release, want 1 (the aborted try must not count)", l2.Record.Attempts)
	}
}

func parkRequeueDiscard(e *env) {
	e.append(rec("r"))
	l := e.leaseOne()
	wantErr(e.t, e.s.Park(e.ctx, "r", l.Token, "poison"), nil)
	e.expectNone()
	if st := e.stats(); st.Parked != 1 || st.Pending != 0 || st.Leased != 0 {
		e.t.Fatalf("stats after park: %+v", st)
	}
	wantErr(e.t, e.s.Ack(e.ctx, "r", l.Token), dlq.ErrLeaseLost)
	e.clock.Advance(24 * time.Hour)
	e.expectNone() // parked records are never retried automatically

	wantErr(e.t, e.s.Requeue(e.ctx, "r"), nil)
	l2 := e.leaseOne()
	if l2.Record.Attempts != 1 || l2.Record.LastError != "poison" {
		e.t.Fatalf("requeued record: %+v (attempts reset, last error kept)", l2.Record)
	}
	wantErr(e.t, e.s.Requeue(e.ctx, "r"), dlq.ErrNotParked)
	wantErr(e.t, e.s.Discard(e.ctx, "r"), dlq.ErrNotParked)

	wantErr(e.t, e.s.Park(e.ctx, "r", l2.Token, "again"), nil)
	wantErr(e.t, e.s.Discard(e.ctx, "r"), nil)
	if e.stats().Total() != 0 {
		e.t.Fatal("discard left the record behind")
	}
	wantErr(e.t, e.s.Discard(e.ctx, "r"), dlq.ErrNotFound)
}

func appendParked(e *env) {
	e.append(rec("bad", func(r *dlq.Record) { r.State = dlq.Parked; r.LastError = "unparseable" }))
	e.expectNone()
	if st := e.stats(); st.Parked != 1 {
		e.t.Fatalf("stats = %+v, want 1 parked", st)
	}
	wantErr(e.t, e.s.Requeue(e.ctx, "bad"), nil)
	if l := e.leaseOne(); l.Record.LastError != "unparseable" {
		e.t.Fatalf("record = %+v", l.Record)
	}
}

func checkpoint(e *env) {
	e.append(rec("r"))
	l := e.leaseOne()
	cp := []byte("step=3")
	wantErr(e.t, e.s.Checkpoint(e.ctx, "r", l.Token, cp), nil)
	cp[0] = 'X' // the store must have copied it
	wantErr(e.t, e.s.Nack(e.ctx, "r", l.Token, dlq.NackOptions{}), nil)

	l2 := e.leaseOne()
	if string(l2.Record.Checkpoint) != "step=3" {
		e.t.Fatalf("checkpoint = %q, want it to survive the retry", l2.Record.Checkpoint)
	}
	wantErr(e.t, e.s.Checkpoint(e.ctx, "r", l2.Token, []byte("step=4")), nil)
	wantErr(e.t, e.s.Park(e.ctx, "r", l2.Token, "x"), nil)
	wantErr(e.t, e.s.Requeue(e.ctx, "r"), nil)
	if l3 := e.leaseOne(); string(l3.Record.Checkpoint) != "step=4" {
		e.t.Fatalf("checkpoint = %q after park and requeue", l3.Record.Checkpoint)
	}
}

func blockedOnFilter(e *env) {
	e.append(rec("p1", func(r *dlq.Record) { r.BlockedOn = "payments" }))
	e.append(rec("i1", func(r *dlq.Record) { r.BlockedOn = "inventory" }))
	e.append(rec("p2", func(r *dlq.Record) { r.BlockedOn = "payments" }))

	if got := e.lease(10, "inventory"); fmt.Sprint(ids(got)) != "[i1]" {
		e.t.Fatalf("filtered lease = %v, want [i1]", ids(got))
	}
	if got := e.lease(10, "payments"); fmt.Sprint(ids(got)) != "[p1 p2]" {
		e.t.Fatalf("filtered lease = %v, want [p1 p2]", ids(got))
	}
	if got := e.lease(10, "nothing"); len(got) != 0 {
		e.t.Fatalf("lease for an unknown dependency returned %v", ids(got))
	}
}

func orderKey(e *env) {
	key := func(k string) func(*dlq.Record) { return func(r *dlq.Record) { r.OrderKey = k } }
	e.append(rec("a1", key("a")))
	e.append(rec("a2", key("a")))
	e.append(rec("b1", key("b")))
	e.append(rec("free", key("")))

	got := e.lease(10, "")
	if fmt.Sprint(ids(got)) != "[a1 b1 free]" {
		e.t.Fatalf("leased %v; a2 must wait behind a1 (head-of-line)", ids(got))
	}
	e.expectNone() // a2 still blocked while a1 is leased
	wantErr(e.t, e.s.Ack(e.ctx, "a1", got[0].Token), nil)
	if next := e.lease(10, ""); fmt.Sprint(ids(next)) != "[a2]" {
		e.t.Fatalf("after acking a1 leased %v, want [a2]", ids(next))
	}

	// A crashed head (expired lease) is reclaimed, and still blocks its successors.
	e.append(rec("c1", key("c")))
	e.append(rec("c2", key("c")))
	e.clock.Advance(time.Minute)
	got = e.lease(10, "")
	if len(got) != 4 {
		e.t.Fatalf("leased %v, want the four expired or free heads (b1 free a2 c1)", ids(got))
	}
	for _, l := range got {
		if l.Record.ID == "c2" {
			e.t.Fatal("c2 was leased while c1 was still in the store")
		}
	}
}

func parkedHeadBlocks(e *env) {
	key := func(r *dlq.Record) { r.OrderKey = "x" }
	e.append(rec("x1", key))
	e.append(rec("x2", key))
	l := e.leaseOne()
	wantErr(e.t, e.s.Park(e.ctx, "x1", l.Token, "poison"), nil)
	e.expectNone() // x2 must not overtake the parked x1

	wantErr(e.t, e.s.Discard(e.ctx, "x1"), nil)
	if l2 := e.leaseOne(); l2.Record.ID != "x2" {
		e.t.Fatalf("leased %s, want x2 after the head was discarded", l2.Record.ID)
	}
}

func seqOrder(e *env) {
	e.append(rec("one"))
	e.append(rec("two"))
	e.append(rec("three"))
	ls := e.lease(3, "")
	for i := 1; i < len(ls); i++ {
		if ls[i].Record.Seq <= ls[i-1].Record.Seq {
			e.t.Fatalf("Seq not increasing: %d then %d", ls[i-1].Record.Seq, ls[i].Record.Seq)
		}
	}
}

func statsContents(e *env) {
	if st := e.stats(); st.Total() != 0 || st.OldestPending != 0 {
		e.t.Fatalf("empty store stats: %+v", st)
	}
	e.append(rec("a"))
	e.clock.Advance(5 * time.Second)
	e.append(rec("b"))
	e.append(rec("c", func(r *dlq.Record) { r.State = dlq.Parked }))
	l := e.leaseOne() // leases a

	st := e.stats()
	if st.Pending != 1 || st.Leased != 1 || st.Parked != 1 || st.Total() != 3 || st.Bytes <= 0 {
		e.t.Fatalf("stats = %+v", st)
	}
	wantErr(e.t, e.s.Ack(e.ctx, "a", l.Token), nil)
	e.clock.Advance(10 * time.Second)
	if st := e.stats(); st.OldestPending < 10*time.Second || st.OldestPending > 20*time.Second {
		e.t.Fatalf("OldestPending = %v, want about 10s (record b)", st.OldestPending)
	}
}

func closed(e *env) {
	e.append(rec("r"))
	wantErr(e.t, e.s.Close(), nil)
	wantErr(e.t, e.s.Close(), nil)
	wantErr(e.t, e.s.Append(e.ctx, rec("s")), dlq.ErrClosed)
	if _, err := e.s.Lease(e.ctx, dlq.LeaseRequest{Max: 1, TTL: time.Second}); !errors.Is(err, dlq.ErrClosed) {
		e.t.Fatalf("Lease err = %v", err)
	}
	if _, err := e.s.Stats(e.ctx); !errors.Is(err, dlq.ErrClosed) {
		e.t.Fatalf("Stats err = %v", err)
	}
}

func cancelled(e *env) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	wantErr(e.t, e.s.Append(ctx, rec("r")), context.Canceled)
	if _, err := e.s.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Second}); !errors.Is(err, context.Canceled) {
		e.t.Fatalf("Lease err = %v", err)
	}
	if e.stats().Total() != 0 {
		e.t.Fatal("a cancelled Append stored a record")
	}
}

func concurrentWorkers(e *env) {
	const total = 200
	for i := 0; i < total; i++ {
		e.append(rec(fmt.Sprintf("r%03d", i)))
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				ls, err := e.s.Lease(e.ctx, dlq.LeaseRequest{Max: 7, TTL: time.Minute})
				if err != nil {
					e.t.Errorf("Lease: %v", err)
					return
				}
				if len(ls) == 0 {
					return
				}
				for _, l := range ls {
					mu.Lock()
					seen[l.Record.ID]++
					mu.Unlock()
					if err := e.s.Ack(e.ctx, l.Record.ID, l.Token); err != nil {
						e.t.Errorf("Ack(%s): %v", l.Record.ID, err)
					}
				}
			}
		}()
	}
	wg.Wait()
	if len(seen) != total {
		e.t.Fatalf("%d distinct records processed, want %d (lost records)", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			e.t.Fatalf("record %s was leased %d times concurrently", id, n)
		}
	}
	if e.stats().Total() != 0 {
		e.t.Fatal("records left after everything was acked")
	}
}

func skipDependencies(e *env) {
	e.append(rec("p1", func(r *dlq.Record) { r.BlockedOn = "payments" }))
	e.append(rec("i1", func(r *dlq.Record) { r.BlockedOn = "inventory" }))
	e.append(rec("n1", func(r *dlq.Record) { r.BlockedOn = "" }))

	lease := func(skip ...string) []string {
		ls, err := e.s.Lease(e.ctx, dlq.LeaseRequest{Max: 10, TTL: time.Second, Skip: skip})
		if err != nil {
			e.t.Fatal(err)
		}
		for _, l := range ls {
			wantErr(e.t, e.s.Release(e.ctx, l.Record.ID, l.Token), nil)
		}
		return ids(ls)
	}
	if got := lease("payments"); fmt.Sprint(got) != "[i1 n1]" {
		e.t.Fatalf("skip payments leased %v, want [i1 n1]", got)
	}
	if got := lease("payments", "inventory"); fmt.Sprint(got) != "[n1]" {
		e.t.Fatalf("skip both leased %v, want [n1]: unblocked records are always offered", got)
	}
	if got := lease(); fmt.Sprint(got) != "[p1 i1 n1]" {
		e.t.Fatalf("no skip leased %v", got)
	}
}

func nackRefund(e *env) {
	e.append(rec("r"))
	l := e.leaseOne()
	wantErr(e.t, e.s.Nack(e.ctx, "r", l.Token, dlq.NackOptions{Refund: true, BlockedOn: "inventory", Err: "circuit open", Delay: 5 * time.Second}), nil)
	e.expectNone() // the delay still applies
	e.clock.Advance(6 * time.Second)

	l2 := e.leaseOne()
	r := l2.Record
	if r.Attempts != 1 || r.BlockedOn != "inventory" || r.LastError != "circuit open" {
		e.t.Fatalf("record = %+v: a refunded nack keeps the error and BlockedOn but not the attempt", r)
	}
	wantErr(e.t, e.s.Nack(e.ctx, "r", l2.Token, dlq.NackOptions{}), nil) // an ordinary nack does count
	if l3 := e.leaseOne(); l3.Record.Attempts != 2 {
		e.t.Fatalf("Attempts = %d, want 2", l3.Record.Attempts)
	}
}

func hasOrderKey(e *env) {
	has := func(k string) bool {
		ok, err := e.s.HasOrderKey(e.ctx, k)
		if err != nil {
			e.t.Fatal(err)
		}
		return ok
	}
	if has("a") || has("") {
		e.t.Fatal("an empty store has no keys, and the empty key is never present")
	}
	key := func(k string) func(*dlq.Record) { return func(r *dlq.Record) { r.OrderKey = k } }
	e.append(rec("a1", key("a")))
	e.append(rec("plain", key("")))
	if !has("a") || has("b") || has("") {
		e.t.Fatalf("has(a)=%v has(b)=%v has(empty)=%v", has("a"), has("b"), has(""))
	}
	l := e.lease(1, "")[0] // a1 is leased, and still counts
	if !has("a") {
		e.t.Fatal("a leased record still holds its key")
	}
	wantErr(e.t, e.s.Park(e.ctx, l.Record.ID, l.Token, "x"), nil)
	if !has("a") {
		e.t.Fatal("a parked record still holds its key")
	}
	wantErr(e.t, e.s.Discard(e.ctx, "a1"), nil)
	if has("a") {
		e.t.Fatal("the key must be free once its last record is gone")
	}
}

func getRecord(e *env) {
	e.append(rec("r", func(r *dlq.Record) { r.Checkpoint = []byte("cp") }))
	got, err := e.s.Get(e.ctx, "r")
	if err != nil || got.State != dlq.Pending || string(got.Value) != "value-r" || string(got.Checkpoint) != "cp" || got.BlockedOn != "payments" {
		e.t.Fatalf("Get = %+v, %v", got, err)
	}
	got.Value[0] = 'X' // the caller must not be able to change the store through it
	got.Headers[0].Value[0] = 'X'
	if again, _ := e.s.Get(e.ctx, "r"); string(again.Value) != "value-r" || string(again.Headers[0].Value) != "t-r" {
		e.t.Fatal("Get returned memory shared with the store")
	}

	l := e.leaseOne()
	if got, _ := e.s.Get(e.ctx, "r"); got.State != dlq.Leased || got.Attempts != 1 {
		e.t.Fatalf("leased record: %+v", got)
	}
	wantErr(e.t, e.s.Park(e.ctx, "r", l.Token, "poison"), nil)
	if got, _ := e.s.Get(e.ctx, "r"); got.State != dlq.Parked || got.LastError != "poison" {
		e.t.Fatalf("parked record: %+v", got)
	}
	if _, err := e.s.Get(e.ctx, "missing"); !errors.Is(err, dlq.ErrNotFound) {
		e.t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func parkedPages(e *env) {
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("p%d", i)
		e.append(rec(id, func(r *dlq.Record) { r.State = dlq.Parked; r.LastError = "why " + id }))
		if i == 1 {
			e.append(rec("pending")) // not parked: never listed
		}
	}
	list := func(after uint64, limit int) []dlq.Record {
		out, err := e.s.Parked(e.ctx, dlq.ParkedQuery{After: after, Limit: limit})
		if err != nil {
			e.t.Fatal(err)
		}
		return out
	}
	names := func(rs []dlq.Record) string {
		var ns []string
		for _, r := range rs {
			ns = append(ns, r.ID)
		}
		return fmt.Sprint(ns)
	}
	page1 := list(0, 2)
	if names(page1) != "[p0 p1]" {
		e.t.Fatalf("first page = %s", names(page1))
	}
	page2 := list(page1[len(page1)-1].Seq, 2)
	if names(page2) != "[p2 p3]" {
		e.t.Fatalf("second page = %s", names(page2))
	}
	if all := list(0, 0); names(all) != "[p0 p1 p2 p3 p4]" || all[0].LastError != "why p0" || string(all[0].Value) != "value-p0" {
		e.t.Fatalf("all = %s %+v", names(all), all[0])
	}
	if last := list(page2[len(page2)-1].Seq, 100); names(last) != "[p4]" {
		e.t.Fatalf("last page = %s", names(last))
	}
	if none := list(1<<40, 10); len(none) != 0 {
		e.t.Fatalf("past the end = %s", names(none))
	}
}

func requeueWith(e *env) {
	parked := func(id, cp string) {
		e.append(rec(id, func(r *dlq.Record) { r.State = dlq.Parked; r.Checkpoint = []byte(cp) }))
	}
	leaseCheckpoint := func() (string, int) {
		l := e.leaseOne()
		cp, attempts := string(l.Record.Checkpoint), l.Record.Attempts
		wantErr(e.t, e.s.Release(e.ctx, l.Record.ID, l.Token), nil)
		return cp, attempts
	}

	parked("a", "old")
	newCP := []byte("new")
	wantErr(e.t, e.s.RequeueWith(e.ctx, "a", dlq.RequeueOptions{ReplaceCheckpoint: true, Checkpoint: newCP}), nil)
	newCP[0] = 'X' // the store must have copied it
	if cp, attempts := leaseCheckpoint(); cp != "new" || attempts != 1 {
		e.t.Fatalf("checkpoint %q attempts %d, want the replacement and a fresh attempt count", cp, attempts)
	}

	// Finish "a", so that only the records below are in play.
	l := e.leaseOne()
	wantErr(e.t, e.s.Ack(e.ctx, "a", l.Token), nil)

	// Without ReplaceCheckpoint the checkpoint is left alone.
	parked("b", "keep")
	wantErr(e.t, e.s.Requeue(e.ctx, "b"), nil)
	if cp, _ := leaseCheckpoint(); cp != "keep" {
		e.t.Fatalf("checkpoint = %q, want it kept", cp)
	}
	// "b" is pending now, not parked.
	wantErr(e.t, e.s.RequeueWith(e.ctx, "b", dlq.RequeueOptions{ReplaceCheckpoint: true, Checkpoint: []byte("ignored")}), dlq.ErrNotParked)

	// A nil replacement clears the checkpoint.
	parked("c", "to-clear")
	wantErr(e.t, e.s.RequeueWith(e.ctx, "c", dlq.RequeueOptions{ReplaceCheckpoint: true}), nil)
	got, _ := e.s.Get(e.ctx, "c")
	if len(got.Checkpoint) != 0 || got.State != dlq.Pending || got.Attempts != 0 {
		e.t.Fatalf("record = %+v, want an empty checkpoint, pending, attempts reset", got)
	}

	wantErr(e.t, e.s.RequeueWith(e.ctx, "missing", dlq.RequeueOptions{ReplaceCheckpoint: true}), dlq.ErrNotFound)
}
