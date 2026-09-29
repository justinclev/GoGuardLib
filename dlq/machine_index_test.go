package dlq

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// naiveSelect is the specification of selectLease: walk every record in Seq order.
func naiveSelect(m *machine, now time.Time, req LeaseRequest) []string {
	var out []string
	for el := m.order.Front(); el != nil && len(out) < req.Max; el = el.Next() {
		it := el.Value.(*memItem)
		switch it.rec.State {
		case Parked:
			continue
		case Pending:
			if it.rec.NextAttempt.After(now) {
				continue
			}
		case Leased:
			if it.leaseUntil.After(now) {
				continue
			}
		}
		if req.BlockedOn != "" && it.rec.BlockedOn != req.BlockedOn {
			continue
		}
		skipped := false
		for _, s := range req.Skip {
			if it.rec.BlockedOn == s {
				skipped = true
			}
		}
		if skipped {
			continue
		}
		if k := it.rec.OrderKey; k != "" && m.byKey[k][0] != it {
			continue
		}
		out = append(out, it.rec.ID)
	}
	return out
}

func idsOf(items []*memItem) []string {
	out := make([]string, len(items))
	for i, it := range items {
		out[i] = it.rec.ID
	}
	return out
}

// checkIndex verifies the incremental index against a full scan.
func checkIndex(t *testing.T, m *machine, now time.Time, step int) {
	t.Helper()
	var counts [3]int
	var parked []string
	var oldest *memItem
	inGroups := 0
	for el := m.order.Front(); el != nil; el = el.Next() {
		it := el.Value.(*memItem)
		counts[it.rec.State]++
		if it.rec.State == Parked {
			parked = append(parked, it.rec.ID)
			if it.grp != nil {
				t.Fatalf("step %d: parked %s is in group %q", step, it.rec.ID, it.grp.dep)
			}
			continue
		}
		inGroups++
		if it.grp == nil || it.grp.dep != it.rec.BlockedOn || m.groups[it.grp.dep] != it.grp {
			t.Fatalf("step %d: %s (blocked on %q) is in the wrong group", step, it.rec.ID, it.rec.BlockedOn)
		}
		if it.rec.State == Pending && oldest == nil {
			oldest = it
		}
	}
	if counts != m.counts {
		t.Fatalf("step %d: counts %v, index says %v", step, counts, m.counts)
	}
	n := 0
	for _, g := range m.groups {
		n += g.items.Len()
		if g.items.Len() == 0 {
			t.Fatalf("step %d: empty group %q kept", step, g.dep)
		}
		var last uint64
		for el := g.items.Front(); el != nil; el = el.Next() {
			if s := el.Value.(*memItem).rec.Seq; s <= last {
				t.Fatalf("step %d: group %q is not in Seq order", step, g.dep)
			} else {
				last = s
			}
		}
	}
	if n != inGroups {
		t.Fatalf("step %d: groups hold %d items, want %d", step, n, inGroups)
	}
	if got := idsOf(m.parkedItems(0, 1000)); fmt.Sprint(got) != fmt.Sprint(parked) {
		t.Fatalf("step %d: parked list %v, want %v", step, got, parked)
	}
	st := m.stats(now)
	if st.Pending != counts[Pending] || st.Leased != counts[Leased] || st.Parked != counts[Parked] {
		t.Fatalf("step %d: stats %+v vs counts %v", step, st, counts)
	}
	var wantOldest time.Duration
	if first := firstPending(m); first != nil {
		wantOldest = now.Sub(first.rec.FirstFailed)
	}
	if st.OldestPending != wantOldest {
		t.Fatalf("step %d: oldest pending %v, want %v", step, st.OldestPending, wantOldest)
	}
}

// firstPending is the first Pending record in Seq order, found the slow way.
func firstPending(m *machine) *memItem {
	for el := m.order.Front(); el != nil; el = el.Next() {
		if it := el.Value.(*memItem); it.rec.State == Pending {
			return it
		}
	}
	return nil
}

// TestQueueIndexAgreesWithAFullScan drives the state machine with random
// operations and checks after every one that the group index, counters and the
// parked list agree with a scan of everything, and that selectLease returns exactly
// what the slow specification says: including through cached "nothing due" times.
func TestQueueIndexAgreesWithAFullScan(t *testing.T) {
	for seed := int64(1); seed <= 40; seed++ {
		rng := rand.New(rand.NewSource(seed))
		m := newMachine(limits{})
		now := time.Unix(1_700_000_000, 0)
		deps := []string{"", "a", "b", "c"}
		keys := []string{"", "", "k1", "k2"}
		tokens := map[string]string{}
		next := 0

		pick := func(state State) *memItem {
			var all []*memItem
			for el := m.order.Front(); el != nil; el = el.Next() {
				if it := el.Value.(*memItem); it.rec.State == state {
					all = append(all, it)
				}
			}
			if len(all) == 0 {
				return nil
			}
			return all[rng.Intn(len(all))]
		}

		for step := 0; step < 600; step++ {
			switch op := rng.Intn(12); op {
			case 0, 1, 2: // append
				next++
				r := Record{ID: fmt.Sprintf("r%d", next), BlockedOn: deps[rng.Intn(len(deps))], OrderKey: keys[rng.Intn(len(keys))], Value: []byte("v")}
				if rng.Intn(8) == 0 {
					r.State = Parked
				}
				rec, dup, err := m.prepare(r, now)
				if err != nil || dup {
					t.Fatalf("prepare: %v %v", err, dup)
				}
				m.insert(rec)
			case 3, 4, 5: // lease
				req := LeaseRequest{Max: 1 + rng.Intn(4), TTL: time.Duration(1+rng.Intn(3)) * time.Second}
				if rng.Intn(3) == 0 {
					req.BlockedOn = deps[1+rng.Intn(3)]
				}
				for _, d := range deps {
					if rng.Intn(4) == 0 {
						req.Skip = append(req.Skip, d)
					}
				}
				got := idsOf(m.selectLease(now, req))
				want := naiveSelect(m, now, req)
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("seed %d step %d: selectLease %v, want %v (req %+v)", seed, step, got, want, req)
				}
				for _, l := range m.grant(m.selectLease(now, req), now, req.TTL) {
					tokens[l.Record.ID] = l.Token
				}
			case 6: // ack
				if it := pick(Leased); it != nil {
					m.remove(it)
				}
			case 7: // nack, possibly moving to another dependency, possibly refunding
				if it := pick(Leased); it != nil {
					m.applyNack(it, now, now.Add(time.Duration(rng.Intn(4))*time.Second), "e", deps[rng.Intn(len(deps))], rng.Intn(2) == 0)
				}
			case 8: // release or park
				if it := pick(Leased); it != nil {
					if rng.Intn(2) == 0 {
						m.applyRelease(it)
					} else {
						m.applyPark(it, now, "why")
					}
				}
			case 9: // requeue or discard a parked record
				if it := pick(Parked); it != nil {
					if rng.Intn(2) == 0 {
						m.applyRequeue(it, false, nil)
					} else {
						m.remove(it)
					}
				}
			case 10: // a pending record is removed outright (a snapshot-style discard of a head)
				if it := pick(Pending); it != nil && rng.Intn(3) == 0 {
					m.remove(it)
				}
			case 11: // time passes, sometimes less than a wake-skew
				now = now.Add(time.Duration(rng.Intn(1500)) * time.Millisecond)
			}
			checkIndex(t, m, now, step)
		}
	}
}

func TestSelectLeaseSkipsGroupsWithNothingDue(t *testing.T) {
	m := newMachine(limits{})
	now := time.Unix(1_700_000_000, 0)
	for i := 0; i < 1000; i++ {
		rec, _, _ := m.prepare(Record{ID: fmt.Sprintf("r%04d", i), BlockedOn: "slow", NextAttempt: now.Add(time.Hour)}, now)
		m.insert(rec)
	}
	req := LeaseRequest{Max: 1, TTL: time.Minute}
	if got := m.selectLease(now, req); len(got) != 0 {
		t.Fatalf("leased %d records that are not due", len(got))
	}
	g := m.groups["slow"]
	if g.wake.IsZero() {
		t.Fatal("a scan that found nothing should remember when to look again")
	}
	// A new due record in that group must be found at once, not after the wake time.
	rec, _, _ := m.prepare(Record{ID: "fresh", BlockedOn: "slow"}, now)
	m.insert(rec)
	if got := idsOf(m.selectLease(now, req)); fmt.Sprint(got) != "[fresh]" {
		t.Fatalf("got %v, want the new record", got)
	}
}

func TestParkedPagesAreBoundedByBytes(t *testing.T) {
	m := newMachine(limits{})
	now := time.Unix(1_700_000_000, 0)
	for i := 0; i < 5; i++ {
		rec, _, _ := m.prepare(Record{ID: fmt.Sprintf("p%d", i), State: Parked}, now)
		m.insert(rec)
		m.items[rec.ID].size = 20 << 20 // pretend each carries 20 MiB
	}
	page := m.parkedItems(0, 1000)
	if len(page) != 1 {
		t.Fatalf("page holds %d records of 20 MiB, want 1 (limit %d bytes)", len(page), maxParkedPageBytes)
	}
	// Paging by Seq still reaches everything.
	var seen int
	for after := uint64(0); ; {
		p := m.parkedItems(after, 1000)
		if len(p) == 0 {
			break
		}
		seen += len(p)
		after = p[len(p)-1].rec.Seq
	}
	if seen != 5 {
		t.Fatalf("paging saw %d records, want 5", seen)
	}
}

func TestMemoryStoreIsBoundedByDefault(t *testing.T) {
	if got := NewMemoryStore(MemoryOptions{}).m.lim.maxBytes; got != DefaultMemoryMaxBytes {
		t.Fatalf("default cap = %d, want %d", got, DefaultMemoryMaxBytes)
	}
	if got := NewMemoryStore(MemoryOptions{MaxBytes: -1}).m.lim.maxBytes; got > 0 {
		t.Fatalf("a negative MaxBytes must remove the cap, got %d", got)
	}
}
