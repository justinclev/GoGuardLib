package dlq

import (
	"container/list"
	"fmt"
	"time"
)

// limits caps what a machine will accept.
type limits struct {
	maxRecords     int
	maxBytes       int64
	maxRecordBytes int
}

type memItem struct {
	rec        Record
	token      string
	leaseUntil time.Time
	el         *list.Element
	size       int // logical size, counting payload wherever it lives

	// When a store keeps payloads on disk, rec.Key, rec.Value, rec.Headers and
	// rec.Checkpoint are empty and these say where to find them and how many bytes
	// they stand for. In-memory stores leave all four zero.
	bodyRef, ckptRef blobRef
	bodyN, ckptN     int

	// Position in the queue index: a Pending or Leased item sits in the group of its
	// BlockedOn, a Parked item in the parked list (grp is nil then).
	grp *group
	gel *list.Element

	// reserved marks an item a Lease has picked and is still reading: other leases
	// skip it. ver changes whenever the item's state or checkpoint changes, so a
	// lease can tell that what it read is stale.
	reserved bool
	ver      uint64
}

// group holds the Pending and Leased items that wait on one dependency, in Seq
// order, so a Lease can skip a whole dependency (an open circuit) in O(1) and never
// walks records that belong to someone else.
type group struct {
	dep   string
	items *list.List    // *memItem
	hint  *list.Element // where the last item was inserted, for runs of inserts in Seq order
	// wake is the earliest time a scan of this group can find something to lease,
	// as of the last scan that found nothing; zero means "unknown, scan". Every
	// change that can make something eligible earlier clears it, and it is never
	// trusted for longer than maxWakeSkew, so a missed clear delays work by at most
	// that long.
	wake time.Time
}

// maxWakeSkew bounds how long a cached group wake time is believed.
const maxWakeSkew = time.Second

// maxParkedPageBytes bounds the payload one Parked page may carry, so listing a
// queue of large parked records cannot exhaust memory. A page always holds at
// least one record.
const maxParkedPageBytes = 32 << 20

func (it *memItem) unlease() {
	it.token = ""
	it.leaseUntil = time.Time{}
}

// machine is the queue's state machine. It holds no locks and reads no clock:
// every method takes its inputs explicitly, so the same transitions can be driven
// live (MemoryStore) or replayed from a log (WALStore) and always agree.
type machine struct {
	lim   limits
	items map[string]*memItem
	order *list.List            // *memItem in Seq order
	byKey map[string][]*memItem // OrderKey -> items in Seq order
	seq   uint64
	bytes int64

	groups     map[string]*group // BlockedOn -> Pending and Leased items
	parked     *list.List        // Parked items in Seq order
	parkedHint *list.Element
	counts     [3]int // items per State
}

func newMachine(lim limits) *machine {
	return &machine{
		lim:   lim,
		items: make(map[string]*memItem),
		order: list.New(),
		byKey: make(map[string][]*memItem),

		groups: make(map[string]*group),
		parked: list.New(),
	}
}

// insertOrdered puts it in l keeping Seq order and returns its element. New records
// carry the highest Seq, so searching from the back is O(1) for them. Records that
// come back in bulk (a whole parked queue requeued, oldest first) are near the
// previous insert instead, which hint remembers, so a run of them costs O(1) each
// rather than a walk over everything newer.
func insertOrdered(l *list.List, it *memItem, hint **list.Element) *list.Element {
	seq := it.rec.Seq
	place := func(e *list.Element) *list.Element {
		*hint = e
		return e
	}
	if h := *hint; h != nil {
		if hs := h.Value.(*memItem).rec.Seq; hs < seq {
			if n := h.Next(); n == nil || n.Value.(*memItem).rec.Seq > seq {
				return place(l.InsertAfter(it, h))
			}
		}
	}
	if f := l.Front(); f == nil || f.Value.(*memItem).rec.Seq > seq {
		return place(l.PushFront(it))
	}
	e := l.Back()
	for e != nil && e.Value.(*memItem).rec.Seq > seq {
		e = e.Prev()
	}
	if e == nil {
		return place(l.PushFront(it))
	}
	return place(l.InsertAfter(it, e))
}

// link adds it to the index according to its state.
func (m *machine) link(it *memItem) {
	m.counts[it.rec.State]++
	if it.rec.State == Parked {
		it.grp, it.gel = nil, insertOrdered(m.parked, it, &m.parkedHint)
		return
	}
	g := m.groups[it.rec.BlockedOn]
	if g == nil {
		g = &group{dep: it.rec.BlockedOn, items: list.New()}
		m.groups[g.dep] = g
	}
	it.grp, it.gel = g, insertOrdered(g.items, it, &g.hint)
	g.wake = time.Time{}
}

// unlink removes it from the index.
func (m *machine) unlink(it *memItem) {
	m.counts[it.rec.State]--
	if g := it.grp; g != nil {
		if g.hint == it.gel {
			g.hint = nil
		}
		g.items.Remove(it.gel)
		g.wake = time.Time{}
		if g.items.Len() == 0 {
			delete(m.groups, g.dep)
		}
	} else {
		if m.parkedHint == it.gel {
			m.parkedHint = nil
		}
		m.parked.Remove(it.gel)
	}
	it.grp, it.gel = nil, nil
}

// move changes an item's state and the dependency it waits on. Moving within one
// group (Pending to Leased and back) keeps its place, so leasing the head of a
// huge group and returning it costs O(1).
func (m *machine) move(it *memItem, state State, dep string) {
	it.ver++
	if it.grp != nil && state != Parked && it.rec.BlockedOn == dep {
		m.counts[it.rec.State]--
		m.counts[state]++
		it.rec.State = state
		it.grp.wake = time.Time{}
		return
	}
	m.unlink(it)
	it.rec.State, it.rec.BlockedOn = state, dep
	m.link(it)
}

// prepare validates r and returns the record exactly as it will be stored
// (defaults filled in, Seq assigned). dup is true if r.ID is already present, in
// which case nothing should be stored.
func (m *machine) prepare(r Record, now time.Time) (rec Record, dup bool, err error) {
	if err := r.Validate(); err != nil {
		return Record{}, false, err
	}
	if r.State != Pending && r.State != Parked {
		return Record{}, false, fmt.Errorf("%w: unknown state %d", ErrInvalidRecord, int(r.State))
	}
	size := r.Size()
	if m.lim.maxRecordBytes > 0 && size > m.lim.maxRecordBytes {
		return Record{}, false, fmt.Errorf("%w: record is %d bytes, limit is %d", ErrInvalidRecord, size, m.lim.maxRecordBytes)
	}
	if _, exists := m.items[r.ID]; exists {
		return Record{}, true, nil // idempotent: the existing record is authoritative
	}
	if m.lim.maxRecords > 0 && len(m.items) >= m.lim.maxRecords {
		return Record{}, false, ErrFull
	}
	if m.lim.maxBytes > 0 && m.bytes+int64(size) > m.lim.maxBytes {
		return Record{}, false, ErrFull
	}

	rec = r.Clone()
	rec.Seq = m.seq + 1
	if rec.FirstFailed.IsZero() {
		rec.FirstFailed = now
	}
	if rec.LastFailed.IsZero() {
		rec.LastFailed = rec.FirstFailed
	}
	if rec.SchemaVersion == 0 {
		rec.SchemaVersion = CurrentSchema
	}
	return rec, false, nil
}

// insert stores a prepared record. It skips capacity checks so that replaying a
// log written under different limits never loses data.
func (m *machine) insert(rec Record) {
	if _, exists := m.items[rec.ID]; exists {
		return
	}
	it := &memItem{rec: rec, size: rec.Size()}
	it.el = m.order.PushBack(it)
	m.items[rec.ID] = it
	m.link(it)
	if rec.OrderKey != "" {
		m.byKey[rec.OrderKey] = append(m.byKey[rec.OrderKey], it)
	}
	m.bytes += int64(it.size)
	if rec.Seq > m.seq {
		m.seq = rec.Seq
	}
}

// eligible reports whether it may be leased at now. Which dependency it waits on
// is decided a level up, by group.
func (m *machine) eligible(it *memItem, now time.Time) bool {
	if it.reserved {
		return false
	}
	switch it.rec.State {
	case Parked:
		return false
	case Pending:
		if it.rec.NextAttempt.After(now) {
			return false
		}
	case Leased:
		if it.leaseUntil.After(now) {
			return false
		}
	}
	if k := it.rec.OrderKey; k != "" && m.byKey[k][0] != it {
		return false // an earlier record with this key is still in the store
	}
	return true
}

// scanGroup returns up to max eligible items of g, oldest first. A scan that finds
// nothing records when the group could next have something.
func (m *machine) scanGroup(g *group, now time.Time, max int) []*memItem {
	var out []*memItem
	var next time.Time
	for el := g.items.Front(); el != nil; el = el.Next() {
		it := el.Value.(*memItem)
		if m.eligible(it, now) {
			out = append(out, it)
			if len(out) == max {
				return out
			}
			continue
		}
		var t time.Time
		switch it.rec.State {
		case Pending:
			t = it.rec.NextAttempt
		case Leased:
			t = it.leaseUntil
		}
		if t.After(now) && (next.IsZero() || t.Before(next)) {
			next = t
		}
	}
	if len(out) == 0 {
		if limit := now.Add(maxWakeSkew); next.IsZero() || next.After(limit) {
			next = limit
		}
		g.wake = next
	}
	return out
}

// selectLease picks the records a Lease call would hand out, without changing
// anything. Groups for skipped dependencies, and groups known to have nothing due,
// cost nothing.
func (m *machine) selectLease(now time.Time, req LeaseRequest) []*memItem {
	var skip map[string]struct{}
	if len(req.Skip) > 0 {
		skip = make(map[string]struct{}, len(req.Skip))
		for _, d := range req.Skip {
			skip[d] = struct{}{}
		}
	}
	var lists [][]*memItem
	consider := func(g *group) {
		if _, skipped := skip[g.dep]; skipped {
			return
		}
		if !g.wake.IsZero() && now.Before(g.wake) {
			return
		}
		if items := m.scanGroup(g, now, req.Max); len(items) > 0 {
			lists = append(lists, items)
		}
	}
	if req.BlockedOn != "" {
		if g := m.groups[req.BlockedOn]; g != nil {
			consider(g)
		}
	} else {
		for _, g := range m.groups {
			consider(g)
		}
	}
	// Merge the groups' candidates by Seq so the oldest records go first.
	var out []*memItem
	for len(out) < req.Max {
		best := -1
		for i, l := range lists {
			if len(l) > 0 && (best < 0 || l[0].rec.Seq < lists[best][0].rec.Seq) {
				best = i
			}
		}
		if best < 0 {
			break
		}
		out = append(out, lists[best][0])
		lists[best] = lists[best][1:]
	}
	return out
}

// reserve keeps other leases away from an item while its payload is read.
func (m *machine) reserve(it *memItem) { it.reserved = true }

// unreserve releases a reservation and lets the next scan see the item at once.
func (m *machine) unreserve(it *memItem) {
	if !it.reserved {
		return
	}
	it.reserved = false
	if it.grp != nil {
		it.grp.wake = time.Time{}
	}
}

// grant leases the selected items under fresh tokens.
func (m *machine) grant(items []*memItem, now time.Time, ttl time.Duration) []Lease {
	out := make([]Lease, 0, len(items))
	for _, it := range items {
		it.token = newToken()
		it.leaseUntil = now.Add(ttl)
		m.move(it, Leased, it.rec.BlockedOn)
		it.rec.Attempts++
		out = append(out, Lease{Record: it.rec.Clone(), Token: it.token, Until: it.leaseUntil})
	}
	return out
}

// held returns the item if token is its current lease.
func (m *machine) held(id, token string) (*memItem, error) {
	it, ok := m.items[id]
	if !ok {
		return nil, ErrNotFound
	}
	if it.rec.State != Leased || it.token == "" || it.token != token {
		return nil, ErrLeaseLost
	}
	return it, nil
}

func (m *machine) resize(it *memItem) {
	n := it.rec.Size() + it.bodyN + it.ckptN
	m.bytes += int64(n - it.size)
	it.size = n
}

func (m *machine) remove(it *memItem) {
	it.ver++
	m.unlink(it)
	m.order.Remove(it.el)
	delete(m.items, it.rec.ID)
	m.bytes -= int64(it.size)
	if k := it.rec.OrderKey; k != "" {
		list := m.byKey[k]
		for i, x := range list {
			if x != it {
				continue
			}
			if i == 0 { // the head leaving is the usual case: O(1), and it may unblock its successor
				list[0] = nil
				list = list[1:]
				if len(list) > 0 && list[0].grp != nil {
					list[0].grp.wake = time.Time{}
				}
			} else {
				list = append(list[:i], list[i+1:]...)
			}
			break
		}
		if len(list) == 0 {
			delete(m.byKey, k)
		} else {
			m.byKey[k] = list
		}
	}
}

// checkCheckpoint reports whether cp fits in the limits.
func (m *machine) checkCheckpoint(it *memItem, cp []byte) error {
	grow := len(cp) - len(it.rec.Checkpoint) - it.ckptN
	if m.lim.maxBytes > 0 && grow > 0 && m.bytes+int64(grow) > m.lim.maxBytes {
		return ErrFull
	}
	if m.lim.maxRecordBytes > 0 && it.size+grow > m.lim.maxRecordBytes {
		return fmt.Errorf("%w: checkpoint would exceed the record size limit", ErrInvalidRecord)
	}
	return nil
}

func (m *machine) setCheckpoint(it *memItem, cp []byte) {
	it.ver++
	it.rec.Checkpoint = cloneBytes(cp)
	it.ckptRef, it.ckptN = blobRef{}, 0
	m.resize(it)
}

// setCheckpointRef records that the checkpoint (n bytes) now lives on disk at ref.
// A nil checkpoint (ref zero) is simply absent.
func (m *machine) setCheckpointRef(it *memItem, n int, ref blobRef) {
	it.ver++
	it.rec.Checkpoint = nil
	it.ckptRef, it.ckptN = ref, n
	m.resize(it)
}

// bodySize is the bytes of the parts of a record that go to disk with its body.
func bodySize(r *Record) int {
	n := len(r.Key) + len(r.Value)
	for _, h := range r.Headers {
		n += len(h.Key) + len(h.Value)
	}
	return n
}

// insertOffloaded stores a record whose payload lives on disk. rec still carries
// its payload here; it is dropped, and only the references and sizes are kept.
// body points at the frame holding the record; a non-nil checkpoint lives in that
// same frame.
func (m *machine) insertOffloaded(rec Record, body blobRef) {
	if _, exists := m.items[rec.ID]; exists {
		return
	}
	total := rec.Size()
	it := &memItem{size: total, bodyRef: body, bodyN: bodySize(&rec), ckptN: len(rec.Checkpoint)}
	if rec.Checkpoint != nil {
		it.ckptRef = body
	}
	rec.Key, rec.Value, rec.Headers, rec.Checkpoint = nil, nil, nil, nil
	it.rec = rec
	it.el = m.order.PushBack(it)
	m.items[rec.ID] = it
	m.link(it)
	if rec.OrderKey != "" {
		m.byKey[rec.OrderKey] = append(m.byKey[rec.OrderKey], it)
	}
	m.bytes += int64(total)
	if rec.Seq > m.seq {
		m.seq = rec.Seq
	}
}

func (m *machine) applyNack(it *memItem, at, next time.Time, errText, blockedOn string, refund bool) {
	dep := it.rec.BlockedOn
	if blockedOn != "" {
		dep = blockedOn
	}
	m.move(it, Pending, dep)
	if refund && it.rec.Attempts > 0 {
		it.rec.Attempts--
	}
	it.rec.LastFailed = at
	it.rec.NextAttempt = next
	it.rec.LastError = errText
	it.unlease()
	m.resize(it)
}

func (m *machine) applyRelease(it *memItem) {
	m.move(it, Pending, it.rec.BlockedOn)
	if it.rec.Attempts > 0 {
		it.rec.Attempts--
	}
	it.unlease()
}

func (m *machine) applyPark(it *memItem, at time.Time, reason string) {
	m.move(it, Parked, it.rec.BlockedOn)
	it.rec.LastFailed = at
	it.rec.LastError = reason
	it.unlease()
	m.resize(it)
}

func (m *machine) applyRequeue(it *memItem, replace bool, checkpoint []byte) {
	m.move(it, Pending, it.rec.BlockedOn)
	it.rec.Attempts = 0
	it.rec.NextAttempt = time.Time{}
	if replace {
		m.setCheckpoint(it, checkpoint)
	}
}

// parkedItems returns up to limit parked items with Seq greater than after, and
// stops early once the page would carry maxParkedPageBytes of payload. Callers page
// until they get an empty result, not until a short one.
func (m *machine) parkedItems(after uint64, limit int) []*memItem {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	var out []*memItem
	var bytes int
	for el := m.parked.Front(); el != nil && len(out) < limit; el = el.Next() {
		it := el.Value.(*memItem)
		if it.rec.Seq <= after {
			continue
		}
		if len(out) > 0 && bytes+it.size > maxParkedPageBytes {
			break
		}
		out = append(out, it)
		bytes += it.size
	}
	return out
}

// parkedList returns copies of up to limit parked records with Seq greater than
// after. It is for stores that keep payloads in memory.
func (m *machine) parkedList(after uint64, limit int) []Record {
	var out []Record
	for _, it := range m.parkedItems(after, limit) {
		out = append(out, it.rec.Clone())
	}
	return out
}

func (m *machine) hasOrderKey(k string) bool { return k != "" && len(m.byKey[k]) > 0 }

func (m *machine) stats(now time.Time) StoreStats {
	st := StoreStats{Bytes: m.bytes, Pending: m.counts[Pending], Leased: m.counts[Leased], Parked: m.counts[Parked]}
	// The oldest waiting record is the first Pending item of some group. Only
	// leased items can precede it in a group, and those are few.
	var oldest *memItem
	for _, g := range m.groups {
		for el := g.items.Front(); el != nil; el = el.Next() {
			if it := el.Value.(*memItem); it.rec.State == Pending {
				if oldest == nil || it.rec.Seq < oldest.rec.Seq {
					oldest = it
				}
				break
			}
		}
	}
	if oldest != nil {
		st.OldestPending = now.Sub(oldest.rec.FirstFailed)
	}
	if front := m.parked.Front(); front != nil {
		st.OldestParked = now.Sub(front.Value.(*memItem).rec.FirstFailed)
	}
	for el := m.parked.Front(); el != nil; el = el.Next() {
		it := el.Value.(*memItem)
		if k := it.rec.OrderKey; k != "" && m.byKey[k][0] == it {
			st.BlockedKeys++
		}
	}
	for dep, g := range m.groups {
		if dep != "" {
			if st.ByDependency == nil {
				st.ByDependency = map[string]int{}
			}
			st.ByDependency[dep] = g.items.Len()
		}
	}
	return st
}

// Replay helpers. A log records what happened; these re-apply it, tolerating
// entries about records that a later entry (or a snapshot) already removed.

func (m *machine) replayLease(id string) {
	if it, ok := m.items[id]; ok {
		it.rec.Attempts++
		m.move(it, Leased, it.rec.BlockedOn)
	}
}

func (m *machine) replayAck(id string) {
	if it, ok := m.items[id]; ok {
		m.remove(it)
	}
}

func (m *machine) replayNack(id string, at, next time.Time, errText, blockedOn string, refund bool) {
	if it, ok := m.items[id]; ok {
		m.applyNack(it, at, next, errText, blockedOn, refund)
	}
}

func (m *machine) replayRelease(id string) {
	if it, ok := m.items[id]; ok {
		m.applyRelease(it)
	}
}

func (m *machine) replayPark(id string, at time.Time, reason string) {
	if it, ok := m.items[id]; ok {
		m.applyPark(it, at, reason)
	}
}

func (m *machine) replayDiscard(id string) {
	if it, ok := m.items[id]; ok && it.rec.State == Parked {
		m.remove(it)
	}
}

// endReplay turns leases held by a process that no longer exists back into
// pending records. Their attempt counts are kept: that is what lets a poison
// pill which kills its worker eventually be parked.
func (m *machine) endReplay() {
	for el := m.order.Front(); el != nil; el = el.Next() {
		it := el.Value.(*memItem)
		if it.rec.State == Leased {
			m.move(it, Pending, it.rec.BlockedOn)
			it.unlease()
		}
	}
}
