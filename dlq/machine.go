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
	size       int
}

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
}

func newMachine(lim limits) *machine {
	return &machine{
		lim:   lim,
		items: make(map[string]*memItem),
		order: list.New(),
		byKey: make(map[string][]*memItem),
	}
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
	if rec.OrderKey != "" {
		m.byKey[rec.OrderKey] = append(m.byKey[rec.OrderKey], it)
	}
	m.bytes += int64(it.size)
	if rec.Seq > m.seq {
		m.seq = rec.Seq
	}
}

// eligible reports whether it may be leased at now.
func (m *machine) eligible(it *memItem, now time.Time, req LeaseRequest) bool {
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
	if req.BlockedOn != "" && it.rec.BlockedOn != req.BlockedOn {
		return false
	}
	for _, skip := range req.Skip {
		if it.rec.BlockedOn == skip {
			return false
		}
	}
	if k := it.rec.OrderKey; k != "" && m.byKey[k][0] != it {
		return false // an earlier record with this key is still in the store
	}
	return true
}

// selectLease picks the records a Lease call would hand out, without changing
// anything.
func (m *machine) selectLease(now time.Time, req LeaseRequest) []*memItem {
	var out []*memItem
	for el := m.order.Front(); el != nil && len(out) < req.Max; el = el.Next() {
		if it := el.Value.(*memItem); m.eligible(it, now, req) {
			out = append(out, it)
		}
	}
	return out
}

// grant leases the selected items under fresh tokens.
func (m *machine) grant(items []*memItem, now time.Time, ttl time.Duration) []Lease {
	out := make([]Lease, 0, len(items))
	for _, it := range items {
		it.token = newToken()
		it.leaseUntil = now.Add(ttl)
		it.rec.State = Leased
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
	m.bytes += int64(it.rec.Size() - it.size)
	it.size = it.rec.Size()
}

func (m *machine) remove(it *memItem) {
	m.order.Remove(it.el)
	delete(m.items, it.rec.ID)
	m.bytes -= int64(it.size)
	if k := it.rec.OrderKey; k != "" {
		list := m.byKey[k]
		for i, x := range list {
			if x == it {
				list = append(list[:i], list[i+1:]...)
				break
			}
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
	grow := len(cp) - len(it.rec.Checkpoint)
	if m.lim.maxBytes > 0 && grow > 0 && m.bytes+int64(grow) > m.lim.maxBytes {
		return ErrFull
	}
	if m.lim.maxRecordBytes > 0 && it.size+grow > m.lim.maxRecordBytes {
		return fmt.Errorf("%w: checkpoint would exceed the record size limit", ErrInvalidRecord)
	}
	return nil
}

func (m *machine) setCheckpoint(it *memItem, cp []byte) {
	it.rec.Checkpoint = cloneBytes(cp)
	m.resize(it)
}

func (m *machine) applyNack(it *memItem, at, next time.Time, errText, blockedOn string, refund bool) {
	it.rec.State = Pending
	if refund && it.rec.Attempts > 0 {
		it.rec.Attempts--
	}
	it.rec.LastFailed = at
	it.rec.NextAttempt = next
	it.rec.LastError = errText
	if blockedOn != "" {
		it.rec.BlockedOn = blockedOn
	}
	it.unlease()
	m.resize(it)
}

func (m *machine) applyRelease(it *memItem) {
	it.rec.State = Pending
	if it.rec.Attempts > 0 {
		it.rec.Attempts--
	}
	it.unlease()
}

func (m *machine) applyPark(it *memItem, at time.Time, reason string) {
	it.rec.State = Parked
	it.rec.LastFailed = at
	it.rec.LastError = reason
	it.unlease()
	m.resize(it)
}

func (m *machine) applyRequeue(it *memItem, replace bool, checkpoint []byte) {
	it.rec.State = Pending
	it.rec.Attempts = 0
	it.rec.NextAttempt = time.Time{}
	if replace {
		m.setCheckpoint(it, checkpoint)
	}
}

// parkedList returns up to limit parked records with Seq greater than after.
func (m *machine) parkedList(after uint64, limit int) []Record {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	var out []Record
	for el := m.order.Front(); el != nil && len(out) < limit; el = el.Next() {
		it := el.Value.(*memItem)
		if it.rec.State == Parked && it.rec.Seq > after {
			out = append(out, it.rec.Clone())
		}
	}
	return out
}

func (m *machine) hasOrderKey(k string) bool { return k != "" && len(m.byKey[k]) > 0 }

func (m *machine) stats(now time.Time) StoreStats {
	st := StoreStats{Bytes: m.bytes}
	oldest := true
	for el := m.order.Front(); el != nil; el = el.Next() {
		it := el.Value.(*memItem)
		switch it.rec.State {
		case Pending:
			st.Pending++
			if oldest {
				st.OldestPending = now.Sub(it.rec.FirstFailed)
				oldest = false
			}
		case Leased:
			st.Leased++
		case Parked:
			st.Parked++
		}
	}
	return st
}

// Replay helpers. A log records what happened; these re-apply it, tolerating
// entries about records that a later entry (or a snapshot) already removed.

func (m *machine) replayLease(id string) {
	if it, ok := m.items[id]; ok {
		it.rec.Attempts++
		it.rec.State = Leased
	}
}

func (m *machine) replayCheckpoint(id string, cp []byte) {
	if it, ok := m.items[id]; ok {
		m.setCheckpoint(it, cp)
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

func (m *machine) replayRequeue(id string, replace bool, checkpoint []byte) {
	if it, ok := m.items[id]; ok && it.rec.State == Parked {
		m.applyRequeue(it, replace, checkpoint)
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
			it.rec.State = Pending
			it.unlease()
		}
	}
}
