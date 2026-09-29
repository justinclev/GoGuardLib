package dlq

import (
	"container/list"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// MemoryOptions configures a MemoryStore. The zero value is unlimited.
type MemoryOptions struct {
	// MaxRecords caps the number of records held; Append beyond it returns ErrFull.
	MaxRecords int
	// MaxBytes caps the estimated bytes held; Append and Checkpoint beyond it
	// return ErrFull.
	MaxBytes int64
	// MaxRecordBytes rejects any single record larger than this.
	MaxRecordBytes int
	// Clock replaces time.Now, for tests.
	Clock func() time.Time
}

// MemoryStore is a Store that keeps records in memory.
//
// It is a complete reference implementation and is right for tests and for work
// that is acceptable to lose on restart. It is NOT durable: use it where a
// process crash losing the queue is fine. Lease scans are linear in the number of
// records, which suits bounded queues.
type MemoryStore struct {
	opts MemoryOptions

	mu     sync.Mutex
	items  map[string]*memItem
	order  *list.List            // *memItem in Seq order
	byKey  map[string][]*memItem // OrderKey -> items in Seq order
	seq    uint64
	bytes  int64
	closed bool
}

type memItem struct {
	rec        Record
	token      string
	leaseUntil time.Time
	el         *list.Element
	size       int
}

// NewMemoryStore creates an empty store.
func NewMemoryStore(opts MemoryOptions) *MemoryStore {
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	return &MemoryStore{
		opts:  opts,
		items: make(map[string]*memItem),
		order: list.New(),
		byKey: make(map[string][]*memItem),
	}
}

var _ Store = (*MemoryStore)(nil)

func (m *MemoryStore) begin(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.closed {
		return ErrClosed
	}
	return nil
}

func newToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("dlq: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// Append implements Store.
func (m *MemoryStore) Append(ctx context.Context, r Record) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if r.State != Pending && r.State != Parked {
		return fmt.Errorf("%w: unknown state %d", ErrInvalidRecord, int(r.State))
	}
	size := r.Size()
	if m.opts.MaxRecordBytes > 0 && size > m.opts.MaxRecordBytes {
		return fmt.Errorf("%w: record is %d bytes, limit is %d", ErrInvalidRecord, size, m.opts.MaxRecordBytes)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.begin(ctx); err != nil {
		return err
	}
	if _, exists := m.items[r.ID]; exists {
		return nil // idempotent: the existing record is authoritative
	}
	if m.opts.MaxRecords > 0 && len(m.items) >= m.opts.MaxRecords {
		return ErrFull
	}
	if m.opts.MaxBytes > 0 && m.bytes+int64(size) > m.opts.MaxBytes {
		return ErrFull
	}

	now := m.opts.Clock()
	rec := r.Clone()
	m.seq++
	rec.Seq = m.seq
	if rec.FirstFailed.IsZero() {
		rec.FirstFailed = now
	}
	if rec.LastFailed.IsZero() {
		rec.LastFailed = rec.FirstFailed
	}
	if rec.SchemaVersion == 0 {
		rec.SchemaVersion = CurrentSchema
	}
	it := &memItem{rec: rec, size: size}
	it.el = m.order.PushBack(it)
	m.items[rec.ID] = it
	if rec.OrderKey != "" {
		m.byKey[rec.OrderKey] = append(m.byKey[rec.OrderKey], it)
	}
	m.bytes += int64(size)
	return nil
}

// eligible reports whether it may be leased at now.
func (m *MemoryStore) eligible(it *memItem, now time.Time, req LeaseRequest) bool {
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
	if k := it.rec.OrderKey; k != "" && m.byKey[k][0] != it {
		return false // an earlier record with this key is still in the store
	}
	return true
}

// Lease implements Store.
func (m *MemoryStore) Lease(ctx context.Context, req LeaseRequest) ([]Lease, error) {
	if req.Max < 1 || req.TTL <= 0 {
		return nil, fmt.Errorf("%w: Max must be at least 1 and TTL positive", ErrInvalidRequest)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.begin(ctx); err != nil {
		return nil, err
	}

	now := m.opts.Clock()
	var out []Lease
	for el := m.order.Front(); el != nil && len(out) < req.Max; el = el.Next() {
		it := el.Value.(*memItem)
		if !m.eligible(it, now, req) {
			continue
		}
		it.token = newToken()
		it.leaseUntil = now.Add(req.TTL)
		it.rec.State = Leased
		it.rec.Attempts++
		out = append(out, Lease{Record: it.rec.Clone(), Token: it.token, Until: it.leaseUntil})
	}
	return out, nil
}

// held returns the item if token is its current lease.
func (m *MemoryStore) held(id, token string) (*memItem, error) {
	it, ok := m.items[id]
	if !ok {
		return nil, ErrNotFound
	}
	if it.rec.State != Leased || it.token == "" || it.token != token {
		return nil, ErrLeaseLost
	}
	return it, nil
}

func (m *MemoryStore) resize(it *memItem) {
	m.bytes += int64(it.rec.Size() - it.size)
	it.size = it.rec.Size()
}

func (m *MemoryStore) remove(it *memItem) {
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

func (it *memItem) unlease() {
	it.token = ""
	it.leaseUntil = time.Time{}
}

// Checkpoint implements Store.
func (m *MemoryStore) Checkpoint(ctx context.Context, id, token string, checkpoint []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.begin(ctx); err != nil {
		return err
	}
	it, err := m.held(id, token)
	if err != nil {
		return err
	}
	grow := len(checkpoint) - len(it.rec.Checkpoint)
	if m.opts.MaxBytes > 0 && grow > 0 && m.bytes+int64(grow) > m.opts.MaxBytes {
		return ErrFull
	}
	if m.opts.MaxRecordBytes > 0 && it.size+grow > m.opts.MaxRecordBytes {
		return fmt.Errorf("%w: checkpoint would exceed the record size limit", ErrInvalidRecord)
	}
	it.rec.Checkpoint = cloneBytes(checkpoint)
	m.resize(it)
	return nil
}

// Ack implements Store.
func (m *MemoryStore) Ack(ctx context.Context, id, token string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.begin(ctx); err != nil {
		return err
	}
	it, err := m.held(id, token)
	if err != nil {
		return err
	}
	m.remove(it)
	return nil
}

// Nack implements Store.
func (m *MemoryStore) Nack(ctx context.Context, id, token string, opts NackOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.begin(ctx); err != nil {
		return err
	}
	it, err := m.held(id, token)
	if err != nil {
		return err
	}
	now := m.opts.Clock()
	it.rec.State = Pending
	it.rec.LastFailed = now
	it.rec.NextAttempt = now.Add(opts.Delay)
	it.rec.LastError = opts.Err
	if opts.BlockedOn != "" {
		it.rec.BlockedOn = opts.BlockedOn
	}
	it.unlease()
	m.resize(it)
	return nil
}

// Release implements Store.
func (m *MemoryStore) Release(ctx context.Context, id, token string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.begin(ctx); err != nil {
		return err
	}
	it, err := m.held(id, token)
	if err != nil {
		return err
	}
	it.rec.State = Pending
	if it.rec.Attempts > 0 {
		it.rec.Attempts--
	}
	it.unlease()
	return nil
}

// Park implements Store.
func (m *MemoryStore) Park(ctx context.Context, id, token, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.begin(ctx); err != nil {
		return err
	}
	it, err := m.held(id, token)
	if err != nil {
		return err
	}
	it.rec.State = Parked
	it.rec.LastFailed = m.opts.Clock()
	it.rec.LastError = reason
	it.unlease()
	m.resize(it)
	return nil
}

// Requeue implements Store.
func (m *MemoryStore) Requeue(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.begin(ctx); err != nil {
		return err
	}
	it, ok := m.items[id]
	if !ok {
		return ErrNotFound
	}
	if it.rec.State != Parked {
		return ErrNotParked
	}
	it.rec.State = Pending
	it.rec.Attempts = 0
	it.rec.NextAttempt = time.Time{}
	return nil
}

// Discard implements Store.
func (m *MemoryStore) Discard(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.begin(ctx); err != nil {
		return err
	}
	it, ok := m.items[id]
	if !ok {
		return ErrNotFound
	}
	if it.rec.State != Parked {
		return ErrNotParked
	}
	m.remove(it)
	return nil
}

// Stats implements Store.
func (m *MemoryStore) Stats(ctx context.Context) (StoreStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.begin(ctx); err != nil {
		return StoreStats{}, err
	}
	st := StoreStats{Bytes: m.bytes}
	now := m.opts.Clock()
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
	return st, nil
}

// Close implements Store. Later calls return ErrClosed.
func (m *MemoryStore) Close() error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	return nil
}
