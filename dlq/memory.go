package dlq

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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
// that is acceptable to lose on restart. It is NOT durable: use OpenWAL where a
// process crash must not lose the queue. Lease scans are linear in the number of
// records, which suits bounded queues.
type MemoryStore struct {
	clock func() time.Time

	mu     sync.Mutex
	m      *machine
	closed bool
}

// NewMemoryStore creates an empty store.
func NewMemoryStore(opts MemoryOptions) *MemoryStore {
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	return &MemoryStore{
		clock: opts.Clock,
		m:     newMachine(limits{maxRecords: opts.MaxRecords, maxBytes: opts.MaxBytes, maxRecordBytes: opts.MaxRecordBytes}),
	}
}

var _ Store = (*MemoryStore)(nil)

func (s *MemoryStore) begin(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
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
func (s *MemoryStore) Append(ctx context.Context, r Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin(ctx); err != nil {
		return err
	}
	rec, dup, err := s.m.prepare(r, s.clock())
	if err != nil || dup {
		return err
	}
	s.m.insert(rec)
	return nil
}

// Lease implements Store.
func (s *MemoryStore) Lease(ctx context.Context, req LeaseRequest) ([]Lease, error) {
	if err := validateLease(req); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin(ctx); err != nil {
		return nil, err
	}
	now := s.clock()
	return s.m.grant(s.m.selectLease(now, req), now, req.TTL), nil
}

// Checkpoint implements Store.
func (s *MemoryStore) Checkpoint(ctx context.Context, id, token string, checkpoint []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin(ctx); err != nil {
		return err
	}
	it, err := s.m.held(id, token)
	if err != nil {
		return err
	}
	if err := s.m.checkCheckpoint(it, checkpoint); err != nil {
		return err
	}
	s.m.setCheckpoint(it, checkpoint)
	return nil
}

// Ack implements Store.
func (s *MemoryStore) Ack(ctx context.Context, id, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin(ctx); err != nil {
		return err
	}
	it, err := s.m.held(id, token)
	if err != nil {
		return err
	}
	s.m.remove(it)
	return nil
}

// Nack implements Store.
func (s *MemoryStore) Nack(ctx context.Context, id, token string, opts NackOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin(ctx); err != nil {
		return err
	}
	it, err := s.m.held(id, token)
	if err != nil {
		return err
	}
	now := s.clock()
	s.m.applyNack(it, now, now.Add(opts.Delay), opts.Err, opts.BlockedOn, opts.Refund)
	return nil
}

// Release implements Store.
func (s *MemoryStore) Release(ctx context.Context, id, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin(ctx); err != nil {
		return err
	}
	it, err := s.m.held(id, token)
	if err != nil {
		return err
	}
	s.m.applyRelease(it)
	return nil
}

// Park implements Store.
func (s *MemoryStore) Park(ctx context.Context, id, token, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin(ctx); err != nil {
		return err
	}
	it, err := s.m.held(id, token)
	if err != nil {
		return err
	}
	s.m.applyPark(it, s.clock(), reason)
	return nil
}

// Requeue implements Store.
func (s *MemoryStore) Requeue(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin(ctx); err != nil {
		return err
	}
	it, err := s.parked(id)
	if err != nil {
		return err
	}
	s.m.applyRequeue(it)
	return nil
}

// Discard implements Store.
func (s *MemoryStore) Discard(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin(ctx); err != nil {
		return err
	}
	it, err := s.parked(id)
	if err != nil {
		return err
	}
	s.m.remove(it)
	return nil
}

func (s *MemoryStore) parked(id string) (*memItem, error) {
	it, ok := s.m.items[id]
	if !ok {
		return nil, ErrNotFound
	}
	if it.rec.State != Parked {
		return nil, ErrNotParked
	}
	return it, nil
}

// Stats implements Store.
func (s *MemoryStore) Stats(ctx context.Context) (StoreStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin(ctx); err != nil {
		return StoreStats{}, err
	}
	return s.m.stats(s.clock()), nil
}

// Close implements Store. Later calls return ErrClosed.
func (s *MemoryStore) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}
