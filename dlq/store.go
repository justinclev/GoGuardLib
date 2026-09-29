package dlq

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrInvalidRequest means a request had unusable parameters.
var ErrInvalidRequest = errors.New("dlq: invalid request")

// LeaseRequest asks for records to work on.
type LeaseRequest struct {
	// BlockedOn, if not empty, only leases records waiting on that dependency.
	BlockedOn string
	// Skip lists dependencies whose records must not be leased now, typically the
	// ones whose circuit is open. Records blocked on anything else, including
	// records blocked on nothing, are still offered.
	Skip []string
	// Max is the largest number of records to return (at least 1).
	Max int
	// TTL is how long the worker may hold each record before it is offered again
	// (greater than zero). Choose it longer than the slowest handler.
	TTL time.Duration
}

func validateLease(req LeaseRequest) error {
	if req.Max < 1 || req.TTL <= 0 {
		return fmt.Errorf("%w: Max must be at least 1 and TTL positive", ErrInvalidRequest)
	}
	return nil
}

// Lease is a record held by a worker. Every follow-up call must present Token.
type Lease struct {
	// Record is the record as it was when leased, with Attempts already counting
	// this lease.
	Record Record
	// Token fences the lease: it identifies this grant and no other.
	Token string
	// Until is when the lease expires if it is not finished.
	Until time.Time
}

// NackOptions describes a failed attempt.
type NackOptions struct {
	// Delay is how long to wait before the record may be leased again.
	Delay time.Duration
	// Err describes the failure. Stores keep it as given, so pass redacted text
	// (wrap the store with Secure to enforce that).
	Err string
	// BlockedOn, if not empty, replaces the record's BlockedOn: a later step may
	// be waiting on a different dependency than the earlier one.
	BlockedOn string
	// Refund does not count this attempt, as Release does, while still recording
	// the error, delay and BlockedOn. Use it when the work was never really tried
	// because the dependency was known to be down.
	Refund bool
}

// RequeueOptions adjusts a record as it leaves the parked state.
type RequeueOptions struct {
	// ReplaceCheckpoint, when true, replaces the record's checkpoint with Checkpoint
	// (nil clears it) in the same durable step as the requeue, so the record is
	// never pending with the old checkpoint. This is how a multi-step record is
	// sent back to an earlier step.
	ReplaceCheckpoint bool
	Checkpoint        []byte
}

// ParkedQuery pages through parked records, oldest first.
type ParkedQuery struct {
	// After returns only records whose Seq is greater than this. Pass the Seq of
	// the last record of the previous page (0 for the first page).
	After uint64
	// Limit is the page size. Default 100, at most 1000. A page can be shorter (it
	// is cut once it would carry about 32 MiB of payload), so keep paging until a
	// page comes back empty.
	Limit int
}

// StoreStats summarises a store. All fields are cheap to compute.
type StoreStats struct {
	Pending int
	Leased  int
	Parked  int
	// Bytes is the estimated size of everything stored.
	Bytes int64
	// OldestPending is how long the oldest waiting record has existed, a good
	// alert signal for a DLQ that is not draining.
	OldestPending time.Duration
}

// Total is the number of records held.
func (s StoreStats) Total() int { return s.Pending + s.Leased + s.Parked }

// Store keeps records safe until they are handled. Implementations must be safe
// for concurrent use and must satisfy the storetest conformance suite.
type Store interface {
	// Append stores r durably and returns only after it would survive a crash. If
	// a record with r.ID already exists in any state it is left untouched and nil
	// is returned. It returns ErrFull, without storing anything, at capacity.
	Append(ctx context.Context, r Record) error

	// Lease hands out up to req.Max records that are due, oldest first, each
	// under a new token with Attempts incremented. Records whose lease expired
	// are offered again. Records sharing an OrderKey are offered one at a time in
	// order. It returns an empty slice when nothing is due.
	Lease(ctx context.Context, req LeaseRequest) ([]Lease, error)

	// Checkpoint durably saves progress on a leased record.
	Checkpoint(ctx context.Context, id, token string, checkpoint []byte) error

	// Ack removes a finished record.
	Ack(ctx context.Context, id, token string) error

	// Nack returns a record to pending after a failed attempt.
	Nack(ctx context.Context, id, token string, opts NackOptions) error

	// Release returns a record to pending without counting the attempt, for when
	// the work was not really tried (the dependency is still down).
	Release(ctx context.Context, id, token string) error

	// Park sets a record aside for a human. It is not retried until Requeue.
	Park(ctx context.Context, id, token, reason string) error

	// Requeue moves a parked record back to pending with its attempts reset, for
	// after an operator has fixed the cause. It returns ErrNotParked otherwise.
	Requeue(ctx context.Context, id string) error

	// RequeueWith is Requeue that can also replace the record's checkpoint,
	// atomically. It returns ErrNotFound or ErrNotParked when the record is missing
	// or not parked.
	RequeueWith(ctx context.Context, id string, opts RequeueOptions) error

	// Get returns a copy of a record in any state, for inspection. A leased
	// record's lease token is not exposed. It returns ErrNotFound if there is none.
	Get(ctx context.Context, id string) (Record, error)

	// Parked lists parked records oldest first, a page at a time, so an operator
	// can see what needs attention and why (Record.LastError).
	Parked(ctx context.Context, q ParkedQuery) ([]Record, error)

	// Discard permanently deletes a parked record. It is the only way data leaves
	// a store without being handled, so it is explicit and limited to parked
	// records. It returns ErrNotParked otherwise.
	Discard(ctx context.Context, id string) error

	// HasOrderKey reports whether any record with that OrderKey is still in the
	// store, in any state. A producer of ordered work uses it to avoid processing a
	// later message ahead of an earlier one that is waiting. The empty key is never
	// present.
	HasOrderKey(ctx context.Context, orderKey string) (bool, error)

	// Stats summarises the store.
	Stats(ctx context.Context) (StoreStats, error)

	// Close releases resources. Records already stored remain durable.
	Close() error
}
