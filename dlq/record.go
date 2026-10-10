// Package dlq defines the dead-letter queue: the record that captures a piece of
// work that could not be completed, and the Store contract that keeps it safe
// until it can be retried.
//
// Design rules that every Store honours:
//
//   - Append returns only once the record is durable, and never drops a record:
//     a full store returns ErrFull so the caller can pause instead.
//   - Append is idempotent on Record.ID, so replaying an input after a crash does
//     not create duplicates.
//   - Records are handed out under a lease with a fencing token. A worker that
//     crashes loses its lease and the record is offered again; a stale worker
//     cannot ack or overwrite a record someone else now holds.
//   - Delivery is at-least-once. Handlers must tolerate seeing a record twice.
//   - Errors never contain payload bytes.
package dlq

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// CurrentSchema is the record layout version written by this package.
const CurrentSchema = 1

const maxIDLen = 256

// State is where a record is in its life.
type State int

const (
	// Pending records wait for their NextAttempt time and a Lease.
	Pending State = iota
	// Leased records are held by a worker until the lease expires or it finishes.
	Leased
	// Parked records need a human: they are never retried automatically and never
	// deleted automatically.
	Parked
)

func (s State) String() string {
	switch s {
	case Pending:
		return "pending"
	case Leased:
		return "leased"
	case Parked:
		return "parked"
	default:
		return "unknown"
	}
}

// Header is one message header. Names are ordered and may repeat, as in Kafka.
type Header struct {
	Key   string
	Value []byte
}

// Source says where a record came from.
type Source struct {
	// Kind is the transport, for example "kafka" or "http".
	Kind string
	// Name is the topic, queue or host.
	Name      string
	Partition int32
	Offset    int64
}

// Record is a unit of work set aside for later.
type Record struct {
	// ID identifies the record. It is required. Use DeterministicID or KafkaID for
	// records derived from an input so re-appending after a crash is a no-op, and
	// NewID otherwise.
	ID string
	// Seq is assigned by the Store in append order; any value supplied is ignored.
	Seq uint64
	// State is maintained by the Store. When appending, Pending (the zero value)
	// queues the record and Parked stores it directly for a human, for failures
	// known to be permanent.
	State State

	Source  Source
	Key     []byte
	Value   []byte // nil is preserved (a Kafka tombstone) and is different from empty
	Headers []Header

	// OrderKey, when set, makes the Store hand out records with the same OrderKey
	// strictly one at a time in Seq order (head-of-line), for per-key ordering. A
	// parked head blocks the records behind it until it is requeued or discarded.
	// It is stored in clear unless the Store is wrapped by Secure with a pepper.
	OrderKey string

	// BlockedOn names the dependency the record is waiting for, so recovery of
	// that dependency releases exactly these records.
	BlockedOn string

	// Attempts counts leases handed out for this record, including ones that
	// ended in a crash. It is what lets a poison pill that kills its worker
	// eventually be parked.
	Attempts    int
	FirstFailed time.Time
	LastFailed  time.Time
	// NextAttempt is the earliest time the record may be leased.
	NextAttempt time.Time
	// LastError is a redacted description of the last failure.
	LastError string

	// Checkpoint is opaque progress saved by a multi-step handler so a retry
	// resumes where the last attempt stopped.
	Checkpoint []byte

	SchemaVersion int
}

// Size estimates the bytes the record occupies, for capacity accounting.
func (r *Record) Size() int {
	n := len(r.ID) + len(r.Key) + len(r.Value) + len(r.Checkpoint) + len(r.OrderKey) +
		len(r.BlockedOn) + len(r.LastError) + len(r.Source.Kind) + len(r.Source.Name) + 64
	for _, h := range r.Headers {
		n += len(h.Key) + len(h.Value)
	}
	return n
}

// Clone returns a deep copy, so callers and stores never share mutable memory.
func (r Record) Clone() Record {
	r.Key = cloneBytes(r.Key)
	r.Value = cloneBytes(r.Value)
	r.Checkpoint = cloneBytes(r.Checkpoint)
	if r.Headers != nil {
		hs := make([]Header, len(r.Headers))
		for i, h := range r.Headers {
			hs[i] = Header{Key: h.Key, Value: cloneBytes(h.Value)}
		}
		r.Headers = hs
	}
	return r
}

// cloneBytes copies b, keeping nil distinct from empty.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte{}, b...)
}

// Validate reports whether the record can be stored.
func (r *Record) Validate() error {
	if r.ID == "" || len(r.ID) > maxIDLen {
		return fmt.Errorf("%w: id must be 1 to %d bytes", ErrInvalidRecord, maxIDLen)
	}
	if r.Attempts < 0 {
		return fmt.Errorf("%w: negative attempts", ErrInvalidRecord)
	}
	if r.State == Leased {
		return fmt.Errorf("%w: a record cannot be appended already leased", ErrInvalidRecord)
	}
	return nil
}

// NewID returns a time-ordered random identifier (UUIDv7).
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[6:]); err != nil {
		panic("dlq: crypto/rand failed: " + err.Error())
	}
	ms := uint64(time.Now().UnixMilli())
	b[0], b[1], b[2] = byte(ms>>40), byte(ms>>32), byte(ms>>24)
	b[3], b[4], b[5] = byte(ms>>16), byte(ms>>8), byte(ms)
	b[6] = b[6]&0x0f | 0x70
	b[8] = b[8]&0x3f | 0x80
	return formatUUID(b)
}

// DeterministicID derives a stable identifier from parts. The same parts always
// give the same ID, and different parts (including different groupings such as
// "ab","c" versus "a","bc") give different IDs.
func DeterministicID(parts ...string) string {
	h := sha256.New()
	var n [8]byte
	for _, p := range parts {
		binary.BigEndian.PutUint64(n[:], uint64(len(p)))
		h.Write(n[:])
		h.Write([]byte(p))
	}
	var b [16]byte
	copy(b[:], h.Sum(nil))
	b[6] = b[6]&0x0f | 0x80
	b[8] = b[8]&0x3f | 0x80
	return formatUUID(b)
}

// KafkaID is the identifier of the record for one Kafka message. A consumer that
// crashes after appending but before committing gets the same ID on redelivery.
func KafkaID(topic string, partition int32, offset int64) string {
	return DeterministicID("kafka", topic, strconv.FormatInt(int64(partition), 10), strconv.FormatInt(offset, 10))
}

// KafkaIDIn is KafkaID scoped to a namespace: the name of the cluster (or of the
// disaster-recovery pairing) the message came from. A message's topic, partition
// and offset identify it only within one cluster. When a topic is deleted and
// recreated, or a consumer fails over to a replica cluster whose offsets differ,
// the same triple names a different message, and a stored record from the old one
// would make the store treat the new message as a duplicate and drop it. Give
// every cluster its own namespace. An empty namespace gives KafkaID.
func KafkaIDIn(namespace, topic string, partition int32, offset int64) string {
	if namespace == "" {
		return KafkaID(topic, partition, offset)
	}
	return DeterministicID("kafka", namespace, topic, strconv.FormatInt(int64(partition), 10), strconv.FormatInt(offset, 10))
}

func formatUUID(b [16]byte) string {
	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:], b[10:])
	return string(out[:])
}

var (
	// ErrInvalidRecord means the record failed validation.
	ErrInvalidRecord = errors.New("dlq: invalid record")
	// ErrNotFound means no record has that ID.
	ErrNotFound = errors.New("dlq: record not found")
	// ErrLeaseLost means the caller's lease is no longer current: the token is
	// wrong or the record was leased again after expiry. The caller must stop
	// working on the record.
	ErrLeaseLost = errors.New("dlq: lease lost")
	// ErrNotParked means the operation only applies to parked records.
	ErrNotParked = errors.New("dlq: record is not parked")
	// ErrFull means the store is at capacity. The record was NOT stored; the
	// caller must apply backpressure (pause consuming) rather than drop it.
	ErrFull = errors.New("dlq: store is full")
	// ErrClosed means the store has been closed.
	ErrClosed = errors.New("dlq: store is closed")
)

// BlockedError tells the redriver that a record could not be processed because a
// dependency is unavailable. The record is returned to the queue tagged with that
// dependency, so it is held until the dependency recovers.
type BlockedError struct {
	// Dependency names what is unavailable (a circuit breaker or step name).
	Dependency string
	// Err is the underlying failure.
	Err error
	// Refund, when true, does not count the attempt against the record's budget.
	// Set it when the call was never made because the circuit was open; leave it
	// false when the call was made and failed, so a message that itself breaks the
	// dependency is eventually parked instead of retried forever.
	Refund bool
}

func (e *BlockedError) Error() string {
	return fmt.Sprintf("dlq: blocked on %s: %v", e.Dependency, e.Err)
}

func (e *BlockedError) Unwrap() error { return e.Err }
