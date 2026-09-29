// Package obs defines the signals GoGuardLib emits.
//
// The library never logs or prints. Everything observable is delivered as a
// typed Event to a Sink the application supplies, or read on demand through
// Stats methods. Events carry metadata only (names, states, counts, timings);
// payloads, keys, headers and credentials never appear in them.
package obs

import (
	"time"

	"github.com/justinclev/GoGuardLib/internal/engine"
)

// State is the state of a circuit breaker.
type State = engine.BreakerState

const (
	StateClosed   = engine.StateClosed
	StateOpen     = engine.StateOpen
	StateHalfOpen = engine.StateHalfOpen
)

// Kind identifies an event type.
type Kind string

const (
	KindStateChanged Kind = "state_changed"
	KindRetried      Kind = "retried"
	KindProbeResult  Kind = "probe_result"
	KindRedrive      Kind = "redrive"
)

// Event is a signal emitted by the library.
type Event interface {
	EventKind() Kind
}

// StateChanged reports a circuit breaker transition.
type StateChanged struct {
	Dependency string
	From, To   State
	At         time.Time
}

func (StateChanged) EventKind() Kind { return KindStateChanged }

// Retried reports that an attempt failed and another will be made after Delay.
type Retried struct {
	Dependency string
	Attempt    int // 1 for the first retry
	Delay      time.Duration
	At         time.Time
}

func (Retried) EventKind() Kind { return KindRetried }

// ProbeResult reports one health check of a dependency whose circuit is open.
// It carries no error text: a check's error can embed the URL it called.
type ProbeResult struct {
	Dependency  string
	OK          bool
	Status      int // HTTP status when the check reported one, else 0
	Latency     time.Duration
	Consecutive int // healthy checks in a row, including this one
	At          time.Time
}

func (ProbeResult) EventKind() Kind { return KindProbeResult }

// RedriveOutcome says what happened to a stored record when it was retried.
type RedriveOutcome string

const (
	RedriveSucceeded RedriveOutcome = "succeeded" // handled and removed
	RedriveRetry     RedriveOutcome = "retry"     // failed; will be tried again later
	RedriveBlocked   RedriveOutcome = "blocked"   // its dependency is down; returned without penalty
	RedriveParked    RedriveOutcome = "parked"    // needs a human
	RedriveLeaseLost RedriveOutcome = "lease_lost"
)

// Redrive reports one attempt to reprocess a stored record. RecordID is the
// record's identifier (never its contents).
type Redrive struct {
	Dependency string
	RecordID   string
	Outcome    RedriveOutcome
	Attempts   int
	Latency    time.Duration
	At         time.Time
}

func (Redrive) EventKind() Kind { return KindRedrive }

// Sink receives events. Emit is called on hot paths, so implementations must
// return quickly and must not block; use a Dispatcher to hand events to code
// that may be slow.
type Sink interface {
	Emit(Event)
}

// SinkFunc adapts a function to a Sink.
type SinkFunc func(Event)

func (f SinkFunc) Emit(e Event) { f(e) }

// Emit sends e to s if s is non-nil.
func Emit(s Sink, e Event) {
	if s != nil {
		s.Emit(e)
	}
}

type multi []Sink

func (m multi) Emit(e Event) {
	for _, s := range m {
		s.Emit(e)
	}
}

// Multi fans events out to every non-nil sink, in order. It returns nil when
// there are none.
func Multi(sinks ...Sink) Sink {
	out := make(multi, 0, len(sinks))
	for _, s := range sinks {
		if s != nil {
			out = append(out, s)
		}
	}
	switch len(out) {
	case 0:
		return nil
	case 1:
		return out[0]
	}
	return out
}
