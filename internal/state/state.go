package state

import "sync/atomic"

type BreakerState int32

const (
	StateClosed BreakerState = iota
	StateOpen
	StateHalfOpen
)

type AtomicState struct {
	current int32
}

func NewAtomicState() *AtomicState {
	return &AtomicState{}
}

func (s *AtomicState) Get() BreakerState {
	return BreakerState(atomic.LoadInt32(&s.current))
}

func (s *AtomicState) Transition(from, to BreakerState) bool {
	return atomic.CompareAndSwapInt32(&s.current, int32(from), int32(to))
}
