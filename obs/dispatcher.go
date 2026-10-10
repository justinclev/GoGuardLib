package obs

import (
	"sync"
	"sync/atomic"
)

const defaultBuffer = 1024

// Dispatcher delivers events to a hook on its own goroutine, in emission
// order, so a slow hook can never stall the code that emits.
//
// Delivery is best effort: when the buffer is full the event is dropped and
// counted (see Dropped) instead of blocking the caller. A panicking hook is
// recovered and counted (see Panics); it does not stop delivery.
type Dispatcher struct {
	hook    func(Event)
	ch      chan Event
	done    chan struct{}
	mu      sync.RWMutex
	closed  bool
	dropped atomic.Uint64
	panics  atomic.Uint64
}

// NewDispatcher starts a dispatcher. buffer <= 0 selects a default of 1024.
func NewDispatcher(hook func(Event), buffer int) *Dispatcher {
	if buffer <= 0 {
		buffer = defaultBuffer
	}
	d := &Dispatcher{hook: hook, ch: make(chan Event, buffer), done: make(chan struct{})}
	go d.run()
	return d
}

// Emit queues e without blocking.
func (d *Dispatcher) Emit(e Event) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		d.dropped.Add(1)
		return
	}
	select {
	case d.ch <- e:
	default:
		d.dropped.Add(1)
	}
}

// Dropped returns how many events were discarded because the buffer was full
// or the dispatcher was closed.
func (d *Dispatcher) Dropped() uint64 { return d.dropped.Load() }

// Panics returns how many hook invocations panicked.
func (d *Dispatcher) Panics() uint64 { return d.panics.Load() }

// Close stops accepting events, delivers everything already queued and waits
// for the hook to return. It is safe to call more than once. A hook that never
// returns blocks Close.
func (d *Dispatcher) Close() {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		<-d.done
		return
	}
	d.closed = true
	close(d.ch)
	d.mu.Unlock()
	<-d.done
}

func (d *Dispatcher) run() {
	defer close(d.done)
	for e := range d.ch {
		d.deliver(e)
	}
}

func (d *Dispatcher) deliver(e Event) {
	defer func() {
		if recover() != nil {
			d.panics.Add(1)
		}
	}()
	d.hook(e)
}
