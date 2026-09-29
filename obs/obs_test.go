package obs

import (
	"sync"
	"testing"
	"time"
)

func TestDispatcherPreservesOrderAndDrainsOnClose(t *testing.T) {
	var mu sync.Mutex
	var got []int
	d := NewDispatcher(func(e Event) {
		mu.Lock()
		got = append(got, e.(Retried).Attempt)
		mu.Unlock()
	}, 0)

	for i := 1; i <= 100; i++ {
		d.Emit(Retried{Attempt: i})
	}
	d.Close()

	if len(got) != 100 {
		t.Fatalf("delivered %d events, want 100", len(got))
	}
	for i, v := range got {
		if v != i+1 {
			t.Fatalf("event %d out of order: got attempt %d", i, v)
		}
	}
}

func TestDispatcherNeverBlocksAndCountsDrops(t *testing.T) {
	release := make(chan struct{})
	d := NewDispatcher(func(Event) { <-release }, 2)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			d.Emit(Retried{Attempt: i})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Emit blocked on a stalled hook")
	}
	if d.Dropped() == 0 {
		t.Fatal("expected drops with a full buffer")
	}
	close(release)
	d.Close()
}

func TestDispatcherSurvivesHookPanic(t *testing.T) {
	var n int
	d := NewDispatcher(func(e Event) {
		n++
		if n == 1 {
			panic("boom")
		}
	}, 0)
	d.Emit(Retried{})
	d.Emit(Retried{})
	d.Close()
	if n != 2 || d.Panics() != 1 {
		t.Fatalf("delivered=%d panics=%d, want 2 and 1", n, d.Panics())
	}
}

func TestDispatcherEmitAfterCloseIsDropped(t *testing.T) {
	d := NewDispatcher(func(Event) {}, 0)
	d.Close()
	d.Close() // idempotent
	d.Emit(Retried{})
	if d.Dropped() != 1 {
		t.Fatalf("dropped = %d, want 1", d.Dropped())
	}
}

func TestMultiAndEmit(t *testing.T) {
	var a, b int
	s := Multi(nil, SinkFunc(func(Event) { a++ }), SinkFunc(func(Event) { b++ }))
	Emit(s, StateChanged{})
	Emit(nil, StateChanged{}) // no-op
	if a != 1 || b != 1 {
		t.Fatalf("a=%d b=%d, want 1 1", a, b)
	}
	if Multi(nil, nil) != nil {
		t.Fatal("Multi of nothing should be nil")
	}
	single := SinkFunc(func(Event) {})
	if Multi(single) == nil {
		t.Fatal("Multi of one sink should return it")
	}
	if (StateChanged{}).EventKind() != KindStateChanged || (Retried{}).EventKind() != KindRetried {
		t.Fatal("wrong event kinds")
	}
}
