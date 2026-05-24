package engine

import (
	"sync"
	"sync/atomic"
	"time"
)

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

type bucket struct {
	success int64
	failure int64
}

type RollingWindow struct {
	mu             sync.RWMutex
	buckets        []bucket
	head           int
	bucketDuration time.Duration
	windowDuration time.Duration
	lastUpdate     int64 // UnixNano
	
	// O(1) Running sums
	totalSuccess int64
	totalFailure int64
}

func NewRollingWindow(window, bucketDuration time.Duration) *RollingWindow {
	numBuckets := int(window / bucketDuration)
	if numBuckets == 0 {
		numBuckets = 1
	}
	return &RollingWindow{
		buckets:        make([]bucket, numBuckets),
		bucketDuration: bucketDuration,
		windowDuration: window,
		lastUpdate:     time.Now().UnixNano(),
	}
}

func (w *RollingWindow) rotate(now int64) {
	last := atomic.LoadInt64(&w.lastUpdate)
	if now-last < int64(w.bucketDuration) {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Double-check
	if now-w.lastUpdate < int64(w.bucketDuration) {
		return
	}

	diff := int((now - w.lastUpdate) / int64(w.bucketDuration))
	
	// If diff > 1, we must clear all skipped buckets to fix L75 bug
	clearCount := diff
	if clearCount > len(w.buckets) {
		clearCount = len(w.buckets)
	}

	for i := 0; i < clearCount; i++ {
		w.head = (w.head + 1) % len(w.buckets)
		// Subtract evicted bucket from running sums
		atomic.AddInt64(&w.totalSuccess, -w.buckets[w.head].success)
		atomic.AddInt64(&w.totalFailure, -w.buckets[w.head].failure)
		// Clear bucket
		w.buckets[w.head] = bucket{}
	}

	atomic.StoreInt64(&w.lastUpdate, now)
}

func (w *RollingWindow) Success(now int64) {
	w.rotate(now)
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buckets[w.head].success++
	atomic.AddInt64(&w.totalSuccess, 1)
}

func (w *RollingWindow) Failure(now int64) {
	w.rotate(now)
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buckets[w.head].failure++
	atomic.AddInt64(&w.totalFailure, 1)
}

func (w *RollingWindow) FailureRate(now int64) float64 {
	w.rotate(now)
	
	// O(1) read
	success := atomic.LoadInt64(&w.totalSuccess)
	failure := atomic.LoadInt64(&w.totalFailure)
	total := success + failure

	if total <= 0 {
		return 0.0
	}
	return float64(failure) / float64(total)
}
