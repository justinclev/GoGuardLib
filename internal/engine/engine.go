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

func (s *AtomicState) Get() BreakerState {
	return BreakerState(atomic.LoadInt32(&s.current))
}

func (s *AtomicState) Transition(from, to BreakerState) bool {
	return atomic.CompareAndSwapInt32(&s.current, int32(from), int32(to))
}

func NewAtomicState() *AtomicState {
	return &AtomicState{}
}

type bucket struct {
	success      int64
	failure      int64
	totalLatency int64
	retryCount   int64
}

type RollingWindow struct {
	mu             sync.RWMutex
	buckets        []bucket
	lastUpdate     int64
	totalSuccess   int64
	totalFailure   int64
	totalLatency   int64
	totalRetries   int64
	bucketDuration time.Duration
	windowDuration time.Duration
	head           int
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

	if now-w.lastUpdate < int64(w.bucketDuration) {
		return
	}

	diff := int((now - w.lastUpdate) / int64(w.bucketDuration))
	clearCount := diff
	if clearCount > len(w.buckets) {
		clearCount = len(w.buckets)
	}

	for i := 0; i < clearCount; i++ {
		w.head = (w.head + 1) % len(w.buckets)
		atomic.AddInt64(&w.totalSuccess, -w.buckets[w.head].success)
		atomic.AddInt64(&w.totalFailure, -w.buckets[w.head].failure)
		atomic.AddInt64(&w.totalLatency, -w.buckets[w.head].totalLatency)
		atomic.AddInt64(&w.totalRetries, -w.buckets[w.head].retryCount)
		w.buckets[w.head] = bucket{}
	}

	atomic.StoreInt64(&w.lastUpdate, now)
}

func (w *RollingWindow) Success(now int64, latencyUs int64) {
	w.rotate(now)
	w.mu.RLock()
	defer w.mu.RUnlock()
	atomic.AddInt64(&w.buckets[w.head].success, 1)
	atomic.AddInt64(&w.totalSuccess, 1)
	if latencyUs > 0 {
		atomic.AddInt64(&w.buckets[w.head].totalLatency, latencyUs)
		atomic.AddInt64(&w.totalLatency, latencyUs)
	}
}

func (w *RollingWindow) Failure(now int64) {
	w.rotate(now)
	w.mu.RLock()
	defer w.mu.RUnlock()
	atomic.AddInt64(&w.buckets[w.head].failure, 1)
	atomic.AddInt64(&w.totalFailure, 1)
}

func (w *RollingWindow) RecordRetry(now int64) {
	w.rotate(now)
	w.mu.RLock()
	defer w.mu.RUnlock()
	atomic.AddInt64(&w.buckets[w.head].retryCount, 1)
	atomic.AddInt64(&w.totalRetries, 1)
}

func (w *RollingWindow) FailureRateBps(now int64) int64 {
	w.rotate(now)
	success := atomic.LoadInt64(&w.totalSuccess)
	failure := atomic.LoadInt64(&w.totalFailure)
	total := success + failure
	if total <= 0 {
		return 0
	}
	return (failure * 10000) / total
}

func (w *RollingWindow) RetryRateBps() int64 {
	success := atomic.LoadInt64(&w.totalSuccess)
	retries := atomic.LoadInt64(&w.totalRetries)
	if success <= 0 {
		return 0
	}
	return (retries * 10000) / (success + retries)
}

func (w *RollingWindow) AvgLatencyUs() int64 {
	success := atomic.LoadInt64(&w.totalSuccess)
	if success <= 0 {
		return 0
	}
	return atomic.LoadInt64(&w.totalLatency) / success
}

func (w *RollingWindow) Counts() (success, failure int64) {
	now := time.Now().UnixNano()
	if now-atomic.LoadInt64(&w.lastUpdate) > int64(w.bucketDuration) {
		w.rotate(now)
	}
	return atomic.LoadInt64(&w.totalSuccess), atomic.LoadInt64(&w.totalFailure)
}
