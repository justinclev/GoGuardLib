package metrics

import (
	"sync"
	"time"
)

type bucket struct {
	success int64
	failure int64
}

type RollingWindow struct {
	mu             sync.RWMutex
	buckets        []bucket
	bucketDuration time.Duration
	windowDuration time.Duration
	lastUpdate     time.Time
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
		lastUpdate:     time.Now(),
	}
}

func (w *RollingWindow) rotate(now time.Time) {
	elapsed := now.Sub(w.lastUpdate)
	if elapsed < w.bucketDuration {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Re-check after lock
	elapsed = now.Sub(w.lastUpdate)
	if elapsed < w.bucketDuration {
		return
	}

	shift := int(elapsed / w.bucketDuration)
	if shift >= len(w.buckets) {
		for i := range w.buckets {
			w.buckets[i] = bucket{}
		}
	} else {
		for i := 0; i < shift; i++ {
			copy(w.buckets[0:], w.buckets[1:])
			w.buckets[len(w.buckets)-1] = bucket{}
		}
	}
	w.lastUpdate = now.Truncate(w.bucketDuration)
}

func (w *RollingWindow) Success() {
	now := time.Now()
	w.rotate(now)
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buckets[len(w.buckets)-1].success++
}

func (w *RollingWindow) Failure() {
	now := time.Now()
	w.rotate(now)
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buckets[len(w.buckets)-1].failure++
}

func (w *RollingWindow) FailureRate() float64 {
	w.rotate(time.Now())
	w.mu.RLock()
	defer w.mu.RUnlock()

	var total, failures int64
	for _, b := range w.buckets {
		total += b.success + b.failure
		failures += b.failure
	}

	if total == 0 {
		return 0.0
	}
	return float64(failures) / float64(total)
}
