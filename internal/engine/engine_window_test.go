package engine

import (
	"testing"
	"time"
)

// Buckets must keep their configured length even when traffic is sparse: a
// rotation that resets the clock to "now" loses the part of a bucket that had
// already elapsed, so the window quietly grows.
func TestWindowKeepsItsLengthUnderSparseTraffic(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	w := NewRollingWindow(time.Second, 100*time.Millisecond)
	w.lastUpdate = base.UnixNano()

	w.Failure(base.UnixNano()) // recorded in the first bucket
	for k := 1; k <= 20; k++ {
		now := base.Add(time.Duration(k) * 150 * time.Millisecond)
		w.Success(now.UnixNano(), 1)
		_, failures := w.counts(now.UnixNano())
		age := time.Duration(k) * 150 * time.Millisecond
		if age >= time.Second+100*time.Millisecond && failures != 0 {
			t.Fatalf("a failure %v old (window 1s, bucket 100ms) is still counted", age)
		}
		if age < time.Second && failures != 1 {
			t.Fatalf("a failure %v old was dropped from a 1s window", age)
		}
	}
}

// counts is Counts at an explicit time.
func (w *RollingWindow) counts(now int64) (success, failure int64) {
	w.rotate(now)
	return w.totalSuccess, w.totalFailure
}
