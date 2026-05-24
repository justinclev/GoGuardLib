package metrics

import (
	"testing"
	"time"
)

func TestRollingWindow(t *testing.T) {
	window := 1 * time.Second
	bucket := 100 * time.Millisecond
	rw := NewRollingWindow(window, bucket)

	rw.Success()
	rw.Success()
	rw.Failure()

	rate := rw.FailureRate()
	if rate != 1.0/3.0 {
		t.Errorf("Expected failure rate 0.33, got %f", rate)
	}

	// Wait for rotation
	time.Sleep(200 * time.Millisecond)
	rw.Success()

	rate = rw.FailureRate()
	// Total should be 2 successes, 1 failure, 1 new success = 4 total, 1 failure
	if rate != 1.0/4.0 {
		t.Errorf("Expected failure rate 0.25 after rotation, got %f", rate)
	}

	// Wait for full window to expire
	time.Sleep(1 * time.Second)
	rate = rw.FailureRate()
	if rate != 0.0 {
		t.Errorf("Expected failure rate 0.0 after window expiry, got %f", rate)
	}
}

func TestRollingWindowConcurrency(t *testing.T) {
	rw := NewRollingWindow(100*time.Millisecond, 10*time.Millisecond)
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				rw.Success()
				rw.Failure()
				rw.FailureRate()
			}
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
