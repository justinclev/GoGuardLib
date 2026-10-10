package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

func TestDoRetriesUntilSuccess(t *testing.T) {
	calls := 0
	err := Do(context.Background(), Policy{MaxRetries: 3}, func(context.Context) error {
		calls++
		if calls < 3 {
			return errBoom
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("err=%v calls=%d, want nil and 3", err, calls)
	}
}

func TestDoStopsAtMaxRetries(t *testing.T) {
	calls := 0
	err := Do(context.Background(), Policy{MaxRetries: 2}, func(context.Context) error {
		calls++
		return errBoom
	})
	if !errors.Is(err, errBoom) || calls != 3 {
		t.Fatalf("err=%v calls=%d, want errBoom and 3 (1 try + 2 retries)", err, calls)
	}
}

func TestDoZeroRetriesRunsOnce(t *testing.T) {
	calls := 0
	_ = Do(context.Background(), Policy{}, func(context.Context) error { calls++; return errBoom })
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestPermanentIsNotRetriedAndIsUnwrapped(t *testing.T) {
	calls := 0
	err := Do(context.Background(), Policy{MaxRetries: 5}, func(context.Context) error {
		calls++
		return Permanent(errBoom)
	})
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
	if err != errBoom || IsPermanent(err) {
		t.Fatalf("err = %v, want the bare original error", err)
	}
	if Permanent(nil) != nil || IsPermanent(errBoom) {
		t.Fatal("Permanent(nil) must be nil and plain errors must not be permanent")
	}
}

type stop struct{}

func (stop) Error() string      { return "stop" }
func (stop) NonRetryable() bool { return true }

func TestNonRetryableInterfaceAndCancellation(t *testing.T) {
	calls := 0
	_ = Do(context.Background(), Policy{MaxRetries: 5}, func(context.Context) error { calls++; return stop{} })
	if calls != 1 {
		t.Fatalf("NonRetryable error retried: calls = %d", calls)
	}
	if DefaultRetryable(context.Canceled) || DefaultRetryable(nil) {
		t.Fatal("cancellation and nil must not be retryable")
	}
	if !DefaultRetryable(context.DeadlineExceeded) {
		t.Fatal("a per-attempt timeout should be retryable")
	}
}

func TestCustomRetryable(t *testing.T) {
	calls := 0
	_ = Do(context.Background(), Policy{MaxRetries: 5, Retryable: func(error) bool { return false }},
		func(context.Context) error { calls++; return errBoom })
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestDoStopsWhenContextEndsDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := Do(ctx, Policy{MaxRetries: 10, Backoff: Constant(time.Hour)}, func(context.Context) error { return errBoom })
	if !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want last error", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("Do ignored context cancellation while backing off")
	}
}

func TestOnRetryAndBackoffAreUsed(t *testing.T) {
	var attempts []int
	var delays []time.Duration
	_ = Do(context.Background(), Policy{
		MaxRetries: 3,
		Backoff:    Exponential(time.Millisecond, 3*time.Millisecond),
		OnRetry: func(a int, d time.Duration, err error) {
			attempts = append(attempts, a)
			delays = append(delays, d)
		},
	}, func(context.Context) error { return errBoom })
	if len(attempts) != 3 || attempts[0] != 1 || attempts[2] != 3 {
		t.Fatalf("attempts = %v", attempts)
	}
	want := []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond}
	for i := range want {
		if delays[i] != want[i] {
			t.Fatalf("delays = %v, want %v", delays, want)
		}
	}
}

func TestBudgetStopsRetryStorms(t *testing.T) {
	b := NewBudget(0.1, 2)
	retries := 0
	for i := 0; i < 20; i++ {
		calls := 0
		_ = Do(context.Background(), Policy{MaxRetries: 3, Budget: b}, func(context.Context) error {
			calls++
			return errBoom
		})
		retries += calls - 1
	}
	// 2 burst tokens + 20 requests * 0.1 = at most 4 retries in total.
	if retries > 4 || retries < 2 {
		t.Fatalf("retries = %d, want between 2 and 4", retries)
	}
}

func TestBudgetNilAndCap(t *testing.T) {
	var nilB *Budget
	nilB.Deposit()
	if !nilB.TryWithdraw() {
		t.Fatal("nil budget must allow retries")
	}
	b := NewBudget(1, 0) // burst clamps to 1
	for i := 0; i < 10; i++ {
		b.Deposit()
	}
	if !b.TryWithdraw() || b.TryWithdraw() {
		t.Fatal("tokens must be capped at burst")
	}
}

func TestBackoffs(t *testing.T) {
	if Constant(time.Second)(7) != time.Second {
		t.Fatal("Constant")
	}
	e := Exponential(10*time.Millisecond, 0)
	if e(0) != 0 || e(1) != 10*time.Millisecond || e(4) != 80*time.Millisecond {
		t.Fatalf("Exponential: %v %v %v", e(0), e(1), e(4))
	}
	if Exponential(0, time.Second)(3) != 0 {
		t.Fatal("zero base must give zero delay")
	}
	if d := Exponential(time.Second, 5*time.Second)(100); d != 5*time.Second {
		t.Fatalf("cap/overflow: got %v", d)
	}
	if d := Exponential(time.Second, 0)(200); d <= 0 {
		t.Fatalf("uncapped growth overflowed: %v", d)
	}
}

func TestJitterStaysInRange(t *testing.T) {
	base := Constant(100 * time.Millisecond)
	if Jitter(base, 0)(1) != 100*time.Millisecond {
		t.Fatal("zero fraction must be a no-op")
	}
	full := Jitter(base, 5) // clamps to 1
	half := Jitter(base, 0.5)
	varied := false
	for i := 0; i < 200; i++ {
		f, h := full(1), half(1)
		if f < 0 || f > 100*time.Millisecond {
			t.Fatalf("full jitter out of range: %v", f)
		}
		if h < 50*time.Millisecond || h > 100*time.Millisecond {
			t.Fatalf("half jitter out of range: %v", h)
		}
		if f != 100*time.Millisecond {
			varied = true
		}
	}
	if !varied {
		t.Fatal("jitter produced no variation")
	}
	if Jitter(Constant(0), 1)(1) != 0 {
		t.Fatal("zero delay must stay zero")
	}
}

func TestPermanentErrorMessageAndUnwrap(t *testing.T) {
	err := Permanent(errBoom)
	if err.Error() != "boom" || !errors.Is(err, errBoom) {
		t.Fatalf("err = %v", err)
	}
	// A marker inside a wrapping chain still stops retries but keeps the context.
	wrapped := errors.Join(errors.New("ctx"), Permanent(errBoom))
	calls := 0
	got := Do(context.Background(), Policy{MaxRetries: 3}, func(context.Context) error { calls++; return wrapped })
	if calls != 1 || got != wrapped {
		t.Fatalf("calls=%d got=%v", calls, got)
	}
}
