package pipeline_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/pipeline"
	"github.com/justinclev/GoGuardLib/retry"
)

// parkedAt runs a message through a,b,c,d until c fails permanently, leaving it
// parked with a and b done (and, with saga, both undone).
func parkedAt(t *testing.T, h *harness, saga bool) (*pipeline.Pipeline, dlq.Store) {
	t.Helper()
	h.failWith("c", func() error { return retry.Permanent(errors.New("declined")) })
	var opts []pipeline.Option
	if saga {
		opts = append(opts, pipeline.Saga())
	}
	p := mustPipeline(t, h.steps(saga), opts...)
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	res, err := p.Execute(context.Background(), store, input("m1"))
	if err != nil || res != pipeline.Parked {
		t.Fatalf("setup: res=%v err=%v", res, err)
	}
	return p, store
}

func parkedCheckpoint(t *testing.T, store dlq.Store) []byte {
	t.Helper()
	list, err := store.Parked(context.Background(), dlq.ParkedQuery{})
	if err != nil || len(list) != 1 {
		t.Fatalf("Parked = %d, %v", len(list), err)
	}
	return list[0].Checkpoint
}

func TestDescribeReportsProgressWithoutMessageData(t *testing.T) {
	h := newHarness()
	p, store := parkedAt(t, h, false)
	pr, err := p.Describe(parkedCheckpoint(t, store))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(pr.Completed, ",") != "a,b" || pr.Next != "c" || pr.Phase != "forward" || pr.Pipeline != "orders" ||
		pr.Version != "v1" || strings.Join(pr.StateKeys, ",") != "order-id" || pr.Epoch != 0 {
		t.Fatalf("progress = %+v", pr)
	}

	sp, sstore := parkedAt(t, newHarness(), true)
	pr, _ = sp.Describe(parkedCheckpoint(t, sstore))
	if pr.Phase != "compensating" || pr.FailedStep != "c" || !strings.Contains(pr.FailReason, "declined") ||
		strings.Join(pr.Compensated, ",") != "b,a" || pr.Next != "" {
		t.Fatalf("compensated progress = %+v", pr)
	}

	if _, err := p.Describe([]byte("garbage")); !errors.Is(err, pipeline.ErrPipelineMismatch) {
		t.Fatalf("err = %v", err)
	}
	if fresh, err := p.Describe(nil); err != nil || fresh.Next != "a" || len(fresh.Completed) != 0 {
		t.Fatalf("no checkpoint = %+v, %v", fresh, err)
	}
}

func TestRewindResumesAtTheChosenStepAndKeepsSavedData(t *testing.T) {
	h := newHarness()
	p, store := parkedAt(t, h, false)
	raw := parkedCheckpoint(t, store)

	for step, want := range map[string]string{"a": "", "b": "a", "c": "a,b"} { // c is the step it stopped at
		out, err := p.Rewind(raw, step)
		if err != nil {
			t.Fatalf("Rewind(%s): %v", step, err)
		}
		pr, err := p.Describe(out)
		if err != nil || strings.Join(pr.Completed, ",") != want || pr.Next != step || pr.Phase != "forward" {
			t.Fatalf("Rewind(%s) -> %+v, %v", step, pr, err)
		}
		if strings.Join(pr.StateKeys, ",") != "order-id" {
			t.Fatalf("Rewind(%s) lost the saved data: %v", step, pr.StateKeys)
		}
		if pr.Epoch != 0 {
			t.Fatalf("Rewind(%s) changed the epoch without any compensation", step)
		}
	}
}

func TestRewindRefusesToSkipAheadOrUseAnUnknownStep(t *testing.T) {
	h := newHarness()
	p, store := parkedAt(t, h, false)
	raw := parkedCheckpoint(t, store)
	if _, err := p.Rewind(raw, "d"); !errors.Is(err, pipeline.ErrCannotSkipForward) {
		t.Fatalf("err = %v: resuming at d would silently skip c", err)
	}
	if _, err := p.Rewind(raw, "nope"); !errors.Is(err, pipeline.ErrUnknownStep) {
		t.Fatalf("err = %v", err)
	}
	if _, err := p.Rewind([]byte("garbage"), "a"); !errors.Is(err, pipeline.ErrPipelineMismatch) {
		t.Fatalf("err = %v", err)
	}
	if _, err := p.Rewind(nil, "b"); !errors.Is(err, pipeline.ErrCannotSkipForward) {
		t.Fatalf("a record with no progress cannot resume at step b: %v", err)
	}
	if out, err := p.Rewind(nil, "a"); err != nil || out == nil {
		t.Fatalf("Rewind(nil, a) = %v", err)
	}
	if string(raw) == "" {
		t.Fatal("Rewind must not modify its input")
	}
}

// After compensation the undone steps are redone as NEW work. Their idempotency keys
// must differ from the original run's, or a deduplicating downstream would treat
// them as already done.
func TestRewindPastCompensatedStepsStartsANewIdempotencyEpoch(t *testing.T) {
	h := newHarness()
	p, store := parkedAt(t, h, true) // a,b done then undone; c failed
	originalKeyA := h.keys["a"][0]
	raw := parkedCheckpoint(t, store)

	if _, err := p.Rewind(raw, "c"); !errors.Is(err, pipeline.ErrStepUndone) {
		t.Fatalf("err = %v: c cannot be resumed while b, its predecessor, has been undone", err)
	}
	if _, err := p.Rewind(raw, "b"); !errors.Is(err, pipeline.ErrStepUndone) {
		t.Fatalf("err = %v: b cannot be resumed while a has been undone", err)
	}
	out, err := p.Rewind(raw, "a")
	if err != nil {
		t.Fatal(err)
	}
	pr, _ := p.Describe(out)
	if pr.Epoch != 1 || len(pr.Compensated) != 0 || pr.Phase != "forward" || pr.FailedStep != "" || pr.Next != "a" {
		t.Fatalf("progress = %+v", pr)
	}

	if pipeline.IdempotencyKeyEpoch("m1", "a", 0) != originalKeyA || pipeline.IdempotencyKey("m1", "a") != originalKeyA {
		t.Fatal("epoch 0 must give the original key")
	}
	if pipeline.IdempotencyKeyEpoch("m1", "a", 1) == originalKeyA || pipeline.IdempotencyKeyEpoch("m1", "a", 1) == pipeline.IdempotencyKeyEpoch("m1", "a", 2) {
		t.Fatal("each epoch must give different keys")
	}
}

func TestRedriveResumesAtTheChosenStepWithoutRepeatingEarlierOnes(t *testing.T) {
	h := newHarness()
	p, store := parkedAt(t, h, false)
	keyB := h.keys["b"][0]

	h.failWith("c", nil) // the operator fixed whatever was wrong
	must(t, p.Redrive(context.Background(), store, "m1", "b"))
	startRedriver(t, store, p, nil)
	eventually(t, "the record to finish", func() bool { return total(t, store).Total() == 0 })

	if h.count("a") != 1 {
		t.Fatalf("a ran %d times; it was before the chosen step", h.count("a"))
	}
	if h.count("b") != 2 || h.count("c") != 2 || h.count("d") != 1 {
		t.Fatalf("b=%d c=%d d=%d", h.count("b"), h.count("c"), h.count("d"))
	}
	if h.keys["b"][1] != keyB {
		t.Fatal("b was not compensated, so its rerun must carry the same key: to a downstream it is a repeat")
	}
	// d still saw what step a produced: the saved data survived the rewind.
	if h.count("d") != 1 {
		t.Fatal("d did not complete")
	}
}

func TestRedriveAfterCompensationRunsEverythingAgainWithNewKeys(t *testing.T) {
	h := newHarness()
	p, store := parkedAt(t, h, true)
	first := map[string]string{"a": h.keys["a"][0], "b": h.keys["b"][0], "c": h.keys["c"][0]}

	// Requeueing "as it was" just finishes compensating again and re-parks.
	must(t, p.Redrive(context.Background(), store, "m1", ""))
	r, stop := startRedriver(t, store, p, nil)
	eventually(t, "it to be parked again", func() bool { return r.Stats().Parked == 1 })
	stop()
	if h.count("a") != 1 {
		t.Fatalf("a ran %d times: nothing should have re-run", h.count("a"))
	}

	// Resuming from the start with the cause fixed redoes the work, with fresh keys.
	h.failWith("c", nil)
	must(t, p.Redrive(context.Background(), store, "m1", "a"))
	startRedriver(t, store, p, nil)
	eventually(t, "the record to finish", func() bool { return total(t, store).Total() == 0 })
	for _, step := range []string{"a", "b", "c"} {
		keys := h.keys[step]
		if got := keys[len(keys)-1]; got == first[step] {
			t.Fatalf("step %s reused its original idempotency key after being undone", step)
		}
	}
	if h.count("d") != 1 {
		t.Fatalf("d ran %d times", h.count("d"))
	}
}

func TestRedriveRefusesRecordsThatAreNotParkedOrUnknown(t *testing.T) {
	h := newHarness()
	p, store := parkedAt(t, h, false)
	ctx := context.Background()
	if err := p.Redrive(ctx, store, "nope", "a"); !errors.Is(err, dlq.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
	if err := p.Redrive(ctx, store, "m1", "d"); !errors.Is(err, pipeline.ErrCannotSkipForward) {
		t.Fatalf("err = %v", err)
	}
	if err := p.Redrive(ctx, store, "m1", "zzz"); !errors.Is(err, pipeline.ErrUnknownStep) {
		t.Fatalf("err = %v", err)
	}
	// A rejected redrive leaves the record parked and untouched.
	if list, _ := store.Parked(ctx, dlq.ParkedQuery{}); len(list) != 1 {
		t.Fatal("a rejected Redrive changed the record")
	}

	must(t, p.Redrive(ctx, store, "m1", "b"))
	if err := p.Redrive(ctx, store, "m1", "b"); !errors.Is(err, dlq.ErrNotParked) {
		t.Fatalf("second Redrive: err = %v, want ErrNotParked (it is already pending)", err)
	}
	must(t, store.Append(ctx, dlq.Record{ID: "plain", Value: []byte("x")}))
	if err := p.Redrive(ctx, store, "plain", "a"); !errors.Is(err, dlq.ErrNotParked) {
		t.Fatalf("err = %v", err)
	}
}

func TestRedriveWorksOnTheDurableStoreAcrossARestart(t *testing.T) {
	dir := t.TempDir()
	h := newHarness()
	h.failWith("c", func() error { return retry.Permanent(errors.New("declined")) })
	p := mustPipeline(t, h.steps(false))
	store, err := dlq.OpenWAL(dir, dlq.WALOptions{})
	must(t, err)
	if res, _ := p.Execute(context.Background(), store, input("m1")); res != pipeline.Parked {
		t.Fatalf("res = %v", res)
	}
	must(t, p.Redrive(context.Background(), store, "m1", "b"))
	must(t, store.Close()) // the process ends between the operator's action and the redrive

	h.failWith("c", nil)
	store, err = dlq.OpenWAL(dir, dlq.WALOptions{})
	must(t, err)
	defer store.Close()
	startRedriver(t, store, p, nil)
	eventually(t, "the record to finish", func() bool { return total(t, store).Total() == 0 })
	if h.count("a") != 1 || h.count("d") != 1 {
		t.Fatalf("a=%d d=%d", h.count("a"), h.count("d"))
	}
}

func TestRedriveWorksThroughTheEncryptingStore(t *testing.T) {
	h := newHarness()
	h.failWith("c", func() error { return retry.Permanent(errors.New("declined")) })
	p := mustPipeline(t, h.steps(false))
	store := secureStore(t)
	if res, _ := p.Execute(context.Background(), store, input("m1")); res != pipeline.Parked {
		t.Fatalf("res = %v", res)
	}
	pr, err := p.Describe(parkedCheckpoint(t, store)) // Parked() decrypts for the operator
	if err != nil || strings.Join(pr.Completed, ",") != "a,b" {
		t.Fatalf("progress = %+v, %v", pr, err)
	}
	must(t, p.Redrive(context.Background(), store, "m1", "b"))
	h.failWith("c", nil)
	startRedriver(t, store, p, nil)
	eventually(t, "the record to finish", func() bool { return total(t, store).Total() == 0 })
	if h.count("a") != 1 || h.count("d") != 1 {
		t.Fatalf("a=%d d=%d", h.count("a"), h.count("d"))
	}
}
