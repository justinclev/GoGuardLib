package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/justinclev/GoGuardLib/retry"
)

func twoSteps() *Pipeline {
	ok := func(context.Context, *Exec) error { return nil }
	p, err := New("p", "v1", []Step{{Name: "a", Run: ok}, {Name: "b", Run: ok}})
	if err != nil {
		panic(err)
	}
	return p
}

// If a checkpoint cannot be saved (lease lost, store failed) the run must stop and
// report an ordinary error: the step's work is done but unrecorded, so a retry
// re-runs it, and it must not be mistaken for a permanent failure.
func TestFailedCheckpointSaveStopsTheRunAsATransientError(t *testing.T) {
	p := twoSteps()
	saveErr := errors.New("lease lost")
	r := &run{p: p, x: &Exec{ID: "m"}, cp: p.newCheckpoint(), persist: func(context.Context, []byte) error { return saveErr }}
	err := r.advance(context.Background())
	if !errors.Is(err, saveErr) || retry.IsPermanent(err) {
		t.Fatalf("err = %v", err)
	}
	if len(r.cp.Completed) != 1 {
		t.Fatalf("completed = %v: the run must stop right after the failed save", r.cp.Completed)
	}
}

func FuzzLoadCheckpoint(f *testing.F) {
	p := twoSteps()
	valid := p.newCheckpoint()
	valid.Completed = []doneStep{{Step: "a", At: 1}}
	valid.State = map[string][]byte{"k": []byte("v")}
	f.Add(valid.encode())
	f.Add([]byte(`{"v":1,"pipeline":"p","phase":"compensating","completed":[{"step":"a"}],"compensated":["a"]}`))
	f.Add([]byte(`{`))
	f.Add([]byte(`null`))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, raw []byte) {
		cp, err := p.load(raw)
		if err != nil {
			if !errors.Is(err, ErrPipelineMismatch) || !retry.IsPermanent(err) {
				t.Fatalf("a bad checkpoint must be a permanent mismatch, got %v", err)
			}
			return
		}
		// Anything accepted must fit the pipeline: its completed steps are a prefix.
		for i, d := range cp.Completed {
			if i >= len(p.steps) || d.Step != p.steps[i].Name {
				t.Fatalf("accepted a checkpoint that does not fit: %+v", cp.Completed)
			}
		}
	})
}

// Whatever Rewind is given, the result must be a checkpoint this pipeline accepts
// whose completed steps are a prefix of the input's, so a rewind can never invent
// or reorder progress.
func FuzzRewind(f *testing.F) {
	p := twoSteps()
	cp := p.newCheckpoint()
	cp.Completed = []doneStep{{Step: "a", At: 1}, {Step: "b", At: 2}}
	cp.Compensated = []string{"b"}
	cp.Phase = phaseCompensating
	f.Add(cp.encode(), "a")
	f.Add(cp.encode(), "b")
	f.Add([]byte(`{"v":1,"pipeline":"p","phase":"forward","completed":[{"step":"a"}]}`), "b")
	f.Add([]byte{}, "a")
	f.Fuzz(func(t *testing.T, raw []byte, step string) {
		out, err := p.Rewind(raw, step)
		if err != nil {
			return
		}
		before, err := p.load(raw)
		if err != nil {
			t.Fatalf("Rewind accepted a checkpoint that load rejects: %v", err)
		}
		after, err := p.load(out)
		if err != nil {
			t.Fatalf("Rewind produced a checkpoint that load rejects: %v", err)
		}
		if len(after.Completed) > len(before.Completed) || after.Phase != phaseForward || len(after.Compensated) != 0 {
			t.Fatalf("before %+v after %+v", before, after)
		}
		for i, d := range after.Completed {
			if d.Step != before.Completed[i].Step {
				t.Fatalf("progress was reordered: %+v -> %+v", before.Completed, after.Completed)
			}
		}
		if len(before.Compensated) > 0 && after.Epoch != before.Epoch+1 {
			t.Fatalf("rewinding past compensated steps must start a new epoch: %d -> %d", before.Epoch, after.Epoch)
		}
	})
}
