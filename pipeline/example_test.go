package pipeline_test

import (
	"context"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/pipeline"
)

// A message goes through several calls. If one is down, the message and its
// progress are stored and later resume at that step.
func Example() {
	p, err := pipeline.New("orders", "v1", []pipeline.Step{
		{Name: "reserve", Run: func(ctx context.Context, x *pipeline.Exec) error {
			x.Set("reservation", []byte("r-1")) // available to later steps, and after a restart
			return callInventory(ctx, x.IdempotencyKey())
		}},
		{Name: "charge", Run: func(ctx context.Context, x *pipeline.Exec) error {
			res, _ := x.Get("reservation")
			return callPayments(ctx, x.IdempotencyKey(), res)
		}},
	})
	if err != nil {
		return
	}

	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	res, err := p.Execute(context.Background(), store, pipeline.Input{
		ID:    dlq.KafkaID("orders", 3, 1042),
		Value: []byte(`{"order":"o-1"}`),
	})
	switch res {
	case pipeline.Done, pipeline.Deferred, pipeline.Parked:
		// safe to commit the Kafka offset
	default:
		_ = err // Failed: do not commit; the message will be redelivered
	}

	// Elsewhere: resume whatever was deferred.
	_, _ = dlq.NewRedriver(dlq.RedriveConfig{Store: store, Handler: p.Handler()})
}

func callInventory(context.Context, string) error        { return nil }
func callPayments(context.Context, string, []byte) error { return nil }

// An operator inspects a parked record, fixes the cause, and resumes it at a step.
func ExamplePipeline_Redrive() {
	var p *pipeline.Pipeline // your pipeline
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	ctx := context.Background()

	parked, _ := store.Parked(ctx, dlq.ParkedQuery{Limit: 50})
	for _, rec := range parked {
		progress, err := p.Describe(rec.Checkpoint)
		if err != nil {
			continue // a checkpoint that does not fit the deployed pipeline needs a human look
		}
		_ = progress.Completed // steps already done
		_ = progress.Next      // the step it stopped at
		_ = rec.LastError      // why it was parked

		// Resume at "charge": earlier steps are not repeated. ErrStepUndone and
		// ErrCannotSkipForward explain when a chosen step is not possible.
		_ = p.Redrive(ctx, store, rec.ID, "charge")
	}
}
