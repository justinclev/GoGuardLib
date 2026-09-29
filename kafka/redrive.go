package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/retry"
)

// ErrUnknownSource means a stored record came from a topic this service has no
// Binding for (the binding was removed, or the record is not from Kafka). Nothing
// here knows how to finish it, so it is parked for a person rather than run through
// the wrong pipeline.
var ErrUnknownSource = errors.New("kafka: no binding for the topic this record came from")

// Redrive is what a dlq.Redriver needs to finish the records a Consumer stored,
// for any number of topics.
type Redrive struct {
	// Handler sends each stored record to the pipeline of the topic it came from.
	Handler dlq.RedriveHandler
	// Breakers maps every dependency the bindings' pipelines wait on, by name, to
	// its breaker, so the redriver leaves records alone while their circuit is open.
	Breakers map[string]*breaker.Breaker
}

// RedriveFor builds the redrive setup for a consumer's bindings:
//
//	rd, err := kafka.RedriveFor(bindings)
//	redriver, err := dlq.NewRedriver(dlq.RedriveConfig{Store: store, Handler: rd.Handler, Breakers: rd.Breakers})
//
// Use it whenever a consumer has more than one Binding. A single redriver handler
// built from one pipeline would park the other topics' records as mismatched, or
// run a record that has no progress yet through the wrong pipeline.
//
// Two different breakers with the same name are an error: the redriver could not
// tell which one a record was waiting for.
func RedriveFor(bindings []Binding) (Redrive, error) {
	pipes := make(map[string]dlq.RedriveHandler, len(bindings))
	breakers := map[string]*breaker.Breaker{}
	for _, b := range bindings {
		if b.Topic == "" || b.Pipeline == nil {
			return Redrive{}, errors.New("kafka: every Binding needs a Topic and a Pipeline")
		}
		if _, dup := pipes[b.Topic]; dup {
			return Redrive{}, fmt.Errorf("kafka: topic %q is bound twice", b.Topic)
		}
		pipes[b.Topic] = b.Pipeline.Handler()
		for _, br := range b.Pipeline.Breakers() {
			if prev, ok := breakers[br.Name()]; ok && prev != br {
				return Redrive{}, fmt.Errorf("kafka: two different breakers are named %q", br.Name())
			}
			breakers[br.Name()] = br
		}
	}
	handler := func(ctx context.Context, it *dlq.Item) error {
		src := it.Record.Source
		h, ok := pipes[src.Name]
		if src.Kind != "kafka" || !ok {
			return retry.Permanent(fmt.Errorf("%w (source %s %q)", ErrUnknownSource, src.Kind, src.Name))
		}
		return h(ctx, it)
	}
	return Redrive{Handler: handler, Breakers: breakers}, nil
}
