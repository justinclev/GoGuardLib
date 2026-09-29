package dlq

import (
	"context"
	"time"

	"github.com/justinclev/GoGuardLib/obs"
)

type actorKey struct{}

// WithActor returns a context that names who is acting (a user, a ticket, a
// service account) for Audited to record. The name is yours to choose; it should
// identify a person or system, and must not be a secret.
func WithActor(ctx context.Context, actor string) context.Context {
	return context.WithValue(ctx, actorKey{}, actor)
}

// ActorFrom returns the actor set with WithActor, or "" if none.
func ActorFrom(ctx context.Context) string {
	a, _ := ctx.Value(actorKey{}).(string)
	return a
}

// AuditOptions configures Audited.
type AuditOptions struct {
	// Events receives an obs.OperatorAction for every operator action. It must not
	// block; wrap a slow sink in an obs.Dispatcher. Required.
	Events obs.Sink
	// Clock replaces time.Now, for tests.
	Clock func() time.Time
}

type auditedStore struct {
	Store
	o AuditOptions
}

// Audited wraps a Store so that the actions only a person takes (Requeue,
// RequeueWith and Discard, the one way data leaves a store unhandled) leave a
// record: an obs.OperatorAction naming the action, the record ID, who did it
// (WithActor) and whether it succeeded. Nothing about the record's contents is
// reported. Everything else passes straight through.
func Audited(inner Store, o AuditOptions) Store {
	if o.Clock == nil {
		o.Clock = time.Now
	}
	return &auditedStore{Store: inner, o: o}
}

func (a *auditedStore) note(ctx context.Context, action obs.OperatorActionKind, id string, err error) {
	obs.Emit(a.o.Events, obs.OperatorAction{Action: action, RecordID: id, Actor: ActorFrom(ctx), OK: err == nil, At: a.o.Clock()})
}

func (a *auditedStore) Requeue(ctx context.Context, id string) error {
	err := a.Store.Requeue(ctx, id)
	a.note(ctx, obs.ActionRequeue, id, err)
	return err
}

func (a *auditedStore) RequeueWith(ctx context.Context, id string, o RequeueOptions) error {
	err := a.Store.RequeueWith(ctx, id, o)
	a.note(ctx, obs.ActionRequeue, id, err)
	return err
}

func (a *auditedStore) Discard(ctx context.Context, id string) error {
	err := a.Store.Discard(ctx, id)
	a.note(ctx, obs.ActionDiscard, id, err)
	return err
}

// Durability implements DurabilityReporter.
func (a *auditedStore) Durability() Durability { return StoreDurability(a.Store) }
