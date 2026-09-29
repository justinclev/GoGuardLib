// Package pipeline runs a message through several steps (say, four API calls) so
// that when one of them is down, the work resumes at that step instead of
// starting over or being lost.
//
// After each step succeeds its progress is checkpointed. When a step cannot
// finish, the message and its checkpoint go into a dlq.Store, tagged with the
// dependency that blocked it. A dlq.Redriver later resumes it at the failed step
// once that dependency is usable: the steps that already succeeded are not run
// again, and the data they produced is still there for the steps that follow.
//
// Delivery is at-least-once, not exactly-once. If the process dies after a step
// has done its work but before the checkpoint is durable, that step runs again on
// retry. Give each downstream call Exec.IdempotencyKey so a repeat is harmless.
package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/retry"
	"github.com/justinclev/GoGuardLib/secure"
)

// ErrPipelineMismatch means a stored checkpoint does not fit the pipeline that is
// deployed now (a step was renamed or removed, or it belongs to another
// pipeline). Resuming could skip or repeat the wrong work, so the record is
// parked for a human instead. It is always wrapped with retry.Permanent.
var ErrPipelineMismatch = errors.New("pipeline: checkpoint does not match the deployed pipeline")

// Step is one unit of work.
type Step struct {
	// Name identifies the step in checkpoints. It must be unique in the pipeline
	// and must not change while records for it may still be stored.
	Name string
	// Dependency names what the step needs. A record that cannot get past the step
	// is held until this dependency recovers. Default: Breaker's name if set,
	// else Name.
	Dependency string
	// Breaker, if set, guards the step's call, so an open circuit defers the record
	// without calling the dependency.
	Breaker *breaker.Breaker
	// Timeout bounds one attempt of Run.
	Timeout time.Duration
	// Retry retries Run in-process before the record is deferred. Optional.
	Retry retry.Policy
	// Run does the work. Return retry.Permanent(err) for a failure that retrying
	// cannot fix. Any other error is treated as transient.
	Run func(ctx context.Context, x *Exec) error
	// Compensate undoes Run, for use when a later step fails permanently and the
	// pipeline was built with Saga. Optional.
	Compensate func(ctx context.Context, x *Exec) error
}

func (s *Step) dependency() string {
	switch {
	case s.Dependency != "":
		return s.Dependency
	case s.Breaker != nil && s.Breaker.Name() != "":
		return s.Breaker.Name()
	default:
		return s.Name
	}
}

// Exec is what a step sees: the message, the data earlier steps saved, and the
// idempotency key for the current step.
type Exec struct {
	// ID, Source, Key, Value, Headers are the message being processed. Treat them
	// as read-only.
	ID      string
	Source  dlq.Source
	Key     []byte
	Value   []byte
	Headers []dlq.Header
	// Attempt is the record's attempt count when resumed from the queue, 0 on the
	// first live run.
	Attempt int

	step  string
	epoch int
	state map[string][]byte
}

// Get returns data saved by an earlier step (or an earlier attempt).
func (x *Exec) Get(key string) ([]byte, bool) {
	v, ok := x.state[key]
	return v, ok
}

// Set saves data for later steps. It is checkpointed with the step's progress, so
// it survives a restart. Keep it small: it is stored with the record.
func (x *Exec) Set(key string, value []byte) {
	if x.state == nil {
		x.state = map[string][]byte{}
	}
	x.state[key] = append([]byte{}, value...)
}

// IdempotencyKey is stable for this message and this step across every attempt and
// restart. Pass it to the downstream call (an Idempotency-Key header, a unique
// request ID) so that running the step twice has the effect of running it once.
//
// The key changes only when a record is deliberately rewound past steps that were
// already compensated (see Rewind): work that was undone is new work, and must not
// be mistaken for a repeat of the original.
func (x *Exec) IdempotencyKey() string { return IdempotencyKeyEpoch(x.ID, x.step, x.epoch) }

// IdempotencyKey derives the key for a message and step in the first epoch.
func IdempotencyKey(recordID, step string) string { return IdempotencyKeyEpoch(recordID, step, 0) }

// IdempotencyKeyEpoch derives the key for a message, step and epoch. Epoch 0 gives
// the same key as IdempotencyKey.
func IdempotencyKeyEpoch(recordID, step string, epoch int) string {
	in := "goguard/pipeline/v1\x00" + recordID + "\x00" + step
	if epoch > 0 {
		in += "\x00epoch=" + strconv.Itoa(epoch)
	}
	sum := sha256.Sum256([]byte(in))
	return hex.EncodeToString(sum[:16])
}

// Pipeline is an ordered list of steps.
type Pipeline struct {
	name     string
	version  string
	steps    []Step
	saga     bool
	redactor *secure.Redactor
}

// Option customises a Pipeline.
type Option func(*Pipeline)

// Saga makes a permanent failure undo the completed steps: their Compensate
// functions run in reverse order, each checkpointed so a crash or an outage
// mid-way resumes compensating instead of repeating or skipping.
func Saga() Option { return func(p *Pipeline) { p.saga = true } }

// WithRedactor sets the redactor for error text stored with records. Default
// secure.NewRedactor().
func WithRedactor(r *secure.Redactor) Option { return func(p *Pipeline) { p.redactor = r } }

// New builds a pipeline. name and the step names identify checkpoints; version is
// recorded for diagnosis.
func New(name, version string, steps []Step, opts ...Option) (*Pipeline, error) {
	if name == "" {
		return nil, errors.New("pipeline: a name is required")
	}
	if len(steps) == 0 {
		return nil, errors.New("pipeline: at least one step is required")
	}
	seen := map[string]bool{}
	for i, s := range steps {
		if s.Name == "" || s.Run == nil {
			return nil, fmt.Errorf("pipeline: step %d needs a Name and a Run", i)
		}
		if seen[s.Name] {
			return nil, fmt.Errorf("pipeline: duplicate step name %q", s.Name)
		}
		seen[s.Name] = true
	}
	p := &Pipeline{name: name, version: version, steps: append([]Step(nil), steps...)}
	for _, o := range opts {
		o(p)
	}
	if p.redactor == nil {
		p.redactor = secure.NewRedactor()
	}
	return p, nil
}

// Breakers returns the distinct circuit breakers that guard the steps. A consumer
// pauses its topic while any of them is open.
func (p *Pipeline) Breakers() []*breaker.Breaker {
	seen := map[*breaker.Breaker]bool{}
	var out []*breaker.Breaker
	for _, s := range p.steps {
		if s.Breaker != nil && !seen[s.Breaker] {
			seen[s.Breaker] = true
			out = append(out, s.Breaker)
		}
	}
	return out
}

// StepNames lists the steps in order.
func (p *Pipeline) StepNames() []string {
	out := make([]string, len(p.steps))
	for i, s := range p.steps {
		out[i] = s.Name
	}
	return out
}

// ---- checkpoint ----

const (
	checkpointVersion = 1
	phaseForward      = "forward"
	phaseCompensating = "compensating"
)

type doneStep struct {
	Step string `json:"step"`
	At   int64  `json:"at"` // unix nanoseconds
}

type checkpoint struct {
	V           int               `json:"v"`
	Pipeline    string            `json:"pipeline"`
	Version     string            `json:"version"`
	Completed   []doneStep        `json:"completed"`
	Phase       string            `json:"phase"`
	Compensated []string          `json:"compensated,omitempty"`
	FailedStep  string            `json:"failed_step,omitempty"`
	FailReason  string            `json:"fail_reason,omitempty"`
	Epoch       int               `json:"epoch,omitempty"`
	State       map[string][]byte `json:"state,omitempty"`
}

func (p *Pipeline) newCheckpoint() *checkpoint {
	return &checkpoint{V: checkpointVersion, Pipeline: p.name, Version: p.version, Phase: phaseForward}
}

func (c *checkpoint) encode() []byte {
	b, _ := json.Marshal(c) // only strings, ints and byte slices: cannot fail
	return b
}

func mismatch(format string, args ...any) error {
	return retry.Permanent(fmt.Errorf("%w: %s", ErrPipelineMismatch, fmt.Sprintf(format, args...)))
}

// load decodes a stored checkpoint and checks it against the deployed pipeline.
func (p *Pipeline) load(raw []byte) (*checkpoint, error) {
	if len(raw) == 0 {
		return p.newCheckpoint(), nil // stored without progress: start from the top
	}
	var c checkpoint
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, mismatch("checkpoint is unreadable")
	}
	if c.V != checkpointVersion {
		return nil, mismatch("checkpoint format %d is not supported", c.V)
	}
	if c.Pipeline != p.name {
		return nil, mismatch("checkpoint belongs to pipeline %q, this is %q", c.Pipeline, p.name)
	}
	if c.Phase != phaseForward && c.Phase != phaseCompensating {
		return nil, mismatch("unknown phase %q", c.Phase)
	}
	if len(c.Completed) > len(p.steps) {
		return nil, mismatch("checkpoint has %d completed steps but the pipeline has %d", len(c.Completed), len(p.steps))
	}
	for i, d := range c.Completed {
		if d.Step != p.steps[i].Name {
			return nil, mismatch("step %d was %q when checkpointed but is %q now", i+1, d.Step, p.steps[i].Name)
		}
	}
	return &c, nil
}

// ---- running steps ----

type run struct {
	p       *Pipeline
	x       *Exec
	cp      *checkpoint
	persist func(context.Context, []byte) error // nil when running live, before anything is stored
}

func (r *run) save(ctx context.Context) error {
	r.cp.State = r.x.state
	if r.persist == nil {
		return nil
	}
	if err := r.persist(ctx, r.cp.encode()); err != nil {
		return fmt.Errorf("pipeline: saving checkpoint: %w", err)
	}
	return nil
}

// call runs fn for a step with its timeout, breaker and in-process retries.
func (r *run) call(ctx context.Context, st *Step, fn func(context.Context, *Exec) error) error {
	r.x.step = st.Name
	attempt := func(ctx context.Context) (err error) {
		if st.Timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, st.Timeout)
			defer cancel()
		}
		defer func() {
			if p := recover(); p != nil {
				err = errors.New("step panicked") // the panic value may hold message data
			}
		}()
		return fn(ctx, r.x)
	}
	guarded := attempt
	if st.Breaker != nil {
		guarded = func(ctx context.Context) error { return st.Breaker.Do(ctx, attempt) }
	}
	if st.Retry.MaxRetries > 0 {
		return retry.Do(ctx, st.Retry, guarded)
	}
	return guarded(ctx)
}

// advance runs the pipeline from wherever its checkpoint says. It returns nil
// when every step is done, a *dlq.BlockedError when a dependency is unavailable,
// an error matching retry.Permanent when retrying cannot help, or another error
// (for example a cancelled context) that says nothing about the record.
func (r *run) advance(ctx context.Context) error {
	if r.cp.Phase == phaseCompensating {
		return r.compensate(ctx)
	}
	for i := len(r.cp.Completed); i < len(r.p.steps); i++ {
		st := &r.p.steps[i]
		if err := r.call(ctx, st, st.Run); err != nil {
			return r.failed(ctx, st, err)
		}
		r.cp.Completed = append(r.cp.Completed, doneStep{Step: st.Name, At: time.Now().UnixNano()})
		if err := r.save(ctx); err != nil {
			return err
		}
	}
	return nil
}

// classify turns a step's error into what the caller should do. A run that
// outlives its own deadline (Kafka's ProcessTimeout, the redriver's lease) is held
// against the step it was in: the message is stored with its progress and the
// attempt counts, so a message that always hangs is eventually parked instead of
// being retried for ever, blocking everything behind it. A cancelled context is
// different: that is the caller stopping, and says nothing about the message.
func (r *run) classify(ctx context.Context, st *Step, err error) error {
	switch {
	case retry.IsPermanent(err):
		return err
	case ctx.Err() != nil && !errors.Is(ctx.Err(), context.DeadlineExceeded):
		return err // the caller is shutting down or gave up: not the dependency's fault
	case errors.Is(err, breaker.ErrOpen):
		return &dlq.BlockedError{Dependency: st.dependency(), Err: err, Refund: true}
	default:
		return &dlq.BlockedError{Dependency: st.dependency(), Err: err}
	}
}

func (r *run) failed(ctx context.Context, st *Step, err error) error {
	err = r.classify(ctx, st, err)
	if !retry.IsPermanent(err) {
		return err
	}
	text := r.p.redactor.Error(err)
	if r.p.saga && r.anyCompensation() {
		r.cp.Phase = phaseCompensating
		r.cp.FailedStep = st.Name
		r.cp.FailReason = text
		if serr := r.save(ctx); serr != nil {
			return serr
		}
		return r.compensate(ctx)
	}
	return retry.Permanent(fmt.Errorf("pipeline %s: step %q failed permanently: %s", r.p.name, st.Name, text))
}

func (r *run) anyCompensation() bool {
	for i := range r.cp.Completed {
		if r.p.steps[i].Compensate != nil {
			return true
		}
	}
	return false
}

// compensate undoes completed steps, newest first, checkpointing each one.
func (r *run) compensate(ctx context.Context) error {
	done := map[string]bool{}
	for _, n := range r.cp.Compensated {
		done[n] = true
	}
	for i := len(r.cp.Completed) - 1; i >= 0; i-- {
		st := &r.p.steps[i]
		if st.Compensate == nil || done[st.Name] {
			continue
		}
		if err := r.call(ctx, st, st.Compensate); err != nil {
			err = r.classify(ctx, st, err)
			if retry.IsPermanent(err) {
				return retry.Permanent(fmt.Errorf("pipeline %s: compensating step %q failed (%s); manual intervention is needed",
					r.p.name, st.Name, r.p.redactor.Error(err)))
			}
			return err // blocked or interrupted: resume compensating later
		}
		r.cp.Compensated = append(r.cp.Compensated, st.Name)
		if err := r.save(ctx); err != nil {
			return err
		}
	}
	return retry.Permanent(fmt.Errorf("pipeline %s: step %q failed permanently (%s); %d step(s) compensated",
		r.p.name, r.cp.FailedStep, r.cp.FailReason, len(r.cp.Compensated)))
}

// ---- resuming from the queue ----

// Handler returns a dlq.RedriveHandler that resumes stored records at the step
// that had not finished. Completed steps are not run again, and each newly
// completed step is checkpointed before the next begins.
func (p *Pipeline) Handler() dlq.RedriveHandler {
	return func(ctx context.Context, item *dlq.Item) error {
		cp, err := p.load(item.Record.Checkpoint)
		if err != nil {
			return err
		}
		x := &Exec{
			ID: item.Record.ID, Source: item.Record.Source, Key: item.Record.Key, Value: item.Record.Value,
			Headers: item.Record.Headers, Attempt: item.Record.Attempts, epoch: cp.Epoch, state: cp.State,
		}
		r := &run{p: p, x: x, cp: cp, persist: item.Checkpoint}
		return r.advance(ctx)
	}
}

// ---- live processing ----

// Input is a message to process.
type Input struct {
	// ID identifies the message and must be the same every time the same message is
	// delivered, for example dlq.KafkaID(topic, partition, offset). It seeds the
	// idempotency keys and makes storing the message idempotent.
	ID       string
	Source   dlq.Source
	Key      []byte
	Value    []byte
	Headers  []dlq.Header
	OrderKey string
}

// Result says what became of a message given to Execute.
type Result int

const (
	// Failed: nothing was stored and the error says why. The caller must not
	// acknowledge the message (do not commit the Kafka offset): it will be
	// redelivered. This includes the store being full, which is the cue to pause.
	Failed Result = iota
	// Done: every step ran.
	Done
	// Deferred: a step could not finish, and the message with its progress is now
	// durable in the store, held until the blocking dependency recovers. It is safe
	// to acknowledge the message.
	Deferred
	// Parked: a step failed in a way retrying cannot fix. The message with its
	// progress is durable in the store, parked for a human. It is safe to
	// acknowledge the message.
	Parked
)

func (r Result) String() string {
	switch r {
	case Done:
		return "done"
	case Deferred:
		return "deferred"
	case Parked:
		return "parked"
	default:
		return "failed"
	}
}

// Execute processes a message from the start, in this goroutine. If a step cannot
// finish, the message and its progress are stored so a Redriver running
// Handler resumes it; otherwise nothing is stored.
//
// It returns Done or Deferred or Parked (with a nil error) only when the message
// is safe to acknowledge.
func (p *Pipeline) Execute(ctx context.Context, store dlq.Store, in Input) (Result, error) {
	if in.ID == "" {
		return Failed, errors.New("pipeline: Input.ID is required")
	}
	cp := p.newCheckpoint()
	x := &Exec{ID: in.ID, Source: in.Source, Key: in.Key, Value: in.Value, Headers: in.Headers}
	r := &run{p: p, x: x, cp: cp}
	err := r.advance(ctx)
	r.cp.State = x.state

	var be *dlq.BlockedError
	switch {
	case err == nil:
		return Done, nil
	case errors.As(err, &be):
		return p.store(ctx, store, in, cp, dlq.Pending, be.Dependency, p.redactor.Error(err), Deferred)
	case retry.IsPermanent(err):
		return p.store(ctx, store, in, cp, dlq.Parked, "", p.redactor.Error(err), Parked)
	default:
		return Failed, err
	}
}

func (p *Pipeline) store(ctx context.Context, s dlq.Store, in Input, cp *checkpoint, state dlq.State, dep, text string, ok Result) (Result, error) {
	rec := dlq.Record{
		ID: in.ID, State: state, Source: in.Source, Key: in.Key, Value: in.Value, Headers: in.Headers,
		OrderKey: in.OrderKey, BlockedOn: dep, LastError: text, Checkpoint: cp.encode(),
	}
	// Detach from cancellation: the work is done, and the only safe outcomes are
	// "stored" or "reported as not stored".
	if err := s.Append(context.WithoutCancel(ctx), rec); err != nil {
		return Failed, err
	}
	return ok, nil
}

// Completed returns the names of the steps recorded as finished in a stored
// checkpoint, for tooling that inspects parked records.
func Completed(raw []byte) ([]string, error) {
	var c checkpoint
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, err
	}
	out := make([]string, len(c.Completed))
	for i, d := range c.Completed {
		out[i] = d.Step
	}
	return out, nil
}

// StateKeys lists the keys of the data saved in a stored checkpoint (never the
// values), for tooling.
func StateKeys(raw []byte) ([]string, error) {
	var c checkpoint
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(c.State))
	for k := range c.State {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, nil
}

// ---- inspecting and rewinding parked records ----

var (
	// ErrUnknownStep means the step named is not in the pipeline.
	ErrUnknownStep = errors.New("pipeline: no such step")
	// ErrCannotSkipForward means the record has not completed the steps before the
	// one asked for. Resuming there would silently skip work.
	ErrCannotSkipForward = errors.New("pipeline: cannot resume at a step whose predecessors have not completed")
	// ErrStepUndone means an earlier step was already compensated, so the record
	// cannot resume after it: its effects are gone. Rewind to that step or before.
	ErrStepUndone = errors.New("pipeline: an earlier step was already compensated")
)

// Progress describes how far a stored record got, for tools and operators. It
// contains no message data: only step names and the keys of the saved state.
type Progress struct {
	Pipeline, Version string
	Completed         []string
	// Next is the step that runs next, or "" if every step is complete or the
	// record is compensating.
	Next string
	// Phase is "forward" or "compensating".
	Phase       string
	Compensated []string
	FailedStep  string
	FailReason  string
	StateKeys   []string
	// Epoch counts the times the record was rewound past compensated steps.
	Epoch int
}

// Describe reports the progress recorded in a stored checkpoint. It fails with
// ErrPipelineMismatch if the checkpoint does not fit this pipeline.
func (p *Pipeline) Describe(raw []byte) (Progress, error) {
	cp, err := p.load(raw)
	if err != nil {
		return Progress{}, err
	}
	pr := Progress{
		Pipeline: cp.Pipeline, Version: cp.Version, Phase: cp.Phase, Compensated: cp.Compensated,
		FailedStep: cp.FailedStep, FailReason: cp.FailReason, Epoch: cp.Epoch,
	}
	for _, d := range cp.Completed {
		pr.Completed = append(pr.Completed, d.Step)
	}
	if cp.Phase == phaseForward && len(cp.Completed) < len(p.steps) {
		pr.Next = p.steps[len(cp.Completed)].Name
	}
	for k := range cp.State {
		pr.StateKeys = append(pr.StateKeys, k)
	}
	sort.Strings(pr.StateKeys)
	return pr, nil
}

// Rewind returns a copy of a stored checkpoint that resumes at toStep: the steps
// from toStep onward are forgotten and will run again, the data earlier steps
// saved is kept, and any failure or compensation state is cleared so the record
// runs forward.
//
// It refuses to skip forward (ErrCannotSkipForward), because that would drop
// work, and to resume after a step that was already compensated (ErrStepUndone),
// because that step's effects no longer exist. Rewinding to a compensated step or
// before it is allowed, and starts a new idempotency epoch: the undone work is
// being done afresh, so its keys must differ from the original run's, or a
// downstream service that deduplicates would treat it as already done.
//
// Steps that run again without having been compensated keep their keys; they were
// really done, and a repeat of them must look like one.
func (p *Pipeline) Rewind(raw []byte, toStep string) ([]byte, error) {
	cp, err := p.load(raw)
	if err != nil {
		return nil, err
	}
	idx := -1
	for i := range p.steps {
		if p.steps[i].Name == toStep {
			idx = i
		}
	}
	if idx < 0 {
		return nil, fmt.Errorf("%w: %q", ErrUnknownStep, toStep)
	}
	if idx > len(cp.Completed) {
		return nil, fmt.Errorf("%w: %q comes after %q, which has not completed", ErrCannotSkipForward, toStep, p.steps[len(cp.Completed)].Name)
	}
	undone := map[string]bool{}
	for _, n := range cp.Compensated {
		undone[n] = true
	}
	for i := 0; i < idx; i++ {
		if undone[cp.Completed[i].Step] {
			return nil, fmt.Errorf("%w: %q was undone, so resume at %q or earlier", ErrStepUndone, cp.Completed[i].Step, cp.Completed[i].Step)
		}
	}
	if len(cp.Compensated) > 0 {
		cp.Epoch++ // whatever was undone will be redone: it is new work, not a repeat
	}
	cp.Completed = cp.Completed[:idx]
	cp.Compensated = nil
	cp.Phase = phaseForward
	cp.FailedStep, cp.FailReason = "", ""
	return cp.encode(), nil
}

// Redrive sends a parked record back through the pipeline, resuming at fromStep
// (or where it stopped, if fromStep is empty), after an operator has fixed the
// cause. The checkpoint change and the requeue happen in one durable step. It
// returns dlq.ErrNotFound or dlq.ErrNotParked if there is no such parked record.
//
// A record that was compensated cannot simply be requeued as it was: it would
// finish compensating again and re-park. Give the step to resume at.
func (p *Pipeline) Redrive(ctx context.Context, store dlq.Store, id, fromStep string) error {
	rec, err := store.Get(ctx, id)
	if err != nil {
		return err
	}
	if rec.State != dlq.Parked {
		return dlq.ErrNotParked
	}
	if fromStep == "" {
		return store.Requeue(ctx, id)
	}
	cp, err := p.Rewind(rec.Checkpoint, fromStep)
	if err != nil {
		return err
	}
	return store.RequeueWith(ctx, id, dlq.RequeueOptions{ReplaceCheckpoint: true, Checkpoint: cp})
}
