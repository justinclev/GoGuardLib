package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/health"
	"github.com/justinclev/GoGuardLib/obs"
	"github.com/justinclev/GoGuardLib/pipeline"
)

// steps is the order every request goes through. Each is a separate API.
var steps = []string{"inventory", "payments", "shipping", "notifications"}

var servicePorts = map[string]int{"inventory": 9101, "payments": 9102, "shipping": 9103, "notifications": 9104}

type app struct {
	hub      *Hub
	host     string // where the simulated services live
	control  string // their control API
	client   *http.Client
	breakers map[string]*breaker.Breaker

	// counters for the HTTP flow (the Kafka flow's come from the consumer)
	httpSent, httpOK, httpFailed, httpRejected atomic.Uint64
	kafkaProduced                              atomic.Uint64
	httpSeq, kafkaSeq                          atomic.Uint64

	httpRPS, kafkaRPS atomic.Uint64 // requests per second times 10

	orders sync.Map // record ID -> order ID, so store events can name the order

	pipe   *pipeline.Pipeline
	ledger *ledger
}

func (a *app) emit(e Event) {
	a.ledger.observe(e)
	a.hub.Emit(e)
}

func (a *app) serviceURL(name, path string) string {
	return fmt.Sprintf("http://%s:%d%s", a.host, servicePorts[name], path)
}

// call is the one place the demo talks to a downstream API.
func (a *app) call(ctx context.Context, name, orderID string) error {
	ctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.serviceURL(name, "/work"), bytes.NewReader([]byte(`{"order":"`+orderID+`"}`)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", pipeline.IdempotencyKey(orderID, name))
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 500 {
		return fmt.Errorf("%s returned %d", name, resp.StatusCode)
	}
	return nil
}

// short turns an error into something a person can read in the UI.
func short(err error) string {
	var oe *breaker.OpenError
	switch {
	case errors.As(err, &oe):
		return "circuit open: call skipped"
	case errors.Is(err, context.DeadlineExceeded):
		return "timed out"
	case strings.Contains(err.Error(), "connection refused"):
		return "connection refused"
	case strings.Contains(err.Error(), "EOF"), strings.Contains(err.Error(), "reset by peer"):
		return "connection dropped"
	}
	s := err.Error()
	if len(s) > 80 {
		s = s[:80]
	}
	return s
}

// ---- the HTTP flow: the caller waits, and a dead dependency fails fast ----

func (a *app) runHTTP() {
	n := a.httpSeq.Add(1)
	id := fmt.Sprintf("H-%d", n)
	a.httpSent.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a.emit(Event{Type: "request", ID: id, Flow: "http", Status: "new"})
	for _, s := range steps {
		a.emit(Event{Type: "step", ID: id, Flow: "http", Step: s, Status: "started"})
		start := time.Now()
		err := a.breakers[s].Do(ctx, func(ctx context.Context) error { return a.call(ctx, s, id) })
		lat := time.Since(start).Milliseconds()
		if err != nil {
			var oe *breaker.OpenError
			status := "failed"
			if errors.As(err, &oe) {
				status = "rejected"
				a.httpRejected.Add(1)
			} else {
				a.httpFailed.Add(1)
			}
			a.emit(Event{Type: "step", ID: id, Flow: "http", Step: s, Status: status, LatencyMs: lat, Detail: short(err)})
			a.emit(Event{Type: "request", ID: id, Flow: "http", Status: status, Step: s, LatencyMs: lat, Detail: short(err)})
			return
		}
		a.emit(Event{Type: "step", ID: id, Flow: "http", Step: s, Status: "ok", LatencyMs: lat})
	}
	a.httpOK.Add(1)
	a.emit(Event{Type: "request", ID: id, Flow: "http", Status: "success"})
}

// ---- the Kafka flow: a failing step defers the message, it is never lost ----

type order struct {
	Order    string  `json:"order"`
	Customer string  `json:"customer"`
	Amount   float64 `json:"amount"`
}

func orderID(value []byte) string {
	var o order
	if json.Unmarshal(value, &o) != nil {
		return "?"
	}
	return o.Order
}

// stepRun is one pipeline step. When the step's circuit is open the pipeline
// never calls it; the store wrapper below reports that as a deferral.
func (a *app) stepRun(name string, last bool) func(ctx context.Context, x *pipeline.Exec) error {
	return func(ctx context.Context, x *pipeline.Exec) error {
		id := orderID(x.Value)
		redriven := x.Attempt > 0
		a.emit(Event{Type: "step", ID: id, Flow: "kafka", Step: name, Status: "started", Redriven: redriven})
		start := time.Now()
		err := a.call(ctx, name, id)
		lat := time.Since(start).Milliseconds()
		if err != nil {
			a.emit(Event{Type: "step", ID: id, Flow: "kafka", Step: name, Status: "failed", LatencyMs: lat, Detail: short(err), Redriven: redriven})
			return err
		}
		a.emit(Event{Type: "step", ID: id, Flow: "kafka", Step: name, Status: "ok", LatencyMs: lat, Redriven: redriven})
		if last {
			a.emit(Event{Type: "request", ID: id, Flow: "kafka", Status: "success", Redriven: redriven})
		}
		return nil
	}
}

func (a *app) buildPipeline() (*pipeline.Pipeline, error) {
	var ss []pipeline.Step
	for i, name := range steps {
		ss = append(ss, pipeline.Step{
			Name: name, Breaker: a.breakers[name], Timeout: 2 * time.Second,
			Run: a.stepRun(name, i == len(steps)-1),
		})
	}
	return pipeline.New("orders", "v1", ss)
}

func (a *app) produce(p interface {
	Produce(ctx context.Context, topic string, key, value []byte, headers []dlq.Header) error
}, topic string) {
	n := a.kafkaSeq.Add(1)
	id := fmt.Sprintf("K-%d", n)
	cust := fmt.Sprintf("c%d", n%6) // a few customers, so per-key ordering is visible
	val, _ := json.Marshal(order{Order: id, Customer: cust, Amount: float64(10+n%90) + 0.99})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Produce(ctx, topic, []byte(cust), val, nil); err != nil {
		return
	}
	a.kafkaProduced.Add(1)
	a.emit(Event{Type: "request", ID: id, Flow: "kafka", Status: "new"})
}

// eventStore wraps the dead-letter store so the UI can see what the library does
// with each message: set it aside, hand it back to the redriver, park it.
type eventStore struct {
	dlq.Store
	a *app
}

func (s *eventStore) Append(ctx context.Context, r dlq.Record) error {
	// Storing is idempotent: after a crash Kafka redelivers messages that are already
	// in the log. Report a message the first time it is stored, not every time.
	_, existed := s.Store.Get(ctx, r.ID)
	err := s.Store.Append(ctx, r)
	if err != nil || r.Source.Kind != "kafka" || existed == nil {
		return err
	}
	id := orderID(r.Value)
	s.a.orders.Store(r.ID, id)
	status, step, detail := "rejected", r.BlockedOn, "circuit open: call skipped"
	switch {
	case r.BlockedOn == "":
		status, step, detail = "held", "", "held behind an earlier order for the same customer"
	case !strings.Contains(r.LastError, "is open"):
		status, detail = "failed", r.LastError
	}
	s.a.emit(Event{Type: "dlq", ID: id, Flow: "kafka", Status: "stored", Step: step, Detail: detail, Cause: status})
	return nil
}

func (s *eventStore) Lease(ctx context.Context, req dlq.LeaseRequest) ([]dlq.Lease, error) {
	ls, err := s.Store.Lease(ctx, req)
	for _, l := range ls {
		id := orderID(l.Record.Value)
		s.a.orders.Store(l.Record.ID, id)
		next := ""
		if pr, derr := s.a.pipe.Describe(l.Record.Checkpoint); derr == nil {
			next = pr.Next
		}
		s.a.emit(Event{Type: "dlq", ID: id, Flow: "kafka", Status: "redrive", Step: next, Redriven: true})
	}
	return ls, err
}

func (s *eventStore) name(recID string) string {
	if v, ok := s.a.orders.Load(recID); ok {
		return v.(string)
	}
	return "?"
}

func (s *eventStore) Nack(ctx context.Context, id, token string, o dlq.NackOptions) error {
	err := s.Store.Nack(ctx, id, token, o)
	if err == nil {
		s.a.emit(Event{Type: "dlq", ID: s.name(id), Flow: "kafka", Status: "requeued", Step: o.BlockedOn, Detail: o.Err, Redriven: true})
	}
	return err
}

func (s *eventStore) Park(ctx context.Context, id, token, reason string) error {
	err := s.Store.Park(ctx, id, token, reason)
	if err == nil {
		s.a.emit(Event{Type: "dlq", ID: s.name(id), Flow: "kafka", Status: "parked", Detail: reason})
		s.a.emit(Event{Type: "request", ID: s.name(id), Flow: "kafka", Status: "parked", Detail: reason})
	}
	return err
}

// ---- breakers and the events the library emits ----

type hubSink struct {
	a    *app
	wake atomic.Pointer[func()]
}

func (h *hubSink) Emit(e obs.Event) {
	switch ev := e.(type) {
	case obs.StateChanged:
		h.a.emit(Event{Type: "breaker", Service: ev.Dependency, From: ev.From.String(), To: ev.To.String()})
		if w := h.wake.Load(); w != nil && ev.To != obs.StateOpen {
			(*w)()
		}
	case obs.ProbeResult:
		ok := ev.OK
		h.a.emit(Event{Type: "probe", Service: ev.Dependency, OK: &ok, LatencyMs: ev.Latency.Milliseconds()})
	}
}

func (a *app) newBreakers(sink obs.Sink) {
	a.breakers = map[string]*breaker.Breaker{}
	for _, name := range steps {
		check, _ := health.HTTP(a.serviceURL(name, "/health"))
		a.breakers[name] = breaker.New(breaker.Config{
			Name: name, FailureThreshold: 0.5, MinSamples: 5,
			SamplingWindow: 8 * time.Second, SleepWindow: 8 * time.Second,
			Events: sink,
			Health: &health.Config{
				Check: check, Interval: time.Second, MaxInterval: 2 * time.Second, Timeout: 800 * time.Millisecond,
				SuccessThreshold: 2, MaxOpen: time.Minute,
			},
		})
	}
}
