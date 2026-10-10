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

	goguard "github.com/justinclev/GoGuardLib"
	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/health"
	"github.com/justinclev/GoGuardLib/obs"
	"github.com/justinclev/GoGuardLib/pipeline"
	"github.com/justinclev/GoGuardLib/retry"
)

// steps is the order every request goes through. Each is a separate API.
var steps = []string{"inventory", "payments", "shipping", "notifications"}

var servicePorts = map[string]int{"inventory": 9101, "payments": 9102, "shipping": 9103, "notifications": 9104}

// The two Kafka topics the demo consumes. They run different pipelines, and both use
// the payments and notifications breakers, so taking payments down pauses both.
const (
	orderTopic  = "orders"
	refundTopic = "refunds"
)

type topicDef struct {
	Name   string
	Steps  []string
	Prefix string // order IDs on this topic start with it
}

var topicDefs = []topicDef{
	{Name: orderTopic, Steps: steps, Prefix: "K"},
	{Name: refundTopic, Steps: []string{"payments", "notifications"}, Prefix: "R"},
}

// topicCounters follow what happened on one topic, from the events the flows emit.
type topicCounters struct {
	produced, done, stored, parked atomic.Uint64
}

// orderRef remembers which order a stored record belongs to, so store events can name it.
type orderRef struct {
	id, topic, flow string
}

// httpWebhookEndpoint names the guarded HTTP endpoint for the notifications call.
const httpWebhookEndpoint = "notifications-webhook"

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

	orders sync.Map // record ID -> orderRef, so store events can name the order

	pipes  map[string]*pipeline.Pipeline // by topic
	topics map[string]*topicCounters
	poison func() // sends one order that can never succeed
	ledger *ledger

	// The notifications call of the HTTP flow goes through a guarded client that can
	// save a request it cannot send. deferOn is the switch the UI flips.
	guard        *goguard.ResilientTransport
	hclient      *http.Client
	deferOn      atomic.Bool
	httpDeferred atomic.Uint64
	kafkaMix     atomic.Uint64

	// boot tags order IDs with this process start, so a restart never reuses an ID that is
	// already in the durable log (the sequence counters start again from zero).
	boot string
}

func (a *app) emit(e Event) {
	a.ledger.observe(e)
	a.count(e)
	a.hub.Emit(e)
}

// count keeps the per-topic tallies the UI shows.
func (a *app) count(e Event) {
	if e.Flow != "kafka" || e.Topic == "" {
		return
	}
	c := a.topics[e.Topic]
	if c == nil {
		return
	}
	switch {
	case e.Type == "request" && e.Status == "new":
		c.produced.Add(1)
	case e.Type == "request" && e.Status == "success":
		c.done.Add(1)
	case e.Type == "request" && e.Status == "parked":
		c.parked.Add(1)
	case e.Type == "dlq" && e.Status == "stored":
		c.stored.Add(1)
	}
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
	if r := []rune(s); len(r) > 80 {
		s = string(r[:80]) // by characters, so a multi-byte one is never cut in half
	}
	return s
}

// ---- the HTTP flow: the caller waits, and a dead dependency fails fast ----

func (a *app) runHTTP() {
	n := a.httpSeq.Add(1)
	id := fmt.Sprintf("H-%s-%d", a.boot, n)
	a.httpSent.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a.emit(Event{Type: "request", ID: id, Flow: "http", Status: "new"})
	for _, s := range steps {
		a.emit(Event{Type: "step", ID: id, Flow: "http", Step: s, Status: "started"})
		start := time.Now()
		var err error
		if s == "notifications" {
			// The one call that can wait: a guarded client that may save it for later.
			err = a.notify(ctx, id)
		} else {
			err = a.breakers[s].Do(ctx, func(ctx context.Context) error { return a.call(ctx, s, id) })
		}
		lat := time.Since(start).Milliseconds()
		if err != nil {
			if errors.Is(err, goguard.ErrDeferred) {
				// Not sent, not lost: the request is in the dead-letter log and goes out
				// when notifications is healthy. The order itself is complete.
				a.httpDeferred.Add(1)
				a.emit(Event{Type: "step", ID: id, Flow: "http", Step: s, Status: "deferred", LatencyMs: lat, Detail: "saved for later"})
				a.emit(Event{Type: "request", ID: id, Flow: "http", Status: "deferred", Step: s, LatencyMs: lat, Detail: "notification saved, will be sent when notifications is back"})
				return
			}
			var oe *breaker.OpenError
			status := "failed"
			if errors.As(err, &oe) || errors.Is(err, goguard.ErrCircuitOpen) {
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

// notify sends the notification through the guarded client. With the deferral switch
// on, a request the client cannot send is saved and reported as goguard.ErrDeferred.
func (a *app) notify(ctx context.Context, orderID string) error {
	ctx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.serviceURL("notifications", "/work"), bytes.NewReader([]byte(`{"order":"`+orderID+`"}`)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", pipeline.IdempotencyKey(orderID, "notifications")) // required for a request to be saved
	req.Header.Set("Authorization", "Bearer demo-token")                                 // never saved: added again when the request is replayed
	req.Header.Set("X-Order-Id", orderID)
	resp, err := a.hclient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 500 {
		return fmt.Errorf("notifications returned %d", resp.StatusCode)
	}
	return nil
}

// newGuard builds the guarded client for the notifications call. The Enabled switch is
// how the UI turns saving on and off without rebuilding the client (and losing its
// circuit).
func (a *app) newGuard(store dlq.Store) error {
	g, err := goguard.New(goguard.Config{Transport: a.client.Transport},
		goguard.WithEndpoint(httpWebhookEndpoint, goguard.Host(fmt.Sprintf("%s:%d", a.host, servicePorts["notifications"])), goguard.Policy{
			FailureThreshold: 0.5, MinSamples: 5, SamplingWindow: 8 * time.Second, SleepWindow: 8 * time.Second,
			RequestTimeout: 1500 * time.Millisecond,
			HealthPath:     "/health",
			Health:         &health.Config{Interval: time.Second, MaxInterval: 2 * time.Second, Timeout: 800 * time.Millisecond, SuccessThreshold: 2, MaxOpen: time.Minute},
			Defer:          &goguard.Defer{Store: store, Enabled: a.deferOn.Load},
		}),
	)
	if err != nil {
		return err
	}
	a.guard = g
	a.hclient = &http.Client{Transport: g}
	return nil
}

// replay sends the saved HTTP requests. It adds the credential that was never stored,
// and reports each one that finishes.
func (a *app) replay() (dlq.RedriveHandler, error) {
	h, err := goguard.ReplayHandler(goguard.ReplayConfig{
		Client: a.hclient,
		Prepare: func(_ context.Context, req *http.Request) error {
			req.Header.Set("Authorization", "Bearer demo-token")
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, it *dlq.Item) error {
		err := h(ctx, it)
		if err == nil {
			a.emit(Event{Type: "request", ID: headerOf(it.Record, "X-Order-Id"), Flow: "http", Status: "success", Redriven: true})
		}
		return err
	}, nil
}

func headerOf(r dlq.Record, name string) string {
	for _, h := range r.Headers {
		if strings.EqualFold(h.Key, name) {
			return string(h.Value)
		}
	}
	return "?"
}

// ---- the Kafka flow: a failing step defers the message, it is never lost ----

type order struct {
	Order    string  `json:"order"`
	Customer string  `json:"customer"`
	Amount   float64 `json:"amount"`
	// Poison marks an order the payments service will always refuse (an invalid card,
	// say). Retrying can never help, so the pipeline parks it for a person.
	Poison bool `json:"poison,omitempty"`
}

func parseOrder(value []byte) order {
	var o order
	if json.Unmarshal(value, &o) != nil {
		return order{Order: "?"}
	}
	return o
}

func orderID(value []byte) string { return parseOrder(value).Order }

// stepRun is one pipeline step. When the step's circuit is open the pipeline
// never calls it; the store wrapper below reports that as a deferral.
func (a *app) stepRun(topic, name string, last bool) func(ctx context.Context, x *pipeline.Exec) error {
	return func(ctx context.Context, x *pipeline.Exec) error {
		o := parseOrder(x.Value)
		id := o.Order
		redriven := x.Attempt > 0
		a.emit(Event{Type: "step", ID: id, Flow: "kafka", Topic: topic, Step: name, Status: "started", Redriven: redriven})
		if name == "payments" && o.Poison {
			const why = "payment refused: invalid card (permanent)"
			a.emit(Event{Type: "step", ID: id, Flow: "kafka", Topic: topic, Step: name, Status: "failed", Detail: why, Redriven: redriven})
			return retry.Permanent(errors.New(why))
		}
		start := time.Now()
		err := a.call(ctx, name, id)
		lat := time.Since(start).Milliseconds()
		if err != nil {
			a.emit(Event{Type: "step", ID: id, Flow: "kafka", Topic: topic, Step: name, Status: "failed", LatencyMs: lat, Detail: short(err), Redriven: redriven})
			return err
		}
		a.emit(Event{Type: "step", ID: id, Flow: "kafka", Topic: topic, Step: name, Status: "ok", LatencyMs: lat, Redriven: redriven})
		if last {
			a.emit(Event{Type: "request", ID: id, Flow: "kafka", Topic: topic, Status: "success", Redriven: redriven})
		}
		return nil
	}
}

// buildPipelines makes one pipeline per topic. They have different steps, so a stored
// record has to be finished by the pipeline of its own topic (kafka.RedriveFor does that).
func (a *app) buildPipelines() error {
	a.pipes = map[string]*pipeline.Pipeline{}
	a.topics = map[string]*topicCounters{}
	for _, def := range topicDefs {
		var ss []pipeline.Step
		for i, name := range def.Steps {
			ss = append(ss, pipeline.Step{
				Name: name, Breaker: a.breakers[name], Timeout: 2 * time.Second,
				Run: a.stepRun(def.Name, name, i == len(def.Steps)-1),
			})
		}
		p, err := pipeline.New(def.Name, "v1", ss)
		if err != nil {
			return err
		}
		a.pipes[def.Name] = p
		a.topics[def.Name] = &topicCounters{}
	}
	return nil
}

type producer interface {
	Produce(ctx context.Context, topic string, key, value []byte, headers []dlq.Header) error
}

// produceAuto sends the next message: one in four is a refund, the rest are orders.
func (a *app) produceAuto(p producer) {
	topic := orderTopic
	if a.kafkaMix.Add(1)%4 == 0 {
		topic = refundTopic
	}
	a.produceOrder(p, topic, false)
}

func prefixOf(topic string) string {
	for _, d := range topicDefs {
		if d.Name == topic {
			return d.Prefix
		}
	}
	return "K"
}

func (a *app) produceOrder(p producer, topic string, poison bool) {
	n := a.kafkaSeq.Add(1)
	id := fmt.Sprintf("%s-%s-%d", prefixOf(topic), a.boot, n)
	cust := fmt.Sprintf("c%d", n%6) // a few customers, so per-key ordering is visible
	if poison {
		// Its own key: a parked order holds back later orders with the same key, on
		// purpose, until a person deals with it. Keeping the bad order off the
		// customers' keys shows that nothing else is affected.
		cust = fmt.Sprintf("bad-%d", n)
	}
	val, _ := json.Marshal(order{Order: id, Customer: cust, Amount: float64(10+n%90) + 0.99, Poison: poison})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Produce(ctx, topic, []byte(cust), val, nil); err != nil {
		return
	}
	a.kafkaProduced.Add(1)
	a.emit(Event{Type: "request", ID: id, Flow: "kafka", Topic: topic, Status: "new"})
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
	_, getErr := s.Store.Get(ctx, r.ID)
	err := s.Store.Append(ctx, r)
	if err != nil || getErr == nil {
		return err
	}
	switch r.Source.Kind {
	case "http":
		// A request the guarded client saved instead of failing it (Policy.Defer).
		id := headerOf(r, "X-Order-Id")
		s.a.orders.Store(r.ID, orderRef{id: id, flow: "http"})
		s.a.emit(Event{Type: "dlq", ID: id, Flow: "http", Status: "stored", Step: "notifications", Cause: "rejected",
			Detail: "saved by the HTTP client: sent when notifications is healthy"})
		return nil
	case "kafka":
	default:
		return nil
	}
	topic := r.Source.Name
	id := orderID(r.Value)
	s.a.orders.Store(r.ID, orderRef{id: id, topic: topic, flow: "kafka"})
	if r.State == dlq.Parked {
		// Refused for good: it is kept for a person, never retried and never deleted.
		s.a.emit(Event{Type: "dlq", ID: id, Flow: "kafka", Topic: topic, Status: "parked", Step: r.BlockedOn, Detail: r.LastError})
		s.a.emit(Event{Type: "request", ID: id, Flow: "kafka", Topic: topic, Status: "parked", Detail: r.LastError})
		return nil
	}
	status, step, detail := "rejected", r.BlockedOn, "circuit open: call skipped"
	switch {
	case r.BlockedOn == "":
		status, step, detail = "held", "", "held behind an earlier order for the same customer"
	case !strings.Contains(r.LastError, "is open"):
		status, detail = "failed", r.LastError
	}
	s.a.emit(Event{Type: "dlq", ID: id, Flow: "kafka", Topic: topic, Status: "stored", Step: step, Detail: detail, Cause: status})
	return nil
}

func (s *eventStore) Lease(ctx context.Context, req dlq.LeaseRequest) ([]dlq.Lease, error) {
	ls, err := s.Store.Lease(ctx, req)
	for _, l := range ls {
		if l.Record.Source.Kind == "http" {
			id := headerOf(l.Record, "X-Order-Id")
			s.a.orders.Store(l.Record.ID, orderRef{id: id, flow: "http"})
			s.a.emit(Event{Type: "dlq", ID: id, Flow: "http", Status: "redrive", Step: "notifications", Redriven: true})
			continue
		}
		topic := l.Record.Source.Name
		id := orderID(l.Record.Value)
		s.a.orders.Store(l.Record.ID, orderRef{id: id, topic: topic, flow: "kafka"})
		next := ""
		if p := s.a.pipes[topic]; p != nil {
			if pr, derr := p.Describe(l.Record.Checkpoint); derr == nil {
				next = pr.Next
			}
		}
		s.a.emit(Event{Type: "dlq", ID: id, Flow: "kafka", Topic: topic, Status: "redrive", Step: next, Redriven: true})
	}
	return ls, err
}

func (s *eventStore) ref(recID string) orderRef {
	if v, ok := s.a.orders.Load(recID); ok {
		return v.(orderRef)
	}
	return orderRef{id: "?", flow: "kafka"}
}

func (s *eventStore) Nack(ctx context.Context, id, token string, o dlq.NackOptions) error {
	err := s.Store.Nack(ctx, id, token, o)
	if err == nil {
		ref := s.ref(id)
		s.a.emit(Event{Type: "dlq", ID: ref.id, Flow: ref.flow, Topic: ref.topic, Status: "requeued", Step: o.BlockedOn, Detail: o.Err, Redriven: true})
	}
	return err
}

func (s *eventStore) Park(ctx context.Context, id, token, reason string) error {
	err := s.Store.Park(ctx, id, token, reason)
	if err == nil {
		ref := s.ref(id)
		s.a.emit(Event{Type: "dlq", ID: ref.id, Flow: ref.flow, Topic: ref.topic, Status: "parked", Detail: reason})
		s.a.emit(Event{Type: "request", ID: ref.id, Flow: ref.flow, Topic: ref.topic, Status: "parked", Detail: reason, Redriven: ref.flow == "http"})
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
