package kafka_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/pipeline"
	"github.com/justinclev/GoGuardLib/retry"
)

// twoTopics builds a consumer over two topics with different pipelines. Both fail
// while down is set, so their messages are stored.
type twoTopics struct {
	r        *rig
	down     atomic.Bool
	mu       sync.Mutex
	handled  map[string]string // message value -> step that finished it
	bindings []kafka.Binding
}

func newTwoTopics(t *testing.T) *twoTopics {
	tt := &twoTopics{r: newRig(t), handled: map[string]string{}}
	step := func(name string) pipeline.Step {
		return pipeline.Step{Name: name, Run: func(ctx context.Context, x *pipeline.Exec) error {
			if tt.down.Load() {
				return errors.New("dependency down")
			}
			tt.mu.Lock()
			tt.handled[string(x.Value)] = name
			tt.mu.Unlock()
			return nil
		}}
	}
	for _, c := range []struct{ topic, pipe, step string }{{"orders", "orders", "charge"}, {"invoices", "invoices", "bill"}} {
		p, err := pipeline.New(c.pipe, "v1", []pipeline.Step{step(c.step)})
		must(t, err)
		tt.bindings = append(tt.bindings, kafka.Binding{Topic: c.topic, Pipeline: p})
	}
	return tt
}

func (tt *twoTopics) consume() {
	tt.down.Store(true)
	tt.r.broker.produce("orders", 0, "a", "order-1")
	tt.r.broker.produce("invoices", 0, "b", "invoice-1")
	c := tt.r.newConsumer(func(cfg *kafka.Config) { cfg.Bindings = tt.bindings })
	tt.r.client.inject(assignedEvent{Partitions: []kafka.TopicPartition{tp("orders", 0), tp("invoices", 0)}})
	stop := tt.r.run(c)
	eventually(tt.r.t, "both messages stored", func() bool { return tt.r.storeStats().Total() == 2 })
	must(tt.r.t, stop())
	tt.down.Store(false)
}

func (tt *twoTopics) redrive(h dlq.RedriveHandler) {
	rd, err := dlq.NewRedriver(dlq.RedriveConfig{Store: tt.r.store, Handler: h, PollInterval: 5 * time.Millisecond, Backoff: retry.Constant(5 * time.Millisecond)})
	must(tt.r.t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = rd.Run(ctx); close(done) }()
	tt.r.t.Cleanup(func() { cancel(); <-done })
	eventually(tt.r.t, "the redriver to settle", func() bool {
		st := tt.r.storeStats()
		return st.Pending+st.Leased == 0
	})
}

// The bug this guards against: one pipeline's handler given records from another.
func TestOnePipelineHandlerMisroutesAnotherTopicsRecords(t *testing.T) {
	tt := newTwoTopics(t)
	tt.consume()
	tt.redrive(tt.bindings[0].Pipeline.Handler()) // only the orders pipeline
	tt.mu.Lock()
	defer tt.mu.Unlock()
	if tt.handled["invoice-1"] == "bill" {
		t.Fatal("expected the single-handler setup to get invoices wrong; the test no longer shows the problem")
	}
}

func TestRedriveForSendsEachRecordToItsOwnTopicsPipeline(t *testing.T) {
	tt := newTwoTopics(t)
	tt.consume()
	rd, err := kafka.RedriveFor(tt.bindings)
	must(t, err)
	tt.redrive(rd.Handler)

	tt.mu.Lock()
	defer tt.mu.Unlock()
	if tt.handled["order-1"] != "charge" || tt.handled["invoice-1"] != "bill" {
		t.Fatalf("handled = %v, want each message finished by its own topic's step", tt.handled)
	}
	if st := tt.r.storeStats(); st.Parked != 0 || st.Total() != 0 {
		t.Fatalf("store = %+v, want everything finished and nothing parked", st)
	}
}

func TestRedriveForParksARecordFromAnUnboundTopic(t *testing.T) {
	tt := newTwoTopics(t)
	rd, err := kafka.RedriveFor(tt.bindings[:1]) // the invoices binding was removed
	must(t, err)
	must(t, tt.r.store.Append(context.Background(), dlq.Record{ID: "old", Source: dlq.Source{Kind: "kafka", Name: "invoices"}, Value: []byte("x")}))
	must(t, tt.r.store.Append(context.Background(), dlq.Record{ID: "http", Source: dlq.Source{Kind: "http", Name: "orders"}, Value: []byte("y")}))
	tt.redrive(rd.Handler)
	eventually(t, "both to be parked", func() bool { return tt.r.storeStats().Parked == 2 })
	recs, err := tt.r.store.Parked(context.Background(), dlq.ParkedQuery{})
	must(t, err)
	for _, rec := range recs {
		if rec.LastError == "" {
			t.Errorf("record %s parked without a reason", rec.ID)
		}
	}
}

func TestRedriveForRejectsBadBindings(t *testing.T) {
	tt := newTwoTopics(t)
	if _, err := kafka.RedriveFor(append(tt.bindings, tt.bindings[0])); err == nil {
		t.Error("a topic bound twice must be an error")
	}
	if _, err := kafka.RedriveFor([]kafka.Binding{{Topic: "x"}}); err == nil {
		t.Error("a binding without a pipeline must be an error")
	}
}

func TestRedriveForCollectsBreakersByName(t *testing.T) {
	mk := func(name string) *breaker.Breaker { return breaker.New(breaker.Config{Name: name}) }
	shared, own := mk("payments"), mk("shipping")
	defer shared.Close()
	defer own.Close()
	step := func(n string, b *breaker.Breaker) pipeline.Step {
		return pipeline.Step{Name: n, Breaker: b, Run: func(context.Context, *pipeline.Exec) error { return nil }}
	}
	p1, _ := pipeline.New("a", "v1", []pipeline.Step{step("s1", shared)})
	p2, _ := pipeline.New("b", "v1", []pipeline.Step{step("s2", shared), step("s3", own)})
	rd, err := kafka.RedriveFor([]kafka.Binding{{Topic: "a", Pipeline: p1}, {Topic: "b", Pipeline: p2}})
	must(t, err)
	if len(rd.Breakers) != 2 || rd.Breakers["payments"] != shared || rd.Breakers["shipping"] != own {
		t.Fatalf("Breakers = %v, want payments (shared by both topics) and shipping", rd.Breakers)
	}

	other := mk("payments") // a different breaker under the same name
	defer other.Close()
	p3, _ := pipeline.New("c", "v1", []pipeline.Step{step("s4", other)})
	if _, err := kafka.RedriveFor([]kafka.Binding{{Topic: "a", Pipeline: p1}, {Topic: "c", Pipeline: p3}}); err == nil {
		t.Fatal("two different breakers with one name must be an error")
	}
}
