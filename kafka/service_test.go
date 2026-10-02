package kafka_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	goguard "github.com/justinclev/GoGuardLib"
	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/obs"
	"github.com/justinclev/GoGuardLib/pipeline"
	"github.com/justinclev/GoGuardLib/retry"
)

// runService starts the service and returns a stop function that returns Run's error.
func runService(t *testing.T, s *kafka.Service) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	var once sync.Once
	var result error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case result = <-done:
			case <-time.After(10 * time.Second):
				t.Error("Run did not stop")
			}
		})
		return result
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

// The whole point of the service: a message that fails is saved by the consumer and finished by the
// redriver, with nothing wired by hand. The breaker has no health check, so this also covers the
// timer-only recovery path.
func TestServiceSavesAFailedMessageAndFinishesItWhenTheDependencyRecovers(t *testing.T) {
	r := newRig(t)
	var healthy atomic.Bool
	var mu sync.Mutex
	var done []string
	payments := breaker.New(breaker.Config{Name: "payments", FailureThreshold: 0.1, MinSamples: 1, SleepWindow: 80 * time.Millisecond})
	defer payments.Close()

	svc, err := kafka.NewService(kafka.ServiceConfig{
		Client: r.client, Store: r.store,
		Handlers: []kafka.HandlerConfig{{Topic: topic, Breaker: payments, Handle: func(ctx context.Context, m kafka.HandledMessage) error {
			if !healthy.Load() {
				return errors.New("payments 503")
			}
			mu.Lock()
			done = append(done, string(m.Value))
			mu.Unlock()
			return nil
		}}},
		Consumer: kafka.Config{PollTimeout: 5 * time.Millisecond, CommitInterval: 10 * time.Millisecond, RetryBackoff: retry.Constant(15 * time.Millisecond)},
		Redriver: dlq.RedriveConfig{PollInterval: 10 * time.Millisecond, Backoff: retry.Constant(20 * time.Millisecond)},
	})
	must(t, err)
	r.produceN(6, "m")
	r.assign(0)
	stop := runService(t, svc)

	eventually(t, "messages to be saved while the dependency is down", func() bool { return r.storeStats().Total() > 0 })
	healthy.Store(true)
	eventually(t, "every message to be finished", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(done) == 6 && r.storeStats().Total() == 0
	})
	must(t, stop())
	if got := r.broker.committedOffset(topic, 0); got != 6 {
		t.Fatalf("committed %d, want 6", got)
	}
}

// Topics with different pipelines share one service: each stored message is finished by the
// pipeline of its own topic, and a single-function topic sits beside a multi-step one.
func TestServiceRunsTopicsWithDifferentPipelines(t *testing.T) {
	r := newRig(t)
	var down atomic.Bool
	down.Store(true)
	var mu sync.Mutex
	finished := map[string]string{}
	finish := func(by string) func(context.Context, *pipeline.Exec) error {
		return func(ctx context.Context, x *pipeline.Exec) error {
			if down.Load() {
				return errors.New("down")
			}
			mu.Lock()
			finished[string(x.Value)] = by
			mu.Unlock()
			return nil
		}
	}
	refunds, err := pipeline.New("refunds", "v1", []pipeline.Step{{Name: "refund", Run: finish("refunds-pipeline")}})
	must(t, err)

	svc, err := kafka.NewService(kafka.ServiceConfig{
		Client: r.client, Store: r.store,
		Handlers: []kafka.HandlerConfig{{Topic: "orders", Handle: func(ctx context.Context, m kafka.HandledMessage) error {
			return finish("orders-handler")(ctx, &pipeline.Exec{Value: m.Value})
		}}},
		Pipelines: []kafka.Binding{{Topic: "refunds", Pipeline: refunds}},
		Consumer:  kafka.Config{PollTimeout: 5 * time.Millisecond, CommitInterval: 10 * time.Millisecond, RetryBackoff: retry.Constant(15 * time.Millisecond)},
		Redriver:  dlq.RedriveConfig{PollInterval: 10 * time.Millisecond, Backoff: retry.Constant(20 * time.Millisecond)},
	})
	must(t, err)
	r.broker.produce("orders", 0, "a", "order-1")
	r.broker.produce("refunds", 0, "b", "refund-1")
	r.client.inject(assignedEvent{Partitions: []kafka.TopicPartition{tp("orders", 0), tp("refunds", 0)}})
	stop := runService(t, svc)

	eventually(t, "both messages to be saved", func() bool { return r.storeStats().Total() == 2 })
	down.Store(false)
	eventually(t, "both to be finished", func() bool { return r.storeStats().Total() == 0 })
	must(t, stop())
	mu.Lock()
	defer mu.Unlock()
	if finished["order-1"] != "orders-handler" || finished["refund-1"] != "refunds-pipeline" {
		t.Fatalf("finished = %v, want each by its own topic's code", finished)
	}
}

// DataDir opens the log, Secure encrypts it, Close releases it, and NewClient is given the topics.
func TestServiceOpensTheLogAndTheClientAndClosesThem(t *testing.T) {
	r := newRig(t)
	dir := t.TempDir()
	var gotTopics []string
	svc, err := kafka.NewService(kafka.ServiceConfig{
		NewClient: func(topics []string) (kafka.Client, error) { gotTopics = topics; return r.client, nil },
		DataDir:   dir,
		Handlers: []kafka.HandlerConfig{
			{Topic: "orders", Handle: func(context.Context, kafka.HandledMessage) error { return nil }},
			{Topic: "refunds", Handle: func(context.Context, kafka.HandledMessage) error { return nil }},
		},
	})
	must(t, err)
	if len(gotTopics) != 2 || gotTopics[0] == gotTopics[1] {
		t.Fatalf("NewClient got topics %v, want both", gotTopics)
	}
	if _, err := dlq.OpenWAL(dir, dlq.WALOptions{}); !errors.Is(err, dlq.ErrLocked) {
		t.Fatalf("the service should hold the log open: %v", err)
	}
	must(t, svc.Close())
	must(t, svc.Close()) // idempotent
	w, err := dlq.OpenWAL(dir, dlq.WALOptions{})
	if err != nil {
		t.Fatalf("Close should release the log: %v", err)
	}
	_ = w.Close()
}

func TestServiceGivesTheSharedEventSinkToTheConsumer(t *testing.T) {
	r := newRig(t)
	var seen atomic.Int32
	svc, err := kafka.NewService(kafka.ServiceConfig{
		Client: r.client, Store: r.store,
		Handlers: []kafka.HandlerConfig{{Topic: topic, Handle: func(context.Context, kafka.HandledMessage) error { return nil }}},
		Events:   obs.SinkFunc(func(e obs.Event) { seen.Add(1) }),
		Consumer: kafka.Config{PollTimeout: 5 * time.Millisecond},
	})
	must(t, err)
	r.assign(0)
	stop := runService(t, svc)
	eventually(t, "the assignment to be reported to the shared sink", func() bool { return seen.Load() > 0 })
	must(t, stop())
}

func TestServiceRejectsConfigurationsThatCannotWork(t *testing.T) {
	r := newRig(t)
	h := []kafka.HandlerConfig{{Topic: topic, Handle: func(context.Context, kafka.HandledMessage) error { return nil }}}
	ok := func() kafka.ServiceConfig { return kafka.ServiceConfig{Client: r.client, Store: r.store, Handlers: h} }
	if s, err := kafka.NewService(ok()); err != nil {
		t.Fatalf("the baseline must be valid: %v", err)
	} else {
		_ = s.Close()
	}
	cases := map[string]func(*kafka.ServiceConfig){
		"no client": func(c *kafka.ServiceConfig) { c.Client = nil },
		"client and factory": func(c *kafka.ServiceConfig) {
			c.NewClient = func([]string) (kafka.Client, error) { return r.client, nil }
		},
		"no store":                func(c *kafka.ServiceConfig) { c.Store = nil },
		"store and directory":     func(c *kafka.ServiceConfig) { c.DataDir = t.TempDir() },
		"no handlers":             func(c *kafka.ServiceConfig) { c.Handlers = nil },
		"a topic twice":           func(c *kafka.ServiceConfig) { c.Handlers = append(h, h...) },
		"client on the consumer":  func(c *kafka.ServiceConfig) { c.Consumer.Client = r.client },
		"store on the redriver":   func(c *kafka.ServiceConfig) { c.Redriver.Store = r.store },
		"handler without a topic": func(c *kafka.ServiceConfig) { c.Handlers = []kafka.HandlerConfig{{Handle: h[0].Handle}} },
	}
	for name, mutate := range cases {
		cfg := ok()
		mutate(&cfg)
		if s, err := kafka.NewService(cfg); err == nil {
			_ = s.Close()
			t.Errorf("%s: want an error", name)
		}
	}
}

func TestServiceCannotRunTwiceOrAfterClose(t *testing.T) {
	r := newRig(t)
	svc, err := kafka.NewService(kafka.ServiceConfig{
		Client: r.client, Store: r.store,
		Handlers: []kafka.HandlerConfig{{Topic: topic, Handle: func(context.Context, kafka.HandledMessage) error { return nil }}},
		Consumer: kafka.Config{PollTimeout: 5 * time.Millisecond},
	})
	must(t, err)
	r.assign(0)
	stop := runService(t, svc)
	eventually(t, "the service to be running", func() bool { return r.client.pollCount() > 0 })
	if err := svc.Run(context.Background()); err == nil {
		t.Fatal("a second Run must be refused")
	}
	must(t, stop())
	must(t, svc.Close())
	if err := svc.Run(context.Background()); err == nil {
		t.Fatal("Run after Close must be refused")
	}
}

// One service finishes saved HTTP requests as well: ServiceConfig.HTTP registers the client's replay
// handler and lets the redriver watch its circuits.
func TestServiceFinishesSavedHTTPRequestsThroughTheGuardedClient(t *testing.T) {
	r := newRig(t)
	var up atomic.Bool
	var sent atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !up.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		sent.Add(1)
	}))
	defer srv.Close()

	client, err := goguard.NewClient(goguard.Policy{
		FailureThreshold: 0.5, MinSamples: 2, SleepWindow: 50 * time.Millisecond,
		Defer: &goguard.Defer{Store: r.store},
	})
	must(t, err)
	defer func() { _ = client.Close() }()

	svc, err := kafka.NewService(kafka.ServiceConfig{
		Client: r.client, Store: r.store, HTTP: client,
		Handlers: []kafka.HandlerConfig{{Topic: topic, Handle: func(context.Context, kafka.HandledMessage) error { return nil }}},
		Consumer: kafka.Config{PollTimeout: 5 * time.Millisecond, CommitInterval: 10 * time.Millisecond},
		Redriver: dlq.RedriveConfig{PollInterval: 10 * time.Millisecond, Backoff: retry.Constant(20 * time.Millisecond)},
	})
	must(t, err)
	r.assign(0)

	for i := 0; i < 3; i++ {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/hook", nil)
		req.Header.Set("Idempotency-Key", string(rune('a'+i)))
		if resp, err := client.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}
	if r.storeStats().Total() != 3 {
		t.Fatalf("saved %d requests, want 3", r.storeStats().Total())
	}
	up.Store(true)
	runService(t, svc)
	eventually(t, "the saved requests to be sent", func() bool { return sent.Load() == 3 && r.storeStats().Total() == 0 })
}

// With BindLater the client needs no store of its own: the service binds the log it opened, so
// DataDir and saved HTTP requests work together.
func TestServiceBindsItsLogToTheHTTPClient(t *testing.T) {
	r := newRig(t)
	var up atomic.Bool
	var sent atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !up.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		sent.Add(1)
	}))
	defer srv.Close()
	client, err := goguard.NewClient(goguard.Policy{
		FailureThreshold: 0.5, MinSamples: 2, SleepWindow: 50 * time.Millisecond,
		Defer: &goguard.Defer{BindLater: true},
	})
	must(t, err)
	defer func() { _ = client.Close() }()

	post := func(id string) error {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/hook", nil)
		req.Header.Set("Idempotency-Key", id)
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
		return err
	}
	if err := post("before"); errors.Is(err, goguard.ErrDeferred) {
		t.Fatal("nothing may be saved before a store is bound")
	}

	svc, err := kafka.NewService(kafka.ServiceConfig{
		Client: r.client, DataDir: t.TempDir(), HTTP: client,
		Handlers: []kafka.HandlerConfig{{Topic: topic, Handle: func(context.Context, kafka.HandledMessage) error { return nil }}},
		Consumer: kafka.Config{PollTimeout: 5 * time.Millisecond, CommitInterval: 10 * time.Millisecond},
		Redriver: dlq.RedriveConfig{PollInterval: 10 * time.Millisecond, Backoff: retry.Constant(20 * time.Millisecond)},
	})
	must(t, err)
	defer func() { _ = svc.Close() }()
	r.assign(0)
	for _, id := range []string{"a", "b", "c"} {
		if err := post(id); !errors.Is(err, goguard.ErrDeferred) {
			t.Fatalf("post %s = %v, want ErrDeferred", id, err)
		}
	}
	st, err := svc.Stats(context.Background())
	must(t, err)
	if st.Store.Total() != 3 {
		t.Fatalf("store holds %d, want 3", st.Store.Total())
	}
	up.Store(true)
	runService(t, svc)
	eventually(t, "the saved requests to be sent", func() bool { return sent.Load() == 3 })
}

// Close called while Run is active stops Run and waits for it, instead of closing the log under it.
func TestServiceCloseStopsARunningService(t *testing.T) {
	r := newRig(t)
	svc, err := kafka.NewService(kafka.ServiceConfig{
		Client: r.client, DataDir: t.TempDir(),
		Handlers: []kafka.HandlerConfig{{Topic: topic, Handle: func(context.Context, kafka.HandledMessage) error { return nil }}},
		Consumer: kafka.Config{PollTimeout: 5 * time.Millisecond},
	})
	must(t, err)
	r.assign(0)
	done := make(chan error, 1)
	go func() { done <- svc.Run(context.Background()) }()
	time.Sleep(50 * time.Millisecond)
	must(t, svc.Close())
	select {
	case err := <-done:
		must(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not stop Run")
	}
}

func TestServiceStatsCombinesTheCounts(t *testing.T) {
	r := newRig(t)
	svc, err := kafka.NewService(kafka.ServiceConfig{
		Client: r.client, Store: r.store,
		Handlers: []kafka.HandlerConfig{{Topic: topic, Handle: func(context.Context, kafka.HandledMessage) error { return nil }}},
		Consumer: kafka.Config{PollTimeout: 5 * time.Millisecond, CommitInterval: 10 * time.Millisecond},
	})
	must(t, err)
	r.produceN(4, "m")
	r.assign(0)
	stop := runService(t, svc)
	eventually(t, "four messages done", func() bool {
		st, _ := svc.Stats(context.Background())
		return st.Consumer.Done == 4
	})
	must(t, stop())
}

func TestServiceRunUntilSignalStopsOnSIGTERM(t *testing.T) {
	r := newRig(t)
	svc, err := kafka.NewService(kafka.ServiceConfig{
		Client: r.client, Store: r.store,
		Handlers: []kafka.HandlerConfig{{Topic: topic, Handle: func(context.Context, kafka.HandledMessage) error { return nil }}},
		Consumer: kafka.Config{PollTimeout: 5 * time.Millisecond},
	})
	must(t, err)
	done := make(chan error, 1)
	go func() { done <- svc.RunUntilSignal(context.Background()) }()
	time.Sleep(100 * time.Millisecond) // let it install the handler
	must(t, syscall.Kill(os.Getpid(), syscall.SIGTERM))
	select {
	case err := <-done:
		must(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("RunUntilSignal did not stop on SIGTERM")
	}
}
