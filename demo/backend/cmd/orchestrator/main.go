// Command orchestrator is the demo's brain. It generates HTTP and Kafka traffic,
// sends every order through four downstream APIs guarded by GoGuardLib, and streams
// everything that happens to the UI.
//
// It is deliberately ordinary application code: the resilience comes from the
// library (circuit breakers with health checks, the durable dead-letter store, the
// redriver and the Kafka consumer), not from anything written here.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/retry"
)

const topic = "orders"

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		resp, err := http.Get("http://localhost:" + env("PORT", "8080") + "/healthz")
		if err != nil || resp.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "orchestrator:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	host := env("SERVICES_HOST", "localhost")
	a := &app{
		hub:     NewHub(),
		ledger:  newLedger(),
		host:    host,
		control: fmt.Sprintf("http://%s:9100", host),
		client: &http.Client{Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: time.Second}).DialContext,
			MaxIdleConnsPerHost: 64, IdleConnTimeout: 30 * time.Second,
		}},
	}
	sink := &hubSink{a: a}
	a.newBreakers(sink)
	defer func() {
		for _, b := range a.breakers {
			b.Close()
		}
	}()
	var err error
	if a.pipe, err = a.buildPipeline(); err != nil {
		return err
	}

	// The durable dead-letter store: a write-ahead log on disk.
	dir := env("DLQ_DIR", "./data/dlq")
	if env("DLQ_FRESH", "true") == "true" {
		_ = os.RemoveAll(dir) // a demo starts clean; production would keep the queue
	}
	wal, err := dlq.OpenWAL(dir, dlq.WALOptions{})
	if err != nil {
		return fmt.Errorf("opening the dead-letter store: %w", err)
	}
	defer func() { _ = wal.Close() }()
	store := &eventStore{Store: wal, a: a}
	if st, err := wal.Stats(ctx); err == nil {
		a.ledger.seed(st.Pending + st.Leased) // orders a crashed run left in the log
	}

	client, producer, mode, closeKafka, err := kafkaBackend(ctx, env("KAFKA_BROKERS", ""), topic)
	if err != nil {
		return err
	}
	defer closeKafka()

	consumer, err := kafka.NewConsumer(kafka.Config{
		Client: client, Store: store,
		Bindings: []kafka.Binding{{Topic: topic, Pipeline: a.pipe}},
		Workers:  4, PollTimeout: 50 * time.Millisecond, CommitInterval: 500 * time.Millisecond,
		ProcessTimeout: 10 * time.Second,
	})
	if err != nil {
		return err
	}
	redriver, err := dlq.NewRedriver(dlq.RedriveConfig{
		Store: store, Handler: a.pipe.Handler(), Breakers: a.breakers,
		Workers: 4, LeaseTTL: 20 * time.Second, PollInterval: 200 * time.Millisecond, MaxAttempts: 8,
		Backoff: retry.Jitter(retry.Constant(time.Second), 0.3),
		Rate:    40, Burst: 10, RampUp: 5 * time.Second, // don't flood a service that just came back
	})
	if err != nil {
		return err
	}
	wake := redriver.Wake
	sink.wake.Store(&wake)

	var wg sync.WaitGroup
	spawn := func(f func()) { wg.Add(1); go func() { defer wg.Done(); f() }() }
	spawn(func() {
		if err := consumer.Run(ctx); err != nil {
			fmt.Fprintln(os.Stderr, "consumer:", err)
		}
	})
	spawn(func() {
		if err := redriver.Run(ctx); err != nil {
			fmt.Fprintln(os.Stderr, "redriver:", err)
		}
	})

	sem := make(chan struct{}, 400)
	fire := func(f func()) func() {
		return func() {
			select {
			case sem <- struct{}{}:
				go func() { defer func() { <-sem }(); f() }()
			default: // saturated: drop rather than pile up
			}
		}
	}
	spawn(func() { generate(ctx, &a.httpRPS, fire(a.runHTTP)) })
	spawn(func() { generate(ctx, &a.kafkaRPS, fire(func() { a.produce(producer, topic) })) })

	sv := &serviceView{a: a}
	spawn(func() { sv.poll(ctx) })
	snap := &snapshotter{a: a, store: store, consumer: consumer, redriver: redriver, sv: sv, mode: mode}
	spawn(func() { snap.loop(ctx) })

	srv := &http.Server{Addr: ":" + env("PORT", "8080"), Handler: a.routes(snap), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, err)
			stop()
		}
	}()
	fmt.Printf("orchestrator on %s (kafka: %s, services: %s)\n", srv.Addr, mode, host)

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	wg.Wait()
	return nil
}

// generate calls fire at the rate held in rps (requests per second times ten).
func generate(ctx context.Context, rps *atomic.Uint64, fire func()) {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	last := time.Now()
	var acc float64
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			acc += float64(rps.Load()) / 10 * now.Sub(last).Seconds()
			last = now
			if acc > 50 {
				acc = 50
			}
			for ; acc >= 1; acc-- {
				fire()
			}
		}
	}
}

// ---- what the UI sees of the downstream services ----

type serviceState struct {
	Name   string `json:"name"`
	Port   int    `json:"port"`
	Mode   string `json:"mode"`
	Served uint64 `json:"served"`
	Failed uint64 `json:"failed"`
}

type serviceView struct {
	a  *app
	mu sync.Mutex
	st []serviceState
}

func (v *serviceView) get() []serviceState {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.st == nil {
		out := make([]serviceState, 0, len(steps))
		for _, s := range steps {
			out = append(out, serviceState{Name: s, Port: servicePorts[s], Mode: "unknown"})
		}
		return out
	}
	return append([]serviceState(nil), v.st...)
}

func (v *serviceView) poll(ctx context.Context) {
	t := time.NewTicker(700 * time.Millisecond)
	defer t.Stop()
	for {
		c, cancel := context.WithTimeout(ctx, time.Second)
		req, _ := http.NewRequestWithContext(c, http.MethodGet, v.a.control+"/control/state", nil)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			var st []serviceState
			if json.NewDecoder(resp.Body).Decode(&st) == nil {
				v.mu.Lock()
				v.st = st
				v.mu.Unlock()
			}
			_ = resp.Body.Close()
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// ---- snapshots: the numbers the UI shows next to the animation ----

type breakerState struct {
	Name          string  `json:"name"`
	State         string  `json:"state"`
	FailureRate   float64 `json:"failureRate"` // percent, over the sampling window
	Success       int64   `json:"success"`
	Failure       int64   `json:"failure"`
	Inflight      int32   `json:"inflight"`
	Rejected      uint64  `json:"rejected"`
	Opens         uint64  `json:"opens"`
	AvgLatencyMs  int64   `json:"avgLatencyMs"`
	LastChangedMs int64   `json:"lastChangedMs,omitempty"`
}

type Snapshot struct {
	T       int64  `json:"t"`
	Backend string `json:"backend"` // "kafka" or "sim"
	Traffic struct {
		HTTPRPS  float64 `json:"httpRps"`
		KafkaRPS float64 `json:"kafkaRps"`
	} `json:"traffic"`
	Services []serviceState `json:"services"`
	Breakers []breakerState `json:"breakers"`
	DLQ      struct {
		Pending   int     `json:"pending"`
		Leased    int     `json:"leased"`
		Parked    int     `json:"parked"`
		OldestSec float64 `json:"oldestSec"`
		Redriven  uint64  `json:"redriven"`
		Blocked   uint64  `json:"blocked"`
	} `json:"dlq"`
	Kafka struct {
		Produced uint64 `json:"produced"`
		Received uint64 `json:"received"`
		Done     uint64 `json:"done"`
		Deferred uint64 `json:"deferred"`
		Held     uint64 `json:"held"`
		Parked   uint64 `json:"parked"`
		Paused   int    `json:"pausedPartitions"`
		InFlight int    `json:"inFlight"`
		Backlog  int64  `json:"backlog"`
	} `json:"kafka"`
	Ledger        LedgerView `json:"ledger"`
	DroppedEvents uint64     `json:"droppedEvents"`
	HTTP          struct {
		Sent     uint64 `json:"sent"`
		OK       uint64 `json:"ok"`
		Failed   uint64 `json:"failed"`
		Rejected uint64 `json:"rejected"`
	} `json:"http"`
}

type snapshotter struct {
	a        *app
	store    dlq.Store
	consumer *kafka.Consumer
	redriver *dlq.Redriver
	sv       *serviceView
	mode     string
}

func (s *snapshotter) take(ctx context.Context) Snapshot {
	var sn Snapshot
	sn.T = time.Now().UnixMilli()
	sn.Backend = s.mode
	sn.Traffic.HTTPRPS = float64(s.a.httpRPS.Load()) / 10
	sn.Traffic.KafkaRPS = float64(s.a.kafkaRPS.Load()) / 10
	sn.Services = s.sv.get()
	for _, name := range steps {
		st := s.a.breakers[name].Stats()
		var changed int64
		if !st.LastTransition.IsZero() {
			changed = st.LastTransition.UnixMilli()
		}
		sn.Breakers = append(sn.Breakers, breakerState{
			Name: name, State: st.State.String(), FailureRate: float64(st.FailureRateBps) / 100,
			Success: st.Success, Failure: st.Failure, Inflight: st.Inflight, Rejected: st.Rejected, Opens: st.Opens,
			AvgLatencyMs: st.AvgLatency.Milliseconds(), LastChangedMs: changed,
		})
	}
	onDisk := 0
	if ds, err := s.store.Stats(ctx); err == nil {
		sn.DLQ.Pending, sn.DLQ.Leased, sn.DLQ.Parked = ds.Pending, ds.Leased, ds.Parked
		sn.DLQ.OldestSec = ds.OldestPending.Seconds()
		onDisk = ds.Pending + ds.Leased
	}
	sn.Ledger = s.a.ledger.view(onDisk)
	sn.DroppedEvents = s.a.hub.Dropped()
	rs := s.redriver.Stats()
	sn.DLQ.Redriven, sn.DLQ.Blocked = rs.Succeeded, rs.Blocked
	cs := s.consumer.Stats()
	sn.Kafka.Produced = s.a.kafkaProduced.Load()
	sn.Kafka.Received, sn.Kafka.Done, sn.Kafka.Deferred = cs.Received, cs.Done, cs.Deferred
	sn.Kafka.Held, sn.Kafka.Parked, sn.Kafka.Paused, sn.Kafka.InFlight = cs.OrderHeld, cs.Parked, cs.Paused, cs.InFlight
	sn.Kafka.Backlog = int64(sn.Kafka.Produced) - int64(cs.Received)
	if sn.Kafka.Backlog < 0 {
		sn.Kafka.Backlog = 0
	}
	sn.HTTP.Sent, sn.HTTP.OK, sn.HTTP.Failed, sn.HTTP.Rejected = s.a.httpSent.Load(), s.a.httpOK.Load(), s.a.httpFailed.Load(), s.a.httpRejected.Load()
	return sn
}

func (s *snapshotter) loop(ctx context.Context) {
	t := time.NewTicker(400 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.a.hub.publish("s", s.take(ctx))
		}
	}
}

// ---- HTTP API for the UI ----

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (a *app) routes(snap *snapshotter) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) })

	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, snap.take(r.Context()))
	})

	mux.HandleFunc("GET /api/stream", func(w http.ResponseWriter, r *http.Request) {
		fl, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		h := w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("Connection", "keep-alive")
		h.Set("X-Accel-Buffering", "no") // let a proxy pass events straight through
		ch, unsub := a.hub.Subscribe()
		defer unsub()
		first, _ := json.Marshal(snap.take(r.Context()))
		_, _ = fmt.Fprintf(w, "event: s\ndata: %s\n\n", first)
		fl.Flush()
		beat := time.NewTicker(15 * time.Second)
		defer beat.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case msg := <-ch:
				if _, err := w.Write(msg); err != nil {
					return
				}
				fl.Flush()
			case <-beat.C:
				_, _ = io.WriteString(w, ": keep-alive\n\n")
				fl.Flush()
			}
		}
	})

	mux.HandleFunc("POST /api/traffic", func(w http.ResponseWriter, r *http.Request) {
		var b struct {
			HTTPRPS  *float64 `json:"httpRps"`
			KafkaRPS *float64 `json:"kafkaRps"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&b); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		set := func(dst *atomic.Uint64, v *float64) {
			if v == nil {
				return
			}
			x := *v
			if x < 0 {
				x = 0
			}
			if x > 200 {
				x = 200
			}
			dst.Store(uint64(x * 10))
		}
		set(&a.httpRPS, b.HTTPRPS)
		set(&a.kafkaRPS, b.KafkaRPS)
		writeJSON(w, http.StatusOK, map[string]float64{"httpRps": float64(a.httpRPS.Load()) / 10, "kafkaRps": float64(a.kafkaRPS.Load()) / 10})
	})

	mux.HandleFunc("POST /api/services/{name}/mode", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if _, ok := servicePorts[name]; !ok {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<12))
		a.forward(w, r, a.control+"/control/"+name, body)
	})
	// A hard kill, on purpose: no shutdown, no flush, no deferred Close. It shows what
	// the durable log is for. The container restarts and recovers from disk.
	mux.HandleFunc("POST /api/crash", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		go func() {
			time.Sleep(150 * time.Millisecond)
			os.Exit(137)
		}()
	})
	mux.HandleFunc("POST /api/reset", func(w http.ResponseWriter, r *http.Request) {
		a.forward(w, r, a.control+"/control/reset", nil)
	})
	return cors(mux)
}

func (a *app) forward(w http.ResponseWriter, r *http.Request, url string, body []byte) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, "services unreachable", http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "content-type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
