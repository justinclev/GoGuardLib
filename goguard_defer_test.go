package goguard_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/health"
	"github.com/justinclev/GoGuardLib/retry"
)

// service is a downstream API that can be taken down and records what reached it.
type service struct {
	*httptest.Server
	down atomic.Bool
	mu   sync.Mutex
	got  []received
}

type received struct{ method, path, auth, idem, body string }

func newService() *service {
	s := &service{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			if s.down.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			return
		}
		if s.down.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		b, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.got = append(s.got, received{r.Method, r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("Idempotency-Key"), string(b)})
		s.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
	}))
	return s
}

func (s *service) received() []received {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]received(nil), s.got...)
}

func stores(t *testing.T) map[string]dlq.Store {
	wal, err := dlq.OpenWAL(t.TempDir(), dlq.WALOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	return map[string]dlq.Store{"memory": dlq.NewMemoryStore(dlq.MemoryOptions{}), "disk": wal}
}

func guardWithDefer(t *testing.T, host string, d *goguard.Defer) (*goguard.ResilientTransport, *http.Client) {
	t.Helper()
	g, err := goguard.New(goguard.Config{}, goguard.WithEndpoint("svc", goguard.Host(host), goguard.Policy{
		FailureThreshold: 0.5, MinSamples: 2, SleepWindow: time.Hour, Defer: d,
		HealthPath: "/health", // the circuit reopens when the service says it is healthy
		Health:     &health.Config{Interval: 20 * time.Millisecond, SuccessThreshold: 1, TrustHealth: true},
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g, &http.Client{Transport: g}
}

func post(client *http.Client, url, body string, hdr map[string]string) error {
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
	}
	return err
}


// The whole story, for both stores: the service is down, requests are saved and
// not sent; the service returns; a redriver sends each once, with fresh credentials.
func TestDeferredRequestsAreSavedThenReplayedOnce(t *testing.T) {
	for name, store := range stores(t) {
		t.Run(name, func(t *testing.T) {
			svc := newService()
			defer svc.Close()
			svc.down.Store(true)
			guard, client := guardWithDefer(t, svc.Listener.Addr().String(), &goguard.Defer{Store: store})

			// The failures that open the circuit are saved too; later calls are refused
			// by the open circuit and saved without being sent.
			for i, id := range []string{"a", "b", "c", "d"} {
				h := map[string]string{"Idempotency-Key": id, "Authorization": "Bearer SECRET-TOKEN"}
				err := post(client, svc.URL+"/hook", "body-"+id, h)
				if !errors.Is(err, goguard.ErrDeferred) {
					t.Fatalf("call %d = %v, want ErrDeferred", i, err)
				}
			}
			if st, _ := store.Stats(context.Background()); st.Total() != 4 {
				t.Fatalf("store holds %d records, want 4", st.Total())
			}
			if len(svc.received()) != 0 {
				t.Fatal("nothing should reach a service that is down")
			}

			// No credential may be on the record.
			recs, _ := drain(t, store)
			for _, r := range recs {
				for _, h := range r.Headers {
					if strings.EqualFold(h.Key, "Authorization") || strings.Contains(string(h.Value), "SECRET-TOKEN") {
						t.Fatalf("a credential was saved: %s: %s", h.Key, h.Value)
					}
				}
			}

			svc.down.Store(false)
			handler, err := goguard.ReplayHandler(goguard.ReplayConfig{Client: client, Prepare: func(_ context.Context, r *http.Request) error {
				r.Header.Set("Authorization", "Bearer FRESH")
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			rd, err := dlq.NewRedriver(dlq.RedriveConfig{
				Store: store, Handler: handler, Breakers: guard.Breakers(),
				PollInterval: 10 * time.Millisecond, Backoff: retry.Constant(10 * time.Millisecond),
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { _ = rd.Run(ctx); close(done) }()
			defer func() { cancel(); <-done }()

			deadline := time.Now().Add(15 * time.Second)
			for len(svc.received()) < 4 && time.Now().Before(deadline) {
				time.Sleep(20 * time.Millisecond)
			}
			got := svc.received()
			if len(got) != 4 {
				t.Fatalf("service received %d requests, want 4: %+v", len(got), got)
			}
			seen := map[string]bool{}
			for _, g := range got {
				if g.method != http.MethodPost || g.path != "/hook" || g.auth != "Bearer FRESH" || g.body != "body-"+g.idem {
					t.Errorf("replayed request = %+v", g)
				}
				if seen[g.idem] {
					t.Errorf("request %s was sent twice", g.idem)
				}
				seen[g.idem] = true
			}
			for i := 0; i < 100; i++ {
				if st, _ := store.Stats(context.Background()); st.Total() == 0 {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
			t.Fatal("the store did not drain after the replays")
		})
	}
}

// drain lists what is in the store without changing it (parked or pending).
func drain(t *testing.T, store dlq.Store) ([]dlq.Record, error) {
	t.Helper()
	ls, err := store.Lease(context.Background(), dlq.LeaseRequest{Max: 100, TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	var out []dlq.Record
	for _, l := range ls {
		out = append(out, l.Record)
		if err := store.Release(context.Background(), l.Record.ID, l.Token); err != nil {
			t.Fatal(err)
		}
	}
	return out, nil
}

// A request that cannot be replayed safely, or that the store cannot take, fails
// exactly as before: the caller must never be told it is saved when it is not.
func TestRequestsThatCannotBeSavedFailAsBefore(t *testing.T) {
	svc := newService()
	defer svc.Close()
	svc.down.Store(true)
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	_, client := guardWithDefer(t, svc.Listener.Addr().String(), &goguard.Defer{Store: store, MaxBodyBytes: 16})

	cases := map[string]func() error{
		"no idempotency key": func() error { return post(client, svc.URL+"/x", "b", nil) },
		"body too large": func() error {
			return post(client, svc.URL+"/x", strings.Repeat("z", 100), map[string]string{"Idempotency-Key": "k"})
		},
		"credentials in url": func() error {
			u := strings.Replace(svc.URL, "http://", "http://user:pw@", 1)
			return post(client, u+"/x", "b", map[string]string{"Idempotency-Key": "k"})
		},
		"body that cannot be read again": func() error {
			req, _ := http.NewRequest(http.MethodPost, svc.URL+"/x", io.NopCloser(strings.NewReader("b")))
			req.Header.Set("Idempotency-Key", "k")
			resp, err := client.Do(req)
			if err == nil {
				_ = resp.Body.Close()
			}
			return err
		},
	}
	for name, call := range cases {
		if err := call(); errors.Is(err, goguard.ErrDeferred) {
			t.Errorf("%s: request was reported saved", name)
		}
	}
	if st, _ := store.Stats(context.Background()); st.Total() != 0 {
		t.Fatalf("store holds %d records, want none", st.Total())
	}

	full := dlq.NewMemoryStore(dlq.MemoryOptions{MaxRecords: 1})
	_ = full.Append(context.Background(), dlq.Record{ID: "occupant"})
	_, client2 := guardWithDefer(t, svc.Listener.Addr().String(), &goguard.Defer{Store: full})
	req, _ := http.NewRequest(http.MethodPost, svc.URL+"/x", strings.NewReader("b"))
	req.Header.Set("Idempotency-Key", "k")
	resp, err := client2.Do(req)
	if errors.Is(err, goguard.ErrDeferred) {
		t.Fatal("a full store must not report the request saved")
	}
	if err == nil {
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("status %d, want the service's own 503", resp.StatusCode)
		}
	}
}

// Sending the same saved request again (the caller retried after ErrDeferred) must
// not create a second copy.
func TestSavingTheSameRequestTwiceKeepsOneCopy(t *testing.T) {
	svc := newService()
	defer svc.Close()
	svc.down.Store(true)
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	_, client := guardWithDefer(t, svc.Listener.Addr().String(), &goguard.Defer{Store: store})
	for i := 0; i < 3; i++ {
		if err := post(client, svc.URL+"/x", "same", map[string]string{"Idempotency-Key": "same-key"}); !errors.Is(err, goguard.ErrDeferred) {
			t.Fatalf("call %d = %v", i, err)
		}
	}
	if st, _ := store.Stats(context.Background()); st.Total() != 1 {
		t.Fatalf("store holds %d records for one logical request, want 1", st.Total())
	}
}

// A replay that meets an open circuit waits without being saved again or spending
// an attempt; a permanent answer parks the record.
func TestReplayWaitsOnAnOpenCircuitAndParksPermanentAnswers(t *testing.T) {
	svc := newService()
	defer svc.Close()
	svc.down.Store(true)
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})
	guard, client := guardWithDefer(t, svc.Listener.Addr().String(), &goguard.Defer{Store: store})
	for _, id := range []string{"a", "b", "c"} { // opens the circuit
		_ = post(client, svc.URL+"/x", "b", map[string]string{"Idempotency-Key": id})
	}
	handler, _ := goguard.ReplayHandler(goguard.ReplayConfig{Client: client})
	ls, _ := store.Lease(context.Background(), dlq.LeaseRequest{Max: 1, TTL: time.Hour})
	err := handler(context.Background(), &dlq.Item{Record: ls[0].Record})
	var blocked *dlq.BlockedError
	if !errors.As(err, &blocked) || !blocked.Refund {
		t.Fatalf("replay with the circuit open = %v, want a refunded BlockedError", err)
	}
	if _, ok := guard.Breakers()[blocked.Dependency]; !ok {
		t.Fatalf("BlockedError names %q, which Breakers() does not know", blocked.Dependency)
	}
	if st, _ := store.Stats(context.Background()); st.Total() != 3 {
		t.Fatalf("the replay saved another copy: %d records", st.Total())
	}

	gone := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotFound) }))
	defer gone.Close()
	rec := dlq.Record{ID: "r", Source: dlq.Source{Kind: "http", Name: "x"}, Headers: []dlq.Header{{Key: "x-goguard-http-method", Value: []byte("POST")}, {Key: "x-goguard-http-url", Value: []byte(gone.URL)}}}
	if err := handler(context.Background(), &dlq.Item{Record: rec}); !retry.IsPermanent(err) {
		t.Fatalf("a 404 replay = %v, want a permanent failure", err)
	}
	if err := handler(context.Background(), &dlq.Item{Record: dlq.Record{ID: "z"}}); !retry.IsPermanent(err) {
		t.Fatalf("a record not saved by Defer = %v, want a permanent failure", err)
	}
}

func TestDeferNeedsAStore(t *testing.T) {
	_, err := goguard.New(goguard.Config{}, goguard.WithEndpoint("svc", goguard.Host("x"), goguard.Policy{Defer: &goguard.Defer{}}))
	if err == nil {
		t.Fatal("Defer without a Store must be rejected")
	}
}
