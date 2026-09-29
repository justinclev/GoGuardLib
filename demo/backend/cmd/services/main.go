// Command services simulates the APIs an order flows through (inventory, payments,
// shipping, notifications) and a control API to take each one down, slow it, or
// make it flaky. Taking a service down closes its listener, so callers see a real
// "connection refused", not a canned error.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type mode string

const (
	modeUp    mode = "up"
	modeDown  mode = "down"
	modeSlow  mode = "slow"
	modeFlaky mode = "flaky"
)

type service struct {
	Name    string
	Port    int
	Latency time.Duration // typical response time

	mu     sync.Mutex
	mode   mode
	srv    *http.Server
	served atomic.Uint64
	failed atomic.Uint64
}

func (s *service) currentMode() mode {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mode
}

func (s *service) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if s.currentMode() == modeFlaky && rand.Float64() < 0.2 {
			http.Error(w, "degraded", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/work", func(w http.ResponseWriter, r *http.Request) {
		m := s.currentMode()
		d := s.Latency/2 + time.Duration(rand.Int64N(int64(s.Latency)))
		if m == modeSlow {
			d += 2500 * time.Millisecond // slower than any caller is willing to wait
		}
		select {
		case <-time.After(d):
		case <-r.Context().Done():
			return
		}
		if m == modeFlaky && rand.Float64() < 0.55 {
			s.failed.Add(1)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		s.served.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"service":%q,"status":"ok","ms":%d}`, s.Name, d.Milliseconds())
	})
	return mux
}

func (s *service) start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.srv != nil {
		return nil
	}
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		return err
	}
	s.srv = &http.Server{Handler: s.handler(), ReadHeaderTimeout: 5 * time.Second}
	go func(srv *http.Server) { _ = srv.Serve(ln) }(s.srv)
	return nil
}

func (s *service) stop() {
	s.mu.Lock()
	srv := s.srv
	s.srv = nil
	s.mu.Unlock()
	if srv != nil {
		_ = srv.Close() // closes the listener and every open connection
	}
}

func (s *service) set(m mode) error {
	s.mu.Lock()
	s.mode = m
	s.mu.Unlock()
	if m == modeDown {
		s.stop()
		return nil
	}
	return s.start()
}

func (s *service) status() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{"name": s.Name, "port": s.Port, "mode": s.mode, "served": s.served.Load(), "failed": s.failed.Load()}
}

func main() {
	base := 9101
	svcs := []*service{
		{Name: "inventory", Port: base, Latency: 60 * time.Millisecond, mode: modeUp},
		{Name: "payments", Port: base + 1, Latency: 120 * time.Millisecond, mode: modeUp},
		{Name: "shipping", Port: base + 2, Latency: 90 * time.Millisecond, mode: modeUp},
		{Name: "notifications", Port: base + 3, Latency: 40 * time.Millisecond, mode: modeUp},
	}
	byName := map[string]*service{}
	for _, s := range svcs {
		byName[s.Name] = s
		if err := s.start(); err != nil {
			fmt.Fprintln(os.Stderr, "cannot start", s.Name, err)
			os.Exit(1)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /control/state", func(w http.ResponseWriter, r *http.Request) {
		out := make([]map[string]any, 0, len(svcs))
		for _, s := range svcs {
			out = append(out, s.status())
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("POST /control/reset", func(w http.ResponseWriter, r *http.Request) {
		for _, s := range svcs {
			_ = s.set(modeUp)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /control/{name}", func(w http.ResponseWriter, r *http.Request) {
		s := byName[r.PathValue("name")]
		if s == nil {
			http.NotFound(w, r)
			return
		}
		var body struct {
			Mode mode `json:"mode"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad body", http.StatusBadRequest)
			return
		}
		switch body.Mode {
		case modeUp, modeDown, modeSlow, modeFlaky:
		default:
			http.Error(w, "mode must be up, down, slow or flaky", http.StatusBadRequest)
			return
		}
		if err := s.set(body.Mode); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.status())
	})
	ctl := &http.Server{Addr: ":9100", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := ctl.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}()
	fmt.Println("services up: inventory :9101, payments :9102, shipping :9103, notifications :9104, control :9100")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	_ = ctl.Shutdown(context.Background())
	for _, s := range svcs {
		s.stop()
	}
}
