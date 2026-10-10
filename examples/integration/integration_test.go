// Package integration holds the snippets from docs/INTEGRATION.md as tests, so the
// guide cannot drift from the API: adding the breaker and retry packages to an
// existing poll-and-process consumer, and keeping failed messages in a durable,
// encrypted log.
package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/health"
	"github.com/justinclev/GoGuardLib/obs"
	"github.com/justinclev/GoGuardLib/retry"
	"github.com/justinclev/GoGuardLib/secure"
)

// statusError is what a client wrapper returns for a non-2xx answer.
type statusError struct{ code int }

func (e *statusError) Error() string { return fmt.Sprintf("status %d", e.code) }

// classify marks errors that retrying cannot fix. A 4xx means the request is wrong,
// not that the service is unhealthy.
func classify(err error) error {
	var se *statusError
	if errors.As(err, &se) && se.code >= 400 && se.code < 500 && se.code != http.StatusTooManyRequests {
		return retry.Permanent(err)
	}
	return err
}

// newGuard is the per-dependency breaker from the guide: health-gated recovery,
// and only server-side failures count against the dependency.
func newGuard(t *testing.T, name, healthURL string, sink obs.Sink) *breaker.Breaker {
	t.Helper()
	check, err := health.HTTP(healthURL) // Health without a Check is ignored
	if err != nil {
		t.Fatal(err)
	}
	b := breaker.New(breaker.Config{
		Name: name, FailureThreshold: 0.5, MinSamples: 4, SleepWindow: time.Minute,
		IsFailure: func(err error) bool { return !retry.IsPermanent(err) },
		Health:    &health.Config{Check: check, Interval: 20 * time.Millisecond, MaxInterval: 50 * time.Millisecond, SuccessThreshold: 1},
		Events:    sink,
	})
	t.Cleanup(b.Close)
	return b
}

var standard = retry.Policy{
	MaxRetries: 2,
	Backoff:    retry.Jitter(retry.Exponential(time.Millisecond, 4*time.Millisecond), 0.3),
}

// call is the layering from the guide: retry outside, breaker inside.
func call(ctx context.Context, b *breaker.Breaker, fn func(context.Context) error) error {
	return retry.Do(ctx, standard, func(ctx context.Context) error {
		return b.Do(ctx, func(ctx context.Context) error { return classify(fn(ctx)) })
	})
}

func TestPermanentErrorsAreNotRetriedAndDoNotOpenTheCircuit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer srv.Close()
	b := newGuard(t, "rules-engine", srv.URL, nil)

	var calls atomic.Int32
	for i := 0; i < 20; i++ {
		if err := call(context.Background(), b, func(context.Context) error { calls.Add(1); return &statusError{400} }); err == nil {
			t.Fatal("expected an error")
		}
	}
	if calls.Load() != 20 {
		t.Fatalf("a 400 must not be retried: %d calls for 20 requests", calls.Load())
	}
	if st := b.Stats(); st.State != breaker.StateClosed || st.Opens != 0 {
		t.Fatalf("bad requests must not open the circuit: %+v", st)
	}
}

func TestOutageOpensTheCircuitAndRecoversThroughTheHealthCheck(t *testing.T) {
	var healthy atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !healthy.Load() {
			w.WriteHeader(503)
		}
	}))
	defer srv.Close()

	var opened, closed atomic.Int32
	b := newGuard(t, "enrollment", srv.URL, obs.SinkFunc(func(e obs.Event) {
		if sc, ok := e.(obs.StateChanged); ok {
			switch sc.To {
			case obs.StateOpen:
				opened.Add(1)
			case obs.StateClosed:
				closed.Add(1)
			}
		}
	}))

	down := func(context.Context) error { return &statusError{503} }
	for i := 0; i < 10; i++ {
		_ = call(context.Background(), b, down)
	}
	err := call(context.Background(), b, down)
	if !errors.Is(err, breaker.ErrOpen) {
		t.Fatalf("expected an open circuit, got %v", err)
	}

	// An open circuit is not retried: one attempt, not MaxRetries+1.
	var calls atomic.Int32
	_ = call(context.Background(), b, func(context.Context) error { calls.Add(1); return nil })
	if calls.Load() != 0 {
		t.Fatal("the protected call ran while the circuit was open")
	}

	healthy.Store(true)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if err := call(context.Background(), b, func(context.Context) error { return nil }); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the circuit never recovered")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if opened.Load() != 1 || closed.Load() < 1 {
		t.Fatalf("events: opened=%d closed=%d", opened.Load(), closed.Load())
	}
	if st := b.Stats(); st.Rejected == 0 {
		t.Fatalf("stats should show the rejections: %+v", st)
	}
}

func TestFailedMessagesSurviveARestartAndAreEncryptedOnDisk(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "dlq")
	key := bytes.Repeat([]byte{7}, 32)
	open := func() dlq.Store {
		wal, err := dlq.OpenWAL(dir, dlq.WALOptions{})
		if err != nil {
			t.Fatal(err)
		}
		enc, err := secure.NewAESGCM(secure.Key{ID: "k1", Material: key})
		if err != nil {
			t.Fatal(err)
		}
		s, err := dlq.Secure(wal, dlq.SecureOptions{Encryptor: enc})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}

	const secret = "member-4711-diagnosis"
	s := open()
	rec := dlq.Record{
		ID:     dlq.KafkaID("letters", 2, 991), // re-appending after a crash is a no-op
		Source: dlq.Source{Kind: "kafka", Name: "letters", Partition: 2, Offset: 991},
		Value:  []byte(secret),
	}
	ctx := context.Background()
	if err := s.Append(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if st, _ := s.Stats(ctx); st.Pending != 1 {
		t.Fatalf("the same offset must be stored once, got %d", st.Pending)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = open()
	defer func() { _ = s.Close() }()
	got, err := s.Get(ctx, rec.ID)
	if err != nil || string(got.Value) != secret {
		t.Fatalf("after a restart: %v, %q", err, got.Value)
	}

	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			b, _ := os.ReadFile(p)
			if strings.Contains(string(b), secret) {
				t.Errorf("%s holds the message in clear", p)
			}
		}
		return nil
	})
}
