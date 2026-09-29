package goguard

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/obs"
	"github.com/justinclev/GoGuardLib/retry"
	"github.com/justinclev/GoGuardLib/secure"
)

// ErrDeferred is matched (errors.Is) by the error a guarded client returns when it
// did not send a request but saved it to Policy.Defer.Store to send later. The
// request is not lost and not sent: treat it as "accepted, will happen".
var ErrDeferred = errors.New("goguard: request saved for later")

// DeferredError is returned instead of the failed response or the circuit error
// when a request was saved. It matches ErrDeferred and also the failure that
// caused it (for example ErrCircuitOpen).
type DeferredError struct {
	// RecordID identifies the saved request in the store.
	RecordID string
	// Cause is what would have been returned had the request not been saved.
	Cause error
}

func (e *DeferredError) Error() string {
	return fmt.Sprintf("goguard: request saved for later (record %s): %v", e.RecordID, e.Cause)
}

func (e *DeferredError) Unwrap() []error { return []error{ErrDeferred, e.Cause} }

// Defer makes an endpoint save the requests it could not send, instead of failing
// them, so that a Redriver can send them when the service is healthy again.
//
// It is for calls that can be delayed: webhooks, notifications, writes whose
// result the caller does not wait for. A caller that needs the response cannot use
// it, because the answer will arrive long after the caller has gone.
//
// A request is saved when the circuit is open or bulkhead full, or when it failed
// after its retries. It is saved only if all of these hold, and otherwise fails
// exactly as it would without Defer:
//
//   - it carries the idempotency header, so sending it twice is safe (a timed-out
//     request may already have reached the service);
//   - its body can be read again (nil, or GetBody is set; http.NewRequest does this
//     for bytes, strings and similar bodies) and is at most MaxBodyBytes;
//   - its URL has no user:password;
//   - the store accepted it. If the store is full or failing, the caller gets the
//     original error and never a false "saved".
//
// Headers that carry credentials (Authorization, Cookie, API-key headers) are never
// stored. Supply them again when the request is replayed (ReplayConfig.Prepare).
// The URL, including its query string, and the body are stored: use dlq.Secure
// around a WAL store if they are sensitive.
type Defer struct {
	// Store holds the saved requests. Required. dlq.NewMemoryStore keeps them until
	// the process exits; dlq.OpenWAL keeps them across restarts.
	Store dlq.Store
	// IdempotencyHeader names the header a request must carry to be saved. Default
	// "Idempotency-Key".
	IdempotencyHeader string
	// OrderKey, when set, groups saved requests: those with the same non-empty key
	// are replayed one at a time, in the order they were saved.
	OrderKey func(*http.Request) string
	// MaxBodyBytes is the largest body that will be saved. Default 1 MiB.
	MaxBodyBytes int
	// Enabled, when set, is asked before every request: while it returns false the
	// endpoint behaves as if Defer were not configured (requests fail as they would
	// without it). It lets a service turn deferral on and off at run time, from a
	// feature flag or an operator switch, without rebuilding the transport and losing
	// its circuits. Requests already saved are unaffected and are still replayed.
	// It is called on the request path, so it must be fast.
	Enabled func() bool
}

const (
	deferMethodHeader = "x-goguard-http-method"
	deferURLHeader    = "x-goguard-http-url"
	defaultDeferBody  = 1 << 20
)

func (d *Defer) idemHeader() string {
	if d.IdempotencyHeader != "" {
		return d.IdempotencyHeader
	}
	return "Idempotency-Key"
}

func (d *Defer) maxBody() int {
	if d.MaxBodyBytes > 0 {
		return d.MaxBodyBytes
	}
	return defaultDeferBody
}

var sensitive = secure.NewRedactor()

// record turns req into a stored record, or reports that it cannot be saved.
func (d *Defer) record(req *http.Request, host string) (dlq.Record, bool) {
	idem := req.Header.Get(d.idemHeader())
	if idem == "" || req.URL == nil || req.URL.User != nil {
		return dlq.Record{}, false
	}
	var body []byte
	if req.Body != nil && req.Body != http.NoBody {
		if req.GetBody == nil {
			return dlq.Record{}, false // the body was consumed or cannot be read again
		}
		rc, err := req.GetBody()
		if err != nil {
			return dlq.Record{}, false
		}
		body, err = io.ReadAll(io.LimitReader(rc, int64(d.maxBody())+1))
		_ = rc.Close()
		if err != nil || len(body) > d.maxBody() {
			return dlq.Record{}, false
		}
	}
	rec := dlq.Record{
		ID:     dlq.DeterministicID("goguard-http", req.Method, req.URL.String(), idem),
		Source: dlq.Source{Kind: "http", Name: host},
		Value:  body,
		Headers: []dlq.Header{
			{Key: deferMethodHeader, Value: []byte(req.Method)},
			{Key: deferURLHeader, Value: []byte(req.URL.String())},
		},
	}
	for name, values := range req.Header {
		if sensitive.IsSensitiveHeader(name) || strings.HasPrefix(strings.ToLower(name), "x-goguard-") {
			continue
		}
		for _, v := range values {
			rec.Headers = append(rec.Headers, dlq.Header{Key: name, Value: []byte(v)})
		}
	}
	if d.OrderKey != nil {
		rec.OrderKey = d.OrderKey(req)
	}
	return rec, true
}

type replayKeyType struct{}

// tryDefer saves req if the endpoint's policy allows it. It returns the error to
// give the caller when the request was saved, and nil when it was not.
func (t *ResilientTransport) tryDefer(ent *entry, pol Policy, req *http.Request, cause error) error {
	d := pol.Defer
	if d == nil || pol.DryRun || (d.Enabled != nil && !d.Enabled()) {
		return nil
	}
	if replaying, _ := req.Context().Value(replayKeyType{}).(bool); replaying {
		return nil // a replay that fails is retried by the redriver, not saved again
	}
	rec, ok := d.record(req, ent.name)
	if !ok {
		return nil
	}
	rec.BlockedOn = ent.name // the redriver leaves it alone while this circuit is open
	err := d.Store.Append(context.WithoutCancel(req.Context()), rec)
	obs.Emit(t.sink, obs.HTTPDeferred{Endpoint: ent.name, RecordID: rec.ID, Saved: err == nil, At: time.Now()})
	if err != nil {
		return nil
	}
	return &DeferredError{RecordID: rec.ID, Cause: cause}
}

// Breakers returns the transport's circuit breakers by the name records are
// blocked on, for dlq.RedriveConfig.Breakers: while a host's circuit is open the
// redriver leaves its saved requests alone.
func (t *ResilientTransport) Breakers() map[string]*breaker.Breaker {
	out := map[string]*breaker.Breaker{}
	for _, e := range t.entries() {
		out[e.name] = e.breaker
	}
	return out
}

// ReplayConfig configures ReplayHandler.
type ReplayConfig struct {
	// Client sends the saved requests. Required. Use the guarded client, so that a
	// replay respects the circuit (and the health check) and is not sent while the
	// service is still down.
	Client *http.Client
	// Prepare, when set, adjusts each request before it is sent: add the credentials
	// that were never stored, refresh a token. Returning an error fails the replay;
	// wrap it with retry.Permanent if trying again cannot help.
	Prepare func(ctx context.Context, req *http.Request) error
}

// ReplayHandler returns a dlq.RedriveHandler that sends the requests saved by
// Policy.Defer:
//
//	redriver, err := dlq.NewRedriver(dlq.RedriveConfig{
//		Store: store, Handler: handler, Breakers: guard.Breakers(),
//	})
//
// A 2xx or 3xx answer finishes the record. A transport error, 408, 425, 429 or 5xx
// is transient and retried with the redriver's backoff; while the circuit is open
// the record waits without spending an attempt. Any other status (a 400, a 404) is
// permanent: the record is parked for a person, since sending it again cannot
// succeed. Records that did not come from Defer are parked.
func ReplayHandler(c ReplayConfig) (dlq.RedriveHandler, error) {
	if c.Client == nil {
		return nil, errors.New("goguard: ReplayConfig.Client is required")
	}
	return func(ctx context.Context, it *dlq.Item) error {
		rec := it.Record
		var method, target string
		for _, h := range rec.Headers {
			switch h.Key {
			case deferMethodHeader:
				method = string(h.Value)
			case deferURLHeader:
				target = string(h.Value)
			}
		}
		if rec.Source.Kind != "http" || method == "" || target == "" {
			return retry.Permanent(errors.New("goguard: record was not saved by Policy.Defer"))
		}
		var body io.Reader
		if len(rec.Value) > 0 {
			body = bytes.NewReader(rec.Value)
		}
		ctx = context.WithValue(ctx, replayKeyType{}, true)
		req, err := http.NewRequestWithContext(ctx, method, target, body)
		if err != nil {
			return retry.Permanent(fmt.Errorf("goguard: rebuilding the request: %w", err))
		}
		for _, h := range rec.Headers {
			if h.Key != deferMethodHeader && h.Key != deferURLHeader {
				req.Header.Add(h.Key, string(h.Value))
			}
		}
		if c.Prepare != nil {
			if err := c.Prepare(ctx, req); err != nil {
				return err
			}
		}
		resp, err := c.Client.Do(req)
		if err != nil {
			var ce *CircuitError
			if errors.As(err, &ce) && errors.Is(err, ErrCircuitOpen) {
				return &dlq.BlockedError{Dependency: ce.Host, Err: err, Refund: true}
			}
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		switch code := resp.StatusCode; {
		case code < 400:
			return nil
		case code == http.StatusRequestTimeout, code == http.StatusTooEarly, code == http.StatusTooManyRequests, code >= 500:
			return fmt.Errorf("goguard: replay answered %d", code)
		default:
			return retry.Permanent(fmt.Errorf("goguard: replay answered %d", code))
		}
	}, nil
}
