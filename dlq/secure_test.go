package dlq_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/dlq/storetest"
	"github.com/justinclev/GoGuardLib/secure"
)

func newKey(t testing.TB, id string) secure.Key {
	t.Helper()
	m := make([]byte, secure.KeySize)
	if _, err := rand.Read(m); err != nil {
		t.Fatal(err)
	}
	return secure.Key{ID: id, Material: m}
}

func enc(t testing.TB, active secure.Key, retired ...secure.Key) *secure.AESGCM {
	t.Helper()
	e, err := secure.NewAESGCM(active, retired...)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func secured(t testing.TB, inner dlq.Store, o dlq.SecureOptions) dlq.Store {
	t.Helper()
	s, err := dlq.Secure(inner, o)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// Wrapping must not change how a store behaves: the whole contract still holds.
func TestSecureStoreConformance(t *testing.T) {
	variants := map[string][]byte{"no pepper": nil, "with pepper": []byte("pepper-pepper-pepper")}
	for name, pepper := range variants {
		t.Run(name, func(t *testing.T) {
			storetest.Run(t, func(t *testing.T, now func() time.Time) dlq.Store {
				return secured(t, dlq.NewMemoryStore(dlq.MemoryOptions{Clock: now}),
					dlq.SecureOptions{Encryptor: enc(t, newKey(t, "k")), OrderKeyPepper: pepper})
			})
		})
	}
}

func TestSecureRequiresEncryptor(t *testing.T) {
	if _, err := dlq.Secure(dlq.NewMemoryStore(dlq.MemoryOptions{}), dlq.SecureOptions{}); err == nil {
		t.Fatal("expected an error without an Encryptor")
	}
}

func TestSecureKeepsPlaintextOutOfTheInnerStore(t *testing.T) {
	ctx := context.Background()
	inner := dlq.NewMemoryStore(dlq.MemoryOptions{})
	s := secured(t, inner, dlq.SecureOptions{Encryptor: enc(t, newKey(t, "k")), OrderKeyPepper: []byte("pepper")})

	err := s.Append(ctx, dlq.Record{
		ID:         "r1",
		Key:        []byte("customer-4711"),
		Value:      []byte("card=4111111111111111"),
		Headers:    []dlq.Header{{Key: "tenant", Value: []byte("acme-corp")}},
		OrderKey:   "customer-4711",
		Checkpoint: []byte("step-2-output"),
		LastError:  "upstream rejected: password=hunter2hunter2",
	})
	if err != nil {
		t.Fatal(err)
	}

	raw, err := inner.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Minute})
	if err != nil || len(raw) != 1 {
		t.Fatalf("raw lease: %v %d", err, len(raw))
	}
	r := raw[0].Record
	for name, b := range map[string][]byte{"key": r.Key, "value": r.Value, "header": r.Headers[0].Value, "checkpoint": r.Checkpoint} {
		for _, secret := range []string{"customer-4711", "4111111111111111", "acme-corp", "step-2-output"} {
			if bytes.Contains(b, []byte(secret)) {
				t.Errorf("%s stored in clear: contains %q", name, secret)
			}
		}
	}
	if strings.Contains(r.OrderKey, "customer") || r.OrderKey == "" {
		t.Errorf("OrderKey = %q, want an opaque keyed hash", r.OrderKey)
	}
	if strings.Contains(r.LastError, "hunter2hunter2") || !strings.Contains(r.LastError, secure.Redacted) {
		t.Errorf("LastError = %q, want the password redacted", r.LastError)
	}
	if r.Headers[0].Key != "tenant" || r.ID != "r1" {
		t.Error("header names and IDs must stay readable for operators")
	}
	_ = inner.Release(ctx, "r1", raw[0].Token)

	// Through the wrapper the original data comes back.
	got, _ := s.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Minute})
	if len(got) != 1 {
		t.Fatalf("leased %d", len(got))
	}
	g := got[0].Record
	if string(g.Key) != "customer-4711" || string(g.Value) != "card=4111111111111111" ||
		string(g.Headers[0].Value) != "acme-corp" || string(g.Checkpoint) != "step-2-output" {
		t.Fatalf("round trip lost data: %+v", g)
	}
}

func TestSecureRedactsNackAndParkText(t *testing.T) {
	ctx := context.Background()
	inner := dlq.NewMemoryStore(dlq.MemoryOptions{})
	s := secured(t, inner, dlq.SecureOptions{Encryptor: enc(t, newKey(t, "k"))})
	_ = s.Append(ctx, dlq.Record{ID: "r"})

	l, _ := s.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Minute})
	if err := s.Nack(ctx, "r", l[0].Token, dlq.NackOptions{Err: "got Bearer abcdefghij1234567890"}); err != nil {
		t.Fatal(err)
	}
	raw, _ := inner.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Minute})
	if strings.Contains(raw[0].Record.LastError, "abcdefghij1234567890") {
		t.Fatalf("Nack error stored unredacted: %q", raw[0].Record.LastError)
	}
	if err := s.Park(ctx, "r", raw[0].Token, "token=supersecretvalue1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Requeue(ctx, "r"); err != nil {
		t.Fatal(err)
	}
	raw, _ = inner.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Minute})
	if strings.Contains(raw[0].Record.LastError, "supersecretvalue1") {
		t.Fatalf("Park reason stored unredacted: %q", raw[0].Record.LastError)
	}
}

func TestSecurePepperKeepsEqualKeysEqualAndDistinctKeysDistinct(t *testing.T) {
	ctx := context.Background()
	inner := dlq.NewMemoryStore(dlq.MemoryOptions{})
	s := secured(t, inner, dlq.SecureOptions{Encryptor: enc(t, newKey(t, "k")), OrderKeyPepper: []byte("p")})
	for _, r := range []dlq.Record{{ID: "1", OrderKey: "a"}, {ID: "2", OrderKey: "a"}, {ID: "3", OrderKey: "b"}} {
		_ = s.Append(ctx, r)
	}
	raw, _ := inner.Lease(ctx, dlq.LeaseRequest{Max: 10, TTL: time.Minute})
	// "2" is blocked behind "1", so the raw lease has 1 and 3.
	if len(raw) != 2 || raw[0].Record.OrderKey == raw[1].Record.OrderKey {
		t.Fatalf("leased %d records with keys %q / %q", len(raw), raw[0].Record.OrderKey, raw[len(raw)-1].Record.OrderKey)
	}
}

// A record whose key is gone must be parked and reported, never returned as
// ciphertext, never dropped, and never left to crash-loop its worker.
func TestSecureParksUndecryptableRecords(t *testing.T) {
	ctx := context.Background()
	inner := dlq.NewMemoryStore(dlq.MemoryOptions{})
	writer := secured(t, inner, dlq.SecureOptions{Encryptor: enc(t, newKey(t, "old"))})
	_ = writer.Append(ctx, dlq.Record{ID: "lost", Value: []byte("precious")})

	var mu sync.Mutex
	var reported []string
	reader := secured(t, inner, dlq.SecureOptions{
		Encryptor: enc(t, newKey(t, "unrelated")),
		OnUndecryptable: func(id string, err error) {
			mu.Lock()
			reported = append(reported, id)
			mu.Unlock()
		},
	})
	got, err := reader.Lease(ctx, dlq.LeaseRequest{Max: 5, TTL: time.Minute})
	if err != nil || len(got) != 0 {
		t.Fatalf("Lease = %d records, err %v; undecryptable data must not be returned", len(got), err)
	}
	mu.Lock()
	if len(reported) != 1 || reported[0] != "lost" {
		t.Fatalf("OnUndecryptable saw %v", reported)
	}
	mu.Unlock()
	st, _ := reader.Stats(ctx)
	if st.Parked != 1 || st.Total() != 1 {
		t.Fatalf("stats = %+v; the record must be kept, parked", st)
	}
	again, _ := reader.Lease(ctx, dlq.LeaseRequest{Max: 5, TTL: time.Minute})
	if len(again) != 0 {
		t.Fatal("a parked record was leased again")
	}
}

func TestSecureSupportsKeyRotation(t *testing.T) {
	ctx := context.Background()
	inner := dlq.NewMemoryStore(dlq.MemoryOptions{})
	oldKey := newKey(t, "2024")
	_ = secured(t, inner, dlq.SecureOptions{Encryptor: enc(t, oldKey)}).Append(ctx, dlq.Record{ID: "r", Value: []byte("v")})

	rotated := secured(t, inner, dlq.SecureOptions{Encryptor: enc(t, newKey(t, "2025"), oldKey)})
	got, _ := rotated.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Minute})
	if len(got) != 1 || string(got[0].Record.Value) != "v" {
		t.Fatalf("data sealed under the retired key must still open: %+v", got)
	}
}

// Ciphertext copied from one record into another must not decrypt.
func TestSecureBindsCiphertextToItsRecord(t *testing.T) {
	ctx := context.Background()
	e := enc(t, newKey(t, "k"))

	innerA := dlq.NewMemoryStore(dlq.MemoryOptions{})
	_ = secured(t, innerA, dlq.SecureOptions{Encryptor: e}).Append(ctx, dlq.Record{ID: "A", Value: []byte("A's secret")})
	stolen, _ := innerA.Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Minute})

	innerB := dlq.NewMemoryStore(dlq.MemoryOptions{})
	_ = innerB.Append(ctx, dlq.Record{ID: "B", Value: stolen[0].Record.Value})
	detected := false
	got, _ := secured(t, innerB, dlq.SecureOptions{Encryptor: e, OnUndecryptable: func(string, error) { detected = true }}).
		Lease(ctx, dlq.LeaseRequest{Max: 1, TTL: time.Minute})
	if len(got) != 0 || !detected {
		t.Fatalf("ciphertext moved between records was accepted: %+v", got)
	}
}

func TestSecureRejectsInvalidRecordsBeforeSealing(t *testing.T) {
	s := secured(t, dlq.NewMemoryStore(dlq.MemoryOptions{}), dlq.SecureOptions{Encryptor: enc(t, newKey(t, "k"))})
	if err := s.Append(context.Background(), dlq.Record{}); err == nil {
		t.Fatal("expected ErrInvalidRecord")
	}
}
