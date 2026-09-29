package dlq

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/justinclev/GoGuardLib/secure"
)

// SecureOptions configures Secure.
type SecureOptions struct {
	// Encryptor seals payload fields. Required.
	Encryptor secure.Encryptor
	// Redactor scrubs error text before it is stored. Default: secure.NewRedactor().
	Redactor *secure.Redactor
	// OrderKeyPepper, when set, replaces every OrderKey with its HMAC-SHA256 under
	// this secret. Ordering still works (equal keys stay equal) but the stored value
	// no longer reveals the message key. Without it OrderKey is stored in clear.
	OrderKeyPepper []byte
	// OnUndecryptable is called when a leased record cannot be decrypted, for
	// example because its key was removed. The record has already been parked. It
	// must not block, and receives no payload data.
	OnUndecryptable func(id string, err error)
}

type secureStore struct {
	inner Store
	o     SecureOptions
}

// Secure wraps a Store so that what reaches it is safe to keep:
//
//   - Key, Value, header values and Checkpoint are sealed with AES-GCM, bound to
//     the record ID and field so ciphertext cannot be moved between records or
//     fields. A nil Value stays nil (its absence is visible; its meaning is not
//     hidden).
//   - LastError, Nack errors and Park reasons are passed through the Redactor.
//   - OrderKey is optionally keyed-hashed.
//
// Everything else (IDs, source, header names, timestamps, counters) is stored in
// clear so the store can index and operators can inspect it. It works with any
// Store, so a Kafka-topic or file store gets the same protection.
//
// A record that cannot be decrypted is parked, not returned and not dropped, so
// a lost key never turns into silent data loss or a crash loop.
func Secure(inner Store, o SecureOptions) (Store, error) {
	if o.Encryptor == nil {
		return nil, errors.New("dlq: SecureOptions.Encryptor is required")
	}
	if o.Redactor == nil {
		o.Redactor = secure.NewRedactor()
	}
	o.OrderKeyPepper = append([]byte(nil), o.OrderKeyPepper...)
	return &secureStore{inner: inner, o: o}, nil
}

func aad(id, field string) []byte {
	return []byte("goguard/dlq/v1\x00" + id + "\x00" + field)
}

func (s *secureStore) seal(id, field string, plain []byte) ([]byte, error) {
	if plain == nil {
		return nil, nil
	}
	return s.o.Encryptor.Seal(plain, aad(id, field))
}

func (s *secureStore) open(id, field string, sealed []byte) ([]byte, error) {
	if sealed == nil {
		return nil, nil
	}
	return s.o.Encryptor.Open(sealed, aad(id, field))
}

func (s *secureStore) orderKey(k string) string {
	if k == "" || len(s.o.OrderKeyPepper) == 0 {
		return k
	}
	m := hmac.New(sha256.New, s.o.OrderKeyPepper)
	m.Write([]byte(k))
	return hex.EncodeToString(m.Sum(nil))
}

func headerField(i int) string { return fmt.Sprintf("header:%d", i) }

func (s *secureStore) sealRecord(r Record) (Record, error) {
	r = r.Clone()
	var err error
	if r.Key, err = s.seal(r.ID, "key", r.Key); err != nil {
		return Record{}, err
	}
	if r.Value, err = s.seal(r.ID, "value", r.Value); err != nil {
		return Record{}, err
	}
	if r.Checkpoint, err = s.seal(r.ID, "checkpoint", r.Checkpoint); err != nil {
		return Record{}, err
	}
	for i := range r.Headers {
		if r.Headers[i].Value, err = s.seal(r.ID, headerField(i), r.Headers[i].Value); err != nil {
			return Record{}, err
		}
	}
	r.LastError = s.o.Redactor.String(r.LastError)
	r.OrderKey = s.orderKey(r.OrderKey)
	return r, nil
}

func (s *secureStore) openRecord(r Record) (Record, error) {
	var err error
	if r.Key, err = s.open(r.ID, "key", r.Key); err != nil {
		return Record{}, err
	}
	if r.Value, err = s.open(r.ID, "value", r.Value); err != nil {
		return Record{}, err
	}
	if r.Checkpoint, err = s.open(r.ID, "checkpoint", r.Checkpoint); err != nil {
		return Record{}, err
	}
	for i := range r.Headers {
		if r.Headers[i].Value, err = s.open(r.ID, headerField(i), r.Headers[i].Value); err != nil {
			return Record{}, err
		}
	}
	return r, nil
}

func (s *secureStore) Append(ctx context.Context, r Record) error {
	if err := r.Validate(); err != nil {
		return err
	}
	sealed, err := s.sealRecord(r)
	if err != nil {
		return fmt.Errorf("dlq: sealing record: %w", err)
	}
	return s.inner.Append(ctx, sealed)
}

func (s *secureStore) Lease(ctx context.Context, req LeaseRequest) ([]Lease, error) {
	leases, err := s.inner.Lease(ctx, req)
	if err != nil {
		return nil, err
	}
	out := leases[:0]
	for _, l := range leases {
		plain, err := s.openRecord(l.Record)
		if err != nil {
			// Never hand out ciphertext and never leave the lease dangling to be
			// retried in a loop: set the record aside for an operator.
			_ = s.inner.Park(ctx, l.Record.ID, l.Token, "payload could not be decrypted: "+decryptKind(err))
			if s.o.OnUndecryptable != nil {
				s.o.OnUndecryptable(l.Record.ID, err)
			}
			continue
		}
		l.Record = plain
		out = append(out, l)
	}
	return out, nil
}

func decryptKind(err error) string {
	switch {
	case errors.Is(err, secure.ErrUnknownKey):
		return "unknown key"
	case errors.Is(err, secure.ErrMalformed):
		return "malformed"
	default:
		return "authentication failed"
	}
}

func (s *secureStore) Checkpoint(ctx context.Context, id, token string, cp []byte) error {
	sealed, err := s.seal(id, "checkpoint", cp)
	if err != nil {
		return fmt.Errorf("dlq: sealing checkpoint: %w", err)
	}
	return s.inner.Checkpoint(ctx, id, token, sealed)
}

func (s *secureStore) Ack(ctx context.Context, id, token string) error {
	return s.inner.Ack(ctx, id, token)
}

func (s *secureStore) Nack(ctx context.Context, id, token string, o NackOptions) error {
	o.Err = s.o.Redactor.String(o.Err)
	return s.inner.Nack(ctx, id, token, o)
}

func (s *secureStore) Release(ctx context.Context, id, token string) error {
	return s.inner.Release(ctx, id, token)
}

func (s *secureStore) Park(ctx context.Context, id, token, reason string) error {
	return s.inner.Park(ctx, id, token, s.o.Redactor.String(reason))
}

func (s *secureStore) Requeue(ctx context.Context, id string) error  { return s.inner.Requeue(ctx, id) }
func (s *secureStore) Discard(ctx context.Context, id string) error  { return s.inner.Discard(ctx, id) }
func (s *secureStore) Stats(ctx context.Context) (StoreStats, error) { return s.inner.Stats(ctx) }
func (s *secureStore) Close() error                                  { return s.inner.Close() }
