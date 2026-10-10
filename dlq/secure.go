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
	// OrderKeyPeppers is OrderKeyPepper with rotation: the first entry is used for
	// new keys, and the rest are earlier peppers whose hashes may still be in the
	// store. A message whose key is already in the store under an older pepper is
	// stored under that same hash, so it stays behind the earlier message and
	// ordering holds across the rotation. Drop an old pepper once dlq.Reseal (or
	// time) has cleared the records that used it. It takes precedence over
	// OrderKeyPepper.
	OrderKeyPeppers [][]byte
	// OnUndecryptable is called when a leased record cannot be decrypted, for
	// example because its key was removed. The record has already been parked. It
	// must not block, and receives no payload data.
	OnUndecryptable func(id string, err error)
}

type secureStore struct {
	inner   Store
	o       SecureOptions
	peppers [][]byte // the first is current; empty means order keys are stored in clear
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
	var peppers [][]byte
	for _, p := range o.OrderKeyPeppers {
		if len(p) == 0 {
			return nil, errors.New("dlq: an empty entry in SecureOptions.OrderKeyPeppers")
		}
		peppers = append(peppers, append([]byte(nil), p...))
	}
	if len(peppers) == 0 && len(o.OrderKeyPepper) > 0 {
		peppers = [][]byte{append([]byte(nil), o.OrderKeyPepper...)}
	}
	o.OrderKeyPepper, o.OrderKeyPeppers = nil, nil
	return &secureStore{inner: inner, o: o, peppers: peppers}, nil
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

func (s *secureStore) hashKey(pepper []byte, k string) string {
	m := hmac.New(sha256.New, pepper)
	m.Write([]byte(k))
	return hex.EncodeToString(m.Sum(nil))
}

// candidates lists every stored form k can have, current pepper first.
func (s *secureStore) candidates(k string) []string {
	out := make([]string, len(s.peppers))
	for i, p := range s.peppers {
		out[i] = s.hashKey(p, k)
	}
	return out
}

// orderKey is the form of k to store: the current pepper's hash, unless the store
// already holds records for k under an older pepper, in which case that hash, so
// the new message queues behind them.
func (s *secureStore) orderKey(ctx context.Context, k string) (string, error) {
	if k == "" || len(s.peppers) == 0 {
		return k, nil
	}
	current := s.hashKey(s.peppers[0], k)
	if len(s.peppers) == 1 {
		return current, nil
	}
	for _, h := range s.candidates(k) { // current first, then older peppers
		held, err := s.inner.HasOrderKey(ctx, h)
		if err != nil {
			return "", err
		}
		if held {
			return h, nil
		}
	}
	return current, nil
}

func headerField(i int) string { return fmt.Sprintf("header:%d", i) }

func (s *secureStore) sealRecord(ctx context.Context, r Record) (Record, error) {
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
	if r.OrderKey, err = s.orderKey(ctx, r.OrderKey); err != nil {
		return Record{}, err
	}
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
	sealed, err := s.sealRecord(ctx, r)
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

func (s *secureStore) Requeue(ctx context.Context, id string) error { return s.inner.Requeue(ctx, id) }

func (s *secureStore) RequeueWith(ctx context.Context, id string, o RequeueOptions) error {
	if o.ReplaceCheckpoint {
		sealed, err := s.seal(id, "checkpoint", o.Checkpoint)
		if err != nil {
			return fmt.Errorf("dlq: sealing checkpoint: %w", err)
		}
		o.Checkpoint = sealed
	}
	return s.inner.RequeueWith(ctx, id, o)
}

// Get decrypts the record. A record that cannot be decrypted (its key is gone, or
// it was tampered with) is reported as an error rather than returned as ciphertext.
func (s *secureStore) Get(ctx context.Context, id string) (Record, error) {
	rec, err := s.inner.Get(ctx, id)
	if err != nil {
		return Record{}, err
	}
	plain, err := s.openRecord(rec)
	if err != nil {
		return Record{}, fmt.Errorf("dlq: record %s cannot be decrypted (%s): %w", id, decryptKind(err), err)
	}
	return plain, nil
}

// Parked decrypts what it can. A record that cannot be decrypted is still listed,
// with its payload fields empty, so an operator can see that it exists and why it
// was parked; OnUndecryptable is told about it.
func (s *secureStore) Parked(ctx context.Context, q ParkedQuery) ([]Record, error) {
	recs, err := s.inner.Parked(ctx, q)
	if err != nil {
		return nil, err
	}
	for i, r := range recs {
		plain, err := s.openRecord(r)
		if err != nil {
			r.Key, r.Value, r.Checkpoint = nil, nil, nil
			for j := range r.Headers {
				r.Headers[j].Value = nil
			}
			recs[i] = r
			if s.o.OnUndecryptable != nil {
				s.o.OnUndecryptable(r.ID, err)
			}
			continue
		}
		recs[i] = plain
	}
	return recs, nil
}
func (s *secureStore) Discard(ctx context.Context, id string) error { return s.inner.Discard(ctx, id) }
func (s *secureStore) HasOrderKey(ctx context.Context, orderKey string) (bool, error) {
	if orderKey == "" || len(s.peppers) == 0 {
		return s.inner.HasOrderKey(ctx, orderKey)
	}
	for _, h := range s.candidates(orderKey) { // the stored key is a keyed hash, under any pepper in use
		held, err := s.inner.HasOrderKey(ctx, h)
		if err != nil || held {
			return held, err
		}
	}
	return false, nil
}
func (s *secureStore) Stats(ctx context.Context) (StoreStats, error) { return s.inner.Stats(ctx) }
func (s *secureStore) Close() error                                  { return s.inner.Close() }

// ResealReport says what Reseal did.
type ResealReport struct {
	Records int // copied to the destination
	Parked  int // of those, records that were parked
}

// Reseal moves every record of src into dst, re-encrypting its payload from one
// key set to another. Use it to retire an encryption key you cannot leave in
// service (a leaked key, a policy): rotation alone keeps old data readable under
// the retired key, which is what you are trying to end. Point dst at a new, empty
// store, check the result, then switch the service over and delete the old
// directory.
//
// from must be able to open what src holds; to seals what dst receives. dst is the
// raw store (an empty WALStore, say), not one wrapped by Secure. Fields other than
// the payload are copied as they are: an OrderKey keeps its stored (hashed) form,
// which cannot be recomputed under a new pepper, so retire a pepper by letting its
// records drain instead (see SecureOptions.OrderKeyPeppers). A record that was
// leased arrives pending, with its attempt count. Reseal stops at the first record
// it cannot open and reports it; nothing already written to dst is removed.
func Reseal(ctx context.Context, src *WALStore, dst Store, from, to secure.Encryptor) (ResealReport, error) {
	if from == nil || to == nil {
		return ResealReport{}, errors.New("dlq: Reseal needs both encryptors")
	}
	fs := &secureStore{o: SecureOptions{Encryptor: from, Redactor: secure.NewRedactor()}}
	ts := &secureStore{o: SecureOptions{Encryptor: to, Redactor: secure.NewRedactor()}}
	var rep ResealReport
	err := src.Scan(ctx, func(r Record) error {
		plain, err := fs.openRecord(r)
		if err != nil {
			return fmt.Errorf("dlq: record %s cannot be opened with the old keys (%s): %w", r.ID, decryptKind(err), err)
		}
		// Seal with the new keys, keeping OrderKey and LastError exactly as stored.
		orderKey, lastErr := r.OrderKey, r.LastError
		out, err := ts.sealRecord(ctx, plain)
		if err != nil {
			return fmt.Errorf("dlq: sealing record %s: %w", r.ID, err)
		}
		out.OrderKey, out.LastError = orderKey, lastErr
		if out.State == Leased {
			out.State = Pending
		}
		if err := dst.Append(ctx, out); err != nil {
			return fmt.Errorf("dlq: writing record %s: %w", r.ID, err)
		}
		rep.Records++
		if out.State == Parked {
			rep.Parked++
		}
		return nil
	})
	return rep, err
}
