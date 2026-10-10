package secure

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
)

// MinHMACKeySize is the shortest accepted signing key.
const MinHMACKeySize = 32

// ErrBadSignature reports a signature that does not match the data.
var ErrBadSignature = errors.New("secure: signature mismatch")

// Signer authenticates data so tampering can be detected, for stores that keep
// records unencrypted. Encrypted data is already authenticated by AES-GCM.
type Signer interface {
	Sign(data []byte) []byte
	Verify(data, signature []byte) error
}

// HMACSigner signs with HMAC-SHA256. Signatures carry the key ID, so keys can be
// rotated like encryption keys.
type HMACSigner struct {
	activeID string
	keys     map[string][]byte
}

// NewHMAC returns a signer that signs with active and verifies with active or any
// retired key. Key material is copied.
func NewHMAC(active Key, retired ...Key) (*HMACSigner, error) {
	s := &HMACSigner{activeID: active.ID, keys: make(map[string][]byte, 1+len(retired))}
	for _, k := range append([]Key{active}, retired...) {
		if err := k.validate(MinHMACKeySize, false); err != nil {
			return nil, err
		}
		if _, dup := s.keys[k.ID]; dup {
			return nil, fmt.Errorf("%w: duplicate key id %q", ErrInvalidKey, k.ID)
		}
		s.keys[k.ID] = append([]byte(nil), k.Material...)
	}
	return s, nil
}

func (s *HMACSigner) mac(key []byte, h, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(h)
	m.Write(data)
	return m.Sum(nil)
}

// Sign returns header | mac.
func (s *HMACSigner) Sign(data []byte) []byte {
	h := header(s.activeID)
	return append(h, s.mac(s.keys[s.activeID], h, data)...)
}

// Verify checks signature against data in constant time.
func (s *HMACSigner) Verify(data, signature []byte) error {
	if len(signature) < 2 || signature[0] != formatV1 {
		return ErrMalformed
	}
	idLen := int(signature[1])
	if len(signature) != 2+idLen+sha256.Size {
		return ErrMalformed
	}
	key, ok := s.keys[string(signature[2:2+idLen])]
	if !ok {
		return ErrUnknownKey
	}
	want := s.mac(key, signature[:2+idLen], data)
	if !hmac.Equal(want, signature[2+idLen:]) {
		return ErrBadSignature
	}
	return nil
}
