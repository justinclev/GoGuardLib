// Package secure protects data at rest and in diagnostics: authenticated
// encryption with key rotation, message signing, and redaction of secrets from
// text before it is stored or emitted.
package secure

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
)

const (
	// KeySize is the required length of an encryption key in bytes (AES-256).
	KeySize = 32

	formatV1 = 1 // one key, random nonce per message
	formatV2 = 2 // a fresh key per message derived with HKDF from the configured key
	saltSize = 16
	maxKeyID = 255
)

var (
	ErrInvalidKey = errors.New("secure: invalid key")
	ErrUnknownKey = errors.New("secure: unknown key id")
	ErrMalformed  = errors.New("secure: malformed input")
	// ErrDecrypt is deliberately uninformative: it covers a wrong key, a wrong
	// context and tampering alike, so it gives an attacker nothing to probe.
	ErrDecrypt = errors.New("secure: decryption failed")
)

// Encryptor seals and opens data. aad is authenticated but not encrypted and
// must be identical on Seal and Open; bind it to the record the data belongs to
// so ciphertext cannot be moved between records.
//
// Implement it to delegate to a KMS or HSM.
type Encryptor interface {
	Seal(plaintext, aad []byte) ([]byte, error)
	Open(sealed, aad []byte) ([]byte, error)
}

// Key is a named key. Names appear in sealed data so the right key can be
// chosen when opening, which is what makes rotation possible.
type Key struct {
	ID       string
	Material []byte
}

func (k Key) validate(minLen int, exact bool) error {
	if k.ID == "" || len(k.ID) > maxKeyID {
		return fmt.Errorf("%w: id must be 1 to %d bytes", ErrInvalidKey, maxKeyID)
	}
	if exact && len(k.Material) != minLen || !exact && len(k.Material) < minLen {
		return fmt.Errorf("%w: key %q must be %d bytes", ErrInvalidKey, k.ID, minLen)
	}
	return nil
}

// AESGCM is an Encryptor using AES-256-GCM with a fresh random nonce per Seal.
//
// Sealed layout: version | len(keyID) | keyID | nonce | ciphertext+tag. The
// header is authenticated together with aad.
//
// Random 96-bit nonces are safe for roughly 2^32 messages per key; rotate keys
// well before that. Rotation: create a new AESGCM with the new key active and
// the previous keys retired; data sealed under retired keys still opens.
//
// With NewAESGCMDerived each message is sealed under its own key, derived from the
// configured key and a random salt (format 2), which removes the message-count
// limit. Both formats open with either constructor.
type AESGCM struct {
	activeID string
	aeads    map[string]cipher.AEAD
	masters  map[string][]byte // key material, kept for derivation
	derive   bool
}

// NewAESGCMDerived is NewAESGCM that seals every message under a key derived per
// message (HKDF-SHA256 over the configured key and a random 16-byte salt). Random
// nonces under one key are safe only for about 2^32 messages; a dead-letter store
// that runs for years can approach that, and deriving a key per message removes
// the bound. Data written this way needs this release or later to be read, so
// deploy readers before enabling it; data written earlier still opens.
func NewAESGCMDerived(active Key, retired ...Key) (*AESGCM, error) {
	e, err := NewAESGCM(active, retired...)
	if err != nil {
		return nil, err
	}
	e.derive = true
	return e, nil
}

// NewAESGCM returns an encryptor that seals with active and opens with active or
// any retired key. Key material is copied.
func NewAESGCM(active Key, retired ...Key) (*AESGCM, error) {
	e := &AESGCM{activeID: active.ID, aeads: make(map[string]cipher.AEAD, 1+len(retired)), masters: make(map[string][]byte, 1+len(retired))}
	for _, k := range append([]Key{active}, retired...) {
		if err := k.validate(KeySize, true); err != nil {
			return nil, err
		}
		if _, dup := e.aeads[k.ID]; dup {
			return nil, fmt.Errorf("%w: duplicate key id %q", ErrInvalidKey, k.ID)
		}
		block, err := aes.NewCipher(k.Material)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidKey, err)
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidKey, err)
		}
		e.aeads[k.ID] = aead
		e.masters[k.ID] = append([]byte(nil), k.Material...)
	}
	return e, nil
}

// ActiveKeyID returns the ID of the key used to seal.
func (e *AESGCM) ActiveKeyID() string { return e.activeID }

func header(keyID string) []byte { return headerV(formatV1, keyID) }

func headerV(version byte, keyID string) []byte {
	h := make([]byte, 0, 2+len(keyID))
	h = append(h, version, byte(len(keyID)))
	return append(h, keyID...)
}

func withHeader(h, aad []byte) []byte {
	out := make([]byte, 0, len(h)+len(aad))
	out = append(out, h...)
	return append(out, aad...)
}

// derivedAEAD returns the AEAD for one message under a key derived from master.
func derivedAEAD(master, salt []byte) (cipher.AEAD, error) {
	k, err := hkdf.Key(sha256.New, master, salt, "goguard/secure/v2/aes256gcm", KeySize)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(k)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// Seal encrypts plaintext under the active key.
func (e *AESGCM) Seal(plaintext, aad []byte) ([]byte, error) {
	version := byte(formatV1)
	aead := e.aeads[e.activeID]
	var salt []byte
	if e.derive {
		version = formatV2
		salt = make([]byte, saltSize)
		if _, err := rand.Read(salt); err != nil {
			return nil, fmt.Errorf("secure: reading random salt: %w", err)
		}
		var err error
		if aead, err = derivedAEAD(e.masters[e.activeID], salt); err != nil {
			return nil, fmt.Errorf("secure: deriving key: %w", err)
		}
	}
	h := headerV(version, e.activeID)
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("secure: reading random nonce: %w", err)
	}
	out := make([]byte, 0, len(h)+len(salt)+len(nonce)+len(plaintext)+aead.Overhead())
	out = append(out, h...)
	out = append(out, salt...)
	out = append(out, nonce...)
	return aead.Seal(out, nonce, plaintext, withHeader(h, aad)), nil
}

// Open decrypts data produced by Seal under any known key.
func (e *AESGCM) Open(sealed, aad []byte) ([]byte, error) {
	if len(sealed) < 2 || (sealed[0] != formatV1 && sealed[0] != formatV2) {
		return nil, ErrMalformed
	}
	version := sealed[0]
	idLen := int(sealed[1])
	if len(sealed) < 2+idLen {
		return nil, ErrMalformed
	}
	h := sealed[:2+idLen]
	keyID := string(sealed[2 : 2+idLen])
	aead, ok := e.aeads[keyID]
	if !ok {
		return nil, ErrUnknownKey
	}
	rest := sealed[2+idLen:]
	if version == formatV2 {
		if len(rest) < saltSize {
			return nil, ErrMalformed
		}
		var err error
		if aead, err = derivedAEAD(e.masters[keyID], rest[:saltSize]); err != nil {
			return nil, ErrDecrypt
		}
		rest = rest[saltSize:]
	}
	if len(rest) < aead.NonceSize()+aead.Overhead() {
		return nil, ErrMalformed
	}
	nonce, ct := rest[:aead.NonceSize()], rest[aead.NonceSize():]
	pt, err := aead.Open(nil, nonce, ct, withHeader(h, aad))
	if err != nil {
		return nil, ErrDecrypt
	}
	if pt == nil {
		pt = []byte{} // an empty message opens as empty, never as nil
	}
	return pt, nil
}
