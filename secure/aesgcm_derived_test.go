package secure_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/justinclev/GoGuardLib/secure"
)

func derivedKey(id string, b byte) secure.Key {
	return secure.Key{ID: id, Material: bytes.Repeat([]byte{b}, secure.KeySize)}
}

func TestDerivedKeysRoundTripAndBindContext(t *testing.T) {
	e, err := secure.NewAESGCMDerived(derivedKey("k1", 1))
	if err != nil {
		t.Fatal(err)
	}
	a, _ := e.Seal([]byte("hello"), []byte("ctx"))
	b, _ := e.Seal([]byte("hello"), []byte("ctx"))
	if bytes.Equal(a, b) {
		t.Fatal("two seals of the same data were identical")
	}
	if a[0] != 2 {
		t.Fatalf("format byte = %d, want 2 (derived)", a[0])
	}
	got, err := e.Open(a, []byte("ctx"))
	if err != nil || string(got) != "hello" {
		t.Fatalf("Open = %q, %v", got, err)
	}
	if _, err := e.Open(a, []byte("other")); !errors.Is(err, secure.ErrDecrypt) {
		t.Fatalf("wrong aad: %v", err)
	}
	a[len(a)-1] ^= 1
	if _, err := e.Open(a, []byte("ctx")); !errors.Is(err, secure.ErrDecrypt) {
		t.Fatalf("tampered: %v", err)
	}
}

// Enabling derived keys must not strand data sealed before, and a plain
// encryptor (this release) must read what a derived one wrote, so a rollout can
// deploy readers first.
func TestDerivedAndPlainEncryptorsReadEachOthersData(t *testing.T) {
	k := derivedKey("k1", 1)
	plain, _ := secure.NewAESGCM(k)
	derived, _ := secure.NewAESGCMDerived(k)

	old, _ := plain.Seal([]byte("old"), nil)
	if got, err := derived.Open(old, nil); err != nil || string(got) != "old" {
		t.Fatalf("derived cannot open v1 data: %q %v", got, err)
	}
	fresh, _ := derived.Seal([]byte("new"), nil)
	if got, err := plain.Open(fresh, nil); err != nil || string(got) != "new" {
		t.Fatalf("plain cannot open v2 data: %q %v", got, err)
	}
}

func TestDerivedKeysRotateLikePlainOnes(t *testing.T) {
	k1, k2 := derivedKey("k1", 1), derivedKey("k2", 2)
	e1, _ := secure.NewAESGCMDerived(k1)
	sealed, _ := e1.Seal([]byte("x"), nil)
	e2, _ := secure.NewAESGCMDerived(k2, k1)
	if got, err := e2.Open(sealed, nil); err != nil || string(got) != "x" {
		t.Fatalf("retired key: %q %v", got, err)
	}
	only2, _ := secure.NewAESGCMDerived(k2)
	if _, err := only2.Open(sealed, nil); !errors.Is(err, secure.ErrUnknownKey) {
		t.Fatalf("unknown key: %v", err)
	}
}
