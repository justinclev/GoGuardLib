package secure

import (
	"bytes"
	"crypto/rand"
	"errors"
	"regexp"
	"strings"
	"testing"
)

func key(t testing.TB, id string) Key {
	t.Helper()
	m := make([]byte, KeySize)
	if _, err := rand.Read(m); err != nil {
		t.Fatal(err)
	}
	return Key{ID: id, Material: m}
}

func TestSealOpenRoundTrip(t *testing.T) {
	e, err := NewAESGCM(key(t, "k1"))
	if err != nil {
		t.Fatal(err)
	}
	for _, pt := range [][]byte{[]byte("hello"), {}, bytes.Repeat([]byte{0xAB}, 1<<16)} {
		sealed, err := e.Seal(pt, []byte("record-1"))
		if err != nil {
			t.Fatal(err)
		}
		if len(pt) > 8 && bytes.Contains(sealed, pt) {
			t.Fatal("ciphertext contains the plaintext")
		}
		got, err := e.Open(sealed, []byte("record-1"))
		if err != nil || !bytes.Equal(got, pt) {
			t.Fatalf("Open = %q, %v", got, err)
		}
	}
	if e.ActiveKeyID() != "k1" {
		t.Fatal("ActiveKeyID")
	}
}

func TestEmptyPlaintextOpensAsEmptyNotNil(t *testing.T) {
	e, _ := NewAESGCM(key(t, "k"))
	sealed, _ := e.Seal([]byte{}, nil)
	got, err := e.Open(sealed, nil)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("Open = %#v, %v; want a non-nil empty slice", got, err)
	}
}

func TestSealUsesFreshNonce(t *testing.T) {
	e, _ := NewAESGCM(key(t, "k"))
	a, _ := e.Seal([]byte("same"), nil)
	b, _ := e.Seal([]byte("same"), nil)
	if bytes.Equal(a, b) {
		t.Fatal("two seals of the same plaintext were identical: nonce reuse")
	}
}

func TestOpenRejectsTamperingAndWrongContext(t *testing.T) {
	e, _ := NewAESGCM(key(t, "k1"))
	sealed, _ := e.Seal([]byte("secret"), []byte("rec-1"))

	if _, err := e.Open(sealed, []byte("rec-2")); !errors.Is(err, ErrDecrypt) {
		t.Fatalf("wrong aad: err = %v, want ErrDecrypt", err)
	}
	// Flip every single byte: nothing may decrypt.
	for i := range sealed {
		mut := append([]byte(nil), sealed...)
		mut[i] ^= 0x01
		if _, err := e.Open(mut, []byte("rec-1")); err == nil {
			t.Fatalf("tampering with byte %d went undetected", i)
		}
	}
}

func TestOpenMalformedInputs(t *testing.T) {
	e, _ := NewAESGCM(key(t, "k1"))
	for name, in := range map[string][]byte{
		"nil":            nil,
		"short":          {1},
		"bad version":    {9, 2, 'k', '1'},
		"truncated id":   {1, 5, 'k'},
		"truncated body": append(header("k1"), 1, 2, 3),
	} {
		if _, err := e.Open(in, nil); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: err = %v, want ErrMalformed", name, err)
		}
	}
	unknown, _ := mustAES(t, key(t, "other")).Seal([]byte("x"), nil)
	if _, err := e.Open(unknown, nil); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("err = %v, want ErrUnknownKey", err)
	}
}

func mustAES(t testing.TB, active Key, retired ...Key) *AESGCM {
	t.Helper()
	e, err := NewAESGCM(active, retired...)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestKeyRotation(t *testing.T) {
	k1, k2 := key(t, "2024"), key(t, "2025")
	old := mustAES(t, k1)
	sealedOld, _ := old.Seal([]byte("legacy"), []byte("a"))

	rotated := mustAES(t, k2, k1)
	if got, err := rotated.Open(sealedOld, []byte("a")); err != nil || string(got) != "legacy" {
		t.Fatalf("data under the retired key must still open: %q, %v", got, err)
	}
	sealedNew, _ := rotated.Seal([]byte("fresh"), []byte("a"))
	if sealedNew[2] == 0 || !bytes.Contains(sealedNew[:2+len("2025")], []byte("2025")) {
		t.Fatal("new data must be sealed under the active key")
	}
	if _, err := old.Open(sealedNew, []byte("a")); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("old service must not open new-key data: %v", err)
	}

	// Swapping the key ID in the header must not let one key impersonate another.
	forged := append([]byte(nil), sealedOld...)
	copy(forged[2:], "2025")
	if _, err := rotated.Open(forged, []byte("a")); err == nil {
		t.Fatal("forged key id was accepted")
	}
}

func TestNewAESGCMValidatesKeys(t *testing.T) {
	good := key(t, "ok")
	bad := map[string][]Key{
		"empty id":   {{ID: "", Material: good.Material}},
		"short key":  {{ID: "s", Material: []byte("short")}},
		"long id":    {{ID: strings.Repeat("x", 256), Material: good.Material}},
		"duplicates": {good, good},
	}
	for name, ks := range bad {
		if _, err := NewAESGCM(ks[0], ks[1:]...); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("%s: err = %v, want ErrInvalidKey", name, err)
		}
	}
}

func TestKeyMaterialIsCopiedOnConstruction(t *testing.T) {
	k := key(t, "k")
	e := mustAES(t, k)
	sealed, _ := e.Seal([]byte("x"), nil)
	for i := range k.Material {
		k.Material[i] = 0 // caller wipes its copy
	}
	if _, err := e.Open(sealed, nil); err != nil {
		t.Fatalf("encryptor depends on the caller's key slice: %v", err)
	}
}

func TestHMACSignVerifyAndRotation(t *testing.T) {
	k1, k2 := key(t, "a"), key(t, "b")
	s1, err := NewHMAC(k1)
	if err != nil {
		t.Fatal(err)
	}
	sig := s1.Sign([]byte("data"))
	if err := s1.Verify([]byte("data"), sig); err != nil {
		t.Fatal(err)
	}
	if err := s1.Verify([]byte("dataX"), sig); !errors.Is(err, ErrBadSignature) {
		t.Fatalf("modified data: %v", err)
	}
	for i := range sig {
		mut := append([]byte(nil), sig...)
		mut[i] ^= 1
		if s1.Verify([]byte("data"), mut) == nil {
			t.Fatalf("tampering with signature byte %d went undetected", i)
		}
	}

	s2, _ := NewHMAC(k2, k1)
	if err := s2.Verify([]byte("data"), sig); err != nil {
		t.Fatalf("retired key must still verify: %v", err)
	}
	if err := s1.Verify([]byte("data"), s2.Sign([]byte("data"))); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("unknown key: %v", err)
	}
	if err := s1.Verify(nil, nil); !errors.Is(err, ErrMalformed) {
		t.Fatalf("empty signature: %v", err)
	}
	if err := s1.Verify(nil, sig[:len(sig)-1]); !errors.Is(err, ErrMalformed) {
		t.Fatalf("truncated signature: %v", err)
	}
}

func TestNewHMACValidatesKeys(t *testing.T) {
	if _, err := NewHMAC(Key{ID: "k", Material: []byte("too short")}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("err = %v", err)
	}
	k := key(t, "k")
	if _, err := NewHMAC(k, k); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("duplicate id: %v", err)
	}
}

func TestRedactorString(t *testing.T) {
	r := NewRedactor()
	jwt := "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"
	cases := []struct{ name, in, mustNotContain string }{
		{"bearer", "upstream said: Bearer abcdefghijklmnop123 rejected", "abcdefghijklmnop123"},
		{"basic", "Authorization: Basic dXNlcjpwYXNzd29yZA==", "dXNlcjpwYXNzd29yZA"},
		{"jwt", "got token " + jwt + " from idp", jwt},
		{"url userinfo", "dial postgres://admin:hunter2secret@db.internal:5432/app failed", "hunter2secret"},
		{"password kv", "connect failed password=s3cr3tValue host=db", "s3cr3tValue"},
		{"json field", `{"user":"bob","password":"p4ssw0rd!x","ok":true}`, "p4ssw0rd!x"},
		{"api key header text", "x-api-key: AKIAIOSFODNN7EXAMPLE", "AKIAIOSFODNN7EXAMPLE"},
		{"kafka sasl", "sasl.password=kafkaSecret99 sasl.username=svc", "kafkaSecret99"},
		{"client secret", "client_secret: 9f8e7d6c5b4a", "9f8e7d6c5b4a"},
		{"query string", "GET /cb?code=1&access_key=AK123456&x=2", "AK123456"},
	}
	for _, c := range cases {
		got := r.String(c.in)
		if strings.Contains(got, c.mustNotContain) {
			t.Errorf("%s: secret survived: %q", c.name, got)
		}
		if !strings.Contains(got, Redacted) {
			t.Errorf("%s: no redaction marker in %q", c.name, got)
		}
	}
}

func TestRedactorKeepsUsefulContext(t *testing.T) {
	r := NewRedactor()
	got := r.String("dial postgres://admin:hunter2secret@db.internal:5432/app failed: password=abc123def")
	for _, want := range []string{"postgres://admin:", "@db.internal:5432/app", "password=", "failed"} {
		if !strings.Contains(got, want) {
			t.Errorf("lost %q in %q", want, got)
		}
	}
	plain := "connection refused to 10.0.0.5:8443 after 3 attempts"
	if r.String(plain) != plain {
		t.Errorf("innocuous text changed: %q", r.String(plain))
	}
	if r.String("the author reviewed it") != "the author reviewed it" {
		t.Error("'auth' inside another word must not trigger redaction")
	}
}

func TestRedactorHeaders(t *testing.T) {
	r := NewRedactor(WithHeaders("x-tenant-secret"))
	in := map[string][]string{
		"authorization":   {"Bearer abc"},
		"Cookie":          {"a=1", "b=2"},
		"X-Tenant-Secret": {"shh"},
		"Content-Type":    {"application/json"},
		"X-Note":          {"password=hunter2hunter2"},
	}
	out := r.Headers(in)
	for _, name := range []string{"authorization", "Cookie", "X-Tenant-Secret"} {
		for _, v := range out[name] {
			if v != Redacted {
				t.Errorf("%s value %q not redacted", name, v)
			}
		}
	}
	if len(out["Cookie"]) != 2 {
		t.Error("value count must be preserved")
	}
	if out["Content-Type"][0] != "application/json" {
		t.Error("harmless header changed")
	}
	if strings.Contains(out["X-Note"][0], "hunter2hunter2") {
		t.Error("secret inside a harmless header survived")
	}
	if in["authorization"][0] != "Bearer abc" {
		t.Error("input map was modified")
	}
	if r.Headers(nil) != nil || !r.IsSensitiveHeader("SET-COOKIE") || r.IsSensitiveHeader("Accept") {
		t.Error("IsSensitiveHeader / nil handling")
	}
}

func TestRedactorErrorAndTruncation(t *testing.T) {
	r := NewRedactor(WithMaxLength(20))
	if r.Error(nil) != "" {
		t.Fatal("nil error")
	}
	got := r.Error(errors.New(strings.Repeat("é", 50))) // multi-byte characters
	if !strings.HasSuffix(got, "…[truncated]") || strings.ContainsRune(got, '�') {
		t.Fatalf("truncation broke a character or lost the marker: %q", got)
	}
	if NewRedactor(WithMaxLength(0)).String(strings.Repeat("a", 5000)) != strings.Repeat("a", 5000) {
		t.Fatal("WithMaxLength(0) must disable truncation")
	}
}

func TestRedactorCustomPatterns(t *testing.T) {
	r := NewRedactor(WithPatterns(regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`)))
	if got := r.String("ssn 123-45-6789 on file"); strings.Contains(got, "123-45-6789") {
		t.Fatalf("custom pattern ignored: %q", got)
	}
}

func TestRedactorIsLinearOnHostileInput(t *testing.T) {
	r := NewRedactor(WithMaxLength(0))
	hostile := strings.Repeat("password=", 20000) + strings.Repeat("a", 100000)
	_ = r.String(hostile) // RE2 guarantees linear time; this must simply finish
}
