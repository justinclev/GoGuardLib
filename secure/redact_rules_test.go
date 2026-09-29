package secure_test

import (
	"strings"
	"testing"

	"github.com/justinclev/GoGuardLib/secure"
)

// The credential-shaped values below are joined from pieces at run time so that no
// literal in this file looks like a real token to a secret scanner.
func join(parts ...string) string { return strings.Join(parts, "") }

// Every credential shape must not survive redaction: each case is the text that
// carries it and the secret that must be gone.
func TestRedactorCatchesCommonCredentialShapes(t *testing.T) {
	var (
		awsID    = join("AK", "IA", "IOSFODNN7", "EXAMPLE")
		awsTemp  = join("AS", "IA", "IOSFODNN7", "EXAMPLE")
		ghPat    = join("gh", "p_", "abcdefghijklmnopqrstuvwxyz", "0123456789")
		ghFine   = join("github", "_pat_", "11ABCDEFG0abcdefghijkl", "_mnopqrstuvwxyz0123456789")
		slack    = join("xo", "xb-", "123456789012", "-abcdefghijklmnop")
		google   = join("AI", "za", "SyA-1234567890abcdefghijklmnopqrstuv")
		stripe   = join("sk", "_live", "_", "abcdefghijklmnopqrstuvwx")
		pemBody  = "MIIEowIBAAKCAQEA1234567890abcdef"
		pemBegin = join("-----BEGIN ", "RSA PRIVATE", " KEY-----")
		pemEnd   = join("-----END ", "RSA PRIVATE", " KEY-----")
	)
	cases := []struct{ name, in, secret string }{
		{"access_token", `{"access_token":"ya29.a0AfH6SMBx1234567890abcdef"}`, "ya29.a0AfH6SMBx1234567890abcdef"},
		{"refresh_token", `refresh_token=1//0gXyZ1234567890abcdefghij`, "1//0gXyZ1234567890abcdefghij"},
		{"id_token", `id_token: abc123def456ghi789`, "abc123def456ghi789"},
		{"camelCase", `{"clientSecret":"s3cr3tvalue99","accessToken":"tok123456789"}`, "s3cr3tvalue99"},
		{"aws key id", "failed for " + awsID + " in us-east-1", awsID},
		{"aws temp id", "key " + awsTemp + " rejected", awsTemp},
		{"github pat", "clone https with " + ghPat + " failed", ghPat},
		{"github fine", ghFine, ghFine},
		{"slack", "posting with " + slack + " failed", slack},
		{"google key", "key=" + google, google},
		{"stripe", stripe, stripe},
		{"pem", pemBegin + "\n" + pemBody + "\nZZZZ\n" + pemEnd, pemBody},
		{"pem truncated", "err: " + pemBegin + "\n" + pemBody, pemBody},
	}
	r := secure.NewRedactor()
	for _, c := range cases {
		if out := r.String(c.in); strings.Contains(out, c.secret) {
			t.Errorf("%s: secret survived: %q", c.name, out)
		}
	}
}

// Ordinary words that merely contain a sensitive word are left alone.
func TestRedactorLeavesOrdinaryTextAlone(t *testing.T) {
	r := secure.NewRedactor()
	for _, in := range []string{"author=jane", "tokenizer=whitespace", "order 12345 shipped", "user authenticated ok"} {
		if out := r.String(in); out != in {
			t.Errorf("%q was changed to %q", in, out)
		}
	}
}

func FuzzRedactorNeverLeaksAKnownSecret(f *testing.F) {
	f.Add("x", "y")
	f.Add("\n", "\x00")
	r := secure.NewRedactor()
	secret := join("AK", "IA", "IOSFODNN7", "EXAMPLE")
	f.Fuzz(func(t *testing.T, pre, post string) {
		in := pre + " " + secret + " " + post
		if len(in) > 2048 { // beyond that the redactor truncates before matching
			return
		}
		if out := r.String(in); strings.Contains(out, secret) {
			t.Fatalf("secret survived in %q", out)
		}
	})
}
