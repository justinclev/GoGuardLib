package secure_test

import (
	"strings"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/secure"
)

func TestHugeInputIsBoundedAndStillRedacted(t *testing.T) {
	r := secure.NewRedactor()
	huge := "password=hunter2 " + strings.Repeat("x", 50<<20)
	start := time.Now()
	out := r.String(huge)
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("redacting 50 MiB took %v: input must be bounded before the patterns run", d)
	}
	if strings.Contains(out, "hunter2") || !strings.Contains(out, secure.Redacted) {
		t.Fatalf("secret survived: %.60q", out)
	}
	if len(out) > 2048+len("…[truncated]") {
		t.Fatalf("output is %d bytes", len(out))
	}
}
