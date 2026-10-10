package breaker_test

import (
	"testing"

	"github.com/justinclev/GoGuardLib/breaker"
)

func TestNewHTTPBuildsABreakerOrReportsABadURL(t *testing.T) {
	b, err := breaker.NewHTTP("payments", "http://payments.internal/health")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if b.Name() != "payments" {
		t.Fatalf("name %q", b.Name())
	}
	if _, err := breaker.NewHTTP("payments", "/health"); err == nil {
		t.Fatal("a relative URL must be an error")
	}
}
