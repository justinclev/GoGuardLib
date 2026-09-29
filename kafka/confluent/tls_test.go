package confluent

import (
	"errors"
	"testing"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestRequireTLSRejectsPlaintextConfigurations(t *testing.T) {
	for _, proto := range []any{nil, "plaintext", "sasl_plaintext", "PLAINTEXT"} {
		cfg := ck.ConfigMap{"bootstrap.servers": "localhost:9092", "group.id": "g"}
		if proto != nil {
			cfg["security.protocol"] = proto
		}
		if _, err := NewClient(cfg, []string{"t"}, WithRequireTLS()); !errors.Is(err, ErrInsecureTransport) {
			t.Errorf("NewClient with security.protocol=%v: %v, want ErrInsecureTransport", proto, err)
		}
		if _, err := NewProducer(cfg, WithProducerRequireTLS()); !errors.Is(err, ErrInsecureTransport) {
			t.Errorf("NewProducer with security.protocol=%v: %v, want ErrInsecureTransport", proto, err)
		}
	}
}

func TestRequireTLSAcceptsTLSProtocols(t *testing.T) {
	for _, proto := range []string{"ssl", "SASL_SSL"} {
		if err := checkTLS(ck.ConfigMap{"security.protocol": proto}); err != nil {
			t.Errorf("%s rejected: %v", proto, err)
		}
	}
}
