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

func TestClientHealthTracksAllBrokersDown(t *testing.T) {
	c := &Client{}
	if c.Health().AllBrokersDown {
		t.Fatal("a fresh client must not report down")
	}
	c.noteError(ck.NewError(ck.ErrTransport, "one broker failed", false))
	if h := c.Health(); h.AllBrokersDown || h.LastError == "" {
		t.Fatalf("a single broker error is not an outage: %+v", h)
	}
	c.noteError(ck.NewError(ck.ErrAllBrokersDown, "all down", false))
	h := c.Health()
	if !h.AllBrokersDown || h.Since.IsZero() {
		t.Fatalf("Health = %+v, want down since a time", h)
	}
	first := h.Since
	c.noteError(ck.NewError(ck.ErrAllBrokersDown, "still down", false))
	if c.Health().Since != first {
		t.Fatal("Since must not move while the outage continues")
	}
	c.noteReachable()
	if c.Health().AllBrokersDown {
		t.Fatal("still down after the cluster answered")
	}
}

func TestOffsetResetDefaultsToEarliestButRespectsTheCaller(t *testing.T) {
	cm := ck.ConfigMap{}
	defaultOffsetReset(cm)
	if cm["auto.offset.reset"] != "earliest" {
		t.Fatalf("default = %v, want earliest", cm["auto.offset.reset"])
	}
	cm = ck.ConfigMap{"auto.offset.reset": "latest"}
	defaultOffsetReset(cm)
	if cm["auto.offset.reset"] != "latest" {
		t.Fatalf("an explicit choice was overwritten: %v", cm["auto.offset.reset"])
	}
}
