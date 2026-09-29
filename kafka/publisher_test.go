package kafka_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/secure"
)

func TestPublisherKeepsThePayloadAndAddsMetadata(t *testing.T) {
	prod := &fakeProducer{}
	pub, err := kafka.NewDLQPublisher(kafka.PublisherConfig{Producer: prod})
	must(t, err)
	rec := dlq.Record{
		ID: "rec-1", Source: dlq.Source{Kind: "kafka", Name: "orders", Partition: 4, Offset: 99},
		Key: []byte("k"), Value: []byte("v"), Attempts: 3, BlockedOn: "payments",
		Headers: []dlq.Header{
			{Key: "trace", Value: []byte("abc")},
			{Key: kafka.HeaderReason, Value: []byte("forged by the message itself")},
			{Key: "X-GoGuard-Attempts", Value: []byte("999")},
		},
	}
	must(t, pub.Publish(context.Background(), rec, "gave up after 3 attempts"))

	m := prod.messages()[0]
	if m.Topic != "orders.dlq" || string(m.Key) != "k" || string(m.Value) != "v" {
		t.Fatalf("message = %+v", m)
	}
	want := map[string]string{
		"trace": "abc", kafka.HeaderOriginalTopic: "orders", kafka.HeaderOriginalPartition: "4", kafka.HeaderOriginalOffset: "99",
		kafka.HeaderReason: "gave up after 3 attempts", kafka.HeaderAttempts: "3", kafka.HeaderRecordID: "rec-1", kafka.HeaderBlockedOn: "payments",
	}
	for k, v := range want {
		if m.Headers[k] != v {
			t.Errorf("header %s = %q, want %q", k, m.Headers[k], v)
		}
	}
	if m.Headers[kafka.HeaderParkedAt] == "" {
		t.Error("no parked-at header")
	}
}

func TestPublisherTopicNaming(t *testing.T) {
	prod := &fakeProducer{}
	pub, _ := kafka.NewDLQPublisher(kafka.PublisherConfig{
		Producer: prod, Topic: func(s string) string { return "dead." + s }, FallbackTopic: "misc.dlq",
	})
	must(t, pub.Publish(context.Background(), dlq.Record{ID: "a", Source: dlq.Source{Kind: "kafka", Name: "orders"}}, "r"))
	must(t, pub.Publish(context.Background(), dlq.Record{ID: "b", Source: dlq.Source{Kind: "http", Name: "svc"}}, "r"))
	got := prod.messages()
	if got[0].Topic != "dead.orders" || got[1].Topic != "misc.dlq" {
		t.Fatalf("topics = %s, %s", got[0].Topic, got[1].Topic)
	}
	if _, ok := got[1].Headers[kafka.HeaderOriginalTopic]; ok {
		t.Fatal("a non-Kafka record must not claim an original topic")
	}

	noFallback, _ := kafka.NewDLQPublisher(kafka.PublisherConfig{Producer: prod})
	if err := noFallback.Publish(context.Background(), dlq.Record{ID: "c"}, "r"); !errors.Is(err, kafka.ErrNoDeadLetterTopic) {
		t.Fatalf("err = %v", err)
	}
	if _, err := kafka.NewDLQPublisher(kafka.PublisherConfig{}); err == nil {
		t.Fatal("a publisher without a producer must be rejected")
	}
}

func TestPublisherReportsProducerFailures(t *testing.T) {
	pub, _ := kafka.NewDLQPublisher(kafka.PublisherConfig{Producer: &fakeProducer{err: errBoom}})
	err := pub.Publish(context.Background(), dlq.Record{ID: "a", Source: dlq.Source{Kind: "kafka", Name: "t"}}, "r")
	if !errors.Is(err, errBoom) {
		t.Fatalf("err = %v", err)
	}
}

// Publish must fit dlq.RedriveConfig.OnPark so a redriver can mirror what it parks.
func TestPublishFitsTheRedriverHook(t *testing.T) {
	pub, _ := kafka.NewDLQPublisher(kafka.PublisherConfig{Producer: &fakeProducer{}})
	_ = dlq.RedriveConfig{OnPark: pub.Publish}
}

type headerProducer struct {
	key, value []byte
	headers    []dlq.Header
}

func (p *headerProducer) Produce(_ context.Context, _ string, key, value []byte, headers []dlq.Header) error {
	p.key, p.value, p.headers = key, value, headers
	return nil
}

func sealer(t *testing.T) secure.Encryptor {
	t.Helper()
	enc, err := secure.NewAESGCM(secure.Key{ID: "k1", Material: bytes.Repeat([]byte{7}, secure.KeySize)})
	must(t, err)
	return enc
}

func TestASealedMirrorNeverCarriesThePayloadInClear(t *testing.T) {
	enc := sealer(t)
	prod := &headerProducer{}
	pub, err := kafka.NewDLQPublisher(kafka.PublisherConfig{Producer: prod, Encryptor: enc})
	must(t, err)
	rec := dlq.Record{
		ID: "rec-1", Source: dlq.Source{Kind: "kafka", Name: "orders", Partition: 1, Offset: 5},
		Key: []byte("customer-77"), Value: []byte("card 4111-1111-1111-1111"),
		Headers: []dlq.Header{{Key: "trace", Value: []byte("t-secret")}, {Key: "tombstone-ish", Value: nil}},
	}
	must(t, pub.Publish(context.Background(), rec, "gave up"))

	all := append([]byte{}, prod.key...)
	all = append(all, prod.value...)
	for _, h := range prod.headers {
		all = append(all, h.Value...)
	}
	for _, secret := range []string{"customer-77", "4111", "t-secret"} {
		if bytes.Contains(all, []byte(secret)) {
			t.Errorf("%q is in the mirrored message in clear", secret)
		}
	}
	names := map[string]bool{}
	for _, h := range prod.headers {
		names[h.Key] = true
	}
	if !names["trace"] || !names[kafka.HeaderReason] || !names[kafka.HeaderSealed] {
		t.Errorf("header names and metadata must stay readable: %v", names)
	}

	k, v, hs, err := kafka.OpenDeadLetter(enc, prod.key, prod.value, prod.headers)
	must(t, err)
	if string(k) != "customer-77" || string(v) != "card 4111-1111-1111-1111" {
		t.Fatalf("opened key %q value %q", k, v)
	}
	if len(hs) != 2 || hs[0].Key != "trace" || string(hs[0].Value) != "t-secret" || hs[1].Value != nil {
		t.Fatalf("opened headers %+v", hs)
	}
}

func TestSealedMirrorDataCannotBeMovedBetweenMessages(t *testing.T) {
	enc := sealer(t)
	prod := &headerProducer{}
	pub, _ := kafka.NewDLQPublisher(kafka.PublisherConfig{Producer: prod, Encryptor: enc})
	must(t, pub.Publish(context.Background(), dlq.Record{ID: "a", Source: dlq.Source{Kind: "kafka", Name: "t"}, Value: []byte("from a")}, "r"))
	valueA, headersA := prod.value, prod.headers
	must(t, pub.Publish(context.Background(), dlq.Record{ID: "b", Source: dlq.Source{Kind: "kafka", Name: "t"}, Value: []byte("from b")}, "r"))
	// b's envelope around a's ciphertext must not open.
	if _, _, _, err := kafka.OpenDeadLetter(enc, prod.key, valueA, prod.headers); err == nil {
		t.Fatal("a value sealed for record a opened under record b")
	}
	if _, _, _, err := kafka.OpenDeadLetter(enc, nil, valueA, headersA); err != nil {
		t.Fatalf("a's own message failed to open: %v", err)
	}
}

func TestOpenDeadLetterRefusesWhatWasNotSealed(t *testing.T) {
	prod := &headerProducer{}
	pub, _ := kafka.NewDLQPublisher(kafka.PublisherConfig{Producer: prod})
	must(t, pub.Publish(context.Background(), dlq.Record{ID: "a", Source: dlq.Source{Kind: "kafka", Name: "t"}, Value: []byte("plain")}, "r"))
	if _, _, _, err := kafka.OpenDeadLetter(sealer(t), prod.key, prod.value, prod.headers); !errors.Is(err, kafka.ErrNotSealed) {
		t.Fatalf("got %v, want ErrNotSealed", err)
	}
}
