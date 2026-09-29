package kafka_test

import (
	"context"
	"errors"
	"testing"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/kafka"
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
