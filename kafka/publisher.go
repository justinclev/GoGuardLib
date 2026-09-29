package kafka

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
)

// Producer sends one message and returns once the broker has acknowledged it. The
// confluent package provides an idempotent, acks=all implementation.
type Producer interface {
	Produce(ctx context.Context, topic string, key, value []byte, headers []dlq.Header) error
}

// Header names added to mirrored messages. Any incoming header with one of these
// names is dropped so a message cannot forge its own dead-letter metadata.
const (
	HeaderOriginalTopic     = "x-goguard-original-topic"
	HeaderOriginalPartition = "x-goguard-original-partition"
	HeaderOriginalOffset    = "x-goguard-original-offset"
	HeaderReason            = "x-goguard-reason"
	HeaderAttempts          = "x-goguard-attempts"
	HeaderRecordID          = "x-goguard-record-id"
	HeaderBlockedOn         = "x-goguard-blocked-on"
	HeaderParkedAt          = "x-goguard-parked-at"
)

// ErrNoDeadLetterTopic means a record has no source topic and the publisher has no
// fallback topic.
var ErrNoDeadLetterTopic = errors.New("kafka: no dead-letter topic for this record")

// PublisherConfig configures a DLQPublisher.
type PublisherConfig struct {
	// Producer sends the messages. Required.
	Producer Producer
	// Topic maps a message's source topic to its dead-letter topic. Default: the
	// source topic with ".dlq" appended.
	Topic func(sourceTopic string) string
	// FallbackTopic is used for records that did not come from Kafka.
	FallbackTopic string
}

// DLQPublisher copies parked records to a Kafka dead-letter topic so other tools
// can inspect or reprocess them. It is a mirror, not the source of truth: the
// record is already parked in the Store, so a publishing failure loses nothing.
// Its Publish method fits dlq.RedriveConfig.OnPark.
type DLQPublisher struct{ cfg PublisherConfig }

// NewDLQPublisher validates cfg.
func NewDLQPublisher(cfg PublisherConfig) (*DLQPublisher, error) {
	if cfg.Producer == nil {
		return nil, errors.New("kafka: PublisherConfig needs a Producer")
	}
	if cfg.Topic == nil {
		cfg.Topic = func(t string) string { return t + ".dlq" }
	}
	return &DLQPublisher{cfg: cfg}, nil
}

// Publish sends rec to its dead-letter topic with metadata headers. The original
// key, value and headers are preserved unchanged. reason should already be
// redacted (the redriver does that).
func (p *DLQPublisher) Publish(ctx context.Context, rec dlq.Record, reason string) error {
	topic := p.cfg.FallbackTopic
	if rec.Source.Kind == "kafka" && rec.Source.Name != "" {
		topic = p.cfg.Topic(rec.Source.Name)
	}
	if topic == "" {
		return ErrNoDeadLetterTopic
	}
	headers := make([]dlq.Header, 0, len(rec.Headers)+8)
	for _, h := range rec.Headers {
		if !strings.HasPrefix(strings.ToLower(h.Key), "x-goguard-") {
			headers = append(headers, h)
		}
	}
	add := func(k, v string) { headers = append(headers, dlq.Header{Key: k, Value: []byte(v)}) }
	if rec.Source.Kind == "kafka" {
		add(HeaderOriginalTopic, rec.Source.Name)
		add(HeaderOriginalPartition, strconv.Itoa(int(rec.Source.Partition)))
		add(HeaderOriginalOffset, strconv.FormatInt(rec.Source.Offset, 10))
	}
	add(HeaderReason, reason)
	add(HeaderAttempts, strconv.Itoa(rec.Attempts))
	add(HeaderRecordID, rec.ID)
	if rec.BlockedOn != "" {
		add(HeaderBlockedOn, rec.BlockedOn)
	}
	add(HeaderParkedAt, time.Now().UTC().Format(time.RFC3339))
	return p.cfg.Producer.Produce(ctx, topic, rec.Key, rec.Value, headers)
}
