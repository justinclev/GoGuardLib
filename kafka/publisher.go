package kafka

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/secure"
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
	// HeaderSealed is set to "1" when the key, value and header values were sealed
	// with PublisherConfig.Encryptor. Open them with OpenDeadLetter.
	HeaderSealed = "x-goguard-sealed"
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
	// Encryptor, when set, seals the key, the value and every header value before
	// they leave the process, so the dead-letter topic never holds message data in
	// clear (header names and the x-goguard-* metadata stay readable). Readers open
	// them with OpenDeadLetter and the same keys. Without it the mirror carries the
	// payload exactly as received, which is right only if the topic is as trusted
	// as the store.
	Encryptor secure.Encryptor
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
	key, value := rec.Key, rec.Value
	headers := make([]dlq.Header, 0, len(rec.Headers)+8)
	for _, h := range rec.Headers {
		if !isMirrorHeader(h.Key) {
			headers = append(headers, h)
		}
	}
	if enc := p.cfg.Encryptor; enc != nil {
		var err error
		if key, err = sealField(enc, rec.ID, "key", key); err != nil {
			return err
		}
		if value, err = sealField(enc, rec.ID, "value", value); err != nil {
			return err
		}
		for i := range headers {
			if headers[i].Value, err = sealField(enc, rec.ID, headerField(i), headers[i].Value); err != nil {
				return err
			}
		}
		headers = append(headers, dlq.Header{Key: HeaderSealed, Value: []byte("1")})
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
	return p.cfg.Producer.Produce(ctx, topic, key, value, headers)
}

func isMirrorHeader(name string) bool { return strings.HasPrefix(strings.ToLower(name), "x-goguard-") }

func headerField(i int) string { return "header:" + strconv.Itoa(i) }

// mirrorAAD binds sealed data to the record and the field it came from, so it
// cannot be moved to another message.
func mirrorAAD(recordID, field string) []byte {
	return []byte("goguard/kafka-mirror/v1\x00" + recordID + "\x00" + field)
}

func sealField(enc secure.Encryptor, id, field string, plain []byte) ([]byte, error) {
	if plain == nil {
		return nil, nil // absence (a tombstone) stays visible
	}
	return enc.Seal(plain, mirrorAAD(id, field))
}

// ErrNotSealed is returned by OpenDeadLetter for a message that was not sealed.
var ErrNotSealed = errors.New("kafka: dead-letter message is not sealed")

// OpenDeadLetter reverses PublisherConfig.Encryptor for a message consumed from a
// dead-letter topic: it returns the original key, value and headers (the
// x-goguard-* metadata is dropped). It fails with ErrNotSealed if the message
// carries no HeaderSealed marker, and with the encryptor's error if the data was
// tampered with, moved from another message, or sealed with an unknown key.
func OpenDeadLetter(enc secure.Encryptor, key, value []byte, headers []dlq.Header) (k, v []byte, hs []dlq.Header, err error) {
	var id string
	sealed := false
	for _, h := range headers {
		switch h.Key {
		case HeaderRecordID:
			id = string(h.Value)
		case HeaderSealed:
			sealed = string(h.Value) == "1"
		}
	}
	if !sealed || id == "" {
		return nil, nil, nil, ErrNotSealed
	}
	open := func(field string, b []byte) ([]byte, error) {
		if b == nil {
			return nil, nil
		}
		return enc.Open(b, mirrorAAD(id, field))
	}
	if k, err = open("key", key); err != nil {
		return nil, nil, nil, err
	}
	if v, err = open("value", value); err != nil {
		return nil, nil, nil, err
	}
	i := 0
	for _, h := range headers {
		if isMirrorHeader(h.Key) {
			continue
		}
		pv, err := open(headerField(i), h.Value)
		if err != nil {
			return nil, nil, nil, err
		}
		hs = append(hs, dlq.Header{Key: h.Key, Value: pv})
		i++
	}
	return k, v, hs, nil
}
