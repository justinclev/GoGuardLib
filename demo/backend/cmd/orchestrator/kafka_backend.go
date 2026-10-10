package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/justinclev/GoGuardLib/demo/backend/internal/simkafka"
	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/kafka/confluent"
)

// kafkaBackend connects to the real broker when KAFKA_BROKERS is set, and falls
// back to an in-memory topic otherwise so the demo also runs without Kafka. The
// consumer under demonstration is the same either way.
func kafkaBackend(ctx context.Context, brokers string, topics []string) (kafka.Client, kafka.Producer, string, func(), error) {
	if brokers == "" {
		b := simkafka.New(topics, 3)
		return b.Client(), b, "sim", func() {}, nil
	}
	for _, topic := range topics {
		if err := ensureTopic(ctx, brokers, topic, 3); err != nil {
			return nil, nil, "", nil, err
		}
	}
	client, err := confluent.NewClient(ck.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           "goguard-demo",
		"auto.offset.reset":  "earliest",
		"session.timeout.ms": 10000,
	}, topics, confluent.WithErrorHandler(func(err error) {
		fmt.Fprintln(os.Stderr, "kafka:", err)
	}))
	if err != nil {
		return nil, nil, "", nil, err
	}
	prod, err := confluent.NewProducer(ck.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		_ = client.Close()
		return nil, nil, "", nil, err
	}
	return client, prod, "kafka", func() { prod.Close(); _ = client.Close() }, nil
}

// ensureTopic creates the topic, waiting for the broker to come up.
func ensureTopic(ctx context.Context, brokers, topic string, partitions int) error {
	deadline := time.Now().Add(90 * time.Second)
	var last error
	for time.Now().Before(deadline) && ctx.Err() == nil {
		adm, err := ck.NewAdminClient(&ck.ConfigMap{"bootstrap.servers": brokers})
		if err == nil {
			c, cancel := context.WithTimeout(ctx, 10*time.Second)
			res, cerr := adm.CreateTopics(c, []ck.TopicSpecification{{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1}})
			cancel()
			adm.Close()
			err = cerr
			if err == nil {
				for _, r := range res {
					if r.Error.Code() != ck.ErrNoError && r.Error.Code() != ck.ErrTopicAlreadyExists {
						err = r.Error
					}
				}
			}
			if err == nil {
				return nil
			}
		}
		last = err
		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}
	if last == nil {
		last = errors.New("cancelled")
	}
	return fmt.Errorf("creating topic %q on %s: %w", topic, strings.TrimSpace(brokers), last)
}
