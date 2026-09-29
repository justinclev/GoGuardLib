package confluent_test

import (
	"context"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/kafka/confluent"
	"github.com/justinclev/GoGuardLib/pipeline"
)

// A consumer that pauses while a dependency is down, stores what cannot finish, and
// commits an offset only when its message is safe.
func Example() {
	client, err := confluent.NewClient(ck.ConfigMap{
		"bootstrap.servers": "kafka:9092",
		"group.id":          "orders-service",
		"auto.offset.reset": "earliest",
	}, []string{"orders"})
	if err != nil {
		return
	}
	defer client.Close()

	var ordersPipeline *pipeline.Pipeline // built with pipeline.New; its steps use breakers
	store, err := dlq.OpenWAL("/var/lib/orders/dlq", dlq.WALOptions{})
	if err != nil {
		return
	}
	defer store.Close()

	producer, err := confluent.NewProducer(ck.ConfigMap{"bootstrap.servers": "kafka:9092"})
	if err != nil {
		return
	}
	defer producer.Close()
	mirror, _ := kafka.NewDLQPublisher(kafka.PublisherConfig{Producer: producer})

	consumer, err := kafka.NewConsumer(kafka.Config{
		Client:   client,
		Store:    store,
		Bindings: []kafka.Binding{{Topic: "orders", Pipeline: ordersPipeline}},
		Mirror:   mirror,
	})
	if err != nil {
		return
	}
	_ = consumer.Run(context.Background())
}
