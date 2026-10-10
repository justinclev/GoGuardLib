package kafka_test

import (
	"context"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/kafka/confluent"
	"github.com/justinclev/GoGuardLib/pipeline"
)

// The same checkout service as example_full_test.go, in the short form. Everything the long form
// writes by hand is still available: a step with Run instead of HTTP, your own Breaker, a
// retry.Policy, Secure instead of EncryptionKey. It only has to compile.
func runCheckoutShort(ctx context.Context, brokers, dataDir string, key []byte, usersURL, inventoryURL, paymentsURL string) error {
	checkout, err := pipeline.New("checkout", "v1", []pipeline.Step{
		{Name: "fetch-user", HealthURL: usersURL + "/health", Retries: 2,
			HTTP: pipeline.Get(usersURL + "/account/{key}"), SaveAs: "account"},
		{Name: "reserve", HealthURL: inventoryURL + "/health",
			HTTP: pipeline.Post(inventoryURL + "/reserve")},
		{Name: "charge", HealthURL: paymentsURL + "/health",
			HTTP: pipeline.Post(paymentsURL + "/charge/{account.id}")},
	})
	if err != nil {
		return err
	}
	return kafka.Run(ctx, kafka.ServiceConfig{
		NewClient:     confluent.NewClientFunc(ck.ConfigMap{"bootstrap.servers": brokers, "group.id": "checkout-service"}),
		DataDir:       dataDir,
		EncryptionKey: key,
		Pipelines:     []kafka.Binding{{Topic: "checkouts", Pipeline: checkout}},
	})
}

func Example_short() {
	_ = runCheckoutShort
}
