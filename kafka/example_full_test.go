package kafka_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/health"
	"github.com/justinclev/GoGuardLib/kafka"
	"github.com/justinclev/GoGuardLib/kafka/confluent"
	"github.com/justinclev/GoGuardLib/pipeline"
	"github.com/justinclev/GoGuardLib/retry"
	"github.com/justinclev/GoGuardLib/secure"
)

// The full example: a topic whose messages go through three services, each behind its own
// breaker, with in-process retries on the read, data passed between the steps, and an encrypted
// log. It only has to compile.

type account struct {
	ID    string `json:"id"`
	Limit int    `json:"limit"`
}

// call sends one request and sorts the answer: success, temporary (retried) or permanent (parked).
func call(ctx context.Context, method, url, idemKey string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return nil, retry.Permanent(err)
	}
	req.Header.Set("Idempotency-Key", idemKey) // the same on every attempt, so a repeat is recognised
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return out, retry.FromHTTP(resp, nil)
}

// guard builds the breaker for one service: opens when half of the recent calls fail, ignores
// permanent errors (a declined card is not an outage), and asks the service when it is back.
func guard(name, baseURL string) *breaker.Breaker {
	return breaker.New(breaker.Config{
		Name: name, FailureThreshold: 0.5, MinSamples: 20, IsFailure: retry.IsOutage,
		Health: health.MustURL(baseURL + "/health"),
	})
}

func runCheckout(ctx context.Context, brokers, dataDir string, key []byte, usersURL, inventoryURL, paymentsURL string) error {
	users, inventory, payments := guard("users", usersURL), guard("inventory", inventoryURL), guard("payments", paymentsURL)
	defer users.Close()
	defer inventory.Close()
	defer payments.Close()

	checkout, err := pipeline.New("checkout", "v1", []pipeline.Step{
		{ // a read: safe to repeat, so retry it here before giving up on the message
			Name: "fetch-user", Breaker: users, Timeout: 3 * time.Second,
			Retry: retry.Policy{MaxRetries: 2, Backoff: retry.Jitter(retry.Exponential(100*time.Millisecond, time.Second), 0.5)},
			Run: func(ctx context.Context, x *pipeline.Exec) error {
				out, err := call(ctx, http.MethodGet, usersURL+"/account/"+string(x.Key), x.IdempotencyKey(), nil)
				if err != nil {
					return err
				}
				var a account
				if err := json.Unmarshal(out, &a); err != nil {
					return retry.Permanent(err)
				}
				return pipeline.SetJSON(x, "account", a) // kept with the message's progress
			},
		},
		{
			Name: "reserve", Breaker: inventory, Timeout: 5 * time.Second,
			Run: func(ctx context.Context, x *pipeline.Exec) error {
				_, err := call(ctx, http.MethodPost, inventoryURL+"/reserve", x.IdempotencyKey(), x.Value)
				return err
			},
		},
		{ // money moves here: no in-process retry, the redriver retries it, safely, from this step
			Name: "charge", Breaker: payments, Timeout: 5 * time.Second,
			Run: func(ctx context.Context, x *pipeline.Exec) error {
				a, err := pipeline.RequireJSON[account](x, "account") // from step 1, even after a restart
				if err != nil {
					return err
				}
				_, err = call(ctx, http.MethodPost, paymentsURL+"/charge/"+a.ID, x.IdempotencyKey(), x.Value)
				return err
			},
		},
	})
	if err != nil {
		return err
	}

	enc, err := secure.NewAESGCM(secure.Key{ID: "2026-10", Material: key})
	if err != nil {
		return err
	}
	return kafka.Run(ctx, kafka.ServiceConfig{
		NewClient: confluent.NewClientFunc(ck.ConfigMap{"bootstrap.servers": brokers, "group.id": "checkout-service", "auto.offset.reset": "earliest"}),
		DataDir:   dataDir,
		Secure:    &dlq.SecureOptions{Encryptor: enc}, // what is saved to disk is encrypted
		Pipelines: []kafka.Binding{{Topic: "checkouts", Pipeline: checkout}},
		Consumer:  kafka.Config{Workers: 4},
		Redriver:  dlq.RedriveConfig{Rate: 50, RampUp: 30 * time.Second, MaxAttempts: 10},
	})
}

func Example_full() {
	_ = runCheckout
}
