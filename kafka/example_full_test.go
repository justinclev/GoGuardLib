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

// The full example. Every message on the "checkouts" topic goes through three services:
//
//	1. users      look up the customer         (a read, so it is retried right away)
//	2. inventory  reserve the stock
//	3. payments   charge the card
//
// Each service has its own circuit breaker. If one is down, the message is saved with a note of
// the steps already done, and finished later from the step that failed. It only has to compile.

type checkoutConfig struct {
	Brokers, DataDir string
	EncryptionKey    []byte
	UsersURL         string
	InventoryURL     string
	PaymentsURL      string
}

type account struct {
	ID    string `json:"id"`
	Limit int    `json:"limit"`
}

// ---- Step 1: look up the customer ----

func (c checkoutConfig) fetchUser(ctx context.Context, x *pipeline.Exec) error {
	body, err := send(ctx, http.MethodGet, c.UsersURL+"/account/"+string(x.Key), x.IdempotencyKey(), nil)
	if err != nil {
		return err
	}
	var a account
	if err := json.Unmarshal(body, &a); err != nil {
		return retry.Permanent(err) // a reply we cannot read will not get better
	}
	return pipeline.SetJSON(x, "account", a) // kept with the message, for the steps after this one
}

// ---- Step 2: reserve the stock ----

func (c checkoutConfig) reserveStock(ctx context.Context, x *pipeline.Exec) error {
	_, err := send(ctx, http.MethodPost, c.InventoryURL+"/reserve", x.IdempotencyKey(), x.Value)
	return err
}

// ---- Step 3: charge the card ----

func (c checkoutConfig) chargeCard(ctx context.Context, x *pipeline.Exec) error {
	a, err := pipeline.RequireJSON[account](x, "account") // saved by step 1, even if we restarted since
	if err != nil {
		return err
	}
	_, err = send(ctx, http.MethodPost, c.PaymentsURL+"/charge/"+a.ID, x.IdempotencyKey(), x.Value)
	return err
}

// ---- Helpers shared by the steps ----

// send makes one HTTP call and sorts the answer for the library:
// success, temporary (try again later), or permanent (a person must look at it).
func send(ctx context.Context, method, url, idempotencyKey string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return nil, retry.Permanent(err)
	}
	req.Header.Set("Idempotency-Key", idempotencyKey) // the same on every attempt, so a repeat is recognised
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	reply, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return reply, retry.FromHTTP(resp, nil) // 5xx and timeouts: temporary. Other 4xx: permanent.
}

// serviceBreaker makes the circuit breaker for one service: it opens when half of the recent calls
// fail, and asks the service's /health endpoint when it is back.
func serviceBreaker(name, baseURL string) *breaker.Breaker {
	return breaker.New(breaker.Config{
		Name:             name,
		FailureThreshold: 0.5,
		MinSamples:       20,
		IsFailure:        retry.IsOutage, // a declined card is not an outage
		Health:           health.MustURL(baseURL + "/health"),
	})
}

// ---- Putting it together ----

func runCheckout(ctx context.Context, cfg checkoutConfig) error {
	users := serviceBreaker("users", cfg.UsersURL)
	inventory := serviceBreaker("inventory", cfg.InventoryURL)
	payments := serviceBreaker("payments", cfg.PaymentsURL)
	defer users.Close()
	defer inventory.Close()
	defer payments.Close()

	// A read can be repeated safely, so the first step retries twice before giving up.
	// The charge is not retried here: if it fails, the library saves the message and
	// retries it later, from this step only.
	readRetry := retry.Policy{
		MaxRetries: 2,
		Backoff:    retry.Jitter(retry.Exponential(100*time.Millisecond, time.Second), 0.5),
	}

	checkout, err := pipeline.New("checkout", "v1", []pipeline.Step{
		{Name: "fetch-user", Breaker: users, Timeout: 3 * time.Second, Retry: readRetry, Run: cfg.fetchUser},
		{Name: "reserve", Breaker: inventory, Timeout: 5 * time.Second, Run: cfg.reserveStock},
		{Name: "charge", Breaker: payments, Timeout: 5 * time.Second, Run: cfg.chargeCard},
	})
	if err != nil {
		return err
	}

	encryptor, err := secure.NewAESGCM(secure.Key{ID: "2026-10", Material: cfg.EncryptionKey})
	if err != nil {
		return err
	}

	return kafka.Run(ctx, kafka.ServiceConfig{
		NewClient: confluent.NewClientFunc(ck.ConfigMap{
			"bootstrap.servers": cfg.Brokers,
			"group.id":          "checkout-service",
			"auto.offset.reset": "earliest",
		}),
		DataDir:   cfg.DataDir,                              // saved messages wait here, on disk
		Secure:    &dlq.SecureOptions{Encryptor: encryptor}, // and are encrypted
		Pipelines: []kafka.Binding{{Topic: "checkouts", Pipeline: checkout}},
		Consumer:  kafka.Config{Workers: 4},                              // four messages at once; offsets still commit in order
		Redriver:  dlq.RedriveConfig{Rate: 50, RampUp: 30 * time.Second}, // go gently when a service comes back
	})
}

func Example_full() {
	_ = runCheckout
}
