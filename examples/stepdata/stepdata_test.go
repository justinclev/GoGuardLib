// Package stepdata_test shows data passing from one breaker-guarded call to the next, and
// surviving a failure in between.
//
// Step 1 asks a "users" API for the customer's account. Step 2 sends that account to an "orders"
// API. The orders API is down at first, so step 2 fails, and the message is saved with how far it
// got, including the data step 1 produced. When orders comes back, the redriver resumes at step 2:
// step 1 is not called again, and step 2 still has the account.
package stepdata_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"time"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
	"github.com/justinclev/GoGuardLib/pipeline"
	"github.com/justinclev/GoGuardLib/retry"
)

// call sends one request and returns the body. A 5xx is an error, so the breaker counts it.
func call(ctx context.Context, method, url, body string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 500 {
		return "", fmt.Errorf("%s answered %d", url, resp.StatusCode)
	}
	return string(b), nil
}

func Example() {
	var usersCalls atomic.Int32
	users := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		usersCalls.Add(1)
		_, _ = io.WriteString(w, "acct-42") // the data step 2 needs
	}))
	defer users.Close()

	var ordersUp atomic.Bool
	received := make(chan string, 1)
	orders := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ordersUp.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		b, _ := io.ReadAll(r.Body)
		received <- string(b)
	}))
	defer orders.Close()

	newBreaker := func(name string) *breaker.Breaker {
		return breaker.New(breaker.Config{Name: name, FailureThreshold: 0.5, MinSamples: 1, SleepWindow: 50 * time.Millisecond})
	}
	usersBr, ordersBr := newBreaker("users"), newBreaker("orders")
	defer usersBr.Close()
	defer ordersBr.Close()

	checkout, err := pipeline.New("checkout", "v1", []pipeline.Step{
		{Name: "fetch-user", Breaker: usersBr, Run: func(ctx context.Context, x *pipeline.Exec) error {
			account, err := call(ctx, http.MethodGet, users.URL, "")
			if err != nil {
				return err
			}
			x.Set("accountId", []byte(account)) // kept with this step's progress
			return nil
		}},
		{Name: "create-order", Breaker: ordersBr, Run: func(ctx context.Context, x *pipeline.Exec) error {
			account, _ := x.Get("accountId") // step 1's data, even after a failure and a restart
			_, err := call(ctx, http.MethodPost, orders.URL, string(account))
			return err
		}},
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	ctx := context.Background()
	store := dlq.NewMemoryStore(dlq.MemoryOptions{})

	// First run: users answers, orders is down. The message is saved with step 1's data.
	res, err := checkout.Execute(ctx, store, pipeline.Input{ID: "order-1", Value: []byte("order-1")})
	fmt.Println("first run:", res, err)
	fmt.Println("users API calls:", usersCalls.Load())

	// Orders comes back. The redriver resumes at step 2.
	ordersUp.Store(true)
	redriver, err := dlq.NewRedriver(dlq.RedriveConfig{
		Store: store, Handler: checkout.Handler(),
		Breakers:     map[string]*breaker.Breaker{"users": usersBr, "orders": ordersBr},
		PollInterval: 10 * time.Millisecond, Backoff: retry.Constant(20 * time.Millisecond),
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _ = redriver.Run(rctx) }()

	select {
	case account := <-received:
		fmt.Println("orders API received account:", account)
	case <-time.After(10 * time.Second):
		fmt.Println("timed out waiting for the redriver")
	}
	cancel()
	<-done
	fmt.Println("users API calls after recovery:", usersCalls.Load())
	if errors.Is(ctx.Err(), context.Canceled) {
		return
	}

	// Output:
	// first run: deferred <nil>
	// users API calls: 1
	// orders API received account: acct-42
	// users API calls after recovery: 1
}
