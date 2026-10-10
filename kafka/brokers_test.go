package kafka_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/justinclev/GoGuardLib/kafka"
)

// downClient is a fake client that also reports the cluster as unreachable.
type downClient struct {
	*fakeClient
	mu     sync.Mutex
	health kafka.ClientHealth
}

func (d *downClient) Health() kafka.ClientHealth {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.health
}

// A consumer that has lost the cluster looks exactly like one on an idle topic;
// the timeout turns that into an error a supervisor can act on.
func TestBrokersDownTimeoutStopsAConsumerThatLostTheCluster(t *testing.T) {
	r := newRig(t)
	dc := &downClient{fakeClient: r.client, health: kafka.ClientHealth{AllBrokersDown: true, Since: time.Now().Add(-time.Hour), LastError: "all brokers down"}}
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.Client = dc; cfg.BrokersDownTimeout = time.Minute })
	r.assign(0)
	if err := <-r.runAsync(c); !errors.Is(err, kafka.ErrBrokersDown) {
		t.Fatalf("Run = %v, want ErrBrokersDown", err)
	}
	if h := c.Stats().Health; !h.AllBrokersDown || h.LastError == "" {
		t.Fatalf("Stats.Health = %+v, want the down state", h)
	}
}

func TestBrokersDownWithinTheTimeoutKeepsRunning(t *testing.T) {
	r := newRig(t)
	dc := &downClient{fakeClient: r.client, health: kafka.ClientHealth{AllBrokersDown: true, Since: time.Now()}}
	c := r.newConsumer(func(cfg *kafka.Config) { cfg.Client = dc; cfg.BrokersDownTimeout = time.Hour })
	r.assign(0)
	stop := r.run(c)
	time.Sleep(50 * time.Millisecond)
	if err := stop(); err != nil {
		t.Fatalf("Run = %v: a brief outage must not stop the consumer", err)
	}
}
