package goguard

import (
	"context"
	"net/http"

	"github.com/justinclev/GoGuardLib/breaker"
	"github.com/justinclev/GoGuardLib/dlq"
)

// Client is an http.Client whose requests go through a ResilientTransport. It is the short way to
// guard outgoing HTTP: one call instead of building the transport and wrapping it.
//
//	client, err := goguard.NewClient(goguard.Policy{HealthPath: "/health"})
//	defer client.Close()
//	resp, err := client.Get("https://payments.internal/status")
//
// Everything the transport offers is still there: pass Options for endpoints, events and
// settings, set fields of the embedded http.Client (Timeout, Jar, CheckRedirect), and reach the
// transport itself with Guard.
type Client struct {
	*http.Client
	guard *ResilientTransport
}

// NewClient guards every request with p, one circuit per host (see GuardAll). Options are applied
// after it, so WithEndpoint can give particular endpoints their own policy and WithEvents adds a
// sink. A Defer with no Store, such as &goguard.Defer{}, takes the store of the kafka.Service you give
// the client to (ServiceConfig.HTTP) or of BindStore. Use New and http.Client directly for full control of the transport's Config.
func NewClient(p Policy, opts ...Option) (*Client, error) {
	all := append([]Option{GuardAll(p)}, opts...)
	return NewClientWithConfig(Config{}, all...)
}

// NewClientWithConfig is NewClient with a transport Config, for settings such as MaxBreakers,
// ResponseHeaderTimeout or a custom Transport.
func NewClientWithConfig(cfg Config, opts ...Option) (*Client, error) {
	for _, opt := range opts {
		opt(&cfg)
	}
	// A Defer without a Store is filled in later (BindStore, or ServiceConfig.HTTP), so a client
	// can be built before the log exists. Nothing is saved until then.
	bindLater := func(d *Defer) {
		if d != nil && d.Store == nil {
			d.BindLater = true
		}
	}
	for _, ep := range cfg.Endpoints {
		bindLater(ep.Policy.Defer)
	}
	if cfg.GuardAll != nil {
		bindLater(cfg.GuardAll.Defer)
	}
	t, err := New(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{Client: &http.Client{Transport: t}, guard: t}, nil
}

// Guard returns the transport, for Stats, Breakers and the like.
func (c *Client) Guard() *ResilientTransport { return c.guard }

// Breakers returns the circuits created so far, by name. It fits dlq.RedriveConfig.BreakerSource.
func (c *Client) Breakers() map[string]*breaker.Breaker { return c.guard.Breakers() }

// Replay returns the handler that sends requests saved by Policy.Defer through this client, so a
// replay respects the circuit. prepare, which may be nil, adjusts each request first (see
// ReplayConfig.Prepare). Register it as the handler for "http" records.
func (c *Client) Replay(prepare func(ctx context.Context, req *http.Request) error) dlq.RedriveHandler {
	h, _ := ReplayHandler(ReplayConfig{Client: c.Client, Prepare: prepare}) // only fails without a client
	return h
}

// BindStore gives store to every Defer that was configured with BindLater and has no store yet.
// kafka.NewService calls it for ServiceConfig.HTTP. A Defer that already has a store keeps it.
func (c *Client) BindStore(store dlq.Store) { c.guard.bindStore(store) }

// Close stops the transport's background work and closes its idle connections.
func (c *Client) Close() error {
	c.guard.CloseIdleConnections()
	return c.guard.Close()
}
