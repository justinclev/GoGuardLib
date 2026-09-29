package goguard

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

// Matcher decides whether a request belongs to an endpoint.
type Matcher func(*http.Request) bool

// Host matches requests whose URL host (including any port other than the default
// 80 or 443) equals one of hosts, ignoring case and any trailing dot.
func Host(hosts ...string) Matcher {
	set := make(map[string]struct{}, len(hosts))
	for _, h := range hosts {
		set[canonHost(h)] = struct{}{}
	}
	return func(r *http.Request) bool {
		_, ok := set[canonHost(r.URL.Host)]
		return ok
	}
}

// HostPath matches requests to host whose path starts with pathPrefix.
func HostPath(host, pathPrefix string) Matcher {
	host = canonHost(host)
	return func(r *http.Request) bool {
		return canonHost(r.URL.Host) == host && strings.HasPrefix(r.URL.Path, pathPrefix)
	}
}

// ErrUnknownEndpoint is returned when a request selects, with Use, an endpoint
// that was never registered. Failing closed beats silently skipping protection.
var ErrUnknownEndpoint = errors.New("goguard: unknown endpoint")

type contextKey string

const (
	// PriorityKey marks a request as VIP when set to true in its context: it
	// bypasses the bulkhead (never the circuit).
	PriorityKey contextKey = "goguard-priority"

	useKey  contextKey = "goguard-use"
	skipKey contextKey = "goguard-skip"
)

// Use makes the request that carries ctx go through the named registered
// endpoint, whatever the matchers say.
func Use(ctx context.Context, endpoint string) context.Context {
	return context.WithValue(ctx, useKey, endpoint)
}

// Skip makes the request that carries ctx bypass all protection.
func Skip(ctx context.Context) context.Context {
	return context.WithValue(ctx, skipKey, true)
}
