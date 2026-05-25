# GoGuardLib

GoGuardLib is a Go library for resilient outbound HTTP requests, providing connection pooling and circuit breaker functionality.

## Features

- **Lock Sharding:** Reduces lock contention with sharded state.
- **Efficient Metrics:** Constant-time failure rate calculations.
- **Adaptive Tripping:** Avoids tripping on low-traffic hosts using minimum sample thresholds.
- **Bulkheading:** Limits concurrent requests per host.
- **Timeouts:** Enforces per-request timeouts at the library level.
- **Safe Retries:** Retries idempotent requests on transient network errors.
- **Memory Management:** Uses LRU eviction to manage breaker memory usage.
- **Panic Handling:** Captures panics in downstream transports to maintain circuit safety.
- **Observability:** Provides hooks for state changes and detailed error reporting.

## Installation

```bash
go get github.com/justinclev/GoGuardLib
```

## Usage Example

```go
cfg := goguard.Config{
    FailureThreshold: 0.50,            // Trip at 50% failure rate
    MinSamples:       10,              // Require at least 10 requests before tripping
    MaxLatency:       2 * time.Second, // Treat responses slower than 2s as failures
    RequestTimeout:   5 * time.Second, // Enforce a 5s timeout per request
    MaxRetries:       2,               // Retry idempotent requests on network errors
    MaxBreakers:      5000,            // LRU limit per shard
    OnStateChange: func(host string, from, to engine.BreakerState) {
        log.Printf("Host %s transitioned from %v to %v", host, from, to)
    },
}

transport := goguard.NewResilientTransport(cfg)
defer transport.Close()

client := &http.Client{Transport: transport}
```

## Performance Notes

- Uses sync.Pool for hashers to reduce allocations.
- Employs ring buffers for efficient metric tracking.
- Atomic state checks are used to minimize locking on the hot path.

## License

MIT
