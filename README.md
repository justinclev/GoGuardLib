# GoGuardLib

GoGuardLib is an enterprise-grade, high-performance, resilient outbound HTTP connection pooler and circuit breaker library for Go.

## Advanced Features

- **Lock Sharding**: 64-way sharded state to eliminate global lock contention.
- **O(1) Metrics**: Constant-time failure rate calculation using atomic running sums.
- **Adaptive Tripping**: MinSamples prevents flapping on low-volume traffic.
- **Bulkheading**: MaxInflight limits concurrent requests per host.
- **Enforced Timeouts**: Library-level per-request timeout enforcement.
- **Safe Retries**: Automatic retries for idempotent methods (GET/HEAD) on transient network errors.
- **Memory Safety**: LRU-based breaker eviction prevents memory exhaustion under high host cardinality.
- **Panic Protection**: Automatically captures panics in downstream transports to trip the circuit safely.
- **Observability**: State-change hooks and programmatic error unwrapping with CircuitError.

## Installation

\`\`\`bash
go get github.com/justinclev/GoGuardLib
\`\`\`

## Advanced Usage

\`\`\`go
cfg := goguard.Config{
    FailureThreshold: 0.50,          // Trip at 50% failure rate
    MinSamples:       10,            // Don't trip until at least 10 requests
    MaxLatency:       2 * time.Second, // Responses slower than 2s count as failures
    RequestTimeout:   5 * time.Second, // Enforce 5s timeout
    MaxRetries:       2,               // Retry idempotent requests on network errors
    MaxBreakers:      5000,            // LRU limit per shard (total 320,000 hosts)
    OnStateChange: func(host string, from, to engine.BreakerState) {
        log.Printf("Host %s transitioned from %v to %v", host, from, to)
    },
}

transport := goguard.NewResilientTransport(cfg)
defer transport.Close()

client := &http.Client{Transport: transport}
\`\`\`

## Performance

- **Zero Allocation Hashing**: Uses sync.Pool for FNV hashers.
- **Ring Buffer Metrics**: O(1) rotation and insertion.
- **Lockless Hot-Path**: Atomic state checks before acquiring shard locks.

## License

MIT
