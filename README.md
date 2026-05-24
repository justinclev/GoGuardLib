# GoGuardLib

GoGuardLib is a high-performance, low-latency, resilient outbound HTTP connection pooler and circuit breaker library for Go. It protects your systems from cascading failures by monitoring downstream health and short-circuiting requests when services degrade.

## Features

- **Circuit Breaking**: Implements Closed, Open, and Half-Open states.
- **Granular Protection**: Automatic breaker isolation per host.
- **Rolling Window Metrics**: Sub-second bucketed metrics for high concurrency with low lock contention.
- **Pluggable \`http.RoundTripper\`: Seamlessly integrates with existing \`http.Client\` setups.
- **Zero External Dependencies**: Built entirely on Go standard library primitives.

## Installation

\`\`\`bash
go get github.com/justinclev/GoGuardLib
\`\`\`

## Usage

Integrate \`GoGuardLib\` into your application by wrapping your HTTP client's transport.

\`\`\`go
package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/justinclev/GoGuardLib"
)

func main() {
	// 1. Configure the transport
	cfg := goguard.Config{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		FailureThreshold:    0.50,          // Trip at 50% failure rate
		SleepWindow:         5 * time.Second, // Wait 5s before retrying
		SamplingWindow:      10 * time.Second,
	}

	// 2. Create the ResilientTransport
	transport := goguard.NewResilientTransport(cfg)

	// 3. Use it with an http.Client
	client := &http.Client{
		Transport: transport,
		Timeout:   15 * time.Second,
	}

	// 4. Make requests
	resp, err := client.Get("https://api.example.com/data")
	if err != nil {
		fmt.Printf("Request failed or short-circuited: %v\n", err)
		return
	}
	defer resp.Body.Close()

	fmt.Printf("Status: %s\n", resp.Status)
}
\`\`\`

## Development

### Running Tests

\`\`\`bash
go test -v ./...
\`\`\`

### Checking Coverage

\`\`\`bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
\`\`\`

## License

MIT
