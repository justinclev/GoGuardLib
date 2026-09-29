# Contributing

## Before you send a change

    make all      # gofmt, vet, golangci-lint, tests with -race, build
    make kafka    # the same for the Kafka module (needs cgo)

Both must pass. A change that fixes a bug comes with a test that fails without it.

## Rules the code follows

- The library never logs or prints. Everything observable is a typed event
  (package obs) or a `Stats` method, and events carry names, counts and timings,
  never payloads, keys, headers, URLs or credentials.
- A new default that changes behaviour is opt-in first and is listed in the
  breaking-change register in docs/AUDIT.md for the next major version.
- A failed write or fsync fails the store closed; nothing retries an fsync.
- Test fixtures must not contain strings shaped like real credentials: build them
  from pieces at run time (see secure/redact_rules_test.go).

## Reporting security problems

See [SECURITY.md](SECURITY.md).
