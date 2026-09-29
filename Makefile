.PHONY: all fmt fmt-check vet lint test cover build kafka kafka-integration

all: fmt-check vet lint test build

fmt:
	gofmt -w .

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	go vet ./...

lint:
	golangci-lint run ./...

test:
	go test -race -count=1 ./...

cover:
	go test -race -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

build:
	go build ./...

# The Kafka adapter is a separate module (cgo + confluent-kafka-go).
kafka:
	cd kafka && gofmt -l . | (! grep .) && go vet ./... && golangci-lint run ./... && go test -race -count=1 ./...

# Needs a broker: KAFKA_BROKERS=host:9092 make kafka-integration
kafka-integration:
	cd kafka && go test -tags integration -count=1 -timeout 15m ./confluent/
