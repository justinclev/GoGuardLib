# syntax=docker/dockerfile:1
#
# Build context is the repository root (see demo/docker-compose.yml), because the
# demo uses the library from the working tree rather than a published release.

FROM golang:1.25-bookworm AS build
WORKDIR /src

# A network that re-signs HTTPS with a company root certificate: put that certificate
# in demo/certs/ (see the README there). With none present this changes nothing.
COPY demo/certs/ /usr/local/share/ca-certificates/extra/
RUN update-ca-certificates
ARG GOPROXY
ENV GOPROXY=${GOPROXY:-https://proxy.golang.org,direct}

# Resolve dependencies first, so source edits do not redownload them. A network or
# certificate problem fails here, with its real error.
COPY go.mod ./go.mod
COPY kafka/go.mod kafka/go.sum ./kafka/
COPY demo/backend/go.mod demo/backend/go.sum ./demo/backend/
RUN cd demo/backend && go mod download

COPY . .
WORKDIR /src/demo/backend
# The orchestrator links librdkafka through confluent-kafka-go (cgo); the simulated
# services are plain Go.
RUN CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o /out/orchestrator ./cmd/orchestrator \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/services ./cmd/services

FROM debian:bookworm-slim AS base
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --create-home --uid 10001 demo

FROM base AS services
COPY --from=build /out/services /usr/local/bin/services
USER demo
EXPOSE 9100 9101 9102 9103 9104
ENTRYPOINT ["/usr/local/bin/services"]

FROM base AS orchestrator
COPY --from=build /out/orchestrator /usr/local/bin/orchestrator
RUN mkdir -p /data && chown demo /data
USER demo
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/orchestrator"]
