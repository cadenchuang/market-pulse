# Build the Go ingestor. Pure-Go (segmentio/kafka-go), so CGO_ENABLED=0 gives a
# static binary and a tiny distroless final image — no librdkafka needed.
# Build context is the repo root (see docker-compose.yml) so we can COPY ingestor/.
FROM golang:1.26 AS build
WORKDIR /src

COPY ingestor/go.mod ingestor/go.sum ./
RUN go mod download

COPY ingestor/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/ingestor ./cmd/ingestor
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/processor ./cmd/processor
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/loadtest ./cmd/loadtest

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ingestor /ingestor
COPY --from=build /out/processor /processor
COPY --from=build /out/loadtest /loadtest
# Each compose service selects its binary via `entrypoint`. Config comes from env
# (see docker-compose.yml); flags may override.
ENTRYPOINT ["/ingestor"]
