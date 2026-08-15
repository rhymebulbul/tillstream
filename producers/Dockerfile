FROM golang:1.22 AS builder
WORKDIR /app
COPY . .
# Pin the confluent-kafka-go version to avoid bleeding-edge go 1.25 requirement
RUN go get github.com/confluentinc/confluent-kafka-go/v2@v2.4.0
# Pin hamba/avro to avoid go 1.24 requirement
RUN go get github.com/hamba/avro/v2@v2.22.0
# Initialize/tidy modules inside the container
RUN go mod tidy
RUN go build -o /tillstream-producer ./cmd/producer/main.go

FROM debian:bookworm-slim
WORKDIR /
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /tillstream-producer /tillstream-producer
ENTRYPOINT ["/tillstream-producer"]
