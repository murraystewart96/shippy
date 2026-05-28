FROM golang:1.25 AS builder

RUN apt-get update && apt-get install -y git gcc

RUN mkdir /app
WORKDIR /app

COPY cmd ./cmd
COPY proto ./proto
COPY pkg ./pkg
COPY go.work go.work
COPY go.work.sum go.work.sum

WORKDIR /app/cmd/consignment-service

RUN go mod download

RUN CGO_ENABLED=1 GOOS=linux go build -o consignment-service .

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

RUN mkdir /app
WORKDIR /app

COPY --from=builder /app/cmd/consignment-service/consignment-service .

CMD ["./consignment-service"]
