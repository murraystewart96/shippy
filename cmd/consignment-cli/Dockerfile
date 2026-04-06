FROM golang:alpine AS builder

RUN apk update && apk upgrade && \
    apk add --no-cache git

RUN mkdir /app
WORKDIR /app

COPY cmd ./cmd
COPY proto ./proto
COPY go.work go.work
COPY go.work.sum go.work.sum

WORKDIR /app/cmd/consignment-cli

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o consignment-cli .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

RUN mkdir /app
WORKDIR /app

COPY --from=builder /app/cmd/consignment-cli/consignment-cli .
COPY cmd/consignment-cli/consignment.json /app/consignment.json

ENTRYPOINT ["./consignment-cli"]
