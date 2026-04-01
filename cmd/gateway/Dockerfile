FROM golang:alpine AS builder

RUN apk update && apk upgrade && \
    apk add --no-cache git

RUN mkdir /app
WORKDIR /app

COPY cmd ./cmd
COPY proto ./proto
COPY go.work go.work
COPY go.work.sum go.work.sum

WORKDIR /app/cmd/gateway

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o gateway .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

RUN mkdir /app
WORKDIR /app

COPY --from=builder /app/cmd/gateway/gateway .

CMD ["./gateway"]
