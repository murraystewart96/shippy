FROM golang:alpine as builder

RUN apk update && apk upgrade && \
    apk add --no-cache git

RUN mkdir -p /app/user-cli /app/user-service
WORKDIR /app

ENV GO111MODULE=on

COPY user-cli ./user-cli
COPY user-service ./user-service

WORKDIR /app/user-cli

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix . -o user-cli .


FROM alpine:latest

RUN apk --no-cache add ca-certificates

RUN mkdir /app
WORKDIR /app
COPY --from=builder /app/user-cli/user-cli .

ENTRYPOINT ["./user-cli"]
