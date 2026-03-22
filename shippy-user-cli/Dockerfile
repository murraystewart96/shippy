FROM golang:alpine as builder

RUN apk update && apk upgrade && \
    apk add --no-cache git

RUN mkdir -p /app/shippy-user-cli /app/shippy-user-service
WORKDIR /app

ENV GO111MODULE=on

COPY shippy-user-cli ./shippy-user-cli
COPY shippy-user-service ./shippy-user-service

WORKDIR /app/shippy-user-cli

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix . -o shippy-user-cli .


FROM alpine:latest

RUN apk --no-cache add ca-certificates

RUN mkdir /app
WORKDIR /app
COPY --from=builder /app/shippy-user-cli/shippy-user-cli .

ENTRYPOINT ["./shippy-user-cli"]
