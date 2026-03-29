FROM golang:alpine AS builder

RUN apk update && apk upgrade && \
    apk add --no-cache git

RUN mkdir -p /app/email-service /app/user-service
WORKDIR /app

ENV GO111MODULE=on

COPY email-service ./email-service
COPY user-service ./user-service

WORKDIR /app/email-service

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix . -o email-service .


FROM alpine:latest

RUN apk --no-cache add ca-certificates

RUN mkdir /app
WORKDIR /app
COPY --from=builder /app/email-service/email-service .

CMD ["./email-service"]
