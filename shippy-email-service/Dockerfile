FROM golang:alpine AS builder

RUN apk update && apk upgrade && \
    apk add --no-cache git

RUN mkdir -p /app/shippy-email-service /app/shippy-user-service
WORKDIR /app

ENV GO111MODULE=on

COPY shippy-email-service ./shippy-email-service
COPY shippy-user-service ./shippy-user-service

WORKDIR /app/shippy-email-service

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix . -o shippy-email-service .


FROM alpine:latest

RUN apk --no-cache add ca-certificates

RUN mkdir /app
WORKDIR /app
COPY --from=builder /app/shippy-email-service/shippy-email-service .

CMD ["./shippy-email-service"]
