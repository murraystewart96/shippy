FROM golang:alpine as builder

RUN apk update && apk upgrade && \
    apk add --no-cache git

RUN mkdir -p /app/cli-consignment /app/consignment-service
WORKDIR /app

ENV GO111MODULE=on

COPY cli-consignment ./cli-consignment
COPY consignment-service ./consignment-service

WORKDIR /app/cli-consignment

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix . -o cli-consignment .


FROM alpine:latest

RUN apk --no-cache add ca-certificates

RUN mkdir /app
WORKDIR /app
COPY --from=builder /app/cli-consignment/cli-consignment .
COPY cli-consignment/consignment.json /app/consignment.json

ENTRYPOINT ["./cli-consignment"]
