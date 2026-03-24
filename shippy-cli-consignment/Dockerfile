FROM golang:alpine as builder

RUN apk update && apk upgrade && \
    apk add --no-cache git

RUN mkdir -p /app/shippy-cli-consignment /app/shippy-consignment-service
WORKDIR /app

ENV GO111MODULE=on

COPY shippy-cli-consignment ./shippy-cli-consignment
COPY shippy-consignment-service ./shippy-consignment-service

WORKDIR /app/shippy-cli-consignment

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix . -o shippy-cli-consignment .


FROM alpine:latest

RUN apk --no-cache add ca-certificates

RUN mkdir /app
WORKDIR /app
COPY --from=builder /app/shippy-cli-consignment/shippy-cli-consignment .
COPY shippy-cli-consignment/consignment.json /app/consignment.json

ENTRYPOINT ["./shippy-cli-consignment"]
