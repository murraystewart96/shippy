FROM golang:alpine AS builder

RUN apk update && apk upgrade && \ 
    apk add --no-cache git

RUN mkdir /app
WORKDIR /app


COPY shippy-consignment-service ./shippy-consignment-service
COPY shippy-vessel-service ./shippy-vessel-service
COPY shippy-user-service ./shippy-user-service

WORKDIR /app/shippy-consignment-service

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix . -o shippy-consignment-service

# Run container

FROM alpine:latest

RUN apk --no-cache add ca-certificates

RUN mkdir /app
WORKDIR /app

COPY --from=builder /app/shippy-consignment-service .

CMD ["./shippy-consignment-service"]


