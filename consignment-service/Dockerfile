FROM golang:alpine AS builder

RUN apk update && apk upgrade && \ 
    apk add --no-cache git

RUN mkdir /app
WORKDIR /app


COPY consignment-service ./consignment-service
COPY vessel-service ./vessel-service
COPY user-service ./user-service

WORKDIR /app/consignment-service

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix . -o consignment-service

# Run container

FROM alpine:latest

RUN apk --no-cache add ca-certificates

RUN mkdir /app
WORKDIR /app

COPY --from=builder /app/consignment-service .

CMD ["./consignment-service"]


