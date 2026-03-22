FROM golang:alpine AS builder

RUN apk update && apk upgrade && \ 
    apk add --no-cache git

RUN mkdir /app
WORKDIR /app

COPY . .

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix . -o shippy-vessel-service

# Run container

FROM alpine:latest

RUN apk --no-cache add ca-certificates

RUN mkdir /app
WORKDIR /app

COPY --from=builder /app/shippy-vessel-service .

CMD ["./shippy-vessel-service"]


