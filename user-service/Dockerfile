FROM golang:alpine AS builder

RUN apk update && apk upgrade && \ 
    apk add --no-cache git

RUN mkdir /app
WORKDIR /app

COPY . .

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o user-service ./user-service

# Run container

FROM alpine:latest

RUN apk --no-cache add ca-certificates

RUN mkdir /app
WORKDIR /app

COPY --from=builder /app/user-service .

CMD ["./user-service"]


