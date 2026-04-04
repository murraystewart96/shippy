TODO

- mount sql files into postgres to create tables


protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/reservation/reservation.proto



SERVICE_ADDRESS=localhost:50051 go run . --name "Test" --email "test@test.com" --password "password" --company "Test Co"

SERVICE_ADDRESS=localhost:50051 go run . ./consignment.json eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VyIjp7ImlkIjoiMGZlNDgxYzgtMmEyOC00YWQ2LTliNWUtN2RjNGQ2NzYyNWI1IiwibmFtZSI6IlRlc3QiLCJjb21wYW55IjoiVGVzdCBDbyIsImVtYWlsIjoidGVzdEB0ZXN0LmNvbSIsInBhc3N3b3JkIjoiJDJhJDEwJGwwNjg1L0FWVTBnT3JMWFN3aEVueU9nLkdTV0cvTi9XcmxaRkFhUnJFbmh3N3RucnlhU0pXIn0sImV4cCI6MTc3NDk3MDE4NCwiaXNzIjoic2hpcHBpbmcuVXNlclNlcnZpY2UifQ.EEOtYmxD5xmN2xBiCY3rqx96dDOywqIlY9HKzEsxQBg


kubectl scale statefulset user-service-database --replicas=0
kubectl delete pvc user-db-data-user-service-database-0
kubectl scale statefulset user-service-database --replicas=1


kubectl port-forward svc/user-service 50051:50051

kind load docker-image shippy-consignment-service:latest         


# 1. Create a user
curl -X POST http://localhost:8080/v1/users \
  -H "Content-Type: application/json" \
  -d '{
    "name": "John Doe",
    "email": "john@example.com",
    "company": "Shippy Inc",
    "password": "secret123"
  }'

# 2. Get a token
curl -X POST http://localhost:8080/auth \
  -H "Content-Type: application/json" \
  -d '{
    "email": "john@example.com",
    "password": "secret123"
  }'

# 3. Create a consignment (replace TOKEN with the value from step 2)
curl -X POST http://localhost:8080/v1/consignments \
  -H "Content-Type: application/json" \
  -H "x-token: TOKEN" \
  -d '{
    "description": "Laptop shipment",
    "weight": 500,
    "containers": [
      {
        "customer_id": "cust-001",
        "origin": "London",
        "user_id": "user-001"
      }
    ]
  }'




## Reservation Service Design

The reservation service manages capacity holds against vessels. Once a payment is confirmed a 
consignment is created and the reservation is confirmed. Reservations are cached until confirmed 
or expired. If a reservation expires or is explicitly cancelled, the held capacity must be 
released back to the vessel.

Capacity releases are handled via a Kafka pipeline. Release capacity events are consumed by the 
reservation service which calls the vessel service to restore the capacity.

### Redis Cache Structure

Each reservation maintains two Redis entries:

| Key | TTL | Purpose |
|-----|-----|---------|
| `reservation:{id}` | 10 min | TTL marker — absence signals expiry |
| `reservation_data:{id}` | 30 min | Holds vessel ID, weight, containers |

### Release Capacity Event Triggers

**Cleanup job** — runs periodically, scans for data keys where the corresponding ID key no 
longer exists. When found, publishes a `capacity.restore` event to Kafka and deletes both keys.

**Immediate release** — triggered by known failures such as payment failure. Publishes directly 
to the `capacity.restore` topic without waiting for the cleanup job.

Both paths publish to the same `capacity.restore` topic and are handled by the same consumer.

### Handling Duplicate Events

The data key acts as the processing lock. On receiving a `capacity.restore` event:

1. Attempt to atomically delete the data key
2. If 0 keys deleted — already processed, stop
3. If 1 key deleted — proceed with capacity restore on the vessel service
4. Also delete the ID key (cleanup, both paths follow the same code)

Because Redis DEL is atomic, only one consumer can successfully delete the data key. All 
subsequent consumers for the same reservation exit cleanly.

### Handling Retries

Events carry a `CacheCleared` flag:

- `CacheCleared: false` — attempt the atomic data key delete before processing
- `CacheCleared: true` — data key was already deleted on a previous attempt, skip straight to 
  the vessel service call

This flag is only set to `true` once the data key has been successfully deleted. If the delete 
fails, the event is retried with `CacheCleared: false` so the atomic delete is attempted again. 
The cleanup job may also pick up the same reservation in the meantime — whichever reaches the 
atomic delete first will process it, the other will exit cleanly.

capacity.restore event received
    → CacheCleared=false:
        → GET reservation_data:{id} — fetch vessel ID, weight, containers
        → DEL reservation_data:{id} — atomic lock
            → 0 deleted: discard fetched data, stop (already processed)
            → 1 deleted: set CacheCleared=true, call vessel service
                → success: done
                → failure: republish with CacheCleared=true, RetryCount++
    → CacheCleared=true: call vessel service using data from event payload
        → success: done
        → failure: republish with RetryCount++

### Retry and DLQ

Choreogrpahy based SAGA - idempotent, retryable operations and eventual consistency via event-driven updates

Failed events are republished to `capacity.release` with an incremented `RetryCount`. Once 
`RetryCount` exceeds the threshold (default: 3), the event is published to 
`capacity.release.dlq` for manual inspection and replay.


## Outbox pattern
Implementing the outbox pattern to prevent lost events. I realised that if we deleted the reservation data and then failed to release the capacity on the vessel we might lose the event forever if we failed to publish the retry event. Because the reservation data has been deleted from the cache the cleanup job will no longer trigger an event for it. We can't afford to lose this event
as it would mean perminantly losing capacity on a vessel. 

The only solution was to use the outbox pattern.
