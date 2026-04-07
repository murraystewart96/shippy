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




## Booking Flow (Choreography-based Saga)

The booking flow uses an authorise-then-capture payment pattern combined with the outbox pattern
to ensure consistency without requiring distributed transactions.

### Happy Path

1. **Reserve capacity** — reservation service holds vessel capacity in Redis with a TTL
2. **Create consignment** — consignment service writes a record with `status: pending`
3. **Authorise payment** — synchronous call to payment provider; a hold is placed on the card but no money is taken. User receives "booking confirmed" response at this point.
4. **Write outbox event** — consignment service atomically updates consignment to `status: confirming` and writes a confirm event to its outbox (carries `payment_auth_id`)
5. **Background — capture payment** — outbox handler calls payment provider to capture the authorised amount
6. **Background — confirm capacity** — outbox handler calls vessel service to confirm the reserved capacity
7. **Background — mark consignment active** — consignment updated to `status: active`

Steps 5–7 are handled by the consignment service outbox publisher. Capture happens before
ConfirmCapacity — capture is the point of no return.

### Failure Handling

| Failure point | Action |
|---------------|--------|
| Reserve capacity fails | Return error to user — nothing to roll back |
| Create consignment fails | Release capacity, return error |
| Authorise payment fails | Release capacity, delete consignment, return error |
| Capture fails (after retries) | Event goes to DLQ → void authorisation, release capacity, mark consignment `cancelled` |
| ConfirmCapacity fails (after retries) | Event goes to DLQ → refund capture, mark consignment `cancelled` |

Capture and ConfirmCapacity are both idempotent — safe to retry. ConfirmCapacity is backed by
a unique compound index on `(reservation_id, operation)` in the vessel service.

---

## Reservation Service Design

The reservation service manages capacity holds against vessels. Reservations are cached in Redis
until confirmed or expired. If a reservation expires or is explicitly cancelled, the held
capacity is released back to the vessel.

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

Capacity events (release and confirm) are delivered **at-least-once**. Exactly-once is not guaranteed, so the system is designed with multiple layers of protection against duplicate processing. Each layer handles a different failure mode.

#### Layer 1 — Atomic cache delete as a processing lock

When a capacity event is received, the first action is an atomic `DEL` of the reservation data key in Redis. Redis `DEL` returns the number of keys deleted — `1` if the key existed, `0` if it was already gone. This acts as a distributed lock: only the first consumer to delete the key proceeds. Any concurrent or duplicate event sees `0` and returns immediately.

#### Layer 2 — CacheCleared flag for retries

If the event deletes the cache entry but then fails (e.g. the vessel service is unavailable), we need to retry — but the cache entry is already gone. The event is rescheduled with `CacheCleared=true`. On reprocessing, the atomic delete check is skipped and execution continues directly to the vessel call. From this point the processing lock no longer applies, so the vessel service must handle duplicates itself.

#### Layer 3 — Vessel service idempotency

The vessel service records each capacity operation (reserve, release, confirm) in a `capacity_operations` collection with a unique compound index on `(reservation_id, operation)`. Any duplicate call for the same reservation and operation is silently ignored. This is the final backstop — it applies to all duplicate events that make it through the layers above.

#### Layer 4 — Outbox lease lock (horizontal scaling)

The outbox publisher reads unpublished events from Postgres, publishes them to Kafka, then marks them as published. Under a single instance this is safe. With multiple instances, two publishers could read the same rows simultaneously and publish duplicates before either marks them as published.

To prevent this, rows are claimed with a **30 second lease** using `SELECT FOR UPDATE SKIP LOCKED`. The first instance to acquire the lock owns the batch for 30 seconds. Other instances skip locked rows entirely.

The publisher is given a **20 second context deadline** — a hard outer bound ensuring it completes well within the lease window. If the deadline is exceeded, remaining events are left unpublished and retried on the next tick.

The one remaining edge case is if the Kafka client internally enqueues a message before the context is cancelled. In this scenario the event may still be delivered to Kafka despite the publisher exiting early. Layer 3 (vessel idempotency) handles this.


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

## Horizontal scaling
The outbox publisher and the cleanup job were not safe for multiple instances. Both poll and act:
If we had two instances of the reservation service they could theoretically process an expired reservation at the same time
and create an outbox event for it. We need to add a contraint so that you cannot create duplicate outbox events.
Also the processing of the outbox event itself. Both instances could process the same event at the same time
and then publish the event before marking the event as published in the DB. As such we need to make the 

## Known Limitations
Talk about everysec in redis and how there is potential reservation loss. We would handle this by
monitoring metrics for reservations and run manual reconciliaiton job if neccessary.


Complete the confirmed flow -> reason about scaling the reservation service - what if we just wanted to scale the event consumers.

NEXT. update consignment to have status field. think through the flow from start to finish. 

Think about integration testing. also how can we prove vessel release is idempotent

Check if mongodb works without manually initialising next time.
