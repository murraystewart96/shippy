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

curl -s -X POST http://localhost:8080/v1/consignments/confirm/af1691f8-a4e7-4104-b378-491091bad1fc \  -H "x-token:  

curl -s http://localhost:8080/v1/consignments \       -H "x-token:   


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

## Payment Service
Assuming perfectly idempotent payment service

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







## FUTURE IMPROVEMENTS

Publishing events by applying the Transaction log tailing pattern to reduce latency (outbox pattern)

Use consignment ID as correlationID for observability. Review how IDs are being passed around and used

Define contracts for messages (source of truth) using protobuf

Add component test for consignment service confirm

Chaos testing and visualising with graphs


### TODO NEXT

finish designing new event flow with explicity event contracts. Don't have looping events (CS -> CS). Get tests working, unit plus integration test. 

Add payment captured state to consigment for SAGA. That is the go no go point. If SAGA gets stuck we need to check if the reservation was cancelled or confirmef or neither. we want to bring the consignment into accordance with the reservation. If confirmed then confirm. if released then cancel and refund. If neither try and confirm.

Fix tests, and add observability.

Make sure all event paths are covered. we need to make sure. think about how we could add a test to cover the situation is just found where the reservation service wasnt consuming the retry topic.

Add logs for each step in the SAGA
. Add log aggretgator 

Add event contracts

Then think about reconcilliation and event sourcing. By this point we should have enough compensation handling in place. But if a service was down and a SAGA was completed halted we need to understand 
 - 1. how does kafka work. when an event keeps on failing how does it move on?
 - 2. Once it has moved on how do we rehydrate that SAGA. i imagine we would need to work our way from the start of the SAGA onwards to find where it stopped. then we would probably reconstruct the neccessary event and publish it. This is where idempotency is key. it doesnt matter if we re-trigger an event that was already consumed if its idempotent

 We essentially need to trace the event chain. We can source the outbox table for this as we use it for both the RS and CS. check if there is the event for a given consignmentID has been published or not.

 Think about redesigning the outbox pattern. We want to do the database update and event write as a transaction. no point publishing to ourselves

 Question - is redis the right choice for the reservation storage. maybe DB would be better. As long as i can articulate

Review alternatives to this microservice pattern. what are the trade offs.


Think about what happens in major failure cases. if a particular service was to go down for a long time. (like the cache for example)

OAuth 2.0


###

For this project i have built on the tutorial series https://web.archive.org/web/20220124115000/https://ewanvalentine.io/microservices-in-golang-part-1/.

The tutorial provided a basic distributed shipping application. The main business logic was a user would create consignments and vessels would be assigned that could handle the required capacity of the shipment.


NOTE PRODUCTION CONCERNS

For failure cases where DB is down etc. kafka will keep retrying the event until is succeeds. this is what we want. A future improvement would be to avoid a hot consumer loop where it keeps retrying a stuck event. This would also block other topics. we could do. This point generalises to all transient errors. DB being down, kafka broker being down. The known limitation is that if the DB or the broker was down for long enough that a reservation expired before they came back up the consignment might be cancelled without refunding the user. Think about if this is worth fixing for the portfolio.

Delayed retry topics — on transient failure, publish the message to a topic.retry topic and commit the original offset. A separate consumer reads from the retry topic with a delay before processing. You can chain these (topic.retry.1, topic.retry.2 with increasing delays) before a final DLQ. This is very common at companies like Uber and it's arguably the cleanest pattern because:

Main consumer never blocks
Retry behaviour is visible (you can inspect retry topic lag)
Delay is enforced by the retry consumer's poll interval, not a sleep



When i come back
- reason about table and finalise docs
- implement the rest of the SAGA
- update tests
- run manually
- update docs to describe other design decisions
  - idempotency
  - outbox pattern and lock

- add observability

The full production stack in one view:

Kubernetes          — orchestration, discovery, health
Istio               — network observability, mTLS, traffic control
Prometheus+Grafana  — metrics and alerting
Loki+Grafana        — logs
Jaeger/Tempo        — traces
OpenTelemetry SDK   — application instrumentation










The project builds on this tutorial series - https://web.archive.org/web/20220124115000/https://ewanvalentine.io/microservices-in-golang-part-1/.

The tutorial creates a basic implementation of a microservice shipping application made up of a consignment and vessel service. Consignments are created and assigned to vessels with enough capacity. My addition was to build the payment flow by adding a payment and reservation service. The payment service is a simple mock implementation of something like stripe. The main piece of work was the reservation service and the changes made to the consignment service and vessel service.

The obvious need for the reservation service is that a user may create a consignmnet, it gets assigned to a vessel but then the payment is never made or fails. As such we need to be able to reserve, confirm and release capacity on vessels. 

When creating a consignment the a reservation is made on a vessel. The reservation has a TTL and if the consignment is not confirmed within that window the reservation is released from the vessel.

The first design decision was the storage type for the reservation service. I decided to use redis because it has built in TTL functionality with expired entries being removed from the cache. As such each reservation is stored in the cache as two entries. One entry for reservation id and the other entry for the reservation data itself. The data would have a longer TTL than the id entry. When an id entry is no longer present in the cache it signals that the entry has expired. We then use the reservation data entry to release the reservaton. NOTE redis is configured with AOF so and syncs to file every second so the reservations are pretty robust.

Handling reservation events.

I decided to use events for handling reservation events (confirm or release). This was for a couple reasons. First kafka provides reservation data durability. If for example the cleanup job that releases expired reservations kept failing to release a reservation, once the reservation data had expired from the cache we would no longer have that reservation data. We want to take all the neccessary precautions to avoid failing to release a reservation because thats valuable space that can no longer be used on the vessel. Furthermore, the consignment service needs to asynchronously confirm reservations so it made sense to handle both confirms and releases in the same manner through events. 

Issues with events 

Using events came with its own design decisions. Kafka by default gaurantees at least once delivery which is what we need to ensure that reservation events are handled. However we don't want to double process release or confirm events as they modify the vessel capacity. Without changing the logic in the vessel service I decided to make so that reservations can only be processed once. The determinent of whether a reseravtion has been processed yet or not is whether its data entry is still in the cache. So if a release of confirm event is being processed it first tries to delete the reservation data from the cache. This is an atomic delete. The first event to delete the entry gets to process the event. The others see there is nothing to delete and return. This makes the cache idempotent. NOTE: There are two retry mechanisms at play for kafka events here. If we want kafka to replay the event we simply return an error from the handler and kafka will retry. If however state has changed in the processing of the event that needs to be persisted to the replay, we will capture that state in a new event and publish it. In the cache example, we might clear the cache and then fail when releasing vessel capacity. In this case we mark the event as cache cleared and republish so on the replay the cache lock check is skipped.

Event durability.

Here i am going to discuss some design decisions that were made, why they were made and why they might have been wrong. Still thinking about guranteeing at least once delivery i was concerned about what happens if the kafka broker is down for long enough that the reservation data entry also expired without the release event ever being published. To mitigate this i used the outbox pattern so that events would be persisted in the database before being published. However this shifts the dependancy from kafka to the database. Its probably unlikely that the broker or the database would be unoperational for longer than the delta between the reservation expiry and the reservation data expiry. Regardless, the choice made at the time was to bet on the database and use the outbox pattern. However this lead me to realise that i probably would have been better of just using a database rather than the cache in the first place. I am essentially adding a database outbox pattern to guarantee durability that a reservation database would have given out of the box. If I were to redesign i would just opt for the database and i would not bother using the outbox pattern in the reservation service. The outbox pattern is used to good effect in the consignment service but we will get to that later. 

With that said lets discuss the outbox pattern and issues it introduced. The outbox poller reads unpublished events from the outbox, publishes them to kafka and then marks the event as published. We still have to consider duplicate events here. If we publish the event but fail to mark it as published then the poller will publish it again next time around. In the case that the event being published is a reservation retry event with cache cleared set to true, it will skip the cache check and call the vessel service. So while the cache check gave some degree of idempotency, it wasn't complete. This lead me to the understanding that all event consumsers should be idempotent. To achieve this i added the following - The vessel service records each capacity operation (reserve, release, confirm) in a `capacity_operations` collection with a unique compound index on `(reservation_id, operation)`. Any duplicate call for the same reservation and operation is silently ignored. This is the final backstop.

The cache check is still useful as it catches most duplicates early, preventing unecessary processing and requests.

Another note on the outbox pattern. The outbox publisher reads unpublished events from Postgres, publishes them to Kafka, then marks them as published. Under a single instance this is safe. With multiple instances, two publishers could read the same rows simultaneously and publish duplicates before either marks them as published.

To prevent this, rows are claimed with a **30 second lease** using `SELECT FOR UPDATE SKIP LOCKED`. The first instance to acquire the lock owns the batch for 30 seconds. Other instances skip locked rows entirely.

The publisher is given a **20 second context deadline** — a hard outer bound ensuring it completes well within the lease window. If the deadline is exceeded, remaining events are left unpublished and retried on the next tick.

This project builds on a tutorial series that implements a basic microservice shipping application with a consignment and vessel service. My extension adds a full payment flow via a payment service and reservation service. The payment service is a mock implementation modelled on something like Stripe. The main engineering work is in the reservation service, the changes it drove in the consignment and vessel services, and the implementation of the confirm consignment SAGA.

Reservation Service
The problem. When a consignment is created, vessel capacity must be held against it before payment is confirmed. If payment fails or times out, that capacity must be released. This is a two-phase resource allocation problem: reserve optimistically, then confirm or release based on the payment outcome. Reservations have a TTL — if the consignment isn't confirmed within the window, capacity is returned automatically.

Storage. Redis is a natural fit for TTL-based expiry. Each reservation is stored as two entries:

A short-lived ID entry that defines the reservation lifetime
A longer-lived data entry holding the vessel ID and capacity figures needed to execute a release
When the ID entry expires, the reservation is void. Redis is configured with AOF persistence syncing every second.

Why events. Confirm and release operations are handled via Kafka events rather than synchronous RPC. The core reason is durability: if the cleanup job repeatedly fails to release an expired reservation, the data eventually expires from the cache and the release opportunity is lost permanently. Modelling the intent to release as a Kafka event means it survives beyond the cache TTL. The consignment service also needs to trigger confirms asynchronously, so both paths follow the same event-driven model.

Idempotency. Kafka's at-least-once delivery guarantee means consumers must tolerate duplicate events — a double-processed confirm or release would corrupt vessel capacity. Three layers of protection are in place:

Cache as distributed lock. On arrival, the handler atomically deletes the reservation data entry. The first event to delete it proceeds; duplicates find nothing and return early.

State-carrying events. If the handler clears the cache then fails before completing the vessel call, a redeliver hits an empty cache and exits early — skipping the vessel call. The fix is to publish a new event with cache_cleared: true on failure, bypassing the cache check on replay and preserving processing state across retry boundaries.

Consumer idempotency. The complete guarantee comes from idempotency at the vessel service. Every capacity operation is recorded in a capacity_operations collection with a unique compound index on (reservation_id, operation). Any duplicate is silently ignored. The cache check still catches most duplicates early, but correctness is guaranteed by the idempotent consumer — not the cache.

Event durability — and an honest design reflection. There is a failure window where the broker is unavailable for longer than the delta between the ID entry and data entry TTLs — a release event may never be published and the data needed to construct it is gone. To close this I introduced the transactional outbox pattern — events are written to Postgres before being published to Kafka, surviving broker downtime.

In practice this shifted the dependency from Kafka to the database. But implementing it revealed a more fundamental issue: I was adding a persistent store to compensate for the cache's lack of durability. The right design is a database from the start — reservations stored in Postgres with a valid_until column, expiry handled by a scheduled scan. That gives durability natively without the cache-plus-outbox complexity. If redesigning, I would drop Redis and drop the outbox in the reservation service entirely. The outbox earns its place in the consignment service, which I'll cover next.

Outbox at scale. With multiple service instances, two pollers could read the same unpublished rows simultaneously and produce duplicates before either marks them as published. Rows are claimed with a 30-second lease via SELECT FOR UPDATE SKIP LOCKED — the first instance to acquire the lock owns the batch, others skip locked rows. The publisher runs under a 20-second context deadline as a hard bound within the lease window.
