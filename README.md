*This project builds on a [tutorial series](https://web.archive.org/web/20220124115000/https://ewanvalentine.io/microservices-in-golang-part-1/) that implements a basic microservice shipping application with a consignment and vessel service. My extension adds an API gateway and a full payment flow via a payment service and reservation service. The payment service is a mock implementation modelled on something similar to Stripe. The main engineering work is in the reservation service and the changes it drove in the consignment and vessel services, and the implementation of the confirm consignment SAGA.*

---

## Services

| Service | Language | Role |
|---------|----------|------|
| `gateway` | Go / Gin | HTTP API -> gRPC, JWT auth |
| `consignment-service` | Go / gRPC | Consignment lifecycle |
| `reservation-service` | Go / gRPC | Vessel capacity reservations |
| `vessel-service` | Go / gRPC | Vessel registry |
| `payment-service` | Go / gRPC | Mock payment processor (authorise → capture) |
| `user-service` | Go / gRPC | User registration and JWT issuance |

---

## Running locally

### Docker Compose

**Prerequisites:** Docker and Docker Compose.

```bash
docker-compose up --build
```

### Endpoints

| Endpoint | Purpose |
|----------|---------|
| `localhost:8080` | HTTP gateway |
| `localhost:3000` | Grafana (dashboards + Tempo trace search) |

**Load test** (requires [k6](https://k6.io)):

```bash
k6 run scripts/k6_load_test.js
```

### Kubernetes (kind)

**Prerequisites:** Docker, [kind](https://kind.sigs.k8s.io/), kubectl.

```bash
make cluster-create  # create a local kind cluster
make build           # build all Docker images
make load            # load images into the cluster
make deploy          # deploy services, Kafka, and observability stack
```

In one terminal, keep the gateway exposed:

```bash
make forward         # localhost:8080 → gateway
```

Then verify end-to-end:

```bash
make smoke           # creates and confirms 5 consignments
```

To access Grafana:

```bash
make forward-grafana # localhost:3000 → Grafana (admin/admin)
```

---

## Reservation Service

**The problem.** When a consignment is created, vessel capacity must be held against it before payment is confirmed. If payment fails or times out, that capacity must be released. This is a **two-phase resource allocation** problem: reserve optimistically, then confirm or release based on the payment outcome. Reservations have a TTL — if the consignment isn't confirmed within the window, capacity is returned automatically.

**Storage.** Redis is a natural fit for **TTL-based expiry**. Each reservation is stored as two entries:
- A short-lived **ID entry** that defines the reservation lifetime
- A longer-lived **data entry** holding the vessel ID and capacity figures needed to execute a release

NOTE: redis configured with append only and syncs everysec to persist reservations.

| Key | TTL | Purpose |
|-----|-----|---------|
| `reservation:{id}` | 10 min | TTL marker — absence signals expiry |
| `reservation_data:{id}` | 30 min | Holds vessel and consignment ID, weight, containers |

When the ID entry expires, the reservation is void. Redis is configured with **AOF persistence** syncing every second.

**Why events.** Confirm and release operations are handled via **Kafka events** rather than synchronous RPC. The core reason is durability: if the cleanup job repeatedly fails to release an expired reservation, the data eventually expires from the cache and the release opportunity is lost permanently. Modelling the intent to release as a Kafka event means it survives beyond the cache TTL. The consignment service also needs to trigger confirms asynchronously, so both paths follow the same event-driven model.

**Idempotency.** Kafka's **at-least-once delivery** guarantee means consumers must tolerate duplicate events — a double-processed confirm or release would corrupt vessel capacity. Three layers of protection are in place:

1. **Cache as distributed lock.** On arrival, the handler atomically deletes the reservation data entry. The first event to delete it proceeds; duplicates find nothing and return early.

2. **State-carrying events.** If the handler clears the cache then fails before completing the vessel call, a redeliver hits an empty cache and exits early — skipping the vessel call. The fix is to publish a new event with `cache_cleared: true` on failure, bypassing the cache check on replay and preserving processing state across retry boundaries.

3. **Consumer idempotency.** The complete guarantee comes from idempotency at the vessel service. Every capacity operation is recorded in a `capacity_operations` collection with a **unique compound index on `(reservation_id, operation)`**. Any duplicate is silently ignored. The cache check still catches most duplicates early, but correctness is guaranteed by the idempotent consumer — not the cache.

**Event durability — and an honest design reflection.** There is a failure window where the broker is unavailable for longer than the delta between the ID entry and data entry TTLs — a release event may never be published and the data needed to construct it is gone. To close this I introduced the **transactional outbox pattern** — events are written to Postgres before being published to Kafka, surviving broker downtime.

In practice this shifted the dependency from Kafka to the database. But implementing it revealed a more fundamental issue: I was adding a persistent store to compensate for the cache's lack of durability. The right design is a **database from the start** — reservations stored in Postgres with a `valid_until` column, expiry handled by a scheduled scan. That gives durability natively without the cache-plus-outbox complexity. If redesigning, I would drop Redis and drop the outbox in the reservation service entirely. The outbox pattern is used to good effect in the consignment service, where a consignment status update and the payment captured event publication needed to be made atomic.

**Outbox at scale.** With multiple service instances, two pollers could read the same unpublished rows simultaneously and produce duplicates before either marks them as published. Rows are claimed with a **30-second lease** via `SELECT FOR UPDATE SKIP LOCKED` — the first instance to acquire the lock owns the batch, others skip locked rows. The publisher runs under a **20-second context deadline** as a hard bound within the lease window. This prevents overlapping with another consumers lease window.

---

## Consignment Confirmation SAGA

After a user has created a consignment they need to confirm it. Confirmation is made up of payment authorisation and capture, reservation and vessel capacity confirmation, and consignment status update. To coordinate these actions across services I used a **choreography-based SAGA**. Choreography was chosen over orchestration because the flow is straightforward, the number of services is small, and there is no need for a central coordinator adding latency and a single point of failure.

For a detailed view of the confirmation SAGA including sequence diagrams and compensating transactions see [saga-overview.md](docs/saga-overview.md).

## Observability

Because the saga spans multiple services and hops through Kafka, no single service has a complete view of a confirmation in flight. Two complementary layers of observability are in place.

**Distributed tracing.** OpenTelemetry traces are propagated across gRPC calls and Kafka messages via header injection, so a single trace in Tempo covers the full saga from `ConfirmConsignment` to terminal state regardless of which service handled each step.

**Metrics.** Two Prometheus metrics instrument the saga pipeline:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `saga_duration_seconds` | Histogram | — | End-to-end duration from `ConfirmConsignment` to terminal state |
| `saga_total` | Counter | `status` (confirmed / cancelled) | Throughput and cancellation rate |

A Grafana dashboard surfaces p50/p95/p99 saga duration, throughput, and cancellation rate in a single view.

**Stress testing and a real finding.** A k6 script hammered the system with concurrent consignment confirmations. The Grafana dashboard immediately showed a non-zero cancellation rate under load — a small but consistent percentage of sagas were being cancelled rather than confirmed. Tracing a failed saga in Tempo pointed to the reservation service: the `ConfirmCapacity` gRPC call to the vessel service was failing under concurrency.

The root cause was a **hot document problem**. With only a handful of vessels in the test dataset, many concurrent confirmations were contending on the same vessel document in MongoDB, causing write conflicts. The reservation service had no backoff on the vessel call — a single failure went straight to the retry queue.

Adding **exponential backoff** to the `ConfirmCapacity` call absorbed the transient contention and the cancellation rate dropped to zero under the same load.

---

## Known limitations

See [docs/known-limitations.md](docs/known-limitations.md).

## Future improvements

See [docs/future-improvements.md](docs/future-improvements.md).

