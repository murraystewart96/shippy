# Known Limitations

## 1. Unbounded outbox tables

Published outbox events are never deleted. Both the consignment service (MongoDB) and reservation service (Postgres) stamp `published_at` on delivery but have no cleanup job or TTL. Under sustained load the tables grow indefinitely.

**Mitigation:** A periodic delete of published events older than a retention window (e.g. 24h) would suffice. Not implemented — correctness is unaffected; stale rows are inert.

---

## 2. Outbox poller latency floor

Saga duration is bounded below by the outbox poll interval. The saga has two outbox hops (CS → RS → CS), so end-to-end latency is at minimum `2 × poll_interval` regardless of how fast individual steps process. At the current 5s interval, the saga floor is ~10s.

**Mitigation:** Reduce the poll interval, or replace polling with change-data-capture (CDC) on the outbox table.

---

## 3. Redis delete and outbox write are not atomic

In `processCapacityEvent`, the Redis reservation data is deleted before the outbox event is written to Postgres. These are separate storage systems and cannot participate in the same transaction. If the process crashes after the Redis delete but before the vessel gRPC call and outbox write, the vessel capacity will not be confirmed and `ReservationConfirmed` is never published and the saga stalls in `confirmation_pending` permanently — the retry will hit the already-deleted cache entry and return early as a duplicate.

**Mitigation:** The reconciliation job described in [reconciliation-job.md](reconciliation-job.md) would detect and recover these stalled sagas. The correct long-term fix is to move reservation data from Redis into Postgres, at which point the delete and outbox write become a single atomic transaction. 

---

## 4. Captured payment with no refund path under extreme outage

Two races exist when outage duration exceeds the reservation TTL:

**Broker outage:** CS captures payment and writes `payment.captured` to its outbox, but the broker is down. The reservation expires. When the broker recovers both events publish — if `payment.captured` arrives first, capacity is confirmed; `reservation.expired` then arrives and is silently skipped as a duplicate. Payment is captured with no refund.

**DB outage:** Payment is captured via gRPC, but the DB goes down before the atomic transaction commits (`payment_id` write + status update + outbox event). The consignment stays in `pending` with no `payment_id`. When the reservation expires, compensation cannot distinguish "authorised but not captured" from "captured but not recorded" — a void is attempted on an already-captured payment.

**Mitigation:** Both require outage durations longer than the reservation TTL, making them low-probability in practice. The pragmatic future fix is a reconciliation job that queries the payment service for consignments in ambiguous states and issues refunds where capture is confirmed. Documented in [reconciliation-job.md](reconciliation-job.md).

---
