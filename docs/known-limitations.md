# Known Limitations

## 1. Unbounded outbox tables

Published outbox events are never deleted. Both the consignment service (MongoDB) and reservation service (Postgres) stamp `published_at` on delivery but have no cleanup job or TTL. Under sustained load the tables grow indefinitely.

**Mitigation:** A periodic delete of published events older than a retention window (e.g. 24h) would suffice. Not implemented — correctness is unaffected; stale rows are inert.

---

## 2. Outbox poller latency floor

Saga duration is bounded below by the outbox poll interval. The saga has two outbox hops (CS → RS → CS), so end-to-end latency is at minimum `2 × poll_interval` regardless of how fast individual steps process. At the current 5s interval, the saga floor is ~10s.

**Mitigation:** Reduce the poll interval, or replace polling with change-data-capture (CDC) on the outbox table.

---

## 4. Captured payment with no refund path under extreme outage

Two races exist when outage duration exceeds the reservation TTL (~300s):

**Broker outage:** CS captures payment and writes `payment.captured` to its outbox, but the broker is down. The reservation expires. When the broker recovers both events publish — if `payment.captured` arrives first, capacity is confirmed; `reservation.expired` then arrives and is silently skipped as a duplicate. Payment is captured with no refund.

**DB outage:** Payment is captured via gRPC, but the DB goes down before the atomic transaction commits (`payment_id` write + status update + outbox event). The consignment stays in `pending` with no `payment_id`. When the reservation expires, compensation cannot distinguish "authorised but not captured" from "captured but not recorded" — a void is attempted on an already-captured payment.

**Mitigation:** Both require outage durations longer than the reservation TTL, making them low-probability in practice. The pragmatic future fix is a reconciliation job that queries the payment service for consignments in ambiguous states and issues refunds where capture is confirmed. Documented in [reconciliation-job.md](reconciliation-job.md).

---

## 5. Single vessel is a write contention hotspot

All capacity operations (reserve, confirm, release) run MongoDB multi-document transactions that modify the same vessel document. Under concurrent load, transactions contend on this hot document and produce `WriteConflict` errors. Retry backoff at the caller mitigates this but does not eliminate it.

**Mitigation:** Sharding the vessel document (N shards each with 1/N capacity) reduces conflict probability proportionally. Not relevant at realistic shipping concurrency levels.

---

## 6. No reservation expiry → consignment cancellation path

When a reservation expires, the reservation service releases vessel capacity and publishes `reservation.expired`. The consignment service consumes this and cancels the consignment — but only if the consignment status is `pending`. If the consignment is in `confirmation_pending` when the reservation expires (payment captured, vessel confirm in-flight), there is no automated cancellation path.

**Mitigation:** The reconciliation job is intended to handle this case by querying the vessel service for the true state of the reservation. Documented in [reconciliation-job.md](reconciliation-job.md).
