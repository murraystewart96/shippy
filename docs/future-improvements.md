# Future Improvements

## Reconciliation job

A background job that periodically scans for consignments stuck in `confirmation_pending` and reconciles them against the vessel and payment services. This handles the edge case where a broker or DB outage causes the saga to stall with no automated recovery path.

See [reconciliation-job.md](reconciliation-job.md) for the proposed design.

---

## Email / notification service

A Kafka consumer that listens for terminal saga events (`consignment.confirmed`, `consignment.cancelled`) and sends email notifications to users. Straightforward to implement — the interesting reliability concerns (retry, deduplication) are already solved by the outbox pattern used elsewhere.

---

## DLQ alerting

A `shippy_dlq_total` Prometheus counter (labelled by `action` and `service`) incremented whenever an event exhausts retries and hits the dead letter queue. A Grafana alert on `rate(shippy_dlq_total[5m]) > 0` would catch the RELEASE capacity exhaustion case in particular — where vessel capacity becomes permanently understated with no automated recovery path.

---

## Outbox cleanup

Published outbox events are never deleted. A periodic job deleting events where `published_at < now() - 24h` would keep both the MongoDB (consignment service) and Postgres (reservation service) outbox tables bounded.

See [known-limitations.md](known-limitations.md) for details.

---

## Kafka producer delivery confirmation

The outbox publisher currently calls `MarkPublished` before Kafka confirms delivery. Switching to a synchronous delivery report (passing a delivery channel to confluent-kafka-go's `Produce` and waiting for acknowledgement) would eliminate the window where an event is marked published but never actually delivered.

See [known-limitations.md](known-limitations.md) for details.

---

## Reduce outbox poll latency

The saga end-to-end duration is bounded below by `2 × outbox_poll_interval` due to two outbox hops in the happy path. Options:
- Reduce the poll interval (simple, increases DB load)
- Replace polling with change-data-capture (CDC) on the outbox table for near-instant event delivery
