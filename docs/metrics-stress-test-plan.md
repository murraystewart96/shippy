# Metrics & Stress Test Plan

## Goal
Instrument the saga pipeline with Prometheus metrics, stress test under load with k6,
identify bottlenecks using Grafana + Jaeger, implement a performance improvement, and
demonstrate the before/after on the same dashboard.

## Tasks

- [ ] Add Prometheus + Grafana to docker-compose
- [ ] Instrument `saga_duration_seconds` histogram in `handleReservationConfirmedEvent` and `handleFailedConfirmationEvent`
- [ ] Instrument `saga_total` counter with `status` label (`confirmed` / `cancelled` / `failed`)
- [ ] Instrument `outbox_pending_events` gauge
- [ ] Build Grafana dashboard showing p50/p95/p99 saga duration, throughput, error rate
- [ ] Write k6 load test script
- [ ] Run baseline stress test and capture metrics
- [ ] Identify bottleneck from Grafana + Jaeger
- [ ] Implement performance improvement
- [ ] Re-run stress test and compare before/after

## Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `saga_duration_seconds` | Histogram | — | Time from `ConfirmConsignment` to terminal state |
| `saga_total` | Counter | `status` (confirmed/cancelled/failed) | Saga throughput and failure rate |
| `outbox_pending_events` | Gauge | `service` | Unpublished outbox events — indicates backpressure |

## Stack

| Component | Purpose |
|---|---|
| Prometheus | Scrapes metrics from services |
| Grafana | Dashboards and visualisation |
| k6 | Load generation |
| Jaeger | Trace-level bottleneck investigation |

## Expected Bottlenecks

In order of likelihood under load:

1. **MongoDB transaction contention** — `WithTransaction` in `handlePaymentAuthorisedEvent` under concurrent confirms
2. **Outbox poll interval** — fixed polling cadence causes saga duration to grow linearly with load
3. **Postgres `SELECT FOR UPDATE SKIP LOCKED`** — reservation service outbox under high throughput


RESULT

Write conflict in vessel service

2026/05/12 08:48:39 ReserveCapacity: failed for reservation_id=f23ff770-dff9-4f3a-ac89-96dbca0b166a: (WriteConflict) Caused by :: Write conflict during plan execution and yielding is disabled. :: Please retry your operation or multi-document transaction.
2026/05/12 08:48:39 ReserveCapacity: failed for reservation_id=e2cb2634-e60b-4854-afba-e2bc947d3ada: (WriteConflict) Caused by :: Write conflict during plan execution and yielding is disabled. :: Please retry your operation or multi-document transaction.
2026/05/12 08:48:39 ReserveCapacity: failed for reservation_id=9f018972-3d2a-4ffb-bdfd-7eae5df82013: (WriteConflict) Caused by :: Write conflict during plan execution and yielding is disabled. :: Please retry your operation or multi-document transaction.

Caused by MongoDBs Optimistic concurrency control. Validates at commit time. Processed are racing to write first. If another process has modified the document it aborts.

Solution steps.
- add backoff (examine where backoff is being used in the project and on what codes we should backoff on). maybe for conflicting writes we need to return a specific error so the client knows to retry. (make sure we have jitter)

See how having multiple vessels affects things. Backoff should we enough to fix temporary contention.

FIXED 

backoff fixed it. All reservations were confirmed

TODO - add graphs again
