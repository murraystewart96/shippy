# Metrics & Stress Test Plan

## Goal
Instrument the saga pipeline with Prometheus metrics, stress test under load with k6,
identify bottlenecks using Grafana + Jaeger, implement a performance improvement, and
demonstrate the before/after on the same dashboard.

## Tasks

- [ ] Add Prometheus + Grafana to docker-compose
- [ ] Instrument `saga_duration_seconds` histogram in `handleReservationConfirmedEvent` and `handleFailedConfirmationEvent`
- [ ] Instrument `saga_total` counter with `status` label (`confirmed` / `cancelled` / `failed`)
- [ ] Build Grafana dashboard showing p50/p95/p99 saga duration, throughput, error rate
- [ ] Write k6 load test script
- [ ] Run baseline stress test and capture metrics
- [ ] Identify bottleneck from Grafana + Tempo
- [ ] Implement performance improvement
- [ ] Re-run stress test and compare before/after

## Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `saga_duration_seconds` | Histogram | — | Time from `ConfirmConsignment` to terminal state |
| `saga_total` | Counter | `status` (confirmed/cancelled/failed) | Saga throughput and failure rate |

## Stack

| Component | Purpose |
|---|---|
| Prometheus | Scrapes metrics from services |
| Grafana | Dashboards and visualisation |
| k6 | Load generation |
| Tempo | Trace-level bottleneck investigation |

## Expected Bottlenecks

In order of likelihood under load:

1. **MongoDB transaction contention** — `WithTransaction` in `handlePaymentAuthorisedEvent` under concurrent confirms
2. **Outbox poll interval** — fixed polling cadence causes saga duration to grow linearly with load
3. **Postgres `SELECT FOR UPDATE SKIP LOCKED`** — reservation service outbox under high throughput

