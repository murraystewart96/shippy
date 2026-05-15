# TODOs

## Observability

- [ ] Add saga cancellation rate Grafana panel — `sum(rate(shippy_saga_total{status="cancelled"}[5m])) / sum(rate(shippy_saga_total[5m]))`
- [ ] Add `shippy_saga_errors_total` counter with `stage` and `error_class` labels; instrument at payment capture, vessel gRPC calls, and outbox writes
- [ ] Add saga error rate Grafana panel showing `errors / saga_total` normalised by throughput

## Known gaps to address

- [ ] Retry backoff too aggressive for external gRPC calls — 3 retries × 100ms exhausts in ~800ms, below typical cloud transient failure recovery time. Increase to 5 retries with 500ms initial interval and add `backoff.WithJitter` to spread load
- [ ] RS `publishRetryEvent` failure — offset is now not committed (manual commit added) so Kafka redelivers, but redeliver hits the cache check and stalls if cache was already cleared. Alert needed: instrument `shippy_saga_errors_total{stage="capacity_retry_schedule", error_class="outbox_write_failed"}`

## Tests

- [ ] RS gRPC handler unit tests: `ReserveCapacity` compensation path (cache store fails → release called)
- [ ] RS gRPC handler unit tests: `ConfirmCapacity` retry path (vessel call fails with backoff)

## Event contracts

- [ ] Define explicit proto schemas for all SAGA integration events (`payment.authorised`, `payment.captured`, `reservation.confirmed`, `reservation.expired`, `consignment.confirmation.failed`) — currently events are ad-hoc Go structs serialised to JSON with no shared contract between producer and consumer. Protobuf schemas would enforce a contract, catch breaking changes at compile time, and remove the implicit coupling between services on JSON field names

## Frontend

- [ ] Simple frontend for creating and confirming consignments
- [ ] WebSocket connection from gateway to frontend — push terminal saga state (`confirmed` / `cancelled`) to the client without polling
- [ ] CS publishes `consignment.status.updated` Kafka event at terminal saga steps; gateway consumes and fans out to the relevant WebSocket connection

## Documentation

- [ ] README: story-led intro → architecture overview → saga deep dive (happy path + compensation) → observability → running locally → known limitations & future improvements
