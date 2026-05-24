# TODOs

## Observability

- [ ] Improve logging

- [ ] Add saga cancellation rate Grafana panel — `sum(rate(shippy_saga_total{status="cancelled"}[5m])) / sum(rate(shippy_saga_total[5m]))`
- [ ] Add `shippy_saga_errors_total` counter with `stage` and `error_class` labels; instrument at payment capture, vessel gRPC calls, and outbox writes
- [ ] Add saga error rate Grafana panel showing `errors / saga_total` normalised by throughput

## Known gaps to address

- [ ] Retry backoff too aggressive for external gRPC calls — 3 retries × 100ms exhausts in ~800ms, below typical cloud transient failure recovery time. Increase to 5 retries with 500ms initial interval.
- [ ] Refine backoff scenarios — skip retries for permanent gRPC errors (`InvalidArgument`, `NotFound`, `FailedPrecondition`, `PermissionDenied`, `Unimplemented`) using `backoff.Permanent`; consider treating `AlreadyExists` on ConfirmCapacity and `NotFound` on ReleaseCapacity as idempotent success.
- [ ] Add circuit breakers to vessel and payment gRPC clients to prevent retry storms during sustained outages.


## Tests

- [ ] RS gRPC handler unit tests: `ReserveCapacity` compensation path (cache store fails → release called)
- [ ] RS gRPC handler unit tests: `ConfirmCapacity` retry path (vessel call fails with backoff)


## Frontend

- [ ] Simple frontend for creating and confirming consignments
- [ ] WebSocket connection from gateway to frontend — push terminal saga state (`confirmed` / `cancelled`) to the client without polling
- [ ] CS publishes `consignment.status.updated` Kafka event at terminal saga steps; gateway consumes and fans out to the relevant WebSocket connection

