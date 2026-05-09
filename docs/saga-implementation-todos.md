# Saga Implementation TODOs

## Consignment record — new fields

Add to the consignment schema:
- `payment_id` (string)

---

## Consignment service

### `handlePaymentAuthorisedEvent`

1. **Atomic transaction.** Wrap the outbox write (`publishPaymentCaptured`), the status update (`confirmation_pending`), and the `payment_id` write in a single DB transaction. On transaction failure, republish via `publishPaymentAuthorised` with `PaymentCaptured: true` rather than returning an error — this avoids Kafka re-delivering the original message (which carries `PaymentCaptured: false`) and re-running the capture unnecessarily.

### `handleExpiredReservationEvent`

2. **Status guard.** Only cancel if status is `pending`. If status is `confirmation_pending`, do nothing — the reconciliation job handles this case by querying the vessel service.

3. **Payment compensation.** When cancelling (status was `pending`):
   - `payment_id` present → refund
   - `payment_id` absent → no payment action needed (capture failure is handled by the `handlePaymentAuthorisedEvent` error path)

---

## Reservation service

### `handleFailedCapacityEvent`

4. **Propagate `notifyConfirmationFailed` errors.** Currently the handler returns `nil` if `notifyConfirmationFailed` fails, committing the Kafka offset and losing the refund signal. Return an error instead — `processCapacityEvent` (release) is idempotent via `ReservationId` so replaying is safe.

### `processCapacityEvent`

5. **Retry `scheduleReservationConfirmed`.** Currently logs an ALERT and returns `nil` on failure (offset committed, no retry). Add a republish via `publishRetryEvent` with `CacheCleared: true` on failure, consistent with the existing retry pattern, before falling back to ALERT.

---

## Vessel service

6. **Add `QueryReservationOperations` gRPC endpoint.** The vessel service already stores all capacity operations in `capacity_operations` (unique index on `reservation_id, operation`). Expose a query endpoint so the reconciliation job can ask "was this reservation confirmed or released?" — this makes the vessel the source of truth for what actually happened to a reservation.

---

## Reconciliation job

See [reconciliation-job.md](reconciliation-job.md).
