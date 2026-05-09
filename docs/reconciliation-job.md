# Consignment Reconciliation Job

Catches consignments stuck after payment capture due to outage scenarios. Runs periodically (e.g. every 5 minutes) against the consignment DB.

## Prerequisites

The consignment record must carry:
- `payment_id` — written atomically with status → `confirmation_pending` and the outbox write

The vessel service must expose a `QueryReservationOperations` gRPC endpoint (see saga-implementation-todos.md) so the reconciliation job can determine what actually happened to a reservation.

---

## Scenario — Stuck in `confirmation_pending`

**Condition:** status = `confirmation_pending` AND stuck for > grace period

**Action:** Query the vessel service for the reservation's operations, then branch:

| Vessel says | Meaning | Action |
|---|---|---|
| `confirm` operation exists | RS confirmed capacity; `reservation.confirmed` is in flight or outbox write failed | Re-publish `reservation.confirmed`, confirm the consignment. Do NOT refund. |
| `release` operation exists, no `confirm` | RS released capacity (expiry won the race) | Refund (`payment_id`) + cancel. |
| Neither exists | Something failed before the vessel was reached | Investigate — ALERT for manual intervention. |

The vessel is the ground truth for what the RS actually did, regardless of which Kafka events arrived at CS or in what order.

---

## Known limitations

If the reconciliation job's refund call fails (e.g. sustained payment service outage), an ALERT should fire for manual intervention. The `payment_id` on the consignment record is the handle needed for a manual refund.
