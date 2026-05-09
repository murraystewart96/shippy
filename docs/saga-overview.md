# Shippy Payment Saga — Technical Overview

Shippy uses a choreographed saga to coordinate consignment confirmation across three services: the Consignment Service (CS), Reservation Service (RS), and Vessel Service (VS). The saga begins when a payment is authorised and ends when vessel capacity is confirmed and the consignment marked as confirmed — or the payment is refunded and the consignment cancelled.

## Pattern

Services communicate via Kafka topics. No central orchestrator exists — each service reacts to events and publishes its own. The outbox pattern is used throughout: events are written to a DB outbox table within the same transaction as state changes, and a relay process publishes them to Kafka. This guarantees at-least-once delivery; all consumers are idempotent to handle duplicate events.

## Consignment Status

The consignment status acts as the saga state machine:

- `pending` — created, awaiting payment capture
- `confirmation_pending` — payment captured, vessel capacity confirmation in flight
- `confirmed` — saga completed successfully
- `cancelled` — saga rolled back

## Happy Path

CS receives `consignment.payment.authorised` and captures the payment via a synchronous call to the Payment Service. On success, CS atomically transitions status to `confirmation_pending` and publishes `consignment.payment.captured`. This atomic write is the saga's point of no return — once committed, the saga must run to completion or be explicitly compensated.

RS consumes `consignment.payment.captured`, confirms capacity on VS, and publishes `reservation.confirmed`. CS consumes `reservation.confirmed` and sets status to `confirmed`.

## Failure Handling

**ConfirmCapacity fails** — RS retries internally. On retry exhaustion it releases the vessel capacity, then publishes `consignment.confirmation.failed`. CS consumes this, refunds the payment, and cancels the consignment.

**Payment capture fails** — CS retries internally. On exhaustion the event routes to `consignment.confirmation.failed` and the consignment is cancelled. No refund is needed as capture never completed.

**Reservation expires while `pending`** — CS cancels the consignment and refunds if payment was captured. If the consignment is already `confirmation_pending` when expiry fires, CS does nothing — the saga is in flight and will resolve itself.

## Reconciliation

A periodic reconciliation job catches consignments stuck in `confirmation_pending`. Rather than inferring state from event ordering, it queries VS directly — VS is the authoritative source of truth for what actually happened to a reservation. If VS recorded a confirm, the job re-triggers confirmation. If VS recorded a release, the job refunds and cancels. If the reservation was neither confirmed or cancelled investigation and event sourcing would be required. 

## Known Limitations

If the Kafka broker is down for longer than the reservation TTL, a race exists where both the expiry and payment capture events publish after recovery. The outcome depends on which the RS processes first, and in the worst case a captured payment may have no automated refund path. This is an accepted limitation given the operational probability involved.
