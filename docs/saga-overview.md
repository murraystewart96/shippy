# Shippy Payment Saga — Technical Overview

Shippy uses a choreographed saga to coordinate consignment confirmation across three services: the Consignment Service (CS), Reservation Service (RS), and Vessel Service (VS). The saga begins when a payment is authorised and ends when vessel capacity is confirmed and the consignment marked as confirmed — or the payment is refunded and the consignment cancelled.

## Pattern

Services communicate via Kafka topics. No central orchestrator exists — each service reacts to events and publishes its own. The outbox pattern is used throughout: events are written to a DB outbox table within the same transaction as state changes, and a relay process publishes them to Kafka. This guarantees at-least-once delivery; all consumers are idempotent to handle duplicate events.

```mermaid
flowchart TD
    classDef success fill:#16a34a,stroke:#15803d,color:#fff
    classDef failure fill:#dc2626,stroke:#b91c1c,color:#fff
    classDef dlq fill:#ea580c,stroke:#c2410c,color:#fff
    classDef atomic fill:#7c3aed,stroke:#6d28d9,color:#fff
    classDef process fill:#2563eb,stroke:#1d4ed8,color:#fff
    classDef skip fill:#6b7280,stroke:#4b5563,color:#fff

    subgraph MAIN["Main Saga Flow"]
        A([Consignment created\nstatus: pending])
        A --> C[Capture payment\nwith retries]
        C -- fail, retries exhausted --> DLQ1[consignment.confirmation.failed]
        DLQ1 --> CANCEL1([Refund + Cancel])
        C -- success --> G["Atomic TX\n• write payment_id\n• status → confirmation_pending\n• outbox: payment.captured"]
        G -- TX fail → republish PaymentCaptured=true --> C
        G -- success --> J[RS: receive payment.captured]
        J --> K{Cache entry\nfound?}
        K -- no → duplicate, skip --> DONE0([done])
        K -- yes --> N[ConfirmCapacity on vessel\nwith retries]
        N -- fail, retries exhausted --> Q[reservation.capacity.failed DLQ]
        Q --> R[ReleaseCapacity on vessel]
        R --> S[consignment.confirmation.failed]
        S --> CANCEL2([Refund + Cancel])
        N -- success --> U[scheduleReservationConfirmed\nwith republish on failure]
        U --> X([status → confirmed ✓])
    end

    subgraph EXPIRY["Expiry Path — status still pending"]
        EXP1([Reservation TTL expires])
        EXP1 --> EXP2{payment_id\npresent?}
        EXP2 -- yes --> EXP3[Refund payment_id]
        EXP2 -- no --> EXP4[No payment action]
        EXP3 & EXP4 --> EXP5([status → cancelled])
    end

    class X success
    class CANCEL1,CANCEL2,EXP5 failure
    class DLQ1,Q,S dlq
    class G atomic
    class C,J,N,R,U,EXP3 process
    class DONE0 skip
```

---

## Consignment Status

The consignment status acts as the saga state machine:

- `pending` — created, awaiting payment capture
- `confirmation_pending` — payment captured, reservation / vessel capacity confirmation in flight
- `confirmed` — saga completed successfully
- `cancelled` — saga rolled back

```mermaid
stateDiagram-v2
    classDef confirmed fill:#16a34a,color:#fff,font-weight:bold
    classDef cancelled fill:#dc2626,color:#fff,font-weight:bold
    classDef inflight fill:#7c3aed,color:#fff,font-weight:bold
    classDef pending fill:#2563eb,color:#fff,font-weight:bold

    [*] --> pending : consignment created
    pending --> confirmation_pending : payment captured (atomic tx)
    pending --> cancelled : reservation expired before capture
    confirmation_pending --> confirmed : reservation.confirmed received
    confirmation_pending --> cancelled : confirmation failed (DLQ)
    confirmed --> [*]
    cancelled --> [*]

    class confirmed confirmed
    class cancelled cancelled
    class confirmation_pending inflight
    class pending pending
```

---

## Happy Path

CS receives `consignment.payment.authorised` and captures the payment via a synchronous call to the Payment Service. On success, CS atomically transitions status to `confirmation_pending` and publishes `consignment.payment.captured`. This atomic write is the saga's point of no return — once committed, the saga must run to completion or be explicitly compensated.

RS consumes `consignment.payment.captured`, confirms capacity on VS, and publishes `reservation.confirmed`. CS consumes `reservation.confirmed` and sets status to `confirmed`.

```mermaid
sequenceDiagram
    participant CS as Consignment Service
    participant PS as Payment Service
    participant RS as Reservation Service
    participant VS as Vessel Service

    rect rgb(37, 99, 235)
        Note over CS: status: pending
        CS->>PS: Capture(auth_id, idempotency_key)
        PS-->>CS: payment_id
    end
    rect rgb(124, 58, 237)
        CS->>CS: atomic tx: status → confirmation_pending<br/>outbox: consignment.payment.captured
    end
    rect rgb(37, 99, 235)
        CS-)RS: consignment.payment.captured
        RS->>RS: delete reservation from cache
        RS->>VS: ConfirmCapacity(reservation_id)
        VS-->>RS: ok
        RS-)CS: reservation.confirmed
    end
    rect rgb(22, 163, 74)
        CS->>CS: status → confirmed ✓
    end
```

---

## Failure Handling

**ConfirmCapacity fails** — RS retries internally. On retry exhaustion it releases the vessel capacity, then publishes `consignment.confirmation.failed`. CS consumes this, refunds the payment, and cancels the consignment.

```mermaid
sequenceDiagram
    participant CS as Consignment Service
    participant PS as Payment Service
    participant RS as Reservation Service
    participant VS as Vessel Service

    rect rgb(37, 99, 235)
        CS-)RS: consignment.payment.captured
        RS->>RS: delete reservation from cache
        RS->>VS: ConfirmCapacity(reservation_id)
        VS-->>RS: error
        RS-)RS: reservation.capacity.confirm (retry)
    end
    rect rgb(234, 88, 12)
        Note over RS,VS: retries exhaust (maxRetries reached)
        RS-)RS: reservation.capacity.failed (DLQ)
        RS->>VS: ReleaseCapacity(reservation_id)
        RS-)CS: consignment.confirmation.failed
    end
    rect rgb(220, 38, 38)
        CS->>PS: Refund(payment_id)
        PS-->>CS: ok
        CS->>CS: status → cancelled
    end
```

**Payment capture fails** — CS retries internally. On exhaustion the event routes to `consignment.confirmation.failed`, payment is voided and the consignment is cancelled.

**Reservation expires while `pending`** — CS cancels the consignment and refunds if payment was captured. If the consignment is already `confirmation_pending` when expiry fires, CS does nothing — the saga is in flight and will resolve itself.

```mermaid
sequenceDiagram
    participant RS as Reservation Service
    participant CS as Consignment Service
    participant PS as Payment Service

    rect rgb(234, 88, 12)
        Note over RS: reservation TTL expires
        RS->>RS: detect expired reservation
        RS-)CS: reservation.expired
    end
    rect rgb(37, 99, 235)
        CS->>CS: check status = pending
    end
    alt payment_id present (payment captured)
        rect rgb(220, 38, 38)
            CS->>PS: Refund(payment_id)
            PS-->>CS: ok
            CS->>CS: status → cancelled
        end
    else no payment_id
        rect rgb(220, 38, 38)
            Note over CS: no payment action needed
            CS->>CS: status → cancelled
        end
    end
```

---

## Compensating Transactions

Each saga step that produces an external side effect has a corresponding compensating transaction. Steps 1 and 3 are the critical compensations — real external side effects (money and capacity). Steps 2 and 5 are pure DB state managed as a consequence of those compensations.

| # | Step | Owner | Compensating Transaction | Triggered By |
|---|------|-------|--------------------------|--------------|
| 1 | Capture payment | CS | Refund(`payment_id`) | `payment.captured` exhausts retries → DLQ and refunded; or reservation expires with `payment_id` present |
| 2 | status → `confirmation_pending` + publish `payment.captured` | CS | status → `cancelled` | Flows from step 3 compensation |
| 3 | ConfirmCapacity on vessel | RS | ReleaseCapacity on vessel | Retry exhaustion → DLQ |
| 4 | Publish `reservation.confirmed` | RS | Re-publish `reservation.confirmed` | Reconciliation job detects stuck `confirmation_pending` |
| 5 | status → `confirmed` | CS | — terminal, no compensation | — |

---

## Reconciliation (future improvement)

A periodic reconciliation job will catch consignments stuck in `confirmation_pending`. Rather than inferring state from event ordering, it queries VS directly — VS is the authoritative source of truth for what actually happened to a reservation. If VS recorded a confirm, the job re-triggers confirmation. If VS recorded a release, the job refunds and cancels. If the reservation was neither confirmed nor cancelled, investigation and event sourcing would be required.

The broker outage scenario below illustrates why the reconciliation job is necessary — the saga can reach a state it cannot self-recover from without external intervention.

```mermaid
sequenceDiagram
    participant CS as Consignment Service
    participant K as Kafka
    participant RS as Reservation Service
    participant VS as Vessel Service
    participant PS as Payment Service
    participant Recon as Reconciliation Job

    rect rgb(124, 58, 237)
        CS->>CS: atomic tx commits:<br/>status → confirmation_pending<br/>payment_id written<br/>outbox: consignment.payment.captured
    end
    rect rgb(220, 38, 38)
        Note over K: broker goes down
        Note over K: reservation TTL expires
        K->>RS: reservation.expired
        RS->>VS: ReleaseCapacity(reservation_id)
        RS->>RS: clear cache
        K->>CS: reservation.expired
        CS->>CS: status = confirmation_pending<br/>→ do nothing, leave status untouched
    end
    rect rgb(234, 88, 12)
        Note over K: broker recovers
        K->>RS: consignment.payment.captured
        RS->>RS: cache already gone<br/>→ skip (treated as duplicate)
        Note over CS: stuck: confirmation_pending
    end
    rect rgb(37, 99, 235)
        Recon->>VS: QueryReservationOperations(reservation_id)
        alt confirm operation exists
            VS-->>Recon: confirm
            Recon-)CS: reservation.confirmed
            CS->>CS: status → confirmed ✓
        else release operation exists
            VS-->>Recon: release
            Recon->>PS: Refund(payment_id)
            PS-->>Recon: ok
            Recon->>CS: status → cancelled
        end
    end
```

---

## Known Limitations

If the Kafka broker is down for longer than the reservation TTL, a race exists where both the expiry and payment capture events publish after recovery. The outcome depends on which the RS processes first, and in the worst case a captured payment may have no automated refund path. This is an accepted limitation given the operational probability involved.

See [known-limitations.md](known-limitations.md) for the full list of accepted limitations.
