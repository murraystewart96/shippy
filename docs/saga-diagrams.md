# Shippy Payment Saga — Diagrams

## Full Saga Overview

```mermaid
flowchart TD
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
```

---

## Consignment Status State Machine

```mermaid
stateDiagram-v2
    [*] --> pending : consignment created
    pending --> confirmation_pending : payment captured (atomic tx)
    pending --> cancelled : reservation expired before capture
    confirmation_pending --> confirmed : reservation.confirmed received
    confirmation_pending --> cancelled : confirmation failed (DLQ)
    confirmed --> [*]
    cancelled --> [*]
```

---

## Happy Path

```mermaid
sequenceDiagram
    participant CS as Consignment Service
    participant PS as Payment Service
    participant RS as Reservation Service
    participant VS as Vessel Service

    Note over CS: status: pending
    CS->>PS: Capture(auth_id, idempotency_key)
    PS-->>CS: payment_id
    CS->>CS: atomic tx: status → confirmation_pending<br/>outbox: consignment.payment.captured
    CS-)RS: consignment.payment.captured
    RS->>RS: delete reservation from cache
    RS->>VS: ConfirmCapacity(reservation_id)
    VS-->>RS: ok
    RS-)CS: reservation.confirmed
    CS->>CS: status → confirmed
```

---

## Failure Path — ConfirmCapacity Exhausts Retries

```mermaid
sequenceDiagram
    participant CS as Consignment Service
    participant PS as Payment Service
    participant RS as Reservation Service
    participant VS as Vessel Service

    CS-)RS: consignment.payment.captured
    RS->>RS: delete reservation from cache
    RS->>VS: ConfirmCapacity(reservation_id)
    VS-->>RS: error
    RS-)RS: reservation.capacity.confirm (retry)
    Note over RS,VS: retries exhaust (maxRetries reached)
    RS-)RS: reservation.capacity.failed (DLQ)
    RS->>VS: ReleaseCapacity(reservation_id)
    RS-)CS: consignment.confirmation.failed
    CS->>PS: Refund(payment_id)
    PS-->>CS: ok
    CS->>CS: status → cancelled
```

---

## Failure Path — Reservation Expires Before `confirmation_pending`

```mermaid
sequenceDiagram
    participant CS as Consignment Service
    participant PS as Payment Service

    Note over CS: status: pending
    Note over CS: reservation TTL expires
    CS->>CS: check status = pending
    alt payment_id present (payment captured)
        CS->>PS: Refund(payment_id)
        PS-->>CS: ok
    else no payment_id
        Note over CS: no payment action needed
    end
    CS->>CS: status → cancelled
```

---

## Failure Path — Broker Outage After `confirmation_pending`, Reservation Expires

```mermaid
sequenceDiagram
    participant CS as Consignment Service
    participant K as Kafka
    participant RS as Reservation Service
    participant VS as Vessel Service
    participant PS as Payment Service
    participant Recon as Reconciliation Job

    CS->>CS: atomic tx commits:<br/>status → confirmation_pending<br/>payment_id written<br/>outbox: consignment.payment.captured
    Note over K: broker goes down
    Note over K: reservation TTL expires
    K->>RS: reservation.expired
    RS->>VS: ReleaseCapacity(reservation_id)
    RS->>RS: clear cache
    K->>CS: reservation.expired
    CS->>CS: status = confirmation_pending<br/>→ do nothing, leave status untouched
    Note over K: broker recovers
    K->>RS: consignment.payment.captured
    RS->>RS: cache already gone<br/>→ skip (treated as duplicate)
    Note over CS: stuck: confirmation_pending
    Recon->>VS: QueryReservationOperations(reservation_id)
    alt confirm operation exists
        VS-->>Recon: confirm
        Recon-)CS: reservation.confirmed
        CS->>CS: status → confirmed
    else release operation exists
        VS-->>Recon: release
        Recon->>PS: Refund(payment_id)
        PS-->>Recon: ok
        Recon->>CS: status → cancelled
    end
```
