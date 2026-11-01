# Booking and Availability Service API

Owner: Ayushi Shringi
Role: B — Booking & Availability
Status: draft for team review

## Scope

The Booking service lets a guest request a stay, lets the host approve or decline it, lets either side cancel, and is the single source of truth for which nights of a property are taken. It owns the MySQL tables `reservations`, `availability_blocks`, `reservation_status_history`, `idempotency_keys` and `processed_events`.

Booking is the project's main asynchronous flow (§6.1):

```text
UI ──POST /bookings/create──▶ Booking API ──booking.requested──▶ Kafka ──▶ Booking Worker ──▶ MySQL
 ▲                                                                              │
 └────────── WebSocket /bookings/stream ◀── booking.updated ◀── Kafka ◀─────────┘
```

The API accepts the request and returns `202 Accepted` immediately. The worker decides whether the dates can be held. The guest's screen updates over WebSocket when the decision is made.

The Property service's availability result is informational only. The Booking worker re-checks availability inside a database transaction, and that check is the only one that can hold dates.

## Shared Conventions

Final authentication, pagination, error-body, event-envelope, retry and dead-letter rules follow `docs/api/00_conventions.md`. Until C finalizes that file, this document follows the conventions already used in `docs/api/ai.md`, `docs/api/review.md` and `docs/api/user_property.md`:

- JSON bodies use `snake_case`; timestamps are UTC ISO 8601; dates are `YYYY-MM-DD`.
- `check_in` is inclusive and `check_out` is exclusive (a stay of 2026-11-20 → 2026-11-22 uses the nights of the 20th and 21st).
- IDs are server-generated: `res-` + ULID for reservations.
- Every endpoint requires `Authorization: Bearer <JWT>`. The guest or host identity always comes from the JWT subject, never from the request body.
- Mutating endpoints require an `Idempotency-Key: <uuid>` header.
- Kafka messages use the shared §6.2 envelope.

### Common Error Body

```json
{
  "error": {
    "code": "DATES_UNAVAILABLE",
    "message": "One or more requested nights are already booked.",
    "trace_id": "tr-01JBOOK001",
    "retryable": false,
    "details": {
      "property_id": "property-2001",
      "conflicting_nights": ["2026-11-21"]
    }
  }
}
```

## Why MySQL (§3.2)

- A booking is a money-and-calendar record: it needs ACID transactions so that holding the nights and changing the reservation status succeed or fail together.
- Double booking is prevented by a database constraint (a unique key on property + night), not by application code alone, so it holds even with several workers and concurrent requests.
- Status changes, refunds and dashboard counts need joins and aggregates (`GROUP BY` status, date ranges), which suit a relational schema.

## Reservation Status Lifecycle

```text
                    worker holds nights            host approves
 requested ───────────────────────────▶ pending_host ───────────▶ confirmed ───▶ completed
     │                                      │    │                    │        (after check_out,
     │ worker finds conflict                │    │ host declines      │         scheduled job)
     ▼                                      │    ▼                    │
  rejected                                  │  declined               │
                                            │                         │
                                            └──── guest/host cancels ─┴──▶ cancelled
```

| Status | Meaning | Nights held? |
|---|---|---|
| `requested` | Accepted by the API, waiting for the worker | No |
| `pending_host` | Worker held the nights; waiting for the host | Yes |
| `confirmed` | Host approved | Yes |
| `rejected` | Worker found a date conflict (system decision) | No |
| `declined` | Host declined | No (released) |
| `cancelled` | Guest or host cancelled | No (released) |
| `completed` | `check_out` has passed for a confirmed stay | Yes (history) |

Allowed transitions are exactly the arrows above. Any other transition returns `409 INVALID_STATUS_TRANSITION`. Every transition writes one row to `reservation_status_history` and publishes one `booking.updated` event.

`rejected` (system: dates taken) and `declined` (host chose no) are kept separate so the host dashboard can report host decisions without counting system conflicts.

## Data Model (MySQL)

### `reservations`

| Field | Type | Rule |
|---|---|---|
| `reservation_id` | VARCHAR(40) PK | `res-` + ULID |
| `property_id` | VARCHAR(40) | required; existing, `active` property |
| `guest_id` | VARCHAR(40) | from JWT |
| `host_id` | VARCHAR(40) | copied from the property, never from the client |
| `check_in` | DATE | inclusive; not in the past |
| `check_out` | DATE | exclusive; later than `check_in`; stay ≤ 30 nights |
| `nights` | INT | `check_out - check_in` |
| `guest_count` | INT | 1 to property `capacity` |
| `nightly_price` | DECIMAL(10,2) | price snapshot at request time |
| `cleaning_fee` | DECIMAL(10,2) | snapshot at request time |
| `total_price` | DECIMAL(10,2) | `nightly_price × nights + cleaning_fee` |
| `status` | ENUM | see lifecycle above |
| `status_reason` | VARCHAR(64) NULL | e.g. `DATES_UNAVAILABLE`, `HOST_DECLINED`, `GUEST_CANCELLED` |
| `message_to_host` | VARCHAR(1000) NULL | optional note from the guest |
| `cancelled_by` | ENUM(`guest`,`host`) NULL | set on cancel |
| `trace_id` | VARCHAR(40) | trace of the create request |
| `publish_pending` | BOOLEAN | `true` while an outgoing event has not been confirmed by Kafka |
| `version` | INT | incremented on every status change |
| `created_datetime` | DATETIME(3) | server |
| `updated_datetime` | DATETIME(3) | server |

Indexes: `(guest_id, created_datetime)`, `(host_id, status, created_datetime)`, `(property_id, check_in)`, `(status, check_out)` for the completion job.

### `availability_blocks` — the double-booking guard

One row per held night.

| Field | Type | Rule |
|---|---|---|
| `property_id` | VARCHAR(40) | |
| `night_date` | DATE | one night |
| `reservation_id` | VARCHAR(40) | the reservation holding the night |
| `created_datetime` | DATETIME(3) | |

```sql
PRIMARY KEY (property_id, night_date)
```

Because the primary key is `(property_id, night_date)`, MySQL physically cannot store two reservations for the same night of the same property. If two workers try to hold the same night, one insert succeeds and the other fails with a duplicate-key error, even if both requests arrive at the same millisecond.

### `reservation_status_history`

| Field | Type |
|---|---|
| `id` | BIGINT PK auto-increment |
| `reservation_id` | VARCHAR(40) |
| `from_status` | ENUM NULL |
| `to_status` | ENUM |
| `actor_id` | VARCHAR(40) — guest, host, `booking-worker` or `completion-job` |
| `reason` | VARCHAR(64) NULL |
| `trace_id` | VARCHAR(40) |
| `changed_datetime` | DATETIME(3) |

### `idempotency_keys` (REST)

| Field | Type | Rule |
|---|---|---|
| `idempotency_key` | VARCHAR(64) | |
| `actor_id` | VARCHAR(40) | from JWT |
| `endpoint` | VARCHAR(64) | e.g. `/bookings/create` |
| `request_hash` | CHAR(64) | SHA-256 of the normalized body |
| `response_code` | INT | |
| `response_body` | JSON | |
| `created_datetime` | DATETIME(3) | rows older than 24 h are purged |

`PRIMARY KEY (actor_id, endpoint, idempotency_key)`.

### `processed_events` (Kafka consumer)

| Field | Type |
|---|---|
| `idempotency_key` | VARCHAR(128) PK |
| `consumer` | VARCHAR(64) — e.g. `booking-worker` |
| `processed_datetime` | DATETIME(3) |

# REST API

## POST `/bookings/create`

Guest requests a stay. Asynchronous: the API validates and records the request, publishes `booking.requested`, and returns `202`.

Headers:

```text
Authorization: Bearer <JWT>
Idempotency-Key: <uuid>
```

Request:

```json
{
  "property_id": "property-2001",
  "check_in": "2026-11-20",
  "check_out": "2026-11-22",
  "guest_count": 2,
  "message_to_host": "Visiting for a conference, arriving around 6 pm."
}
```

Validation (in order):

1. Fields present, dates valid, `check_out > check_in`, `check_in` not in the past, stay ≤ 30 nights, `guest_count ≥ 1` → else `400 VALIDATION_ERROR`
2. Property exists and is `active` (Property service `POST /properties/get`) → else `404 PROPERTY_NOT_FOUND`
3. JWT subject is not the property's host → else `403 HOST_CANNOT_BOOK_OWN_PROPERTY`
4. `guest_count ≤ capacity` → else `422 CAPACITY_EXCEEDED`
5. Fast pre-check: none of the nights are already in `availability_blocks` → else `409 DATES_UNAVAILABLE`

Step 5 only fails obviously-taken dates early so the guest gets an immediate answer. It does not hold anything; the worker's transaction is the real check.

On success, in one MySQL transaction the API inserts the reservation with `status = requested` and price snapshot, writes the first status-history row, and stores the idempotency key. It then publishes `booking.requested`.

Response `202 Accepted`:

```json
{
  "reservation_id": "res-01JBOOK001",
  "status": "requested",
  "property_id": "property-2001",
  "check_in": "2026-11-20",
  "check_out": "2026-11-22",
  "nights": 2,
  "guest_count": 2,
  "total_price": 485.0,
  "trace_id": "tr-01JBOOK001",
  "created_datetime": "2026-11-10T17:00:00.120Z"
}
```

The client then listens on `WebSocket /bookings/stream` (or polls `/bookings/get`) for the change to `pending_host` or `rejected`.

Idempotency:

- Same `Idempotency-Key` and same body → the original `202` response is returned; no second reservation and no second event.
- Same key with a different body → `422 IDEMPOTENCY_KEY_REUSED`.
- A different key for an identical request from the same guest (double-click with a new key) creates a second `requested` row, but the worker can hold the nights for only one of them; the other ends as `rejected`.

If Kafka publishing fails, the row keeps `publish_pending = true` and a background publisher retries with the same event `idempotency_key`, so the request is never lost and never processed twice.

## POST `/bookings/get`

Request: `{ "reservation_id": "res-01JBOOK001" }`

Response `200`:

```json
{
  "reservation_id": "res-01JBOOK001",
  "property_id": "property-2001",
  "guest_id": "guest-1001",
  "host_id": "host-501",
  "check_in": "2026-11-20",
  "check_out": "2026-11-22",
  "nights": 2,
  "guest_count": 2,
  "nightly_price": 220.0,
  "cleaning_fee": 45.0,
  "total_price": 485.0,
  "status": "pending_host",
  "status_reason": null,
  "message_to_host": "Visiting for a conference, arriving around 6 pm.",
  "cancelled_by": null,
  "version": 2,
  "trace_id": "tr-01JBOOK001",
  "created_datetime": "2026-11-10T17:00:00.120Z",
  "updated_datetime": "2026-11-10T17:00:00.480Z",
  "status_history": [
    {"from_status": null, "to_status": "requested", "actor_id": "guest-1001", "reason": null, "changed_datetime": "2026-11-10T17:00:00.120Z"},
    {"from_status": "requested", "to_status": "pending_host", "actor_id": "booking-worker", "reason": null, "changed_datetime": "2026-11-10T17:00:00.480Z"}
  ]
}
```

Only the reservation's guest or host may read it → else `403 NOT_RESERVATION_PARTY`. Unknown ID → `404 RESERVATION_NOT_FOUND`.

## POST `/bookings/byGuest`

Lists the authenticated guest's reservations.

Request:

```json
{
  "status": ["pending_host", "confirmed"],
  "from_date": null,
  "to_date": null,
  "page": 1,
  "page_size": 20
}
```

Response `200`:

```json
{
  "items": [
    {
      "reservation_id": "res-01JBOOK001",
      "property_id": "property-2001",
      "check_in": "2026-11-20",
      "check_out": "2026-11-22",
      "total_price": 485.0,
      "status": "pending_host",
      "created_datetime": "2026-11-10T17:00:00.120Z"
    }
  ],
  "page": 1,
  "page_size": 20,
  "total": 1
}
```

`status` is optional (all statuses when omitted). `page ≥ 1`, `page_size` 1–50. Sorted by `created_datetime` descending.

## POST `/bookings/byHost`

Same request and response shape as `/bookings/byGuest`, for reservations on the authenticated host's properties. Adds an optional `property_id` filter. Each item also includes `guest_id` and `guest_count`. A caller without a host profile → `403 HOST_PROFILE_REQUIRED`.

## POST `/bookings/approve`

Host approves a pending request.

Headers: `Authorization`, `Idempotency-Key`.

Request:

```json
{
  "reservation_id": "res-01JBOOK001",
  "expected_version": 2
}
```

Rules:

- JWT subject is the reservation's `host_id` → else `403 NOT_PROPERTY_HOST`
- Status is `pending_host` → else `409 INVALID_STATUS_TRANSITION`
- `expected_version` matches → else `409 RESERVATION_VERSION_CONFLICT` (protects against approving a request the guest just cancelled)

The update is a guarded single statement:

```sql
UPDATE reservations
SET status = 'confirmed', version = version + 1, updated_datetime = NOW(3)
WHERE reservation_id = ? AND status = 'pending_host' AND version = ?;
-- 0 rows affected → 409
```

Response `200`: the updated reservation object with `status: "confirmed"`. Publishes `booking.updated`.

## POST `/bookings/decline`

Host declines a pending request.

Request:

```json
{
  "reservation_id": "res-01JBOOK001",
  "expected_version": 2,
  "reason": "Property under maintenance those dates"
}
```

Same authorization and version rules as approve. In one transaction: status → `declined`, `status_reason = HOST_DECLINED`, delete the reservation's rows from `availability_blocks`, write history. Response `200` with the updated reservation. Publishes `booking.updated` and `property.availability.updated`.

## POST `/bookings/cancel`

Guest or host cancels.

Request:

```json
{
  "reservation_id": "res-01JBOOK001",
  "expected_version": 3,
  "reason": "Travel plans changed"
}
```

Rules:

- Caller is the reservation's guest or host → else `403 NOT_RESERVATION_PARTY`
- Status is `requested`, `pending_host` or `confirmed` → else `409 INVALID_STATUS_TRANSITION`
- A `confirmed` reservation can be cancelled only before `check_in` → else `422 CANCELLATION_WINDOW_CLOSED`

In one transaction: status → `cancelled`, `cancelled_by` set from the caller's role, `status_reason` = `GUEST_CANCELLED` or `HOST_CANCELLED`, release `availability_blocks`, write history. Response `200`. Publishes `booking.updated` and `property.availability.updated`.

Cancelling a `requested` reservation before the worker runs is safe: the worker sees status ≠ `requested`, holds nothing, and acknowledges the event.

The cancellation rule is in one function (`cancellation_policy.py`) so the §13.12 "change the cancel rule" drill touches one place.

## POST `/bookings/calendar`

Returns the taken nights of a property, for the property detail page and the date picker.

Request:

```json
{
  "property_id": "property-2001",
  "from_date": "2026-11-01",
  "to_date": "2026-12-31"
}
```

Response `200`:

```json
{
  "property_id": "property-2001",
  "from_date": "2026-11-01",
  "to_date": "2026-12-31",
  "unavailable_nights": ["2026-11-20", "2026-11-21"],
  "checked_datetime": "2026-11-10T17:05:00Z"
}
```

Range ≤ 366 days → else `400`. Public read (auth optional). Reservation and guest identities are not exposed.

## WebSocket `/bookings/stream`

The client authenticates with `Authorization: Bearer <JWT>` during the handshake. The server pushes every `booking.updated` event for reservations where the user is the guest or the host. The message is the shared event envelope (see `booking.updated` below).

The client can also subscribe to a property's calendar to see "dates just taken" live while on the detail page:

```json
{ "action": "subscribe_calendar", "property_id": "property-2001" }
```

The server then forwards `property.availability.updated` for that property. After a disconnect, the client calls `/bookings/get` or `/bookings/calendar` to recover state, so a missed push never leaves the UI wrong.

## Dashboards (§5, §8)

Both are read-only and computed from MySQL plus the sources named below.

### POST `/dashboards/guest`

Request: `{ "days": 30 }` (1–365, default 30). Guest from JWT.

Response `200`:

```json
{
  "guest_id": "guest-1001",
  "window_days": 30,
  "bookings_in_window": 3,
  "status_summary": {"requested": 0, "pending_host": 1, "confirmed": 1, "rejected": 0, "declined": 0, "cancelled": 1, "completed": 0},
  "upcoming": [{"reservation_id": "res-01JBOOK001", "property_id": "property-2001", "check_in": "2026-11-20", "status": "confirmed"}],
  "saved_properties_count": 3,
  "total_spent": 485.0
}
```

`saved_properties_count` comes from the User service (`POST /guests/get`).

### POST `/dashboards/host`

Request: `{ "days": 30, "property_id": null }`. Host from JWT.

Response `200`:

```json
{
  "host_id": "host-501",
  "window_days": 30,
  "funnel": {"views": 420, "saves": 38, "requests": 12, "confirmed": 8},
  "occupancy_rate": 0.62,
  "revenue": 6240.0,
  "cancellations": {"by_guest": 1, "by_host": 0},
  "declined": 2,
  "average_rating": 4.7,
  "median_response_minutes": 35
}
```

| Field | Source |
|---|---|
| `requests`, `confirmed`, `revenue`, `cancellations`, `declined`, `occupancy_rate` | Booking MySQL (`occupancy_rate` = booked nights ÷ available nights of active properties in the window) |
| `median_response_minutes` | Booking: time from `pending_host` to `confirmed`/`declined` in status history |
| `views`, `saves` | Analytics service (C), from `property.viewed` / `property.saved` |
| `average_rating` | Property service (A) aggregate `rating` |

If Analytics is unavailable, `views` and `saves` are returned as `null` and the rest of the dashboard still loads.

# Internal Service Interfaces

Internal endpoints require service authentication and are not exposed to the browser.

## GET `/internal/reservations/{reservation_id}`

Requested by E (Review service) before creating a review.

Response `200 OK`:

```json
{
  "reservation_id": "res-3001",
  "property_id": "property-2001",
  "guest_id": "guest-1001",
  "host_id": "host-501",
  "status": "completed",
  "check_in": "2026-11-20",
  "check_out": "2026-11-22"
}
```

Unknown ID → `404 RESERVATION_NOT_FOUND`. MySQL unavailable → `503 DEPENDENCY_UNAVAILABLE`.

The Review service checks `guest_id`, `status = completed` and that `check_out` is in the past. A scheduled completion job moves `confirmed` reservations to `completed` every 15 minutes once `check_out` has passed, so a finished stay becomes reviewable without anyone doing anything. For the demo, `make complete-stays` runs the job once.

## POST `/internal/availability/check`

Requested by A (Property service) for `/properties/availability` and `available_only` search.

Request:

```json
{
  "property_ids": ["property-2001", "property-2007"],
  "check_in": "2026-11-20",
  "check_out": "2026-11-22"
}
```

Response `200 OK`:

```json
{
  "check_in": "2026-11-20",
  "check_out": "2026-11-22",
  "results": [
    {"property_id": "property-2001", "available": false},
    {"property_id": "property-2007", "available": true}
  ],
  "checked_datetime": "2026-11-10T17:05:00Z"
}
```

Up to 200 `property_ids` per call (the full benchmark pool fits in one call). Read-only; it never holds dates.

## GET `/internal/properties/{property_id}/active-reservations`

Requested by A before archiving a property (`409 ACTIVE_RESERVATION_EXISTS`).

Response `200 OK`:

```json
{
  "property_id": "property-2001",
  "has_active_reservations": true,
  "count": 2
}
```

"Active" means status `requested`, `pending_host` or `confirmed` with `check_out` today or later.

The User service can call the same check per guest before a guest profile is deleted: `GET /internal/guests/{guest_id}/active-reservations` returns the same shape with `guest_id`.

# Kafka Events

All events use the shared §6.2 envelope. **Kafka key = `property_id`** for every booking topic, matching `property.*` and `review.*`. All requests for one property go to the same partition, so one worker handles them in order; the `availability_blocks` primary key is still the guarantee if two workers ever race.

| Topic | Producer | Consumer | Key | Purpose |
|---|---|---|---|---|
| `booking.requested` | Booking API | Booking worker | `property_id` | New request to process |
| `booking.updated` | Booking worker, Booking API, completion job | WebSocket gateway, Analytics, Messaging | `property_id` | Any status change |
| `property.availability.updated` | Booking worker, Booking API | Property service cache worker, WebSocket gateway | `property_id` | Nights held or released; invalidate cached availability |
| `booking.dlq` | Booking consumers | Operations / platform dashboard | original key | Permanently failed messages (final name per `00_conventions.md`) |

## `booking.requested`

```json
{
  "event_type": "booking.requested",
  "trace_id": "tr-01JBOOK001",
  "timestamp": "2026-11-10T17:00:00.130Z",
  "actor_id": "guest-1001",
  "entity": {"entity_type": "reservation", "entity_id": "res-01JBOOK001"},
  "payload": {
    "reservation_id": "res-01JBOOK001",
    "property_id": "property-2001",
    "guest_id": "guest-1001",
    "host_id": "host-501",
    "check_in": "2026-11-20",
    "check_out": "2026-11-22",
    "guest_count": 2
  },
  "idempotency_key": "booking.requested:res-01JBOOK001",
  "event_id": "ev-01JBOOK001",
  "producer": "booking-api",
  "schema_version": "1.0"
}
```

## `booking.updated`

```json
{
  "event_type": "booking.updated",
  "trace_id": "tr-01JBOOK001",
  "timestamp": "2026-11-10T17:00:00.480Z",
  "actor_id": "booking-worker",
  "entity": {"entity_type": "reservation", "entity_id": "res-01JBOOK001"},
  "payload": {
    "reservation_id": "res-01JBOOK001",
    "property_id": "property-2001",
    "guest_id": "guest-1001",
    "host_id": "host-501",
    "from_status": "requested",
    "to_status": "pending_host",
    "reason": null,
    "check_in": "2026-11-20",
    "check_out": "2026-11-22",
    "total_price": 485.0,
    "version": 2
  },
  "idempotency_key": "booking.updated:res-01JBOOK001:v2",
  "event_id": "ev-01JBOOK002",
  "producer": "booking-worker",
  "schema_version": "1.0"
}
```

`version` in the idempotency key makes each transition unique, so a redelivered event is ignored while a later transition of the same reservation is not.

## `property.availability.updated`

```json
{
  "event_type": "property.availability.updated",
  "trace_id": "tr-01JBOOK001",
  "timestamp": "2026-11-10T17:00:00.480Z",
  "actor_id": "booking-worker",
  "entity": {"entity_type": "property", "entity_id": "property-2001"},
  "payload": {
    "property_id": "property-2001",
    "change": "held",
    "nights": ["2026-11-20", "2026-11-21"],
    "reservation_id": "res-01JBOOK001"
  },
  "idempotency_key": "availability:res-01JBOOK001:held",
  "producer": "booking-worker",
  "schema_version": "1.0"
}
```

`change` is `held` or `released`.

# Booking Worker

## Processing one `booking.requested`

Auto-commit is off. The worker commits the Kafka offset only after the MySQL transaction commits.

```text
1. Validate the envelope and payload           → invalid: send to booking.dlq, commit offset
2. BEGIN
3.   If idempotency_key in processed_events    → COMMIT, commit offset, stop (duplicate)
4.   SELECT … FROM reservations WHERE reservation_id = ? FOR UPDATE
5.   If status ≠ 'requested'                   → record processed, COMMIT, stop (e.g. guest already cancelled)
6.   INSERT INTO availability_blocks (property_id, night_date, reservation_id)
       VALUES (p, 2026-11-20, r), (p, 2026-11-21, r)
7a.  Success           → status = 'pending_host'
7b.  Duplicate key     → status = 'rejected', reason = 'DATES_UNAVAILABLE'
                         (the failed multi-row insert stores no nights at all)
8.   INSERT status history; INSERT processed_events; set publish_pending = true
9. COMMIT
10. Publish booking.updated (+ property.availability.updated on 7a); clear publish_pending
11. Commit the Kafka offset
```

## Concurrency: preventing double booking (§13.1)

Two guests request the same nights at the same time:

| Time | Worker A (guest-1001) | Worker B (guest-1002) |
|---|---|---|
| t1 | INSERT nights 20, 21 → succeeds (row locks held) | |
| t2 | | INSERT nights 20, 21 → waits on the same keys |
| t3 | COMMIT → `pending_host` | |
| t4 | | duplicate-key error → `rejected` |

Exactly one reservation holds the nights. The demo is `make test-double-booking`: it fires N concurrent `/bookings/create` calls for the same property and dates (with the API pre-check disabled so all of them reach the worker), then asserts that exactly one reservation is `pending_host`, the rest are `rejected`, and `availability_blocks` has exactly `nights` rows for that property. The trace IDs and the SQL query output are saved as evidence.

## Retries, DLQ and idempotency (§6.2, §10)

| Failure | Behavior |
|---|---|
| Malformed envelope or payload (missing field, wrong type, key ≠ `payload.property_id`) | Not retryable. Sent to `booking.dlq` immediately with the original event, `trace_id` and reason; offset committed. |
| Transient error (MySQL deadlock, lock-wait timeout, connection lost) | Retried in place up to 3 times with backoff 200 ms, 1 s, 5 s. Still failing → `booking.dlq`. |
| Reservation ID not found | Retried once after 1 s (covers a read-replica lag), then `booking.dlq`. |
| Duplicate delivery | Detected by `processed_events`; acknowledged with no change. |

A DLQ message contains:

```json
{
  "original_topic": "booking.requested",
  "original_event": { "...": "full original envelope" },
  "failure_reason": "MISSING_FIELD:payload.check_in",
  "attempts": 1,
  "consumer": "booking-worker",
  "failed_datetime": "2026-11-10T17:00:01Z",
  "trace_id": "tr-01JBOOK001"
}
```

## Consumer recovery (§13.2)

`make consumer-recovery` shows that a crash never loses or duplicates a booking:

1. Send a booking request and stop the worker right after the event lands in Kafka (before processing).
2. Show the consumer group's committed offset is behind the topic's end offset (`kafka-consumer-groups --describe`).
3. Restart the worker. It resumes from the committed offset and processes the event once.
4. Repeat, killing the worker after the MySQL commit but before the offset commit. Kafka redelivers; `processed_events` makes it a no-op.
5. Assert exactly one status-history row per transition and one set of `availability_blocks` rows.

# Status Codes

| Code | When |
|---|---|
| `200` | Read, approve, decline, cancel, idempotent replay of approve/decline/cancel |
| `202` | Booking request accepted for async processing |
| `400` | Invalid body, dates or pagination |
| `401` | Missing or invalid JWT |
| `403` | Not the reservation's guest/host; host booking own property |
| `404` | Reservation or property not found |
| `409` | Dates unavailable, invalid status transition, version conflict |
| `422` | Capacity exceeded, cancellation window closed, idempotency key reused with a different body |
| `429` | Rate limit exceeded |
| `503` | MySQL, Kafka or Property service unavailable |

Error codes: `VALIDATION_ERROR`, `UNAUTHORIZED`, `PROPERTY_NOT_FOUND`, `RESERVATION_NOT_FOUND`, `HOST_CANNOT_BOOK_OWN_PROPERTY`, `NOT_PROPERTY_HOST`, `NOT_RESERVATION_PARTY`, `HOST_PROFILE_REQUIRED`, `CAPACITY_EXCEEDED`, `DATES_UNAVAILABLE`, `INVALID_STATUS_TRANSITION`, `RESERVATION_VERSION_CONFLICT`, `CANCELLATION_WINDOW_CLOSED`, `IDEMPOTENCY_KEY_REUSED`, `DEPENDENCY_UNAVAILABLE`.

# Cross-Service Dependencies

| Needs | From | Interface |
|---|---|---|
| Property exists, `active`, `host_id`, `capacity`, `nightly_price`, `cleaning_fee` | A — Property | `POST /properties/get` |
| Guest `saved_properties_count` for guest dashboard | A — User | `POST /guests/get` |
| Property `rating` for host dashboard | A — Property | `POST /properties/get` |
| Views and saves for host funnel | C — Analytics | Analytics query endpoint (to be defined in `messaging_analytics.md`) |
| Auth, error body, pagination, envelope, retry/DLQ naming | C — Platform | `docs/api/00_conventions.md` |
| Reservation check before review | E consumes B | `GET /internal/reservations/{reservation_id}` |
| Availability for search and detail page | A consumes B | `POST /internal/availability/check`, `property.availability.updated` |
| Active-reservation check before archive/delete | A consumes B | `GET /internal/properties/{id}/active-reservations`, `GET /internal/guests/{id}/active-reservations` |
| Booking status for dashboards and notifications | C consumes B | `booking.updated` |
| AI recommendations never book | D | AI agents have no access to Booking endpoints; a guest books an approved recommendation through `/bookings/create` like any other booking |

# Cross-Review Items

1. C: confirm the DLQ topic name (`booking.dlq` vs one shared `platform.dlq`) and the envelope's optional fields.
2. A: confirm `property.availability.updated` payload and the two internal endpoints above.
3. E: confirm the `/internal/reservations/{id}` response fields match what the Review service needs.
4. C: confirm Analytics consumes `booking.updated` for the platform dashboard, and the query endpoint the host dashboard will call for views/saves.
5. A + B: finish `docs/entities.md` (task 1.2) with the reservation fields above.
