# Review Service API

Owner: Xu Wang
Role: E Agent Quality & Evaluation
Status: draft for team review

## Scope

The Review service lets a guest review a completed stay, lets the host respond once, and serves reviews to the client, the Property service and the Review & Risk agent. It owns the MongoDB `reviews` collection and publishes `review.created` and `review.responded`.

Error body and auth header follow `docs/api/00_conventions.md`. Until C finalizes that file, this document uses the same error body as `docs/api/ai.md`.

## Why MongoDB (§3.2)

- `review_text` and `response_text` are free text of very different lengths.
- Optional fields (`tags`, `response_text`, moderation notes) appear on only some reviews; a document model avoids sparse columns.
- Reviews are written once and read many times by property; there are no multi-row transactions to protect.
- Strong consistency is still enforced where it matters: one review per reservation is guaranteed by a unique index, and the reservation itself is checked against the Booking service (MySQL) before insert.

## Data model — `reviews` collection

| Field | Type | Rule |
|---|---|---|
| `review_id` | string | `rev-` + ULID, unique |
| `reservation_id` | string | unique (one review per reservation) |
| `property_id` | string | required |
| `guest_id` | string | required; must be the reservation's guest |
| `host_id` | string | required; must be the property's host |
| `rating` | int | 1–5 |
| `review_text` | string | 10–2000 characters |
| `tags` | string[] | computed by code at insert: `cleanliness`, `cancellation`, `missing_amenity`, `noise`, `host_unresponsive`, `price_mismatch`, `positive` |
| `created_datetime` | string (UTC ISO 8601) | set by server |
| `response_text` | string \| null | host reply, ≤ 1000 characters |
| `response_datetime` | string \| null | set by server |
| `trace_id` | string | trace of the create request |

Indexes:

```js
db.reviews.createIndex({ review_id: 1 }, { unique: true })
db.reviews.createIndex({ reservation_id: 1 }, { unique: true })
db.reviews.createIndex({ property_id: 1, created_datetime: -1 })
db.reviews.createIndex({ host_id: 1, created_datetime: -1 })
db.reviews.createIndex({ property_id: 1, tags: 1 })   // Risk agent: complaints per property
```

`tags` are produced by a deterministic keyword rule (`tagger-v1`), not by an LLM, so the Risk agent's evidence is reproducible.

## REST API

All endpoints require `Authorization: Bearer <JWT>`. Identity always comes from the JWT subject, never from the request body.

### POST `/reviews/create`

Guest submits a review for a completed reservation.

Headers: `Idempotency-Key: <uuid>` (required). A repeated key returns the original review with `200`.

Request:

```json
{
  "reservation_id": "res-3001",
  "rating": 4,
  "review_text": "Clean place and easy parking. The host replied quickly."
}
```

Validation (in order):

1. Body fields present, `rating` integer 1–5, `review_text` 10–2000 chars → else `400 VALIDATION_ERROR`
2. Reservation exists (Booking service internal `GET /internal/reservations/{id}`) → else `404 RESERVATION_NOT_FOUND`
3. JWT subject is the reservation's guest → else `403 NOT_RESERVATION_GUEST`
4. Reservation `status` is `completed` and `check_out` is in the past → else `422 STAY_NOT_COMPLETED`
5. No review exists for this reservation → else `409 REVIEW_ALREADY_EXISTS`

`property_id`, `guest_id` and `host_id` are copied from the reservation, not taken from the client.

Response `201 Created`:

```json
{
  "review_id": "rev-01JREV001",
  "reservation_id": "res-3001",
  "property_id": "property-2001",
  "guest_id": "guest-1001",
  "host_id": "host-501",
  "rating": 4,
  "review_text": "Clean place and easy parking. The host replied quickly.",
  "tags": ["positive"],
  "created_datetime": "2026-11-23T18:04:11Z",
  "response_text": null,
  "response_datetime": null
}
```

If the Booking service is unreachable, the service returns `503 DEPENDENCY_UNAVAILABLE` and writes nothing.

### POST `/reviews/get`

Request: `{ "review_id": "rev-01JREV001" }`

Response `200`: the review object above. Unknown ID → `404 REVIEW_NOT_FOUND`.

### POST `/reviews/byProperty`

Request:

```json
{
  "property_id": "property-2001",
  "sort": "newest",
  "min_rating": null,
  "tag": null,
  "page": 1,
  "page_size": 20
}
```

- `sort`: `newest` (default), `highest`, `lowest`
- `tag`: optional filter, e.g. `cleanliness`
- `page_size`: 1–50

Response `200`:

```json
{
  "property_id": "property-2001",
  "summary": {
    "review_count": 37,
    "average_rating": 4.62,
    "tag_counts": {"cleanliness": 2, "cancellation": 1, "positive": 30}
  },
  "items": [ { "review_id": "rev-01JREV001", "rating": 4, "...": "..." } ],
  "page": 1,
  "page_size": 20,
  "total": 37
}
```

Unknown property → `200` with `review_count: 0` and empty `items` (a new listing has no reviews; this is not an error).

### POST `/reviews/respond`

Host replies once to a review of their own property.

Request:

```json
{
  "review_id": "rev-01JREV001",
  "response_text": "Thank you for staying with us!"
}
```

Validation: review exists (`404`), JWT subject is the review's `host_id` (`403 NOT_PROPERTY_HOST`), `response_text` 1–1000 chars (`400`), no existing response (`409 RESPONSE_ALREADY_EXISTS`).

Response `200`: the updated review object with `response_text` and `response_datetime` set.

## Status codes

| Code | When |
|---|---|
| `200` | read, idempotent replay of create, respond |
| `201` | review created |
| `400` | invalid body or field values |
| `401` | missing or invalid JWT |
| `403` | caller is not the reservation guest / property host |
| `404` | review or reservation not found |
| `409` | second review for a reservation, second host response |
| `422` | stay not completed yet |
| `503` | Booking service or MongoDB unavailable |

## Kafka events

Both events use the shared envelope (§6.2). Key = `property_id`, so all events for one property stay ordered.

### `review.created`

Producer: Review service. Consumers: Property service (updates `rating` and `review_count`), Analytics (rating trend on host dashboard).

```json
{
  "event_type": "review.created",
  "trace_id": "tr-01JREV001",
  "timestamp": "2026-11-23T18:04:11Z",
  "actor_id": "guest-1001",
  "entity": {"entity_type": "review", "entity_id": "rev-01JREV001"},
  "payload": {
    "review_id": "rev-01JREV001",
    "reservation_id": "res-3001",
    "property_id": "property-2001",
    "host_id": "host-501",
    "rating": 4,
    "tags": ["positive"]
  },
  "idempotency_key": "review.created:rev-01JREV001"
}
```

The event carries no `review_text`; consumers that need the text call `/reviews/get`.

Write order: insert into MongoDB first, then publish. If publishing fails, the review is marked `publish_pending` and a background retry republishes with the same `idempotency_key`, so consumers never double-count a rating.

### `review.responded`

Producer: Review service. Consumer: Analytics (host response metrics).

Payload: `review_id`, `property_id`, `host_id`, `response_datetime`. `idempotency_key`: `review.responded:<review_id>`.

## Use by the Review & Risk agent

The Risk agent reads reviews through `/reviews/byProperty` (or the `reviews` collection directly with read-only access) and cites `review_id`s in `risk_flags[].evidence_ids` (see `docs/api/agent_payloads.md`). The seed data includes reviews with cancellation, cleanliness and missing-amenity complaints for a known subset of properties so the risk and fairness tests have real evidence.

## Dependencies on other services

| Needs | From | Interface |
|---|---|---|
| Reservation status, guest, property, host, check_out | B (Booking) | internal `GET /internal/reservations/{reservation_id}` |
| JWT validation | C (conventions) | shared auth header |
| Property `rating` / `review_count` update | A (Property) | consumes `review.created` |
