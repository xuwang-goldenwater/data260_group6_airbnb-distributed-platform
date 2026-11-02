# Messaging and Analytics Service API

Owner: Matthew — Role C Platform
Status: Week 1 API contract

## Scope

Messaging owns guest/host conversations and message delivery. Analytics consumes domain events and exposes read-only aggregates for the platform and dashboards. Both services use the shared HTTP, event-envelope, retry, idempotency, and DLQ rules in `docs/api/00_conventions.md`.

## Messaging data contract

Messages are ordered within a conversation by Kafka key `conversation_id`. A message is immutable after creation; delivery status is a separate state transition.

| Field | Type | Rule |
|---|---|---|
| `message_id` | string | server-generated `msg-` + ULID |
| `conversation_id` | string | required; stable guest/host thread ID |
| `reservation_id` | string/null | optional booking context |
| `sender_id` | string | from JWT, never the body |
| `recipient_id` | string | required participant |
| `body` | string | 1–4000 characters |
| `created_datetime` | string | server UTC timestamp |
| `delivery_status` | string | `queued`, `sent`, `failed` |
| `trace_id` | string | request trace |

## Messaging REST API

All endpoints require `Authorization: Bearer <JWT>`. Create requires `Idempotency-Key`.

### POST `/messages/send`

Request:

```json
{
  "conversation_id": "conv-01JMSG001",
  "recipient_id": "host-501",
  "reservation_id": "res-01JBOOK001",
  "body": "We expect to arrive around 6 pm."
}
```

The service verifies that the caller and recipient are allowed participants in the conversation. It stores the message before publishing `message.created` and returns `201 Created`:

```json
{
  "message_id": "msg-01JMSG002",
  "conversation_id": "conv-01JMSG001",
  "reservation_id": "res-01JBOOK001",
  "sender_id": "guest-1001",
  "recipient_id": "host-501",
  "body": "We expect to arrive around 6 pm.",
  "created_datetime": "2026-11-10T18:00:00Z",
  "delivery_status": "queued",
  "trace_id": "tr-01JMSG001"
}
```

Repeated idempotency key and identical body returns the original message with `200`; the same key with a different body returns `422 IDEMPOTENCY_KEY_REUSED`. Invalid body returns `400`, unauthorized participant `403`, unknown reservation/conversation `404`, and unavailable Kafka `503` with `retryable: true`.

### POST `/messages/conversation`

Request: `{ "conversation_id": "conv-01JMSG001", "page": 1, "page_size": 20 }`
Response `200`: paginated message objects ordered by `created_datetime` ascending. A caller who is not a participant receives `403`.

### POST `/messages/byReservation`

Request: `{ "reservation_id": "res-01JBOOK001", "page": 1, "page_size": 20 }`
Response `200`: paginated conversations/messages authorized for the caller. Unknown reservation returns `404 RESERVATION_NOT_FOUND`.

## Analytics REST API

Analytics endpoints are read-only and require `Authorization: Bearer <JWT>`. They read materialized projections built from Kafka events; each response includes `as_of` so callers can see projection freshness.

### POST `/events/ingest`

Internal endpoint used by event consumers or replay tooling. It accepts one shared envelope:

```json
{
  "event_type": "booking.updated",
  "trace_id": "tr-01JBOOK001",
  "timestamp": "2026-11-10T18:02:00Z",
  "actor_id": "booking-worker",
  "entity": {"entity_type": "reservation", "entity_id": "res-01JBOOK001"},
  "payload": {"reservation_id": "res-01JBOOK001", "property_id": "property-2001", "status": "confirmed"},
  "idempotency_key": "booking.updated:res-01JBOOK001:v2",
  "attempt": 1,
  "event_id": "ev-01JBOOK002",
  "producer": "booking-worker",
  "schema_version": "1.0"
}
```

Response `202`:

```json
{"accepted": true, "event_id": "ev-01JBOOK002", "trace_id": "tr-01JBOOK001"}
```

The endpoint validates the envelope and event-specific payload before writing a projection. A repeated `event_id` or `idempotency_key` returns `200` with `accepted: true` and does not double-count. Schema failure returns `400 INVALID_EVENT_ENVELOPE`; unknown event type returns `422 UNKNOWN_EVENT_TYPE`; invalid Kafka key/payload relation returns `422 EVENT_KEY_MISMATCH`.

### GET `/analytics/platform`

Returns platform totals and event health:

```json
{
  "as_of": "2026-11-10T18:05:00Z",
  "reservations": {"requested": 12, "confirmed": 8, "cancelled": 1},
  "messages": {"sent": 31, "failed": 0},
  "reviews": {"count": 37, "average_rating": 4.62},
  "event_health": {"accepted": 412, "rejected": 2, "dlq": 1}
}
```

### GET `/analytics/host/{host_id}`

Returns authorized host funnel metrics: views, saves, booking requests, confirmed bookings, occupancy, revenue, cancellations, ratings, and response time. Unknown host returns `404`; a caller without access returns `403`.

### GET `/analytics/property/{property_id}`

Returns views, saves, booking conversion, availability, occupancy, revenue, cancellation count, rating, and review count for the property. Response includes `as_of`; unknown property returns `404`.

## Events

### `message.created`

Producer: Messaging. Consumers: Analytics and notification worker. Kafka key: `conversation_id`. Payload contains `message_id`, `conversation_id`, `reservation_id`, `sender_id`, `recipient_id`, and `created_datetime`; it does not include authentication tokens.

Analytics consumes `booking.updated`, `property.viewed`, `property.saved`, `property.updated`, `property.availability.updated`, `review.created`, `review.responded`, `message.created`, `ai.results`, and `ai.approvals`. All records use the shared envelope and each event is applied once per consumer group using its idempotency key.

## Rejection, retry, and DLQ behavior

- Invalid JSON/envelope, missing required fields, unknown event type, and Kafka key mismatch are non-retryable. Return or record the relevant `400`/`422` code and publish the original event to `platform.dlq` when the failure occurs in a consumer.
- Temporary Kafka, MongoDB, MySQL, or downstream connection failures are retryable up to three attempts with 200 ms, 1 s, and 5 s backoff.
- Duplicate event delivery is acknowledged without repeating a projection or notification.
- After the retry limit, the consumer publishes a `platform.dlq` record containing the complete original envelope plus `original_topic`, `original_partition`, `original_offset`, `original_key`, `error_code`, `error_message`, `retry_count`, `failed_at`, and `consumer_group`.
- A source Kafka offset is committed only after projection/message persistence succeeds or the DLQ write succeeds.

## Cross-service dependencies

| Dependency | Consumer | Interface |
|---|---|---|
| Booking status and reservation context | Messaging/Analytics | `booking.updated` and Booking read contract |
| Property views/saves/changes | Analytics | `property.viewed`, `property.saved`, `property.updated` |
| Availability changes | Analytics/cache projection | `property.availability.updated` |
| Reviews and ratings | Analytics | `review.created`, `review.responded` |
| Shared auth, errors, pagination, envelope | All services | `docs/api/00_conventions.md` |
