# Shared API and Event Conventions

Owner: Matthew — Role C Platform
Status: Week 1 API contract

This document is the source of truth for cross-service HTTP and Kafka conventions. Service-specific documents may add fields, but must not rename these shared fields or change the retry/DLQ rules.

## HTTP conventions

### Authentication and identity

- Protected HTTP endpoints require `Authorization: Bearer <JWT>`.
- The authenticated subject and role come from the validated JWT. Services must not trust `guest_id`, `host_id`, or `reviewer_id` supplied in a request body.
- Mutating endpoints that can be retried require `Idempotency-Key: <uuid>`. The key is scoped to the authenticated subject and endpoint.
- JSON field names use `snake_case`. Dates use `YYYY-MM-DD`; timestamps use UTC ISO 8601 with `Z`.

### Common error body

Every non-2xx response uses this shape:

```json
{
  "error": {
    "code": "DATES_UNAVAILABLE",
    "message": "One or more requested nights are already booked.",
    "trace_id": "tr-01JBOOK001",
    "retryable": false,
    "details": {"property_id": "property-2001"}
  }
}
```

`code` is stable and machine-readable; `message` is safe for logs/UI; `trace_id` correlates the request across services; `retryable` tells a caller whether retrying may succeed; `details` is optional structured context and never contains secrets.

| Status | Meaning |
|---:|---|
| 200 | Successful read, update, response, or idempotent replay |
| 201 | Resource created |
| 202 | Accepted for asynchronous processing |
| 400 | Malformed JSON or invalid field values (`VALIDATION_ERROR`) |
| 401 | Missing or invalid authentication |
| 403 | Authenticated caller lacks permission |
| 404 | Referenced resource does not exist |
| 409 | Conflict, duplicate resource, or invalid state transition |
| 422 | Well-formed request cannot be processed (`CAPACITY_EXCEEDED`, etc.) |
| 429 | Rate limit exceeded; retry after the indicated delay |
| 500 | Unexpected service error; do not expose stack traces |
| 503 | Dependency or infrastructure unavailable; retryable |

### Pagination

Collection endpoints accept `page` (1-based, default 1) and `page_size` (default 20, maximum 50). Responses include `items`, `page`, `page_size`, and `total`:

```json
{"items": [], "page": 1, "page_size": 20, "total": 0}
```

## Kafka conventions

### Event envelope

Every Kafka record value is a JSON object validated against `shared/envelope.schema.json`:

```json
{
  "event_type": "booking.requested",
  "trace_id": "tr-01JBOOK001",
  "timestamp": "2026-11-10T17:00:00.120Z",
  "actor_id": "guest-1001",
  "entity": {"entity_type": "reservation", "entity_id": "res-01JBOOK001"},
  "payload": {"reservation_id": "res-01JBOOK001", "property_id": "property-2001"},
  "idempotency_key": "booking.requested:res-01JBOOK001",
  "attempt": 1,
  "event_id": "ev-01JBOOK001",
  "producer": "booking-api",
  "schema_version": "1.0"
}
```

Required fields are `event_type`, `trace_id`, `timestamp`, `actor_id`, `entity`, `payload`, `idempotency_key`, `attempt`, `event_id`, `producer`, and `schema_version`. `attempt` starts at 1. `event_id` identifies the record; `idempotency_key` identifies the logical operation and remains stable across delivery retries.

The Kafka record key is not duplicated in the envelope. Domain events use the key specified in the topic table below. Consumers must reject a record when the Kafka key does not match the event's required payload key.

### Topic, key, and consumer-group registry

| Topic | Producer | Consumers | Kafka key | Consumer group(s) |
|---|---|---|---|---|
| `booking.requested` | Booking API | Booking worker | `property_id` | `booking-worker` |
| `booking.updated` | Booking worker/API | WebSocket gateway, Analytics, Messaging | `property_id` | `booking-status`, `analytics`, `messaging` |
| `property.viewed` | Property service | Analytics | `property_id` | `analytics` |
| `property.saved` | Property service | Analytics | `property_id` | `analytics` |
| `property.updated` | Property service | Analytics, cache invalidator | `property_id` | `analytics`, `property-cache` |
| `property.availability.updated` | Booking service | Property/cache, Analytics | `property_id` | `property-cache`, `analytics` |
| `review.created` | Review service | Property service, Analytics | `property_id` | `property-rating`, `analytics` |
| `review.responded` | Review service | Analytics | `property_id` | `analytics` |
| `message.created` | Messaging service | Analytics, notification worker | `conversation_id` | `analytics`, `notifications` |
| `ai.requests` | AI API | Parser, Chair | `trace_id` | `ai-parser`, `ai-chair` |
| `ai.parsed` | Parser | Chair, analysis agents | `trace_id` | `ai-chair`, `ai-agents` |
| `debate.round.0` | Analysis agents | Chair | `trace_id` | `ai-chair` |
| `debate.round.1` | Chair | Analysis agents | `trace_id` | `ai-agents` |
| `debate.round.2` | Analysis agents | Chair | `trace_id` | `ai-chair` |
| `ai.results` | Chair | AI API, WebSocket gateway, Analytics | `trace_id` | `ai-results`, `analytics` |
| `ai.approvals` | AI API | Audit, Analytics | `trace_id` | `audit`, `analytics` |
| `platform.dlq` | Failed consumer | Operations/platform dashboard | original record key | `platform-ops` |

All events for one property or trace remain ordered within their topic partition. Consumers must not assume that different topics are globally ordered.

### Validation, retry, idempotency, and DLQ

1. Validate the envelope and payload before business processing. Malformed or semantically invalid messages are not retried.
2. Deduplicate using `idempotency_key` within each consumer group. A duplicate is acknowledged without repeating the side effect.
3. Retry transient failures in place up to three times with delays of 200 ms, 1 s, and 5 s. A dependency outage may be retried by the service's bounded publisher using the same event and idempotency key.
4. After the retry limit, publish one record to `platform.dlq`, preserving the full original envelope.
5. Commit the source offset only after the side effect is durable, or after the invalid/permanently failed record is safely written to the DLQ.

A DLQ payload contains `original_topic`, `original_partition`, `original_offset`, `original_key`, `original_event`, `error_code`, `error_message`, `retry_count`, `failed_at`, and `consumer_group`. It must preserve the original `event_id` and `trace_id`. Secrets and credentials must never be copied into the DLQ.

Non-retryable examples: schema failure, missing required field, unknown event type, key/payload mismatch, and permanently invalid business data. Retryable examples: connection reset, timeout, temporary database deadlock, and unavailable dependency.

## Change control

Adding a field is backward-compatible when it is optional. Renaming a field, changing a topic/key, or changing retry/DLQ behavior requires updating this file and every affected service contract in the same API-design change.
