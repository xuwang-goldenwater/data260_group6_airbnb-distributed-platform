# Role C Platform

Matthew owns Messaging, Analytics ingest, shared consumer delivery rules, Mongo indexes, platform UI, CI and Kafka measurements.

## Run from repository root

```sh
make platform-venv
docker compose -f infra/docker-compose.yml up -d kafka mongodb redis
make platform-topics platform-init platform-seed
export JWT_SECRET='use-the-same-local-secret-as-booking-and-ai'
make platform-api       # terminal 1, http://127.0.0.1:8006/platform
make platform-worker    # terminal 2; use the same environment
```

Environment: `MONGO_URI` defaults to `mongodb://localhost:27017`, `MONGO_DATABASE` to `airbnb`, `KAFKA_BOOTSTRAP_SERVERS` to `localhost:9092`; optional `JWT_ISSUER` must match the token issuer. JWT verification uses HS256 and requires sub/role/exp. Tokens are never stored by the UI. Mint development tokens with the existing Booking dev-token tool using the same secret.

`make test-platform` runs isolated Mongo-mock tests. `make test-venv test` runs Platform, seed/importer, AI and Booking tests. Booking integration uses `airbnb_booking_test` and skips when MySQL is unavailable; CI provides MySQL. `make kafka-perf` requires live Kafka and Mongo and saves measured data/graph under `reports/perf/kafka`. It retains uniquely named topics/databases for inspection and does not delete team data.

## Booking integration

Set `ANALYTICS_SERVICE_URL=http://127.0.0.1:8006` in Booking. Its existing service JWT client can call:

`GET /analytics/host/{host_id}?days=30&property_id=property-2001`

Response contains `funnel: {views, saves, unsaves}`, `as_of`, `days`, and `property_source`. Saves are positive save actions; unsaves are reported separately. Host ownership is resolved against the frozen 200-property pool. This is explicit in the response, and newly created listings require a live Property ownership lookup before production use. Booking owns occupancy/revenue/status metrics; this service supplies its missing activity funnel.

The worker consumes `booking.updated` using `to_status` and `version` (not the earlier draft's `status` example), plus property, review, message and AI topics. Observed reservation totals use highest version per reservation so late events cannot roll status backward. No seed activity is fabricated to inflate analytics; funnel is zero until actual activity events arrive.

## Delivery and persistence

Messages are immutable Mongo documents. One insert atomically stores the message, HTTP idempotency receipt, and complete outgoing Kafka event. The worker publishes the event and waits for broker acknowledgment before marking the message sent. Crashes can cause redelivery; immutable event documents with unique event/operation IDs prevent double counting. Message text is omitted from analytics events. Seed import uses `$setOnInsert` and preserves all original fixture IDs.

`shared/consumer.py` validates envelopes and keys, retries transient operations after 0.2/1/5 seconds, sends permanent failures to `platform.dlq`, and commits only after durable processing or DLQ acknowledgment. If that acknowledgment fails, the worker exits without committing, allowing restart from the committed offset. Existing Booking/AI payload validators remain authoritative for their business rules.

## Current limitations for presentation

- Single local Kafka broker, no broker-failure availability guarantee.
- Host/property ownership uses the fixed seed pool; Booking remains source of truth for financial/calendar metrics.
- Analytics includes observed events only and is eventually consistent.
- Kafka throughput is a local single-trial comparison, not a production capacity claim.
- UI lists the first 20 conversations and first 50 messages; paginated REST endpoints support further pages.
