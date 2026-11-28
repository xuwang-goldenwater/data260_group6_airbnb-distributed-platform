# Role C handoff and presentation content

Owner: Jietian (Matthew) Chen. November 27, 2026.

## Implemented tasks

2.3 Mongo collections/indexes and idempotent seed import; 2.8 Messaging plus negative-envelope tests; 3.5 shared validation/retry/DLQ helper (Booking retry adapter uses it); 3.6 Analytics ingest and platform dashboard; 4.6 Messaging UI and repository test workflow; 5.3 real Kafka 1-vs-3 consumer benchmark. Performance evidence is consolidated in `reports/performance.md`.

Messaging and Analytics are packaged together under `services/platform` to share Mongo persistence/authentication while exposing the documented routes. Existing Booking/AI business validators and transactional idempotency remain in their owning services. The AI worker's existing per-trace state/deadline/recovery policy has not been replaced.

## Teammate integration

Ayushi: set `ANALYTICS_SERVICE_URL=http://127.0.0.1:8006` and the same JWT_SECRET. `GET /analytics/host/{host_id}?days=30&property_id=...` returns the existing client's expected `funnel.views` and `funnel.saves`. Analytics consumes `booking.updated` using `to_status`/`version`; highest version wins. Booking updates also create guest/host conversations idempotently when reservation/participant IDs are present.

Carol: original conversation/message seed fields and IDs are preserved. Property view/save events are validated and counted once. Ownership currently resolves against the fixed 200-property pool; newly created listings need a live ownership lookup before general use.

Xu: Mongo review indexes are provided. Analytics consumes AI result/approval events and reads no approval identity from untrusted request fields. Benchmark labeling remains assigned to Xu and Carol per the group decision.

## Slide 4 Event backbone and conventions

The shared Google deck was updated in place: slides 4, 8 and 14 now contain the implemented Role C behavior, measured Kafka results and source notes; slide 16 includes the platform limitation. Their native text styles/layouts were retained and the changed pages were visually checked. The detailed notes below provide additional rehearsal material.

Replace outline heading with “Validate → persist → acknowledge”.

Topics are keyed by property_id for property/booking/review events, conversation_id for messages, and trace_id for AI. Independent services use independent consumer groups. Invalid envelopes go to platform.dlq; transient failures retry after 200 ms, 1 s and 5 s. Offsets commit only after durable processing or acknowledged DLQ delivery.

Suggested visual (native text/boxes): HTTP send → Mongo message + outbox → Kafka message.created → Analytics unique event log. Draw a replay arrow back to the unique event log labelled “same idempotency_key: no double count”.

Speaker notes: The message and pending event are one atomic Mongo insert. A crash after Kafka acknowledgment may cause publication again, so the consumer deduplicates. The shared envelope includes trace and operation IDs; it does not provide exactly-once side effects by itself.

## Slide 14 Measured performance

Replace “Performance plan” with “Measured platform performance”.

Use `reports/perf/kafka/throughput.png`: 1 consumer 471.53 records/s; 3 consumers 585.14 records/s. Three partitions, 600 valid events and 3 invalid events per run; all valid events persisted, all invalid events verified in DLQ. Local, single trial; publication, validation, Mongo insert and synchronous commit included.

Booking context from Role B: p95 HTTP-to-WebSocket latency at concurrency 1/10/25/50 is 10.3/68.3/185.3/688.4 ms. Cite `reports/perf/booking/README.md` and do not present these as measurements from the same machine/run. Search JMeter results remain pending from Role A.

## Slide 15 Timeline contribution

Role C implementation and local end-to-end evidence completed November 27; team review/integration is pending. Rehearsal/content assembly deadline: November 29 noon per Haozhe; final team coding target November 29 per Xu. Do not claim the PR is merged before review.

## Slide 16 Platform limitation

Single-broker Kafka has replication factor 1; worker-replay safety does not protect against losing the broker's storage. Host ownership currently uses the fixed seed pool, and Analytics reflects observed events asynchronously. The local single-trial throughput result is not a production capacity estimate.

## Review notes

PRs #17/#19/#21 are already merged according to the group and main history. Booking's consumer policy and event payloads were inspected to wire the shared retry helper and Analytics interface. This handoff does not claim a historical GitHub approval or rerun of MySQL-backed double-booking/recovery demos. Local contract tests and new Platform live evidence are recorded separately.
