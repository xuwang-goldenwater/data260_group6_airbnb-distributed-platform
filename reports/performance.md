# Distributed platform performance report

Owner: Matthew (Role C). Evidence inventory as of November 27, 2026.

## Kafka and platform

The final-code Kafka-to-Mongo experiment measured 471.53 valid records/s with one consumer and 585.14 with three, using three partitions and 600 valid events per configuration. Both runs verified exactly three rejected malformed messages in the actual DLQ. The comparison is a local single trial, excludes group assignment, and includes publication, durable writes and synchronous offset commits. Raw data, chart, method, machine and resource IDs are in `reports/perf/kafka/`.

The HTTP integration smoke test confirms Messaging send/replay, persistent outbox publication, Analytics receipt, duplicate-view deduplication and authorized Booking host-funnel access. The shared retry tests verify three retries at 0.2/1/5 seconds, and that a failed DLQ publication never commits the source offset.

## Booking evidence supplied by Role B

`reports/perf/booking/README.md` reports 50/60/150/300 completed bookings at concurrency 1/10/25/50, with zero errors. Corresponding p95 end-to-end latency is 10.3/68.3/185.3/688.4 ms; throughput is 107.3/164.3/187.7/136.7 bookings/s. Those are Role B's committed measurements on its documented machine; they were not rerun as part of the Platform benchmark.

Booking restart evidence reports 120 accepted requests, one detected redelivery, zero duplicate database side effects and zero duplicate outgoing idempotency keys. See `duplicates_after_restart.json`, `consumer_recovery/`, and the double-booking evidence in the same directory. These tests distinguish idempotent worker recovery from broker replication guarantees.

## AI evidence supplied by Role D

`reports/perf/ai/` contains committed latency, agent timeout, invalid-output and three-debate JSON evidence. Keep these workload/clock definitions separate from Booking's HTTP-to-WebSocket latency and Kafka's records/s. Shared runtime and deterministic Chair tests run in `make test`.

## Outstanding team measurements

No Search JMeter index-on/off or cache-on/off results were present on main at the time of this inventory. Role A must supply that evidence before the final team performance report claims search throughput or tail latency. A multi-trial Kafka comparison on the team's agreed strongest machine also remains advisable. Missing measurements are not inferred from unit tests.

## Reproduction and CI

`make test` runs Platform, seed/importer, AI and Booking checks. Final local run: 58 Platform/seed/importer tests passed, 5 optional Role A MySQL-import tests skipped (opt-in flag not enabled); 33 AI tests passed; all 130 Booking tests passed against the isolated MySQL test database on port 3307. Total: 221 passed and 5 explicitly skipped. The workflow in `.github/workflows/platform.yml` provisions MySQL, repeats the tests and validates Compose. Local `make kafka-perf` and the smoke script require actual Kafka/Mongo and persist their measured outputs.
