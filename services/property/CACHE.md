# Redis listing-search cache

This slice caches public **listing search data before date filtering**, not final
responses or availability. Booking is still called for every dated request,
including a cache hit. Its result is applied before total/pagination and stamped
with the current Booking check time. A Booking outage is 503, never a stale hit.
Direct availability, property details and frozen benchmark candidates bypass this
cache; the frozen-pool integrity checks remain unchanged.

## Configuration and tests

No MySQL migration or seed import is needed. Existing listing writes and review
projection already increment `properties.version` transactionally. Install Redis
client support in Role A's existing environment and start the existing Redis:

```bash
services/user/.venv/bin/python -m pip install -r services/user/requirements.txt
docker compose -f infra/docker-compose.yml up -d redis
docker compose -f infra/docker-compose.yml exec -T redis redis-cli ping

services/user/.venv/bin/python -m unittest discover -s tests/user_property -p 'test_*.py' -v
LISTINGS_MYSQL_TEST=1 LISTINGS_WRITE_MYSQL_TEST=1 LISTINGS_PROFILE_MYSQL_TEST=1 LISTINGS_RATING_MYSQL_TEST=1 LISTINGS_CACHE_INTEGRATION_TEST=1 services/user/.venv/bin/python -m unittest discover -s tests/user_property -p 'test_*.py' -v
```

The new opt-in integration test uses real MySQL `airbnb_property_test` and Redis
to verify MISS/HIT, immediate write invalidation and atomic event deduplication.
It uses unique fixtures/Redis keys and removes only its own records. Existing
test DB schemas 01/03/04/05 are sufficient. Default tests use a Redis transport
double and SQLite; they do not prove live Redis/Lua or Kafka behavior.

Settings reuse the existing ignored `services/user/.env` and environment overrides:

| Variable | Default | Meaning |
| --- | --- | --- |
| PROPERTY_CACHE_ENABLED | false | Enable only on Property processes |
| PROPERTY_REDIS_URL | redis://localhost:6379/0 | Same Redis DB for API/invalidator/scripts |
| PROPERTY_CACHE_TTL_S | 30 | Expiration, 1-300 seconds |

To enable, restart Property in its terminal using:

```bash
PROPERTY_CACHE_ENABLED=true PYTHONPATH=services/user:services/property services/user/.venv/bin/python -m uvicorn property_api.app:app --host 127.0.0.1 --port 8003
```

This process-scoped switch does not rewrite secrets/config or enable other
services. No User/Booking/rating-worker restart is required by this slice.
The response header `X-Search-Cache` is HIT, MISS, BYPASS, FALLBACK or RACE.
The JSON search contract is unchanged. `GET /internal/cache/stats` requires a
service JWT and returns per-process hit/miss/fallback counts, mean listing lookup
latency, Redis health and this namespace's event-invalidation count. It exposes
neither Redis URLs/credentials nor requests/guest data. It resets on API restart;
event counts survive in Redis until its state expires/is removed.

## Invalidation and consistency

Keys hash all validated filters (set-like lists sorted, defaults included), dates,
page, size, sort, dataset version/checksum, MySQL listing revision, and a random
Redis generation. The prefix is scoped to this service/schema version and
MySQL host/port/database. Only active public listing fields are stored and cache
hits validate their structure. Invalid JSON/rows, Redis connection errors and
failed fills fall back to MySQL. Redis socket timeouts are bounded to 250 ms.
MySQL remains required; cached data cannot mask a MySQL outage.

Before and after reading/filling cached listing data, a MySQL aggregate reads
`COUNT(*)` and `SUM(version)`. All supported Property writes and rating updates
increment version in the same commit as listing changes. Their old keys become
unreachable immediately after commit, even if Kafka is delayed or Redis was
temporarily unavailable. A changing revision/generation causes a fresh SQL
search (RACE); delayed fills use the old key and cannot repopulate a new version.
The check adds two small aggregate queries per enabled search: this is a
deliberate correctness/performance tradeoff for the project's 200-property pool,
not a claim that every workload will become faster. Measure the actual result.

This revision assumes the supported append/soft-archive/version-increment write
protocol. Direct SQL edits, restoring a DB backup or resetting version counters
must be followed by a scoped invalidation before serving traffic. No API in this
slice hard-deletes an existing listing. To invalidate only these keys logically:

```bash
services/user/.venv/bin/python services/property/scripts/consume_cache_events.py --invalidate-now
```

Create required topics and run the separate event consumer:

```bash
bash services/property/scripts/create_cache_topics.sh
services/user/.venv/bin/python services/property/scripts/consume_cache_events.py
```

Group `property-cache` consumes property.created/updated/archived,
property.availability.updated and review.created. It rotates a random namespace
token; it never writes listing values from events or modifies reservations.
Old row keys expire by TTL; no FLUSHDB, global scan/delete or other service's keys
are used. If a generation key is lost/evicted, a new random token prevents reuse
of surviving old cache entries.

A Redis Lua transaction stores a seven-day dedup receipt, rotates the token and
increments the invalidation count atomically. Offset commits only after Redis
acknowledgement. This is a derived cache, not a durable business projection:
receipt expiry/loss can repeat a harmless invalidation. No domain write can be
duplicated. Losing all Redis state is a cold cache, not lost business state.
The shared retry/DLQ logic handles connection/timeouts at 200 ms/1 s/5 s; invalid
payloads do not retry. Exhaustion requires DLQ acknowledgement before source
offset commit. Failed DLQ or offset commits stop the consumer; no later offset
may skip the failed record. Redis auth/Lua/programming errors also stop it for
repair. After repair, restart normally; do not reset offsets.

All processes must use identical MySQL host/port/db and Redis settings for the
same namespace (including `MYSQL_HOST=::1` on Carol's machine). A cache event can
arrive before the rating consumer updates MySQL: the subsequent committed
version increment still invalidates any interim cache fill. Availability event
lag never makes an old available=true reusable because availability is not cached.

## Live correctness check

With Property enabled, cache invalidator, Kafka, Redis, MySQL and Booking running:

```bash
services/user/.venv/bin/python services/property/scripts/smoke_search_cache.py
```

It verifies MISS/HIT and identical undated results, a dated HIT using live Booking
(property-2002 excluded, property-2001 included on manifest demo dates), then
publishes a clearly marked synthetic availability **invalidation hint**. It waits
for that event's own receipt and verifies the next search is a MISS. The hint
changes no listing/reservation/held night. Live Booking changes can legitimately
invalidate the fixed demo assertions; inspect them before interpreting a failure.
This checks cache-consumer integration, not a real Booking mutation end-to-end.

## Measure enabled vs disabled

Keep enabled Property on 8003. In an additional terminal, start the same app with
cache explicitly disabled on a separate port:

```bash
PROPERTY_CACHE_ENABLED=false PYTHONPATH=services/user:services/property services/user/.venv/bin/python -m uvicorn property_api.app:app --host 127.0.0.1 --port 8013
```

Then run:

```bash
services/user/.venv/bin/python services/property/scripts/compare_search_cache.py --requests 100 --concurrency 4
```

The script checks mode headers/equal results, warms both, alternates order in two
rounds, measures actual HTTP mean/p50/p95/p99, throughput and cache outcomes, and
saves raw latencies to `reports/perf/property/search_cache.json`. This is undated
listing-search performance, not Booking or full-system performance. Run in a
quiet environment and record slower results honestly; do not assume improvement.
No report is pre-generated. Stop the extra 8013 server when finished.
