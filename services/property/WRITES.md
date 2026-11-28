# Property writes and event delivery

This slice adds create/update/archive to the read services from PR #32. Start
from that PR's branch until it is merged. It uses the existing User/Booking HTTP
services and database; it neither replaces seed nor changes teammate-owned code.

## Install and apply the additive operational schema

From the repository root, on the new write branch:

```bash
services/user/.venv/bin/python -m pip install -r services/user/requirements.txt
docker compose -f infra/docker-compose.yml exec -T -e MYSQL_PWD=airbnb mysql mysql -uairbnb airbnb < db/mysql/03_property_operations.sql
```

The two new tables are property_http_operations and property_event_outbox. Existing
guest/host/property/Booking tables and data are untouched. Retain the working local
MySQL address (`::1` on Carol's machine) and shared JWT settings in the ignored .env.
No new secrets are required. Restart Property after changing its Python code;
restart User only if its configuration changes.

## Tests

```bash
services/user/.venv/bin/python -m unittest discover -s tests/user_property -p 'test_*.py' -v
```

Default tests exercise writes through FastAPI and SQL fixtures, including atomic
rollback, failed dependencies, permissions, idempotent replay, version conflicts,
archive guard bypass attempts, and stable-envelope outbox retry. They do not
require a live Kafka broker and do not prove live MySQL behavior.

For isolated MySQL write tests, create a NEW test database and grant the existing
development account access. No existing database is dropped or reset:

```bash
docker compose -f infra/docker-compose.yml exec -T -e MYSQL_PWD=root mysql mysql -uroot -e "CREATE DATABASE IF NOT EXISTS airbnb_property_test; GRANT ALL ON airbnb_property_test.* TO 'airbnb'@'%';"
docker compose -f infra/docker-compose.yml exec -T -e MYSQL_PWD=airbnb mysql mysql -uairbnb airbnb_property_test < db/mysql/01_users_properties.sql
docker compose -f infra/docker-compose.yml exec -T -e MYSQL_PWD=airbnb mysql mysql -uairbnb airbnb_property_test < db/mysql/03_property_operations.sql
LISTINGS_WRITE_MYSQL_TEST=1 services/user/.venv/bin/python -m unittest discover -s tests/user_property -p 'test_*.py' -v
```

The two write integration tests use only airbnb_property_test, not the configured
main database. They create uniquely named test listings and remove only their own
listings, join rows, operations, and outbox events afterward. Their User/Booking
dependencies are test doubles. One test sends eight same-key concurrent writes
and checks that exactly one listing/event is created. Existing read integration
tests can additionally be enabled with LISTINGS_MYSQL_TEST=1.

## Real HTTP integration

Keep User on 8001 and Booking on 8002 running with the shared JWT secret. Restart
Property on 8003 using its original command:

```bash
PYTHONPATH=services/user:services/property services/user/.venv/bin/python -m uvicorn property_api.app:app --host 127.0.0.1 --port 8003
```

From another terminal:

```bash
services/user/.venv/bin/python services/property/scripts/smoke_write.py
```

This creates a NEW synthetic listing owned by host-501, checks its draft status,
replays the same create key, activates/updates it, rejects a stale version, then
archives it through the real Booking guard and checks public invisibility. It
does not modify any of the 200 frozen benchmark listings. The new archived
listing remains as integration evidence. It queues three events, but publication
is a separate check below. If interrupted, use the printed/server-log property ID
to inspect the listing before repeating the smoke test.

## Kafka event relay

```bash
docker compose -f infra/docker-compose.yml up -d kafka
# Wait until healthy, then:
bash services/property/scripts/create_topics.sh
services/user/.venv/bin/python services/property/scripts/publish_events.py
```

Keep the relay running in its own terminal. It publishes one pending outbox row
at a time to property.created, property.updated, or property.archived with Kafka
key=property_id and the shared validated envelope. The row is marked published
only after broker acknowledgement. It preserves event_id and idempotency_key on
retries. A MySQL connection-level lock ensures only one relay drains this database
at a time, so multiple relay processes do not reorder one property's queued events.

Each drain attempt makes at most four sends, backing off 200 ms, 1 s, and 5 s.
On persistent failure the row stays pending and the running relay waits 5 s before
trying again. The earliest pending event blocks later ones rather than being
discarded. Investigate invalid manually edited outbox rows; the relay does not
silently send invalid data or discard it to a DLQ.

To attempt one event and exit, use --once. Exit status is nonzero if Kafka did not
acknowledge. Crash after broker acknowledgement but before the MySQL mark can
redeliver an event: consumers must deduplicate by idempotency_key per the shared
convention. This is at-least-once delivery, not a cross-system exactly-once claim.

Verify acknowledgement records (counts should drain to zero pending):

```bash
docker compose -f infra/docker-compose.yml exec -T -e MYSQL_PWD=airbnb mysql mysql -uairbnb airbnb -e "SELECT topic, COUNT(*) AS queued, SUM(published_datetime IS NOT NULL) AS published FROM property_event_outbox GROUP BY topic;"
```

## Write contract

- All three routes require a valid host/admin JWT and Idempotency-Key UUID. Create
  requires a host profile validated through User HTTP. host_id is always the JWT
  subject; clients cannot assign another owner. Guest/service/agent writes fail.
- Update/archive lock the property row, check owning host or admin and compare
  expected_version. Every successful update increments version once. The key is
  scoped to authenticated subject and route; body mismatch returns 409. An exact
  replay returns the original body with HTTP 200 even if dependencies later fail.
- Idempotency claims use a no-op duplicate-key UPDATE to obtain an exclusive
  record lock directly, avoiding shared-to-exclusive lock upgrades on replay.
  MySQL deadlock (1213) and lock timeout (1205) errors trigger at most three
  whole-transaction retries after rollback (50 ms, 200 ms, 500 ms). Authentication,
  connection loss and ambiguous commit failures are not automatically retried.
  Numeric MySQL codes are retained internally and in Python traceback notes;
  database credentials and original driver messages are not returned by the API.
- Create returns HTTP 201, a generated opaque ID, draft status and zero reviews.
  All fields have schema-aligned lengths/ranges. Money accepts at most two decimals,
  coordinates six. Empty changes, null changes, duplicates, and server-owned fields
  are rejected. New normalized amenity names are added to the dictionary atomically.
- Amenities and listing updates, the response record, and event enqueue commit in
  one transaction. Failure rolls back all of them. No event is sent before commit.
- /properties/delete soft-archives; historical records remain. Both deletion and
  update-to-archived call Booking's active-reservation check and fail closed on
  missing/malformed/unavailable results. This is a cross-service precheck, not an
  atomic lock spanning Property and Booking; simultaneous booking/archive races
  require coordination with B. The existing MySQL double-booking guard is unchanged.
- Events include property ID, host ID, version, changed fields and update timestamp.
  Update-to-archived emits both property.updated and property.archived. Consumers
  must not assume ordering across topics. Redis invalidation consumers are next
  work; this slice does not claim cache invalidation is already implemented.
- Existing frozen benchmark properties remain editable, but edits intentionally
  make the frozen candidate endpoint return 409 until its snapshot is reconciled.
  Use newly created properties for write demos so benchmark data stays unchanged.

Still separate: user/profile writes, save/unsave, property.viewed, review aggregation,
Redis, frontend, and public registration/login.
