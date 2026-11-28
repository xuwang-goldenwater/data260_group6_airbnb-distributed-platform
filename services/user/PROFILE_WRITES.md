# Profile updates, guest deletion and saved properties

This slice builds on PR #33. It adds the following endpoints using the existing
JWT identity and shared MySQL database:

| Endpoint | Access / behavior |
| --- | --- |
| POST /guests/update | Own guest profile or admin; editable fields only |
| POST /hosts/update | Own host profile or admin; no access/verification/metric escalation |
| POST /guests/delete | Own guest or admin; Booking active-reservation precheck; soft delete |
| POST /guests/search | Admin or service only; paginated public-profile projection |
| POST /properties/save | Guest only; property_id + boolean saved; guest_id from JWT |

All mutations require an Idempotency-Key UUID. Same key/body replays the original
response, including after deletion or later dependency failures. Reusing a key
with different input returns 409. The existing deadlock/lock-timeout handling is
retained: roll back the entire transaction before at most three retries. Other
database/authentication/ambiguous commit failures are not blindly retried.

## Install and schema

From the repository root, after creating the new branch from a/property-writes:

```bash
services/user/.venv/bin/python -m pip install -r services/user/requirements.txt
docker compose -f infra/docker-compose.yml exec -T -e MYSQL_PWD=airbnb mysql mysql -uairbnb airbnb < db/mysql/04_user_operations.sql
```

The migration adds only user_http_operations; it does not replace existing tables
or data. Existing property_http_operations and property_event_outbox from #33 are
reused for saved-property operations. Keep the working local .env and shared JWT.

## Default and live MySQL tests

```bash
services/user/.venv/bin/python -m unittest discover -s tests/user_property -p 'test_*.py' -v
```

The new profile/save MySQL tests use airbnb_property_test, like the existing write
tests. If it was already created for #33, only add the new operational table:

```bash
docker compose -f infra/docker-compose.yml exec -T -e MYSQL_PWD=airbnb mysql mysql -uairbnb airbnb_property_test < db/mysql/04_user_operations.sql
LISTINGS_MYSQL_TEST=1 LISTINGS_WRITE_MYSQL_TEST=1 LISTINGS_PROFILE_MYSQL_TEST=1 services/user/.venv/bin/python -m unittest discover -s tests/user_property -p 'test_*.py' -v
```

For a new machine, first create/grant airbnb_property_test and apply 01 and 03 as
described in ../property/WRITES.md. No seed import is needed in that isolated DB.
The new tests create unique profiles/listings and remove only their own test
records. They test concurrent same-state saves and durable count consistency.
Their HTTP dependency clients are test doubles. Live HTTP is a separate check.

## Live HTTP check

Restart both User and Property using their usual uvicorn commands. Keep Booking
running with the shared secret. Create the new Kafka topic before issuing saves:

```bash
bash services/property/scripts/create_topics.sh
services/user/.venv/bin/python services/user/scripts/smoke_profiles_saved.py
```

The script explicitly inserts NEW synthetic guest/host fixtures for the check;
this is not a registration API. It prints their IDs without any tokens/secrets.
It edits those profiles, verifies replay, saves a property, repeats the requested
state with a fresh key, reads the User count, unsaves, performs restricted search,
and soft-deletes the new guest through the real Booking guard. The new host and
soft-deleted guest remain as integration evidence. Seed profiles are untouched.

If interrupted, inspect the printed fixture IDs before repeating. The target
property defaults to property-2001; its listing fields are not modified. The
check expects two property.saved events: one save, one unsave. No-op saves create
neither duplicate relationships nor extra events.

Keep the existing property outbox relay running (restart it if needed):

```bash
services/user/.venv/bin/python services/property/scripts/publish_events.py
```

Then verify Kafka acknowledgement marks:

```bash
docker compose -f infra/docker-compose.yml exec -T -e MYSQL_PWD=airbnb mysql mysql -uairbnb airbnb -e "SELECT topic, COUNT(*) AS queued, SUM(published_datetime IS NOT NULL) AS published FROM property_event_outbox GROUP BY topic;"
```

Existing property.created/updated/archived rows remain. For a single successful
profile/save smoke check, property.saved should show 2 queued and 2 published.

## Validation and data ownership

- Profile IDs are target selectors, never caller identity. Wrong-role, cross-user,
  service-token writes, and client-supplied counters/verification/access fields
  are rejected. Service tokens retain their existing private read access.
- Email is validated and uniqueness conflicts return 409. Names, E.164 phones,
  bounded locations and HTTPS photo URLs are validated. No photo is fetched by
  the server. Optional phone/location/photo/preferences may be cleared with null;
  required names/email may not. Empty edits are rejected.
- A preferences object patches its specified keys; omitted keys remain unchanged.
  Null clears the whole optional preferences value. Lists must be unique and
  property types must be supported values.
- Guest search supports literal substring name/location matching, exact email,
  deterministic guest-ID ordering, and pagination. Deleted profiles are excluded.
  Search results omit email, phone, preferences and counters.
- Deletion checks the real Booking guest active-reservation endpoint. Missing,
  malformed or unavailable responses fail closed. Historical profiles, saved
  relationships, bookings and events remain. The check is not an atomic lock
  spanning Property/User and Booking; concurrent booking/deletion needs B-side
  coordination, as with archive in #33.
- Save validates the guest through User HTTP and rechecks it under a MySQL row
  lock. Only active properties may be saved/unsaved per the current contract.
  The lock order is guest, property, saved relationship. The same guest's
  different save operations serialize before the authoritative count query.
- Both Role A services intentionally share the existing MySQL database. The
  User-owned persistence helper updates saved_properties_count in the SAME
  transaction as the Property-owned relationship and queued event. There is no
  eventual-consistency window between those fields. This is shared-database
  coordination, not independently isolated service databases. A future database
  split would require an event projection or coordinated protocol instead.
- Changed state enqueues property.saved with guest/property IDs, saved boolean,
  saved_datetime, shared envelope and key=property_id. Idempotent replay and
  same-state no-ops enqueue nothing. Response saved_datetime is null after unsave.
  Kafka remains at-least-once; Analytics must use the shared deduplication rules.

Still separate work: guest/host creation, public registration/login, saved-list UI,
property.viewed, review aggregation, Redis, and the frontend pages.
