# Review event rating projection

Role A consumes the Review service's existing `review.created` envelope and
payload. Kafka key is `property_id`; group is `property-rating`. Only new files
owned by Role A are added; Review code and the shared consumer are unchanged.

## Prepare the database

Run from the repository root with the existing User virtual environment/config.
The schema creates three operational tables and does not replace listings:

```bash
docker compose -f infra/docker-compose.yml exec -T -e MYSQL_PWD=airbnb mysql mysql -uairbnb airbnb < db/mysql/05_property_ratings.sql
docker compose -f infra/docker-compose.yml exec -T -e MYSQL_PWD=airbnb mysql mysql -uairbnb airbnb_property_test < db/mysql/05_property_ratings.sql
services/user/.venv/bin/python services/property/scripts/initialize_review_ratings.py
services/user/.venv/bin/python services/property/scripts/initialize_review_ratings.py --apply
```

Default is snapshot validation only, without connecting to MySQL. `--apply`
checks all 200 existing seed listings' rating/count and host references against
the checksum-validated seed. In one transaction it records exact integer rating
sums and receipts for the 1,079 imported reviews; listing fields are unchanged.
Previously imported reviews must not increment the ratings again on replay.
If any aggregate differs, initialization rolls back entirely. Do not reset data
to bypass an error: inspect the reported property before reconciling it.

Repeat `--apply` with the same dataset is a no-op, including after live reviews.
A different dataset version/checksum is rejected. The worker refuses startup
without a matching bootstrap marker. Stop the worker before initial bootstrap.
The isolated test database needs schema 01/03/04 from the previous slices and
the new 05; it does not need seed import or the production bootstrap command.

## Tests

```bash
services/user/.venv/bin/python -m unittest discover -s tests/user_property -p 'test_*.py' -v
LISTINGS_MYSQL_TEST=1 LISTINGS_WRITE_MYSQL_TEST=1 LISTINGS_PROFILE_MYSQL_TEST=1 LISTINGS_RATING_MYSQL_TEST=1 services/user/.venv/bin/python -m unittest discover -s tests/user_property -p 'test_*.py' -v
```

The default suite executes projection SQL using SQLite's test adapter and the
actual shared retry/DLQ boundary using controlled publishers/commit callbacks.
Two new opt-in MySQL tests create unique unreviewed listings in
`airbnb_property_test` and verify concurrent duplicate and distinct reviews.
They remove only their own rows. They do not require a running Kafka worker.
These tests do not claim to exercise Kafka delivery or Review HTTP/Mongo.

## Run the worker and live synthetic Kafka check

Keep Docker Kafka and MySQL running. No HTTP service restart is required by this
slice. Create the two topics without changing existing topics:

```bash
bash services/property/scripts/create_review_topics.sh
```

In a dedicated terminal:

```bash
services/user/.venv/bin/python services/property/scripts/consume_reviews.py
```

The worker uses the existing ignored `services/user/.env`, including `MYSQL_HOST`
(`::1` on Carol's machine) and `KAFKA_BOOTSTRAP_SERVERS`. Run one deployment for
this database/group; don't point competing instances in the same group at
different databases. Auto-commit and automatic offset storage are disabled.

In another terminal:

```bash
services/user/.venv/bin/python services/property/scripts/smoke_review_ratings.py
```

This inserts a NEW draft test listing, then sends synthetic 5-star, duplicate
5-star, 3-star and invalid 6-star events with the same property key. It verifies
rating 4.0, count 2, version 3, two receipts, and an acknowledged DLQ event
preserving the invalid original event/key/trace. The fixture is retained as
evidence and stays outside public search/frozen benchmark. This checks Kafka ->
consumer -> MySQL/DLQ; it is not Review HTTP -> Mongo -> Kafka end-to-end proof.
Do not reuse its synthetic reservation IDs in the Review HTTP API.

## Transaction and failure semantics

- The payload must identify review/reservation/property/host with bounded ASCII
  IDs, integer rating 1-5 (booleans rejected), matching review entity, and the
  existing producer's canonical `review.created:<review_id>` idempotency key.
  The shared boundary validates envelope, Kafka key and topic before the handler.
- One transaction claims a uniquely indexed receipt, locks the listing, updates
  exact integer sum/count and the rounded listing average, increments version,
  and commits. Only then may the Kafka offset commit. A replay after a failed
  offset commit is acknowledged without repeating the update.
- Replays must preserve rating identity (review/reservation/property/host/stars).
  Changed event IDs, timestamps or optional tags do not create another rating.
  Reusing a review with changed rating identity goes to DLQ. Seed receipts use
  the same rule. Host ownership and property existence are checked on new reviews.
- The average is calculated from the exact sum and rounded half-up to four
  decimals. It is never reconstructed repeatedly from the rounded old average.
  New unreviewed properties initialize their projection on first review.
  A nonzero property with no baseline or a mismatched projection stops the
  worker for repair without committing its offset. Archived properties may
  still receive reviews for completed historical stays.
- `shared.consumer.process_record` implements the agreed 200 ms/1 s/5 s retries
  for known MySQL connection/lock outages. Invalid events are not retried.
  Permanent invalid events or exhausted transient retries go to `platform.dlq`.
  Source offset commits only after DLQ broker acknowledgement.
- Authentication/schema/programming errors, failed DLQ acknowledgements, and
  offset commit failures terminate the worker so a later record cannot skip the
  failed one. Restart after repairing the dependency; do not reset group offsets.
  DLQ records need controlled repair/replay after the underlying issue is fixed.

There is no Redis search cache yet, so there is no new cache invalidation in this
slice. Rating updates to original seed listings intentionally make the frozen
benchmark pool check fail until a coordinated benchmark snapshot is selected;
the live smoke therefore uses a new listing. A full Review HTTP integration and
review UI remain separate work with Role E.
