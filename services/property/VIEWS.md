# Guest property view events

`POST /properties/get` now queues `property.viewed` for successful authenticated
Guest detail requests. It reuses `property_http_operations` and
`property_event_outbox` from migration 03; no new schema or dependency is needed.
The payload matches `docs/api/user_property.md`: property_id, guest_id and
viewed_datetime. JWT subject supplies guest_id; Kafka key is property_id.

## Semantics

- Anonymous, Host, Admin, service and agent reads emit no guest view. Search,
  availability and internal candidate calls emit none. Invalid JWTs, invalid
  request bodies, missing/nonpublic properties and failed new-view transactions
  emit nothing.
- A fresh Guest view checks the active Guest profile and active property under
  MySQL row locks before inserting the event. This deliberately uses the same
  Role A shared database/persistence convention as save/unsave. It adds no User
  HTTP dependency. Unknown/deleted Guests cannot create new view events.
- No header is required for existing read clients. Each accepted Guest detail
  request without a key is a separate view. For retry suppression, clients may
  send `Idempotency-Key: <uuid>` and reuse it only for retries of that view.
  Keys are scoped to Guest plus the view operation. A key reused for another
  property returns 409; malformed explicit Guest keys return 400.
- The receipt stores only an event ID, not the listing response. Every request
  first reads current listing details and applies current visibility checks,
  including replay requests. `Idempotent-Replay: true` means the event was
  already recorded, not that stale listing data was returned. Replays preserve
  the original event's trace/timestamp and add no event; fresh views use the
  current request's trace. New keys represent new views, even on the same listing.
- Receipt and event commit atomically before HTTP 200. A failed outbox write
  fails the Guest request (MySQL outages use the common 503 envelope), allowing
  retry with the same key. Anonymous/non-Guest detail reads need no outbox write.
  Whole-transaction deadlock retries reuse the existing operations helper.
- The existing relay publishes with broker acknowledgement, retains pending
  events during outages and may redeliver after a crash. Analytics must continue
  deduplicating by event idempotency_key. HTTP key suppression and Kafka delivery
  deduplication cover different retry boundaries. Requests without a client key
  cannot distinguish a new view from a retried HTTP request.
- Listing version, rating, counters and benchmark fields are unchanged by views.
  There is no live Analytics ingestion check in this slice.

## Local verification

Run from the repository root:

```bash
services/user/.venv/bin/python -m unittest discover -s tests/user_property -p 'test_*.py' -v
LISTINGS_MYSQL_TEST=1 LISTINGS_WRITE_MYSQL_TEST=1 LISTINGS_PROFILE_MYSQL_TEST=1 LISTINGS_RATING_MYSQL_TEST=1 LISTINGS_VIEW_MYSQL_TEST=1 services/user/.venv/bin/python -m unittest discover -s tests/user_property -p 'test_*.py' -v
```

The new opt-in MySQL test uses the existing `airbnb_property_test` schema and
unique test fixtures to verify concurrent same-key views create one event.
Default tests use SQLite and actual FastAPI routes; they are not live MySQL proof.

Create the topic before issuing Guest detail requests:

```bash
bash services/property/scripts/create_topics.sh
```

Restart only Property in its existing terminal so the new route code loads:

```bash
PYTHONPATH=services/user:services/property services/user/.venv/bin/python -m uvicorn property_api.app:app --host 127.0.0.1 --port 8003
```

Keep the existing outbox relay running, or start it in another terminal:

```bash
services/user/.venv/bin/python services/property/scripts/publish_events.py
```

Run the live check in an operations terminal:

```bash
services/user/.venv/bin/python services/property/scripts/smoke_view_events.py --wait-published
```

The script defaults to existing Guest guest-1001 and active property-2001.
It makes three Guest detail requests (one is a same-key replay), plus anonymous,
Host, service, search and 404 controls. It verifies exactly two events for its
unique trace, unchanged listing fields, and two relay acknowledgement marks.
It creates no new profile/listing and does not need seed reimport or migrations.
The events are synthetic test activity and remain in the outbox as evidence.
Other smoke/API scripts making Guest detail reads now also create view activity;
global topic totals may therefore exceed two. Use this script's trace/counts.
If waiting for publication times out, repair the relay and inspect existing
pending rows instead of rerunning the smoke just to trigger publication.
