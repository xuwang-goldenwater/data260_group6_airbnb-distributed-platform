# User and Property APIs (Role A)

Property create/update/archive and the transactional Kafka outbox are documented
in [`../property/WRITES.md`](../property/WRITES.md). The read API instructions below
remain applicable.

Profile update/delete/search and property save/unsave are documented in
[`PROFILE_WRITES.md`](PROFILE_WRITES.md).

This first implementation slice connects the existing MySQL schema and imported
seed to two FastAPI services. It does not create or reset tables. It does not
change the CSV labels, benchmark results, Booking service, or AI service.

## Setup from the repository root

Use a dedicated environment so Role B's existing pinned dependencies stay intact:

```bash
python3 -m venv services/user/.venv
services/user/.venv/bin/python -m pip install -r services/user/requirements.txt
python3 services/user/scripts/setup_local.py
docker compose -f infra/docker-compose.yml up -d mysql
```

`setup_local.py` creates only the ignored `services/user/.env`, reusing Booking's
local database/JWT configuration if available. It retains an existing Role A file.
Otherwise it generates a local JWT secret without printing it. Both services read
this same file. Exported environment values take precedence. No activation needed.

The six Role A tables must already exist and contain the seed. For an existing
database, do not repeat `--apply` or reset data; verify it instead:

```bash
python3 scripts/import_user_property_seed.py --verify-only
```

If starting a genuinely empty database, follow `data/seed/IMPORT_MYSQL.md` first.

## Tests

```bash
services/user/.venv/bin/python -m unittest discover -s tests/user_property -p 'test_*.py' -v
LISTINGS_MYSQL_TEST=1 services/user/.venv/bin/python -m unittest discover -s tests/user_property -p 'test_*.py' -v
```

Default tests execute the production SELECT statements using a SQLite test
adapter and call the actual FastAPI routes. They test profile ownership, JWTs,
nonpublic properties, parameterized filters, sorting, availability-before-paging,
common errors, and fixed-pool parity for all 50 requests. Booking HTTP failures
and malformed responses are tested through httpx MockTransport.

The two opt-in tests perform read-only MySQL calls using the configured database
and verify all 50 candidate sets through the APIs. Booking remains a test fixture
in those two tests: this validates MySQL integration, not the live Booking process.
Run on the unchanged seed database; frozen-pool tests reject modified listings.

## Start the services (two terminals, repository root)

Terminal 1:

```bash
PYTHONPATH=services/user:services/property services/user/.venv/bin/python -m uvicorn user_api.app:app --host 127.0.0.1 --port 8001
```

Terminal 2:

```bash
PYTHONPATH=services/user:services/property services/user/.venv/bin/python -m uvicorn property_api.app:app --host 127.0.0.1 --port 8003
```

Interactive API docs: `http://127.0.0.1:8001/docs` and
`http://127.0.0.1:8003/docs`. `/health` is liveness; `/ready` also checks MySQL
and JWT configuration. Schema creation and data loading are intentionally explicit.

Terminal 3:

```bash
services/user/.venv/bin/python services/user/scripts/smoke_read.py
```

No Booking process is needed for undated browsing and the basic smoke check.

## Endpoints

| Service | Endpoint | Access |
| --- | --- | --- |
| User | `POST /guests/get` | Own guest, admin, or service |
| User | `POST /hosts/get` | Own host, admin, or service |
| User | `GET /internal/guests/{guest_id}` | Service only; unknown/deleted -> 404 |
| Property | `POST /properties/get` | Active listing public; nonactive only owning host/admin/service |
| Property | `POST /properties/search` | Public; validates any supplied JWT |
| Property | `POST /properties/availability` | Public; calls Booking |
| Property | `POST /internal/properties/candidates` | Service only; frozen pool and dates required |
| Property | `GET /internal/hosts/{host_id}/properties` | Service only; complete host listing projection |

For local tests, mint a token using the existing shared JWT settings:

```bash
services/user/.venv/bin/python services/user/scripts/dev_token.py guest-1001 --role guest
services/user/.venv/bin/python services/user/scripts/dev_token.py ai-service --role service
```

This CLI is not a login API. There is no unauthenticated HTTP token-minting route.
HS256 with explicit `sub`, `role`, `exp` and optional issuer matches the team's
current service-token convention. Authentication does not infer a role from an ID.
Private profile reads check the caller's role as well as subject.

## Live Booking integration

Run Role B's API separately on port 8002 with the same `JWT_SECRET` and
`JWT_ISSUER`; it must have its own schema and matching Booking seed loaded.
Property calls `POST /internal/availability/check` with a service JWT, dates,
trace ID, and batches of at most 200 property IDs. It validates response dates,
property IDs, booleans, and timestamp. Failures return 503; there is no seed
availability fallback. Search applies availability before pagination/total.

```bash
services/user/.venv/bin/python services/user/scripts/smoke_read.py --booking
```

For Booking -> Role A, configure Role B's existing settings:

```text
PROPERTY_SOURCE=http
PROPERTY_SERVICE_URL=http://127.0.0.1:8003
USER_SERVICE_URL=http://127.0.0.1:8001
```

Restart Booking after configuration changes. Its current host-list client still
uses seed; the new service-authenticated host listing endpoint is available for B
to wire explicitly. Its current fallback behavior is unchanged by this patch.
AI also continues its existing seed-based bridge until D/E explicitly switch it.

## Search and frozen benchmark behavior

- Search uses the names in `docs/api/user_property.md`, including `location`,
  `guest_count`, `maximum_nightly_price`, `required_amenities`, and `minimum_rating`.
  Unknown fields/filter values are rejected with 400. All required amenities must
  match. Date strings are ISO dates, paired, with checkout exclusive (1–365 nights).
  Read-only historical dates remain allowed for reproducing the fixed benchmark.
- Search accepts `price_asc`, `price_desc`, `rating_desc`, `property_id_asc`, and
  `relevance`. This slice defines `relevance` as rating descending. Ties use
  property ID; no personalized ranking is implemented.
- Without dates, `available` is null, not an assertion that the property is free.
  With dates, Booking is checked even if `available_only=false`; the flag controls
  whether unavailable rows are removed. `available_only=true` requires dates.
- Public minimum-rating search is literal. Unrated properties do not satisfy a
  positive minimum. Internal benchmark candidates retain the committed benchmark
  convention that zero-review ratings are unknown and are not excluded by a
  minimum-rating request. This is the R17/R38 exception already in the benchmark.
- The internal endpoint uses the manifest `pool_id`, `dataset_version`, and
  `selection_rule=deterministic_filter_then_property_id`. Its `filters` use the
  Property API field names, not Parser aliases (`city`, `guests`, etc.).
- Frozen JSON supplies IDs/version/checksum for the fixed pool, never response
  values. All properties come from MySQL. A changed version, missing pool record,
  or changed listing fails with 409 DATASET_VERSION_CONFLICT. New listings outside
  the pool do not enter the benchmark. Candidate IDs are lexicographically sorted;
  no pagination truncates the fixed pool. Each agent still computes its own hash.
- The frozen check protects listing identity only; availability is live Booking
  state. If bookings change after seeding, the live candidate set may legitimately
  differ from the offline benchmark. Reproduce benchmarks using its original state.

## Scope still to implement

Guest/host creation, Kafka view events, Redis invalidation,
frontend pages, login/registration and search load tests are later slices.
Property writes now implement HTTP idempotency and enqueue create/update/archive
events; the separate outbox relay must run to deliver them. Authenticated property
reads do not yet emit `property.viewed`.
Profile edits/deletion and save/unsave are implemented in the profile-write slice;
changed saved state enqueues property.saved through the same relay.
Review aggregation and deduplication now have a separate Kafka worker; see
[`../property/RATINGS.md`](../property/RATINGS.md) for the required baseline and tests.
The presence of these read APIs is not completion of the whole User/Property spec.

The common Role A code lives in `services/user/listings_core`; both app entrypoints
use it via PYTHONPATH. No teammate-owned service files or root Makefile are changed.
