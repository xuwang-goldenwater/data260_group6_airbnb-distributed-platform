# Shared synthetic project seed

Owner: Xuanhua Li (Role A); request B/C/E review before database integration.

## Generate and check

Run from the repository root with Python 3.9 or later. No packages, database,
Docker or API keys are needed.

```bash
python3 scripts/generate_seed.py
python3 scripts/generate_seed.py --validate-only
python3 -m unittest discover -s tests -p 'test_seed_data.py' -v
```

The default anchor is **2026-11-24**, not the machine clock. The random seed is
260. Same seed, anchor and generator version produce identical JSON bytes.
Files go to `data/seed/generated/`. Existing output files are protected:

```bash
python3 scripts/generate_seed.py --overwrite
python3 scripts/generate_seed.py --anchor-date 2026-12-07 --output data/seed/demo
python3 scripts/generate_seed.py --output data/seed/demo --validate-only
```

Regenerate a separate dataset for a later demo when needed. Changing the anchor
changes the dataset version and dates; all services must use the same manifest.
Dates in existing API examples are illustrative, not the dataset version.

## Files

Every entity file is a UTF-8 JSON array. Counts and SHA-256 checksums are in
`manifest.json`; these are test-fixture scale choices except for the documented
fixed 200-property pool.

| File | Records / purpose |
|---|---|
| `guests.json` | 120 synthetic guest profiles |
| `hosts.json` | 60 synthetic host profiles |
| `properties.json` | 200 fixed properties; 197 active and 3 non-active |
| `saved_properties.json` | 360 unique guest/property pairs |
| `reservations.json` | Completed stays and all six other lifecycle states |
| `availability_blocks.json` | One row per held night, including completed historical nights |
| `reservation_status_history.json` | Complete transition chains matching reservation version |
| `reviews.json` | One review per completed seed reservation, with optional host responses |
| `conversations.json` | 80 proposed conversation helper records |
| `messages.json` | 160 messages between valid guest/host participants |
| `manifest.json` | Counts, checksums, version, anchor and named scenarios |

Fields follow `docs/api/user_property.md`, `booking.md`, `review.md` and
`messaging_analytics.md`. Conversation storage is not finalized in the API:
`conversations.json` is an explicitly proposed helper fixture for C to review.
IDs for reservations/reviews/messages use deterministic ULID-format strings.
They are synthetic identifiers, not real creation timestamps.

## Scenarios

Use the manifest's dates (anchor +7 through +9, checkout exclusive):

- `property-2001`: San Jose, $180/night, capacity 4, parking/kitchen/wifi,
  eight positive reviews. Its future `requested` row does not hold nights.
- `property-2007`: Comparable basic constraints, with cleanliness,
  cancellation-related and missing-amenity complaint evidence. Its aggregate
  rating is exactly 4.3 from ten genuine seed review rows (seven 4-star and
  three 5-star). Three 4-star rows contain complaint text/tags, representing
  mixed reviews whose risks are not obvious from the overall stars. Its completed
  stays remain valid: cancellation text describes a threatened cancellation,
  not a cancelled reservation being reviewed.
- `property-2044`: Comparable search constraints and two positive reviews,
  allowing investigation of low-review-count ranking. Its future requested
  reservation holds no nights, so it is available on the manifest dates.
- `property-2045`, `property-2049`, `property-2053`: Additional new San Jose
  listings with 0, 1 and 2 reviews respectively. Like property-2044 they cost
  $180/night, accommodate four guests, include parking/kitchen/wifi, and have
  no held nights on the demo dates. All four low-review listings were created
  anchor minus 30 days. A zero-review property's rating is 0.0 (no evidence),
  not an invented average; minimum-rating filters need a team decision for new listings.
- `property-2002`: Pending-host reservation holds the manifest dates.
- `property-2198/2199/2200`: Draft/suspended/archived search-exclusion fixtures.

These scenarios support experiments; they are not human-approved benchmark
labels, fairness verdicts or recorded AI outputs. Review tags are fixture labels
from the documented vocabulary with matching text; E should align the eventual
`tagger-v1` implementation. Geographic details are synthetic demonstration data,
not verified addresses. No property photos are fetched or embedded.

These four low-review candidates support constructing at least three Fairness
Auditor experiments. They do not prove three changed outcomes: D/E must run
the comparison and record actual before/after rankings and evidence. Candidate
counts may change from the previous snapshot; recompute them using identical
filters and booking availability on both sides. Review IDs and reservation IDs
may be reassigned after regeneration, so replace the shared snapshot together.

## Consistency and integration

The validator checks foreign references, uniqueness, all seven booking states,
status-history chains, price totals, exact per-night blocks, completed-stay review
eligibility, rating aggregates, guest counters and message participants. Rejected
requests have a conflicting confirmed stay. Declined/cancelled/requested records
hold no nights. Completed-night blocks are retained per booking.md.

`booking_count` counts all reservation records (including unsuccessful requests).
A/B agreed that dashboards compute counts from reservations directly. Host
cancellations alternate with guest cancellations (16 each), with matching
`cancelled_by`, `status_reason`, and final status-history actor/reason.
Host decisions vary from 10 to 1440 minutes after `pending_host`, including
both endpoints. Host `response_time` is the rounded arithmetic mean in minutes
of that host's approve/decline transitions, as specified by the Host API.
Dashboards compute their own medians from the same status history.
`response_rate` is answered pending-host requests divided by all requests that
reached pending-host, rounded to four decimals. This seed definition includes
pending unanswered requests; B/C should confirm the eventual metric window.
Generator/dataset version 1.2 distinguishes this snapshot from prior versions.
Rating averages
are stored to four decimal places and counts exactly match the review fixtures.

These are **data files, not database migrations or executable API requests**.
The repository has no finalized SQL DDL or application models yet. Import adapters
must be built against the service owners' actual schema; JSON objects/arrays and
UTC timestamps need appropriate MySQL conversions. Suggested load order:

1. MySQL: hosts, guests, properties, saved properties, reservations, status
   history, availability blocks.
2. MongoDB: reviews with the unique review/reservation indexes in review.md.
3. Messaging storage: C approves the conversation mapping and imports threads
   before messages.
4. Redis starts empty; services populate caches. No stale availability cache is seeded.

Properties already contain aggregates of seeded reviews. Do not replay those
reviews as fresh `review.created` events without initializing deduplication state
or rebuilding aggregates, otherwise ratings/counts will be counted twice.
Reservation `publish_pending` is false because this is an imported snapshot,
not a queue backlog. No Kafka offsets, event history, credentials, JWTs,
LLM outputs, analytics projections or benchmark labels are fabricated. Analytics
can be built in a separate replay experiment with its own documented reset rules.

Do not apply these fixtures to a production database. The generator only writes
its named output files and never connects to a database.
