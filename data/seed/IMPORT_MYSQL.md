# Import the User / Property slice

Owner: Xuanhua Li (Role A).

Prerequisite: apply `db/mysql/01_users_properties.sql` from PR #13 to MySQL 8.4.
Use the existing `infra/docker-compose.yml` MySQL service, healthy and running.
Run these commands at the repository root. Python 3.9+ and Docker Compose are
sufficient; no pip dependencies or virtual environment are needed.

```bash
# Validate the complete committed snapshot and preview this slice's counts.
# This command does not connect to Docker or write to the database.
python3 scripts/import_user_property_seed.py

# Import once, only when all six Role A tables are empty.
python3 scripts/import_user_property_seed.py --apply

# Repeat this read-only check anytime; compares every mapped field in all six tables.
python3 scripts/import_user_property_seed.py --verify-only

# Existing seed tests plus importer tests.
python3 -m unittest discover -s tests -p 'test_*seed*.py' -v
python3 -m unittest discover -s tests -p 'test_user_property_import.py' -v

# After import, enable actual MySQL constraint/rollback tests.
AIRBNB_MYSQL_TEST=1 python3 -m unittest discover -s tests -p 'test_user_property_import.py' -v
```

## Behavior and boundaries

- The importer reads the committed JSON and manifest. It does not regenerate or
  modify them. SHA-256, counts, and the existing full cross-file seed validator
  run before database access. Validation reuses `scripts/generate_seed.py`.
- It imports only guests, hosts, properties, amenities, property_amenities,
  and saved_properties. For seed v1.2: 120 guests, 60 hosts, 200 properties,
  5 amenity names, and 360 saves; property-amenity rows are derived from the file.
- Property `location` is flattened into SQL columns; amenities are split into a
  dictionary and join table. Preferences remain JSON. UTC timestamps are converted
  into MySQL DATETIME(6). Guest deleted_datetime starts as NULL.
- Existing profile counts/host response metrics/rating aggregates are preserved
  from the validated full snapshot. Booking and Review must import the matching
  snapshot separately. Those historical counts do not imply those services are loaded.
- All inserts run in one transaction with strict SQL mode. Nonempty target tables
  are rejected; there is no reset, replace, upsert, or destructive option. A SQL
  error before COMMIT rolls back the attempted import on connection close. If a
  connection fails during COMMIT, use --verify-only to determine the actual state.
- Run while application services are stopped and before live writes begin. Readback
  verification expects the unchanged seed; legitimate subsequent user activity
  will produce differences and is not a reason to reset the database.
- On successful import, a separate consistent-read transaction compares every
  stored field to the expected mapping. Readback failures after a successful
  COMMIT are reported explicitly; do not blindly reapply the import.
- MySQL credentials come from its existing container environment. The importer
  adds no credentials, modifies no Compose settings, and needs no root account.
- Availability here is the listing summary only. Reservations, per-night blocks,
  status history, reviews, conversations, and messages are not written by this
  importer. It cannot yet demonstrate live date-aware search or Redis behavior.

## Expected scenario data

Readback checks all properties, including property-2007's rating 4.3 and ten
reviews, and review counts 2 / 0 / 1 / 2 for properties 2044 / 2045 / 2049 / 2053.
Review text/tags remain in reviews.json for the Review owner to import.
Three fairness outcome changes still require real agent runs and evidence.

## Integration checks

The MySQL tests use transactions to attempt an invalid price, invalid capacity,
duplicate saved-property pair, and an orphan property-amenity relation. Each
attempt must be rejected, and every stored field must still match the snapshot.
They also verify that a second import into nonempty tables fails without changes.
These tests are opt-in and must run against the local unchanged seed database.
When the environment flag is absent, those five tests report skipped, not passed.
