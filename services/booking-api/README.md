# Booking & Availability service (Role B)

Owner: Ayushi Shringi. Contract: [`docs/api/booking.md`](../../docs/api/booking.md).

This PR adds the schema and the seed import; the API and worker come in the next PRs.

| Path | What |
|---|---|
| `../../db/mysql/02_bookings.sql` | the 5 booking tables; `availability_blocks` PK `(property_id, night_date)` is the double-booking guard |
| `scripts/import_seed.py` | imports reservations, status history and availability blocks from `data/seed/generated` |
| `booking_core/` | config (env vars), MySQL pool, UTC helpers |

```bash
make booking-venv                 # .venv (Python 3.11) + dependencies; creates services/booking-api/.env
make booking-infra-up             # Kafka + MySQL from infra/docker-compose.yml
make booking-db booking-seed      # apply 02_bookings.sql, import the seed (idempotent, verified against manifest.json)
make test-booking                 # pytest (isolated database airbnb_booking_test)
make booking-reset                # local only: drop + recreate the booking tables and re-import (picks up schema changes)
```

If host port 3306 is already taken, use `make booking-infra-up BOOKING_MYSQL_3307=1` and set `MYSQL_PORT=3307` in `.env`.
