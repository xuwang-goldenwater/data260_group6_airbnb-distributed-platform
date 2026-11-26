# Booking & Availability service (Role B)

Owner: Ayushi Shringi. Contract: [`docs/api/booking.md`](../../docs/api/booking.md). Shared rules: [`docs/api/00_conventions.md`](../../docs/api/00_conventions.md).

```text
Browser ──POST /bookings/create──▶ booking-api ──booking.requested (key=property_id)──▶ Kafka ──▶ booking-worker ×2
   ▲                                  │  (MySQL: reservation + history + idempotency key, then publish)       │
   │                                  │                                     MySQL: FOR UPDATE + availability_blocks
   └──── WebSocket /bookings/stream ◀─┴── booking.updated / property.availability.updated ◀── Kafka ◀───────┘
```

| Path | What |
|---|---|
| `booking_core/` | shared by API and worker: config, DB pool, `dao/` (all SQL), envelope + DLQ, events, producer, outbox, cross-service clients |
| `booking_api/` | FastAPI app: create/get endpoints, outbox publisher |
| `../booking-worker/booking_worker/` | separate worker process (`worker.py`) and `consumer_utils.py` (retry / DLQ helpers) |
| `scripts/` | `import_seed.py`, `dev_token.py` |
| `../../db/mysql/02_bookings.sql` | schema |

## Run it (from the repository root)

Requirements: Docker, Python 3.11 (`uv` installs it automatically), `make`.

```bash
make booking-venv                 # .venv + dependencies; creates services/booking-api/.env from .env.example
#   edit services/booking-api/.env: set JWT_SECRET to a long random value (and see "Ports" below)
make booking-infra-up             # Kafka + MySQL from infra/docker-compose.yml, waits until healthy
make booking-topics               # booking.requested / booking.updated / property.availability.updated / platform.dlq (3 partitions)
make booking-db booking-seed      # apply 02_bookings.sql, import data/seed/generated (idempotent, verified against manifest.json)

make booking-api                                  # terminal 1 → http://localhost:8002  (docs at /docs)
make booking-worker WORKER_ID=worker-1            # terminal 2
make booking-worker WORKER_ID=worker-2            # terminal 3 (same consumer group "booking-worker")
```

**Ports.** If host port 3306 is already in use (for example a Homebrew MySQL), start the infra with
`make booking-infra-up BOOKING_MYSQL_3307=1` and set `MYSQL_PORT=3307` in `.env`. The override file is
`services/booking-api/infra/compose.mysql-3307.yml`; the shared compose file is unchanged. The booking service does not use Redis.

## Demo commands

```bash
# tokens (dev only, signed with JWT_SECRET)
G=$(make -s booking-token SUB=guest-1001)
H=$(make -s booking-token SUB=host-501 ROLE=host)

# 1. async booking: 202 now, the worker decides, /bookings/get shows pending_host
curl -s -X POST localhost:8002/bookings/create -H "Authorization: Bearer $G" -H "Idempotency-Key: $(uuidgen)" \
  -d '{"property_id":"property-2001","check_in":"2027-02-10","check_out":"2027-02-12","guest_count":2}'
curl -s -X POST localhost:8002/bookings/get -H "Authorization: Bearer $G" -d '{"reservation_id":"<id>"}'

make test-booking                   # pytest (uses the isolated database airbnb_booking_test)
make booking-reset                  # local only: drop + recreate the 5 booking tables and re-import the seed
```

## Fallbacks for teammates' services that aren't deployed yet

| Missing piece | Fallback | Switch |
|---|---|---|
| Property service (A) | `data/seed/generated/properties.json` | `PROPERTY_SOURCE=http` + `PROPERTY_SERVICE_URL` |
| User service / JWT issuer (A) | seed `guests.json`; `scripts/dev_token.py` | `USER_SERVICE_URL`; same `JWT_SECRET` as the issuer |
| Shared consumer helper (C, task 3.5) | `booking_worker/consumer_utils.py` (`decode`, `run_with_retry`, `send_to_dlq`) | replace the import in `worker.py` |
