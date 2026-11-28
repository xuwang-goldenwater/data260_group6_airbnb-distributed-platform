# data260_group6_airbnb-distributed-platform

## Platform (Role C: Matthew)

See [Platform setup](services/platform/README.md) for Messaging, Analytics, Mongo seed/indexes,
the platform dashboard and Booking integration. From the repository root:

```sh
make platform-venv
docker compose -f infra/docker-compose.yml up -d kafka mongodb redis
make platform-topics platform-init platform-seed
export JWT_SECRET='use-the-same-local-secret-as-booking-and-ai'
make platform-api
# Separate terminal, with the same environment:
make platform-worker
```

UI: `http://127.0.0.1:8006/platform`. Interactive API: `http://127.0.0.1:8006/docs`.
Run `make test-venv test` for the repository checks; Booking integration checks use the isolated
`airbnb_booking_test` database (`make booking-test-db`) and skip if it is unavailable.
Run `make kafka-perf` for the measured Kafka comparison and
`PYTHONPATH=.:services/platform .venv/bin/python scripts/smoke_platform.py` for live HTTP/Kafka/Mongo verification.
Performance evidence: [report](reports/performance.md). Contribution and presentation notes:
[Role C handoff](docs/role_c_handoff.md).
