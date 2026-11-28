# Group 6 · Airbnb Distributed Platform (DATA 260)

An Airbnb-style rental platform built as separate services that communicate through Kafka.
Bookings are stored in MySQL, while reviews, messages and AI records are stored in MongoDB. AI recommendations come from three agents
that debate each request, and a person approves the final pick.

| Role | Member | Owns |
|---|---|---|
| A | Xuanhua Li | Users, properties, seed data, MySQL schema → [services/user](services/user/README.md), [services/property](services/property/WRITES.md) |
| B | Ayushi Shringi | Booking API, booking worker, availability → [services/booking-api](services/booking-api/README.md) |
| C | Jietian (Matthew) Chen | Kafka platform, Messaging, Analytics, CI → [services/platform](services/platform/README.md) |
| D | Haozhe Wang | AI service, Kafka Chair, debate panel → [services/ai](services/ai/README.md) |
| E | Xu Wang | Agents, evaluation, semantic cache, Review service → [services/agents](services/agents/), [services/review](services/review/), [benchmarks](benchmarks/) |

How we work: one branch per task, merged by pull request ([CONTRIBUTING.md](CONTRIBUTING.md)).

## Architecture

```text
 Browser ──REST/WebSocket──▶ User 8001 · Property 8003 · Booking 8002 · Review 8004 · Platform · AI 8005
                                   │ each service writes its own store, then publishes an event
                                   ▼
                      Kafka (key = property_id / conversation_id / trace_id)
       booking.* · property.* · review.* · message.created · ai.* · debate.round.0/1/2 · platform.dlq
                                   │
            ┌──────────────────────┼──────────────────────────┬─────────────────────┐
     booking worker ×2       Analytics worker           AI Chair + agents       other consumers
     (MySQL transaction)     (Mongo event log)          (Parser, Matching,
                                                         Risk, Fairness)
  MySQL: users, properties, reservations, availability_blocks      MongoDB: reviews, messages, ai_*
  Redis: cache (compose service)                                    Ollama: qwen2.5:3b (local LLM)
```

Every event uses the shared envelope ([shared/envelope.schema.json](shared/envelope.schema.json)) with an
`idempotency_key`. Consumers skip duplicates, retry 3 times (0.2 s / 1 s / 5 s) and then send to
`platform.dlq`. Topic list and rules: [docs/api/00_conventions.md](docs/api/00_conventions.md).

## Repository layout

| Path | What |
|---|---|
| `services/user`, `services/property` | User and Property APIs (A) |
| `services/booking-api`, `services/booking-worker` | Booking API and worker (B) |
| `services/platform` | Messaging, Analytics, platform dashboard (C) |
| `services/ai` | AI API, Chair, agent workers, debate panel and AI dashboard (D, E) |
| `services/agents` | Parser, semantic cache, Matching, Risk, Fairness, Reflection, Chair rule (E) |
| `services/review` | Review service (E) |
| `shared/` | event envelope schema and shared Kafka consumer (retry, DLQ) |
| `db/mysql`, `db/mongo` | MySQL schema (01–05) and Mongo collections/indexes |
| `data/seed` | reproducible synthetic seed (random seed 260) |
| `benchmarks/` | 50 benchmark requests, human labels, runs and evaluation results |
| `reports/` | measured performance evidence |
| `docs/` | API document ([api_design.md](docs/api_design.md)), per-service contracts in `docs/api/`, [entities.md](docs/entities.md) |
| `frontend/` | Booking console |

## Run locally

Requirements: Docker, Python 3.11, and (optional, for the AI parser and explanation) [Ollama](https://ollama.com) with `qwen2.5:3b`.
Use the **same `JWT_SECRET`** for every service.

**1. Infrastructure, topics and data**

```sh
docker compose -f infra/docker-compose.yml up -d kafka mysql mongodb redis
make booking-venv booking-topics booking-db booking-seed    # MySQL booking tables + seed
make platform-venv platform-topics platform-init platform-seed   # remaining topics, Mongo indexes + seed
python3 scripts/import_user_property_seed.py --verify-only  # users/properties (see data/seed/IMPORT_MYSQL.md for an empty DB)
```

**2. Services** (one terminal each, from the repo root)

| Service | Command | URL |
|---|---|---|
| User (A) | `PYTHONPATH=services/user:services/property services/user/.venv/bin/python -m uvicorn user_api.app:app --port 8001` | :8001/docs |
| Property (A) | same, with `property_api.app:app --port 8003` | :8003/docs |
| Booking API (B) | `make booking-api` | :8002/docs, UI at :8002/ui |
| Booking worker (B) | `make booking-worker` (start 2 with different `WORKER_ID`) | |
| Review (E) | `cd services/review && uvicorn review_api.app:app --port 8004` | :8004/docs |
| Platform (C) | `make platform-api` and `make platform-worker` | :8005/platform |
| AI (D) | `PYTHONPATH=services/ai uvicorn ai_api.app:app --port 8005`, plus `python -m ai_workers.chair_worker` and `python -m ai_workers.agent_worker <parser\|matching\|risk\|fairness>` | :8005/ai/panel, :8005/ai/dashboard |

> **Port note:** Platform and AI both default to 8005. To run both, start Platform on another port,
> for example `PLATFORM_PORT=8006 make platform-api`, and set Booking's `ANALYTICS_SERVICE_URL` to match.

Details and environment variables are in each service's README. Dev tokens: `make booking-token SUB=guest-1001 ROLE=guest`
or `python services/ai/scripts/dev_token.py guest-demo guest`.

## Tests

```sh
make test-venv test     # Platform, seed/importer, AI and Booking tests (Booking uses airbnb_booking_test: make booking-test-db)
services/user/.venv/bin/python -m unittest discover -s tests/user_property -p 'test_*.py'
cd services/review && python -m pytest -q tests
```

CI runs the same checks in [.github/workflows/platform.yml](.github/workflows/platform.yml).

## AI evaluation (benchmarks/)

50 fixed requests (`benchmarks/requests.json`) were labeled independently by two people and then adjudicated
(`benchmarks/labeling/`). Each configuration is scored with `benchmarks/eval.py`, and all results
are in [benchmarks/results/summary.csv](benchmarks/results/summary.csv).

| Configuration | Mean rank of human top pick (lower is better) |
|---|---|
| Baseline (filter count) | 3.30 |
| Single agent (matcher-v1) | 2.86 |
| Debate without Auditor | 2.84 |
| Full debate | 2.94 |
| Human agreement ceiling | 1.95 |

Other results: semantic cache 50/50 paraphrase hits with 0 wrong reuses in 89 trap pairs
(`semantic_cache_eval.json`); human approval of 30 recommendations (`approval_summary.json`);
matcher-v2/v3 were tested and rejected (3.34, 3.28), and explanation-v2 was adopted (`explanation_v2_eval.json`).

```sh
python3 benchmarks/run_debate.py      # rerun the debate on all 50 requests
python3 benchmarks/eval.py benchmarks/results/full_debate.csv \
  --labels benchmarks/labeling/labels_xu.csv benchmarks/labeling/labels_xuanhua.csv \
  --final benchmarks/labeling/labels_final.csv
```

## Performance evidence

Summary: [reports/performance.md](reports/performance.md)

- Booking (B): 107.3 / 164.3 / 187.7 / 136.7 bookings/s at concurrency 1 / 10 / 25 / 50, zero double bookings → `reports/perf/booking/`
- Kafka → Mongo (C): 471.5 records/s with 1 consumer, 585.1 with 3 (3 partitions) → `reports/perf/kafka/`
- AI (D): debate latency, agent timeout, invalid output, three concurrent debates → `reports/perf/ai/`
- Search JMeter run (100 threads, 10,000+ rows, B / B+S / B+S+K / B+S+K+Other): **pending**
