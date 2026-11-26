# Root Makefile. Each role keeps its targets in its own section.

# --- Role B --- Booking & Availability (owner: Ayushi Shringi) -------------------------------
# Quick start:  make booking-venv booking-infra-up booking-topics booking-db booking-seed
#               make booking-api          (terminal 1)
#               make booking-worker WORKER_ID=worker-1   (terminal 2)
#               make booking-worker WORKER_ID=worker-2   (terminal 3)
# If host port 3306 is taken (e.g. Homebrew MySQL):  make booking-infra-up BOOKING_MYSQL_3307=1
# and set MYSQL_PORT=3307 in services/booking-api/.env

PY ?= .venv/bin/python
BOOKING_PYTHONPATH := services/booking-api:services/booking-worker
BOOKING_RUN := PYTHONPATH=$(BOOKING_PYTHONPATH) $(PY)
BOOKING_COMPOSE_FILES := -f infra/docker-compose.yml $(if $(BOOKING_MYSQL_3307),-f services/booking-api/infra/compose.mysql-3307.yml)
COMPOSE ?= docker compose $(BOOKING_COMPOSE_FILES)
BOOKING_MYSQL := docker compose -f infra/docker-compose.yml exec -T -e MYSQL_PWD=airbnb mysql mysql -uairbnb airbnb
BOOKING_KAFKA_BIN := docker compose -f infra/docker-compose.yml exec -T kafka /opt/kafka/bin
BOOKING_API_PORT ?= 8002
WORKER_ID ?= worker-1
N ?= 20

.PHONY: booking-venv booking-infra-up booking-topics booking-db booking-seed booking-reset booking-api booking-worker booking-token booking-test-db test-booking

booking-venv: ## create .venv (Python 3.11) with Role B dependencies
	@test -x $(PY) || (command -v uv >/dev/null && uv venv --python 3.11 .venv || python3.11 -m venv .venv)
	@command -v uv >/dev/null && uv pip install --python $(PY) -r services/booking-api/requirements.txt \
	  || $(PY) -m pip install -r services/booking-api/requirements.txt
	@test -f services/booking-api/.env || (cp services/booking-api/.env.example services/booking-api/.env && \
	  echo "created services/booking-api/.env — set JWT_SECRET (and MYSQL_PORT if needed)")

booking-infra-up: ## start Kafka + MySQL from infra/docker-compose.yml and wait until healthy
	$(COMPOSE) up -d kafka mysql
	@for i in $$(seq 1 60); do \
	  s=$$(docker compose -f infra/docker-compose.yml ps --format '{{.Service}}={{.Health}}'); \
	  echo "$$s" | grep -q 'mysql=healthy' && echo "$$s" | grep -q 'kafka=healthy' && echo "$$s" && exit 0; \
	  sleep 3; done; echo "infra not healthy"; exit 1

booking-topics: ## create booking topics (3 partitions) and platform.dlq if missing
	@for t in booking.requested booking.updated property.availability.updated platform.dlq; do \
	  $(BOOKING_KAFKA_BIN)/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
	    --topic $$t --partitions 3 --replication-factor 1 >/dev/null; \
	done
	@$(BOOKING_KAFKA_BIN)/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic booking.requested | head -1

booking-db: ## apply db/mysql/02_bookings.sql
	$(BOOKING_MYSQL) < db/mysql/02_bookings.sql
	@$(BOOKING_MYSQL) -e "SHOW TABLES"

booking-seed: ## import seed reservations/history/blocks (idempotent, verified against manifest.json)
	$(BOOKING_RUN) services/booking-api/scripts/import_seed.py

booking-reset: ## DESTRUCTIVE (local dev only): empty the 5 booking tables, then re-import the seed
	@echo "Emptying Role B tables in the local 'airbnb' database (other teams' tables are untouched)..."
	$(BOOKING_MYSQL) -e "SET FOREIGN_KEY_CHECKS=0; TRUNCATE availability_blocks; TRUNCATE reservation_status_history; \
	  TRUNCATE processed_events; TRUNCATE idempotency_keys; TRUNCATE reservations; SET FOREIGN_KEY_CHECKS=1;"
	$(MAKE) booking-seed

booking-api: ## run the Booking API (FastAPI) on BOOKING_API_PORT
	cd services/booking-api && PYTHONPATH=.:../booking-worker ../../$(PY) -m uvicorn booking_api.app:app \
	  --host 0.0.0.0 --port $(BOOKING_API_PORT)

booking-worker: ## run one Booking worker (start several with different WORKER_ID; same consumer group)
	WORKER_ID=$(WORKER_ID) $(BOOKING_RUN) -m booking_worker.worker

booking-token: ## mint a dev JWT: make booking-token SUB=guest-1001 ROLE=guest
	@$(BOOKING_RUN) services/booking-api/scripts/dev_token.py $(or $(SUB),guest-1001) --role $(or $(ROLE),guest)

booking-test-db: ## create the isolated test database used by tests/booking
	docker compose -f infra/docker-compose.yml exec -T -e MYSQL_PWD=root mysql mysql -uroot -e \
	  "CREATE DATABASE IF NOT EXISTS airbnb_booking_test; GRANT ALL ON airbnb_booking_test.* TO 'airbnb'@'%';"

test-booking: booking-test-db ## pytest for Role B
	$(BOOKING_RUN) -m pytest tests/booking -q

# --- end Role B ---------------------------------------------------------------------------------
