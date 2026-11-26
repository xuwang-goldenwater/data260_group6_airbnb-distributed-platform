# Root Makefile. Each role keeps its targets in its own section.

# --- Role B --- Booking & Availability (owner: Ayushi Shringi) -------------------------------
# Quick start:  make booking-venv booking-infra-up booking-db booking-seed
#               make test-booking
# If host port 3306 is taken (e.g. Homebrew MySQL):  make booking-infra-up BOOKING_MYSQL_3307=1
# and set MYSQL_PORT=3307 in services/booking-api/.env

PY ?= .venv/bin/python
BOOKING_PYTHONPATH := services/booking-api:services/booking-worker
BOOKING_RUN := PYTHONPATH=$(BOOKING_PYTHONPATH) $(PY)
BOOKING_COMPOSE_FILES := -f infra/docker-compose.yml $(if $(BOOKING_MYSQL_3307),-f services/booking-api/infra/compose.mysql-3307.yml)
COMPOSE ?= docker compose $(BOOKING_COMPOSE_FILES)
BOOKING_DB ?= $(or $(MYSQL_DATABASE),airbnb)
BOOKING_MYSQL := docker compose -f infra/docker-compose.yml exec -T -e MYSQL_PWD=airbnb mysql mysql -uairbnb $(BOOKING_DB)
BOOKING_KAFKA_BIN := docker compose -f infra/docker-compose.yml exec -T kafka /opt/kafka/bin
BOOKING_API_PORT ?= 8002
WORKER_ID ?= worker-1
N ?= 20

.PHONY: booking-venv booking-infra-up booking-db booking-seed booking-reset booking-test-db test-booking

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

booking-db: ## apply db/mysql/02_bookings.sql
	$(BOOKING_MYSQL) < db/mysql/02_bookings.sql
	@$(BOOKING_MYSQL) -e "SHOW TABLES"

booking-seed: ## import seed reservations/history/blocks (idempotent, verified against manifest.json)
	MYSQL_DATABASE=$(BOOKING_DB) $(BOOKING_RUN) services/booking-api/scripts/import_seed.py

booking-reset: ## DESTRUCTIVE (local dev only): drop + recreate the 5 booking tables, then re-import the seed
	@echo "Recreating Role B tables in local database '$(BOOKING_DB)' (other teams' tables are untouched)..."
	$(BOOKING_MYSQL) -e "SET FOREIGN_KEY_CHECKS=0; DROP TABLE IF EXISTS availability_blocks, reservation_status_history, \
	  processed_events, idempotency_keys, reservations; SET FOREIGN_KEY_CHECKS=1;"
	$(MAKE) booking-db booking-seed

booking-test-db: ## create the isolated test database used by tests/booking
	docker compose -f infra/docker-compose.yml exec -T -e MYSQL_PWD=root mysql mysql -uroot -e \
	  "CREATE DATABASE IF NOT EXISTS airbnb_booking_test; GRANT ALL ON airbnb_booking_test.* TO 'airbnb'@'%';"

test-booking: booking-test-db ## pytest for Role B
	$(BOOKING_RUN) -m pytest tests/booking -q

# --- end Role B ---------------------------------------------------------------------------------
