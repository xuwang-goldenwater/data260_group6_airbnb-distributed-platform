# Role D demos. Run from services/ai (or `make -C services/ai <target>` from the repo root).
# Needs Kafka + MongoDB: docker compose -f ../../infra/docker-compose.yml up -d kafka mongodb
PY ?= python3

.PHONY: test three-debates agent-timeout invalid-output latency demos token

test:
	PYTHONPATH=. $(PY) -m pytest tests -q

three-debates:          ## ★ 4.8: three concurrent debates, each equal to the offline benchmark
	$(PY) scripts/run_demo.py three-debates --launch

agent-timeout:          ## ★ 4.7: fairness answers after the 10 s deadline -> partial result + limitation
	$(PY) scripts/run_demo.py agent-timeout --launch

invalid-output:         ## 5.4: fairness sends a score of 1.7 -> harness rejects it
	$(PY) scripts/run_demo.py invalid-output --launch

latency:                ## 5.4: 10 sequential debates, p50 / p95 end-to-end
	$(PY) scripts/run_demo.py latency --launch

demos: three-debates agent-timeout invalid-output latency

token:                  ## print a guest JWT for the panel (JWT_SECRET must match the API)
	@$(PY) scripts/dev_token.py guest-demo guest
