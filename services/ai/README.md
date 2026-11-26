# AI service — Role D (Agent Orchestration)

FastAPI service + Kafka Chair that runs the multi-agent debate described in `docs/api/ai.md`.
The decision rules (disagreement, Round 1 challenge, `safety_then_majority`, `final_ranking`) are
Role E's pure functions in `services/agents/chair.py`; this service imports them, so the live demo
and the offline benchmark make the same decision.

## Layout

| Path | What it is |
| --- | --- |
| `ai_core/chair_core.py` | Chair state machine: one debate per `trace_id`, round deadlines, harness rejection, duplicates, late messages, outbox. No Kafka code. |
| `ai_core/agent_logic.py` | What each agent does with one event (Parser → `ai.parsed`, Round 0, Round 2 vote). |
| `ai_core/agents_bridge.py` | Imports Role E's agents and `chair.py`. |
| `ai_core/harness.py` | Rejects invalid agent output (bad scores, unknown property, vote without reason). |
| `ai_core/store.py` | MongoDB persistence (`ai_tasks`, `ai_debates`, `ai_events`, `ai_results`) + in-memory store for tests. |
| `ai_core/envelope.py` | Shared envelope build/validate (`shared/envelope.schema.json`). |
| `ai_workers/chair_worker.py` | Kafka loop for the Chair. |
| `ai_workers/agent_worker.py` | Kafka loop for one agent: `parser`, `matching`, `risk`, `fairness`. |
| `ai_core/explanation.py` | Explanation step: Ollama with a 5 s deadline, template fallback. |
| `ai_core/auth.py` | JWT check (`sub` + `role` from the token only). |
| `ai_api/app.py` | REST + WebSocket API, approve / edit / reject. |
| `ai_api/panel.html` | Live debate panel served at `/ai/panel`. |
| `scripts/run_demo.py` | ★ Demo scenarios + evidence files in `reports/perf/ai/`. |
| `scripts/dev_token.py` | Prints a local JWT for demos. |
| `Makefile` | `make three-debates`, `make agent-timeout`, `make invalid-output`, `make latency`. |

## Kafka flow

```
POST /ai/requests → ai.requests → Parser → ai.parsed ─┬→ Chair (opens the debate, starts the Round 0 clock)
                                                      └→ Matching / Risk / Fairness → debate.round.0 → Chair
Chair: top-1s differ? → debate.round.1 → agents vote → debate.round.2 → Chair → ai.results (+ MongoDB)
```

Every event uses the shared envelope with Kafka key = `trace_id`. Each agent has its own consumer
group (`ai-agent-<name>`) so every agent sees every message; the Chair's group is `ai-chair`.

## Run it locally (from the repo root)

```bash
docker compose -f infra/docker-compose.yml up -d kafka mongodb
pip install -r services/ai/requirements.txt
ollama pull qwen2.5:3b          # Parser + Explanation; benchmark requests are already in the parser cache
```

One command per scenario starts the API, the Chair and the four agents, runs the requests, checks
the outcome and writes evidence to `reports/perf/ai/<scenario>.json` (process logs in `reports/perf/ai/logs/`):

```bash
make -C services/ai three-debates     # ★ 4.8  three concurrent debates = offline benchmark
make -C services/ai agent-timeout     # ★ 4.7  fairness answers after the 10 s deadline
make -C services/ai invalid-output    # 5.4    fairness sends score 1.7 -> rejected by the harness
make -C services/ai latency           # 5.4    10 sequential debates, p50 / p95
```

Manual run (six terminals) for the live panel:

```bash
export PYTHONPATH=services/ai JWT_SECRET=dev-secret-change-me-0123456789abcdef
uvicorn ai_api.app:app --port 8005
python -m ai_workers.chair_worker
python -m ai_workers.agent_worker parser        # and matching, risk, fairness (add --delay-s 15 or --corrupt)
python services/ai/scripts/dev_token.py guest-demo guest      # paste into the panel
open http://localhost:8005/ai/panel
```

`AI_EXPLAIN_MODE=template` skips Ollama for the explanation. Without Ollama the `llm` mode still
works: it falls back to the template and adds `explanation_fallback` to `limitations`.

### What the demos show

* **Agent timeout (4.7)**: the result is published about 10 s after Round 0 opens with
  `limitations: ["fairness timed out in round 0"]` and `approval_required: true`. The panel shows an
  `ai.agent_timeout` notice. When fairness answers later, the Chair logs it under `late_messages`
  (`GET /ai/tasks/{id}`) and does not change the decision.
* **Three concurrent debates (4.8)**: the three traces interleave on the same topics; each result is
  checked against the offline benchmark for its own request.
* **Human decision (4.9)**: approve / edit / reject from the panel. A second decision gets `409`; a
  token with role `agent` gets `403`. The decision is published to `ai.approvals` and books nothing.

## Tests (no Kafka needed)

```bash
python -m pytest services/ai/tests -q
```

29 tests. Covers: Kafka Chair = offline benchmark on consensus / majority / safety-gate requests,
single-agent config, agent timeout with partial result, Matching missing → safe failure, three
interleaved debates, duplicate delivery, Round 0 arriving before `ai.parsed`, late message, invalid
and `--corrupt` output rejected, restart recovery, unsent result republished, explanation +
fallback, HTTP request → result, JWT 401 / 403 / 503, approve / edit / reject / 409 / idempotent
retry, WebSocket replay for one trace only, timeout notice, panel and health.

## Status

Done: 2.10, 3.7 (REST + WebSocket + panel), 4.7, 4.8, 4.9 (Explanation step, approve / edit / reject
→ `ai.approvals`), 5.4 (harness rejection, latency evidence), JWT on every endpoint.

Still to check on a real broker: run the four `make` targets once and commit `reports/perf/ai/*.json`.
