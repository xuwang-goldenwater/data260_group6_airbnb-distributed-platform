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
| `ai_api/app.py` | REST API. |
| `scripts/demo.py` | Sends 1–3 concurrent requests and prints each outcome. |

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
ollama pull qwen2.5:3b          # Parser; benchmark requests are already in the parser cache

export PYTHONPATH=services/ai
uvicorn ai_api.app:app --port 8005                    # terminal 1
python -m ai_workers.chair_worker                     # terminal 2
python -m ai_workers.agent_worker parser              # terminal 3
python -m ai_workers.agent_worker matching            # terminal 4
python -m ai_workers.agent_worker risk                # terminal 5
python -m ai_workers.agent_worker fairness            # terminal 6

python services/ai/scripts/demo.py                    # one debate
```

### ★ Demos

* **Agent timeout (4.7)**: restart the fairness worker with `--delay-s 15` (deadline is 10 s,
  `AI_ROUND_TIMEOUT_S`), then `python services/ai/scripts/demo.py`. The result is published after
  ~10 s with `limitations: ["fairness timed out in round 0"]`, and `approval_required: true`.
  When fairness finally answers, the Chair logs it under `late` and does not change the decision.
* **Three concurrent debates (4.8)**: `python services/ai/scripts/demo.py --count 3`. Each debate's
  result matches the offline benchmark for the same request (also proven by
  `test_three_interleaved_debates_do_not_mix`).

## Tests (no Kafka needed)

```bash
python -m pytest services/ai/tests -q
```

Covers: Kafka Chair = offline benchmark on consensus / majority / safety-gate requests, single-agent
config, agent timeout with partial result, Matching missing → safe failure, three interleaved debates,
duplicate delivery, Round 0 arriving before `ai.parsed`, late message after the deadline, invalid agent
output rejected, restart recovery, unsent result republished, HTTP request → result.

## Status

Done: tasks 2.10 and 3.7 (without the WebSocket panel), the core of 4.7 and 4.8, part of 5.4 (harness rejection).

Still to do:
- [ ] JWT on the API (identity from the token, `401`/`403`)
- [ ] WebSocket `/ai/tasks/{task_id}/stream` for the live debate panel (3.7)
- [ ] Explanation step, approve / edit / reject endpoints → `ai.approvals` (4.9)
- [ ] `make agent-timeout` / `make three-debates` wrappers and evidence files (4.7, 4.8)
- [ ] Debate latency timings report (5.4)
