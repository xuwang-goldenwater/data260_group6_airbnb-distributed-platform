"""API + Chair wired together in one process, with an in-memory "Kafka" (a list) and MemoryStore.

The Parser step is replaced by the benchmark's expected parse so the test needs no Ollama.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient                                   # noqa: E402

from ai_core import agent_logic, envelope as env                            # noqa: E402
from ai_core.chair_core import ChairCore                                    # noqa: E402
from ai_core.store import MemoryStore                                       # noqa: E402
from ai_api.app import create_app                                           # noqa: E402
from test_chair_core import REQUESTS, SCHEMA, Clock, drive                  # noqa: E402


def test_request_to_result_over_http():
    store, bus = MemoryStore(), []
    client = TestClient(create_app(store=store, publisher=lambda t, k, e: bus.append((t, k, e))))
    text = REQUESTS["R07"]["request_text"]
    r = client.post("/ai/requests", json={"request_text": text, "evaluation_config": "full_debate"})
    assert r.status_code == 202
    task_id = r.json()["task_id"]
    [(topic, key, request_event)] = bus
    assert topic == "ai.requests" and key == task_id
    env.validate(request_event, SCHEMA, key=key)

    assert client.get(f"/ai/results/{task_id}").status_code == 404           # not decided yet
    assert client.get(f"/ai/tasks/{task_id}").json()["status"] == "parsing"

    # Parser step (expected parse instead of calling Ollama), then the agents and the Chair
    parsed = env.build(event_type="ai.parsed", trace_id=task_id, actor_id="parser", entity_type="debate",
                       entity_id=task_id, idempotency_key=f"{task_id}:parser:parsed",
                       payload={**{k: request_event["payload"][k] for k in ("task_id", "plan_id", "evaluation_config")},
                                "request": REQUESTS["R07"]["expected_parse"], "error": None})
    chair = ChairCore(store, round_timeout_s=10, clock=Clock())
    round0 = [agent_logic.round0_step(a, parsed)[0][2] for a in ("matching", "risk", "fairness")]
    [result] = drive(chair, [parsed] + round0)

    status = client.get(f"/ai/tasks/{task_id}").json()
    assert status["status"] == "awaiting_approval"
    assert {a["agent_id"]: a["status"] for a in status["agent_status"]} == {
        "parser": "completed", "matching": "completed", "risk": "completed", "fairness": "completed"}
    body = client.get(f"/ai/results/{task_id}").json()
    assert body["recommendation"] == result["recommendation"] and body["approval_required"] is True
    kinds = [e["event_type"] for e in client.get(f"/ai/tasks/{task_id}/events").json()["events"]]
    assert kinds[0] == "ai.parsed" and kinds.count("debate.round.0") == 3 and kinds[-1] == "ai.results"


def test_kafka_down_returns_503():
    def broken(*_):
        raise RuntimeError("broker unreachable")
    client = TestClient(create_app(store=MemoryStore(), publisher=broken))
    r = client.post("/ai/requests", json={"request_text": "two nights in San Jose"})
    assert r.status_code == 503 and r.json()["error"]["code"] == "DEPENDENCY_UNAVAILABLE"


def test_unsent_result_is_republished_after_restart():
    """Crash after the result was saved but before Kafka confirmed it: the restarted Chair resends it."""
    store, clock = MemoryStore(), Clock()
    first = ChairCore(store, round_timeout_s=10, clock=clock)
    parsed = env.build(event_type="ai.parsed", trace_id="tr-x", actor_id="parser", entity_type="debate",
                       entity_id="tr-x", idempotency_key="tr-x:parser:parsed",
                       payload={"task_id": "tr-x", "plan_id": "plan-x", "evaluation_config": "single_agent",
                                "request": REQUESTS["R45"]["expected_parse"], "error": None})
    drive(first, [parsed, agent_logic.round0_step("matching", parsed)[0][2]])   # never mark_sent: "crash"
    second = ChairCore(store, round_timeout_s=10, clock=clock)
    second.recover()
    [(topic, key, ev)] = second.unsent()
    assert topic == "ai.results" and key == "tr-x"
    second.mark_sent([key])
    assert second.unsent() == [] and store.get_debate("tr-x")["outbox"] == []
