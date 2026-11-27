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
from ai_core.auth import make_token                                         # noqa: E402
from ai_core.config import Settings                                         # noqa: E402
from test_chair_core import REQUESTS, SCHEMA, Clock, drive                  # noqa: E402

CFG = Settings(jwt_secret="test-secret-0123456789abcdef0123456789")
GUEST = {"Authorization": "Bearer " + make_token("guest-1", "guest", CFG)}
OTHER = {"Authorization": "Bearer " + make_token("guest-2", "guest", CFG)}
AGENT = {"Authorization": "Bearer " + make_token("agent-risk", "agent", CFG)}


def app_with(store, bus):
    return TestClient(create_app(store=store, publisher=lambda t, k, e: bus.append((t, k, e)),
                                 settings_obj=CFG, poll_s=0.01, stream_max_s=2))


def decided_task(rid="R07"):
    """A task that went all the way to ai.results (full debate, no Kafka)."""
    store, bus = MemoryStore(), []
    client = app_with(store, bus)
    r = client.post("/ai/requests", headers=GUEST,
                    json={"request_text": REQUESTS[rid]["request_text"], "evaluation_config": "full_debate"})
    task_id = r.json()["task_id"]
    request_event = bus[0][2]
    parsed = env.build(event_type="ai.parsed", trace_id=task_id, actor_id="parser", entity_type="debate",
                       entity_id=task_id, idempotency_key=f"{task_id}:parser:parsed",
                       payload={**{k: request_event["payload"][k] for k in ("task_id", "plan_id", "evaluation_config")},
                                "request": REQUESTS[rid]["expected_parse"], "error": None})
    chair = ChairCore(store, round_timeout_s=10, clock=Clock())
    round0 = [agent_logic.round0_step(a, parsed)[0][2] for a in ("matching", "risk", "fairness")]
    [result] = drive(chair, [parsed] + round0)
    return client, store, bus, task_id, result


def test_request_to_result_over_http():
    store, bus = MemoryStore(), []
    client = app_with(store, bus)
    text = REQUESTS["R07"]["request_text"]
    r = client.post("/ai/requests", headers=GUEST,
                    json={"request_text": text, "evaluation_config": "full_debate", "guest_id": "someone-else"})
    assert r.status_code == 202
    task_id = r.json()["task_id"]
    [(topic, key, request_event)] = bus
    assert topic == "ai.requests" and key == task_id
    env.validate(request_event, SCHEMA, key=key)
    assert request_event["payload"]["guest_id"] == "guest-1"           # from the JWT, body value ignored

    assert client.get(f"/ai/results/{task_id}", headers=GUEST).status_code == 404     # not decided yet
    assert client.get(f"/ai/tasks/{task_id}", headers=GUEST).json()["status"] == "parsing"

    # Parser step (expected parse instead of calling Ollama), then the agents and the Chair
    parsed = env.build(event_type="ai.parsed", trace_id=task_id, actor_id="parser", entity_type="debate",
                       entity_id=task_id, idempotency_key=f"{task_id}:parser:parsed",
                       payload={**{k: request_event["payload"][k] for k in ("task_id", "plan_id", "evaluation_config")},
                                "request": REQUESTS["R07"]["expected_parse"], "error": None})
    chair = ChairCore(store, round_timeout_s=10, clock=Clock())
    round0 = [agent_logic.round0_step(a, parsed)[0][2] for a in ("matching", "risk", "fairness")]
    [result] = drive(chair, [parsed] + round0)

    status = client.get(f"/ai/tasks/{task_id}", headers=GUEST).json()
    assert status["status"] == "awaiting_approval"
    assert {a["agent_id"]: a["status"] for a in status["agent_status"]} == {
        "parser": "completed", "matching": "completed", "risk": "completed", "fairness": "completed"}
    assert {s["step_id"]: s["status"] for s in status["plan"]} == {
        "parse": "completed", "retrieve_candidates": "completed", "round_0": "completed",
        "round_1_2": "completed", "adjudicate": "completed", "explain": "completed", "human_approval": "in_progress"}
    body = client.get(f"/ai/results/{task_id}", headers=GUEST).json()
    assert body["recommendation"] == result["recommendation"] and body["approval_required"] is True
    assert body["explanation"]["source"] == "template" and body["recommendation"]["property_id"] in body["explanation"]["text"]
    kinds = [e["event_type"] for e in client.get(f"/ai/tasks/{task_id}/events", headers=GUEST).json()["events"]]
    assert kinds[0] == "ai.parsed" and kinds.count("debate.round.0") == 3 and kinds[-1] == "ai.results"


def test_kafka_down_returns_503():
    def broken(*_):
        raise RuntimeError("broker unreachable")
    client = TestClient(create_app(store=MemoryStore(), publisher=broken, settings_obj=CFG))
    r = client.post("/ai/requests", headers=GUEST, json={"request_text": "two nights in San Jose"})
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


# ---------------------------------------------------------------- authentication (00_conventions.md)
def test_missing_or_bad_token_is_401_and_other_users_get_403():
    client, _, _, task_id, _ = decided_task()
    assert client.post("/ai/requests", json={"request_text": "two nights"}).status_code == 401
    bad = {"Authorization": "Bearer " + make_token("guest-1", "guest", Settings(jwt_secret="wrong-secret-0123456789abcdef012345"))}
    r = client.get(f"/ai/results/{task_id}", headers=bad)
    assert r.status_code == 401 and r.json()["error"]["code"] == "UNAUTHENTICATED"
    assert client.get(f"/ai/results/{task_id}", headers=OTHER).status_code == 403
    admin = {"Authorization": "Bearer " + make_token("ops", "admin", CFG)}
    assert client.get(f"/ai/results/{task_id}", headers=admin).status_code == 200


def test_no_secret_configured_is_503():
    client = TestClient(create_app(store=MemoryStore(), publisher=lambda *a: None, settings_obj=Settings(jwt_secret="")))
    r = client.post("/ai/requests", headers=GUEST, json={"request_text": "two nights"})
    assert r.status_code == 503 and r.json()["error"]["code"] == "AUTH_NOT_CONFIGURED"


# ---------------------------------------------------------------- human decision (task 4.9)
def test_approve_once_then_409_and_ai_approvals_published():
    client, store, bus, task_id, result = decided_task()
    r = client.post(f"/ai/results/{task_id}/approve", headers=GUEST,
                    json={"decision": "approved", "comment": "fine", "reviewer_id": "spoofed"})
    assert r.status_code == 200
    assert r.json()["reviewer_id"] == "guest-1"                       # from the JWT, not the body
    assert r.json()["selected_property_id"] == result["recommendation"]["property_id"]
    topic, key, ev = bus[-1]
    assert topic == "ai.approvals" and key == task_id
    env.validate(ev, SCHEMA, key=key)
    assert ev["payload"]["external_actions"] == []                    # approval books nothing
    assert client.get(f"/ai/results/{task_id}", headers=GUEST).json()["status"] == "approved"
    second = client.post(f"/ai/results/{task_id}/reject", headers=GUEST, json={"decision": "rejected", "reason": "x"})
    assert second.status_code == 409 and second.json()["error"]["code"] == "DECISION_ALREADY_RECORDED"
    plan = client.get(f"/ai/tasks/{task_id}", headers=GUEST).json()["plan"]
    assert plan[-1]["status"] == "completed" and plan[-1]["note"] == "approved"


def test_agent_cannot_approve():
    client, _, bus, task_id, _ = decided_task()
    r = client.post(f"/ai/results/{task_id}/approve", headers=AGENT, json={"decision": "approved"})
    assert r.status_code == 403 and r.json()["error"]["code"] == "AGENT_ACTION_FORBIDDEN"
    assert all(t != "ai.approvals" for t, _, _ in bus)


def test_edit_must_pick_from_ranking_and_give_a_reason():
    client, _, _, task_id, result = decided_task()
    alt = result["final_ranking"][2]
    assert client.post(f"/ai/results/{task_id}/edit", headers=GUEST,
                       json={"decision": "edited", "selected_property_id": "property-9999", "reason": "r"}).status_code == 400
    assert client.post(f"/ai/results/{task_id}/edit", headers=GUEST,
                       json={"decision": "edited", "selected_property_id": alt}).status_code == 400
    r = client.post(f"/ai/results/{task_id}/edit", headers=GUEST,
                    json={"decision": "edited", "selected_property_id": alt, "reason": "cheaper"})
    assert r.status_code == 200
    assert r.json()["changed"] == {"field": "selected_property_id",
                                   "from": result["recommendation"]["property_id"], "to": alt}


def test_retry_with_same_idempotency_key_replays_instead_of_409():
    client, _, bus, task_id, _ = decided_task()
    h = {**GUEST, "Idempotency-Key": "k-1"}
    first = client.post(f"/ai/results/{task_id}/reject", headers=h, json={"decision": "rejected", "reason": "too far"})
    again = client.post(f"/ai/results/{task_id}/reject", headers=h, json={"decision": "rejected", "reason": "too far"})
    assert first.status_code == again.status_code == 200 and first.json() == again.json()
    assert sum(t == "ai.approvals" for t, _, _ in bus) == 1


def test_cannot_decide_on_a_failed_result():
    client, store, _, task_id, _ = decided_task()
    store.results[task_id]["status"] = "failed"
    r = client.post(f"/ai/results/{task_id}/approve", headers=GUEST, json={"decision": "approved"})
    assert r.status_code == 409 and r.json()["error"]["code"] == "RESULT_NOT_PENDING"


# ---------------------------------------------------------------- WebSocket stream (task 3.7)
def test_stream_replays_the_debate_for_its_trace_only():
    client, store, _, task_id, _ = decided_task()
    store.append_event("tr-other", {"event_type": "ai.results", "trace_id": "tr-other", "payload": {}})
    token = GUEST["Authorization"][7:]
    with client.websocket_connect(f"/ai/tasks/{task_id}/stream?token={token}") as ws:
        got = []
        while True:
            ev = ws.receive_json()
            got.append(ev)
            if ev["event_type"] == "ai.results":
                break
    assert {e["trace_id"] for e in got} == {task_id}
    kinds = [e["event_type"] for e in got]
    assert kinds[0] == "ai.parsed" and kinds.count("debate.round.0") == 3 and kinds[-1] == "ai.results"


def test_stream_sends_agent_timeout_notice():
    client, store, _, task_id, _ = decided_task()
    store.debates[task_id]["missing"]["round0"] = ["fairness"]
    token = GUEST["Authorization"][7:]
    with client.websocket_connect(f"/ai/tasks/{task_id}/stream?token={token}") as ws:
        kinds = []
        for _ in range(20):
            ev = ws.receive_json()
            kinds.append(ev)
            if ev["event_type"] == "ai.agent_timeout":
                break
    assert kinds[-1]["payload"] == {"agent_id": "fairness", "round": 0, "message": "fairness missed the round 0 deadline"}


def test_stream_refuses_other_users():
    import pytest
    from starlette.websockets import WebSocketDisconnect
    client, _, _, task_id, _ = decided_task()
    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect(f"/ai/tasks/{task_id}/stream?token={OTHER['Authorization'][7:]}") as ws:
            ws.receive_json()
    assert exc.value.code == 4403


def test_panel_and_health():
    client = app_with(MemoryStore(), [])
    assert "AI Debate Panel" in client.get("/ai/panel").text
    assert client.get("/ai/health").json() == {"status": "ok", "mongo": "ok"}


def test_edit_may_pick_the_entire_place_alternative():
    """In the 7 reviewed cases the alternative is also in final_ranking; this checks the case where
    it is not (the alternative scan covers every candidate, the ranking only the top 10)."""
    client, store, _, task_id, result = decided_task("R01")
    alt = "property-2999"
    assert alt not in result["final_ranking"]
    store.results[task_id]["explanation"]["entire_place_alternative"]["property_id"] = alt
    r = client.post(f"/ai/results/{task_id}/edit", headers=GUEST,
                    json={"decision": "edited", "selected_property_id": alt, "reason": "entire place, similar price"})
    assert r.status_code == 200 and r.json()["selected_property_id"] == alt
