"""AI service REST API (Role D, docs/api/ai.md).

    PYTHONPATH=services/ai uvicorn ai_api.app:app --port 8005          # run from the repo root

POST /ai/requests        start a recommendation; returns 202 + task_id (= trace_id)
GET  /ai/tasks/{id}      workflow status, per-agent status and the plan steps
GET  /ai/results/{id}    the ai.results payload; 404 until the Chair publishes it
GET  /ai/tasks/{id}/events   every stored event for the trace (replay for the debate panel)

Not yet: JWT check (identity comes from the JWT per 00_conventions.md), approve/edit/reject (task 4.9)
and the WebSocket stream (task 3.7). See services/ai/README.md.
"""
from __future__ import annotations

import json
from typing import Any, Literal

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from ai_core import envelope as env
from ai_core.config import EXPECTED_AGENTS, T_REQUESTS, settings
from ai_core.store import MongoStore, Store


class AIRequest(BaseModel):
    guest_id: str = Field(default="guest-demo", min_length=1, max_length=64)   # TODO: take from JWT `sub`
    request_text: str = Field(min_length=3, max_length=2000)
    evaluation_config: Literal["single_agent", "debate_without_auditor", "full_debate"] = "full_debate"


def error(status: int, code: str, message: str, trace_id: str | None = None, retryable: bool = False):
    body = {"error": {"code": code, "message": message, "trace_id": trace_id, "retryable": retryable, "details": {}}}
    return JSONResponse(status_code=status, content=body)


class KafkaPublisher:
    def __init__(self) -> None:
        from ai_workers.kafka_io import make_producer
        self._p = make_producer(settings(), "ai-api")

    def __call__(self, topic: str, key: str, event: dict[str, Any]) -> None:
        self._p.produce(topic, key=key.encode(), value=json.dumps(event).encode())
        if self._p.flush(5):
            raise RuntimeError("Kafka did not confirm the event")


def create_app(store: Store | None = None, publisher=None) -> FastAPI:
    app = FastAPI(title="AI Service (Role D)")
    state: dict[str, Any] = {"store": store, "publish": publisher}

    def get_store() -> Store:
        if state["store"] is None:
            s = settings()
            state["store"] = MongoStore(s.mongo_uri, s.mongo_db)
        return state["store"]

    def get_publisher():
        if state["publish"] is None:
            state["publish"] = KafkaPublisher()
        return state["publish"]

    @app.post("/ai/requests", status_code=202)
    def start(req: AIRequest):
        trace_id = env.new_id("tr")
        task = {"task_id": trace_id, "trace_id": trace_id, "plan_id": "plan-" + trace_id[3:],
                "guest_id": req.guest_id, "request_text": req.request_text,
                "evaluation_config": req.evaluation_config, "created_at": env.utc_now_iso()}
        event = env.build(event_type=T_REQUESTS, trace_id=trace_id, actor_id=req.guest_id,
                          entity_type="task", entity_id=trace_id, idempotency_key=f"{trace_id}:api:request",
                          payload={k: task[k] for k in ("task_id", "plan_id", "guest_id", "request_text",
                                                        "evaluation_config")})
        try:
            get_store().create_task(task)
            get_publisher()(T_REQUESTS, trace_id, event)
        except Exception as exc:   # Kafka or MongoDB down
            return error(503, "DEPENDENCY_UNAVAILABLE", str(exc), trace_id, retryable=True)
        return {"task_id": trace_id, "trace_id": trace_id, "plan_id": task["plan_id"], "status": "started"}

    @app.get("/ai/tasks/{task_id}")
    def task_status(task_id: str):
        store = get_store()
        task = store.get_task(task_id)
        if task is None:
            return error(404, "TASK_NOT_FOUND", f"no task {task_id}", task_id)
        debate = store.get_debate(task_id)
        result = store.get_result(task_id)
        return {
            "task_id": task_id, "trace_id": task_id, "plan_id": task["plan_id"],
            "evaluation_config": task["evaluation_config"],
            "status": _status(debate, result),
            "current_round": None if debate is None else {"round0": 0, "round2": 2, "done": None}[debate["phase"]],
            "agent_status": _agent_status(task, debate),
            "plan": _plan(debate, result),
            "limitations": (result or {}).get("limitations", []),
        }

    @app.get("/ai/results/{task_id}")
    def task_result(task_id: str):
        result = get_store().get_result(task_id)
        if result is None:
            return error(404, "RESULT_NOT_READY", "the debate has not published a result yet", task_id, True)
        return result

    @app.get("/ai/tasks/{task_id}/events")
    def task_events(task_id: str):
        return {"trace_id": task_id, "events": get_store().events(task_id)}

    return app


def _status(debate, result) -> str:
    if result is not None:
        return result["status"]
    if debate is None:
        return "parsing"
    return {"round0": "round_0", "round2": "round_2", "done": "deciding"}[debate["phase"]]


def _agent_status(task, debate) -> list[dict[str, str]]:
    rows = [{"agent_id": "parser", "status": "completed" if debate else "running"}]
    for a in EXPECTED_AGENTS[task["evaluation_config"]]:
        status = "waiting"
        if debate:
            rejected = {r["agent_id"] for r in debate["rejected"]}
            missing = set(debate["missing"]["round0"]) | set(debate["missing"]["round2"])
            status = ("rejected" if a in rejected else "timed_out" if a in missing
                      else "completed" if a in debate["round0"] else "waiting")
        rows.append({"agent_id": a, "status": status})
    return rows


def _plan(debate, result) -> list[dict[str, str]]:
    phase = debate["phase"] if debate else None
    done = result is not None
    return [
        {"step_id": "parse", "owner": "parser", "status": "completed" if debate else "in_progress"},
        {"step_id": "round_0", "owner": "chair", "status": "in_progress" if phase == "round0" else
         ("completed" if phase in ("round2", "done") else "pending")},
        {"step_id": "round_1_2", "owner": "chair", "status": "in_progress" if phase == "round2" else
         ("completed" if done else "pending")},
        {"step_id": "adjudicate", "owner": "chair", "status": "completed" if done else "pending"},
        {"step_id": "explain", "owner": "explanation", "status": "not_implemented"},       # task 4.9
        {"step_id": "human_approval", "owner": "reviewer", "status": "pending"},
    ]


app = create_app()
