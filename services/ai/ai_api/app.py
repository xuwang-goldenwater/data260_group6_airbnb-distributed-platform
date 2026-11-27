"""AI service REST + WebSocket API (Role D, docs/api/ai.md).

    PYTHONPATH=services/ai uvicorn ai_api.app:app --port 8005          # run from the repo root

POST /ai/requests                  start a recommendation; 202 + task_id (= trace_id)
GET  /ai/tasks/{id}                workflow status, per-agent status, plan steps
GET  /ai/results/{id}              the ai.results payload; 404 until the Chair publishes it
GET  /ai/tasks/{id}/events         every stored event for the trace
WS   /ai/tasks/{id}/stream         live debate events (replay first, then new ones)
POST /ai/results/{id}/approve|edit|reject   one human decision -> ai.approvals
GET  /ai/panel                     live debate panel (HTML)
GET  /ai/health                    liveness + MongoDB check

Every endpoint except /ai/panel and /ai/health needs `Authorization: Bearer <JWT>`. Identity comes
from the token (`sub`, `role`), never from the request body. A browser cannot set headers on a
WebSocket, so the stream also accepts `?token=<JWT>`.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, Header, Request, WebSocket, WebSocketDisconnect
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from ai_core import envelope as env
from ai_core.auth import AuthError, Principal, bearer, verify
from ai_core.config import EXPECTED_AGENTS, T_APPROVALS, T_REQUESTS, Settings, settings

from ai_core.store import MongoStore, Store

PANEL = Path(__file__).with_name("panel.html")
STREAM_TYPES = {"ai.parsed", "debate.round.0", "debate.round.1", "debate.round.2", "ai.results", "ai.approvals"}
DECISION_STATUS = {"approved": "approved", "edited": "edited", "rejected": "rejected"}


class AIRequest(BaseModel):
    # no guest_id: the guest is the token's `sub`; a guest_id sent in the body is ignored
    request_text: str = Field(min_length=3, max_length=2000)
    evaluation_config: Literal["single_agent", "debate_without_auditor", "full_debate"] = "full_debate"


class Decision(BaseModel):
    decision: Literal["approved", "edited", "rejected"]
    selected_property_id: str | None = None
    comment: str | None = Field(default=None, max_length=1000)
    reason: str | None = Field(default=None, max_length=1000)


class APIError(Exception):
    def __init__(self, status: int, code: str, message: str, trace_id: str | None = None,
                 retryable: bool = False, details: dict | None = None):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message
        self.trace_id, self.retryable, self.details = trace_id, retryable, details or {}


def error(status: int, code: str, message: str, trace_id: str | None = None, retryable: bool = False,
          details: dict | None = None):
    body = {"error": {"code": code, "message": message, "trace_id": trace_id, "retryable": retryable,
                      "details": details or {}}}
    return JSONResponse(status_code=status, content=body)


class KafkaPublisher:
    def __init__(self, s: Settings) -> None:
        from ai_workers.kafka_io import make_producer
        self._p = make_producer(s, "ai-api")

    def __call__(self, topic: str, key: str, event: dict[str, Any]) -> None:
        self._p.produce(topic, key=key.encode(), value=json.dumps(event).encode())
        if self._p.flush(5):
            raise RuntimeError("Kafka did not confirm the event")


def create_app(store: Store | None = None, publisher=None, settings_obj: Settings | None = None,
               poll_s: float = 0.5, stream_max_s: float = 600) -> FastAPI:
    app = FastAPI(title="AI Service (Role D)")
    cfg = settings_obj or settings()
    state: dict[str, Any] = {"store": store, "publish": publisher}

    def get_store() -> Store:
        if state["store"] is None:
            state["store"] = MongoStore(cfg.mongo_uri, cfg.mongo_db)
        return state["store"]

    def get_publisher():
        if state["publish"] is None:
            state["publish"] = KafkaPublisher(cfg)
        return state["publish"]

    @app.exception_handler(APIError)
    async def _api_error(_: Request, exc: APIError):
        return error(exc.status, exc.code, exc.message, exc.trace_id, exc.retryable, exc.details)

    @app.exception_handler(AuthError)
    async def _auth_error(_: Request, exc: AuthError):
        return error(exc.status, exc.code, exc.message, retryable=exc.status == 503)

    def who(authorization: str | None) -> Principal:
        return verify(bearer(authorization), cfg)

    def owned_task(task_id: str, p: Principal) -> dict:
        task = get_store().get_task(task_id)
        if task is None:
            raise APIError(404, "TASK_NOT_FOUND", f"no task {task_id}", task_id)
        if not p.can_see(task["guest_id"]):
            # same answer as "not found" would leak less, but 403 is what 00_conventions asks for
            raise APIError(403, "FORBIDDEN", "this task belongs to another user", task_id)
        return task

    # ------------------------------------------------------------------ requests and reads
    @app.post("/ai/requests", status_code=202)
    def start(req: AIRequest, authorization: str | None = Header(default=None)):
        p = who(authorization)
        if not p.is_human:
            raise APIError(403, "AGENT_ACTION_FORBIDDEN", f"role {p.role!r} cannot start a guest request")
        trace_id = env.new_id("tr")
        task = {"task_id": trace_id, "trace_id": trace_id, "plan_id": "plan-" + trace_id[3:],
                "guest_id": p.sub, "request_text": req.request_text,
                "evaluation_config": req.evaluation_config, "created_at": env.utc_now_iso()}
        event = env.build(event_type=T_REQUESTS, trace_id=trace_id, actor_id=p.sub,
                          entity_type="task", entity_id=trace_id, idempotency_key=f"{trace_id}:api:request",
                          payload={k: task[k] for k in ("task_id", "plan_id", "guest_id", "request_text",
                                                        "evaluation_config")})
        try:
            get_store().create_task(task)
            get_publisher()(T_REQUESTS, trace_id, event)
        except Exception as exc:   # Kafka or MongoDB down
            raise APIError(503, "DEPENDENCY_UNAVAILABLE", str(exc), trace_id, retryable=True) from exc
        return {"task_id": trace_id, "trace_id": trace_id, "plan_id": task["plan_id"], "status": "started"}

    @app.get("/ai/tasks/{task_id}")
    def task_status(task_id: str, authorization: str | None = Header(default=None)):
        task = owned_task(task_id, who(authorization))
        store = get_store()
        debate = store.get_debate(task_id)
        result = store.get_result(task_id)
        return {
            "task_id": task_id, "trace_id": task_id, "plan_id": task["plan_id"],
            "evaluation_config": task["evaluation_config"],
            "status": _status(debate, result),
            "current_round": None if debate is None else {"round0": 0, "round2": 2, "done": None}[debate["phase"]],
            "round_deadline_at": _deadline_iso(debate),
            "agent_status": _agent_status(task, debate),
            "plan": _plan(debate, result),
            "limitations": (result or {}).get("limitations", []),
            "late_messages": (debate or {}).get("late", []),
        }

    @app.get("/ai/results/{task_id}")
    def task_result(task_id: str, authorization: str | None = Header(default=None)):
        owned_task(task_id, who(authorization))
        result = get_store().get_result(task_id)
        if result is None:
            raise APIError(404, "RESULT_NOT_READY", "the debate has not published a result yet", task_id, True)
        return result

    @app.get("/ai/tasks/{task_id}/events")
    def task_events(task_id: str, authorization: str | None = Header(default=None)):
        owned_task(task_id, who(authorization))
        return {"trace_id": task_id, "events": get_store().events(task_id)}

    # ------------------------------------------------------------------ human decision (task 4.9)
    def decide(task_id: str, kind: str, body: Decision, authorization: str | None, idem: str | None):
        p = who(authorization)
        if not p.is_human:      # ai.md: an agent attempting an approval action receives 403
            raise APIError(403, "AGENT_ACTION_FORBIDDEN", "only an authenticated human can decide", task_id)
        task = owned_task(task_id, p)
        if body.decision != kind:
            raise APIError(400, "VALIDATION_ERROR", f"decision must be {kind!r} on this endpoint", task_id)
        store = get_store()
        result = store.get_result(task_id)
        if result is None:
            raise APIError(404, "RESULT_NOT_READY", "nothing to decide on yet", task_id, True)

        existing = store.get_approval(task_id)
        if existing is not None:
            same = idem is not None and existing.get("idempotency_key") == idem and existing["reviewer_id"] == p.sub
            if not same:
                raise APIError(409, "DECISION_ALREADY_RECORDED", "this result already has a human decision",
                               task_id, details={"decision": existing["decision"],
                                                 "decided_at": existing["decided_at"]})
            if not existing.get("published"):          # retry after Kafka failed last time
                _publish_approval(existing)
            return _decision_view(existing)
        if result["status"] != "awaiting_approval":
            raise APIError(409, "RESULT_NOT_PENDING", f"result status is {result['status']!r}", task_id)

        original = (result.get("recommendation") or {}).get("property_id")
        selected = body.selected_property_id
        note = body.reason if kind != "approved" else body.comment
        if kind == "approved":
            if selected not in (None, original):
                raise APIError(400, "VALIDATION_ERROR", "approve keeps the recommendation; use /edit to change it",
                               task_id)
            selected = original
        elif kind == "edited":
            if not selected or selected not in result.get("final_ranking", []):
                raise APIError(400, "VALIDATION_ERROR", "selected_property_id must be one of final_ranking",
                               task_id, details={"final_ranking": result.get("final_ranking", [])})
            if not body.reason:
                raise APIError(400, "VALIDATION_ERROR", "an edit needs a reason", task_id)
        else:
            if not body.reason:
                raise APIError(400, "VALIDATION_ERROR", "a rejection needs a reason", task_id)
            selected = None

        approval = {
            "trace_id": task_id, "task_id": task_id, "plan_id": task["plan_id"],
            "decision": kind, "original_property_id": original, "selected_property_id": selected,
            "changed": ({"field": "selected_property_id", "from": original, "to": selected}
                        if kind == "edited" and selected != original else None),
            "note": note, "reviewer_id": p.sub, "reviewer_role": p.role,     # from the JWT only
            "decided_at": env.utc_now_iso(), "idempotency_key": idem, "published": False,
            # an approval is a record, not an action: nothing is booked, messaged or blocked
            "external_actions": [],
        }
        if not store.insert_approval(approval):          # lost a race with another request
            raise APIError(409, "DECISION_ALREADY_RECORDED", "this result already has a human decision", task_id)
        store.set_result_decision(task_id, DECISION_STATUS[kind],
                                  {k: approval[k] for k in ("decision", "selected_property_id", "reviewer_id",
                                                            "decided_at", "changed", "note")})
        _publish_approval(approval)
        return _decision_view(approval)

    def _publish_approval(approval: dict) -> None:
        payload = {k: v for k, v in approval.items() if k not in ("published", "idempotency_key")}
        event = env.build(event_type=T_APPROVALS, trace_id=approval["trace_id"], actor_id=approval["reviewer_id"],
                          entity_type="task", entity_id=approval["task_id"],
                          idempotency_key=f"{approval['trace_id']}:approval", payload=payload)
        try:
            get_publisher()(T_APPROVALS, approval["trace_id"], event)
        except Exception as exc:
            raise APIError(503, "DEPENDENCY_UNAVAILABLE", f"decision saved, ai.approvals not confirmed: {exc}",
                           approval["trace_id"], retryable=True) from exc
        store = get_store()
        store.mark_approval_published(approval["trace_id"])
        store.append_event(approval["trace_id"], event)

    def _decision_view(a: dict) -> dict:
        return {k: a[k] for k in ("task_id", "decision", "original_property_id", "selected_property_id",
                                  "changed", "note", "reviewer_id", "reviewer_role", "decided_at")}

    @app.post("/ai/results/{task_id}/approve")
    def approve(task_id: str, body: Decision, authorization: str | None = Header(default=None),
                idempotency_key: str | None = Header(default=None)):
        return decide(task_id, "approved", body, authorization, idempotency_key)

    @app.post("/ai/results/{task_id}/edit")
    def edit(task_id: str, body: Decision, authorization: str | None = Header(default=None),
             idempotency_key: str | None = Header(default=None)):
        return decide(task_id, "edited", body, authorization, idempotency_key)

    @app.post("/ai/results/{task_id}/reject")
    def reject(task_id: str, body: Decision, authorization: str | None = Header(default=None),
               idempotency_key: str | None = Header(default=None)):
        return decide(task_id, "rejected", body, authorization, idempotency_key)

    # ------------------------------------------------------------------ live stream (task 3.7)
    @app.websocket("/ai/tasks/{task_id}/stream")
    async def stream(ws: WebSocket, task_id: str):
        token = bearer(ws.headers.get("authorization")) or ws.query_params.get("token")
        try:
            p = verify(token, cfg)
            await run_in_threadpool(owned_task, task_id, p)
        except (AuthError, APIError) as exc:
            await ws.close(code=4401 if exc.status == 401 else 4403 if exc.status == 403 else 4404,
                           reason=exc.code)
            return
        await ws.accept()
        sent, notified = 0, set()
        loop = asyncio.get_running_loop()
        started = loop.time()
        try:
            while loop.time() - started < stream_max_s:
                store = get_store()
                events = await run_in_threadpool(store.events, task_id)
                debate = await run_in_threadpool(store.get_debate, task_id)
                for ev in events[sent:]:
                    if ev["event_type"] in STREAM_TYPES and ev["trace_id"] == task_id:
                        await ws.send_json(ev)
                sent = len(events)
                for notice in _timeout_notices(task_id, debate, notified):
                    await ws.send_json(notice)
                types = {e["event_type"] for e in events}
                if "ai.approvals" in types or ("ai.results" in types and ws.query_params.get("until") != "approval"):
                    break
                await asyncio.sleep(poll_s)
            await ws.close()
        except WebSocketDisconnect:
            return        # the client recovers with GET /ai/tasks/{id} and /ai/results/{id}

    # ------------------------------------------------------------------ panel + health
    @app.get("/ai/panel", response_class=HTMLResponse)
    def panel():
        return PANEL.read_text(encoding="utf-8")

    @app.get("/ai/health")
    def health():
        try:
            get_store().get_task("__health__")
            return {"status": "ok", "mongo": "ok"}
        except Exception as exc:
            return error(503, "DEPENDENCY_UNAVAILABLE", f"MongoDB: {exc}", retryable=True)

    return app


def _timeout_notices(task_id: str, debate: dict | None, notified: set) -> list[dict]:
    """ai.agent_timeout is a dashboard notice derived from the persisted timeout state, not a Kafka topic."""
    out = []
    for rnd in ("round0", "round2"):
        for agent in (debate or {}).get("missing", {}).get(rnd, []):
            if (rnd, agent) in notified:
                continue
            notified.add((rnd, agent))
            out.append({"event_type": "ai.agent_timeout", "trace_id": task_id, "timestamp": env.utc_now_iso(),
                        "payload": {"agent_id": agent, "round": int(rnd[-1]),
                                    "message": f"{agent} missed the round {rnd[-1]} deadline"}})
    return out


def _deadline_iso(debate) -> str | None:
    if not debate or debate["phase"] == "done" or not debate.get("deadline"):
        return None
    from datetime import datetime, timezone
    return datetime.fromtimestamp(debate["deadline"], tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


def _plan(debate, result) -> list[dict[str, Any]]:
    if debate and debate.get("plan"):
        plan = [dict(s) for s in debate["plan"]]
        decision = (result or {}).get("human_decision")
        if decision:
            plan[-1].update(status="completed", note=decision["decision"])
        return plan
    return [{"step_id": "parse", "owner": "parser", "status": "in_progress"}] + [
        {"step_id": s, "owner": o, "status": "pending"}
        for s, o in (("retrieve_candidates", "agents"), ("round_0", "chair"), ("round_1_2", "chair"),
                     ("adjudicate", "chair"), ("explain", "explanation"), ("human_approval", "guest"))]


app = create_app()
