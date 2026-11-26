"""What each agent process does with one incoming event (no Kafka code here).

Each agent runs as its own process with its OWN consumer group, so every agent receives every
ai.parsed / debate.round.1 message (a shared group would split the messages between agents).
Agents talk only through Kafka; they never call each other or the Chair over HTTP.
"""
from __future__ import annotations

from typing import Any

from . import agents_bridge, envelope as env
from .config import T_PARSED, T_ROUND0, T_ROUND2

Outgoing = tuple[str, str, dict[str, Any]]


def parser_step(event: dict[str, Any]) -> list[Outgoing]:
    """ai.requests -> ai.parsed. The Parser is a plan step, not a debating agent."""
    p = event["payload"]
    parsed = agents_bridge.parse_request(p["request_text"])
    payload = {"task_id": p["task_id"], "plan_id": p["plan_id"], "evaluation_config": p["evaluation_config"],
               "request": parsed.get("request"), "error": parsed.get("error"),
               "parser_cache": parsed.get("cache"), "model": parsed.get("model")}
    return [_out(event, T_PARSED, "parser", payload, "parser:parsed")]


def round0_step(agent_id: str, event: dict[str, Any]) -> list[Outgoing]:
    """ai.parsed -> this agent's independent Round 0 opinion."""
    p = event["payload"]
    if p.get("request") is None:
        return []                       # parse failed: the Chair publishes the failed result
    try:
        body = agents_bridge.round0_payload(agent_id, p["request"])
        payload = {**body, "round": 0, "agent_id": agent_id, "status": "completed", "error": None}
    except Exception as exc:            # report the failure instead of going silent
        payload = {"round": 0, "agent_id": agent_id, "status": "error",
                   "error": {"code": "AGENT_ERROR", "message": str(exc)}}
    payload.update({"task_id": p.get("task_id"), "plan_id": p.get("plan_id")})
    return [_out(event, T_ROUND0, agent_id, payload, f"{agent_id}:round-0")]


def round2_step(agent_id: str, event: dict[str, Any]) -> list[Outgoing]:
    """debate.round.1 -> this agent's answer and vote, if the Chair asked it to vote."""
    p = event["payload"]
    if agent_id not in p.get("target_agent_ids", []):
        return []
    own = agents_bridge.round0_payload(agent_id, p["request"])     # deterministic: same as Round 0
    vote = agents_bridge.round2_vote(agent_id, own, p["original_property_id"], p["challenger_property_id"])
    answers = [{"question_id": q["question_id"], "answer": vote["reason"], "evidence_ids": vote["evidence_ids"]}
               for q in p.get("questions", []) if q.get("target_agent_id") == agent_id]
    payload = {"round": 2, "agent_id": agent_id, "debate_id": p.get("debate_id"), "plan_id": p.get("plan_id"),
               "responses": answers,
               "vote": {k: vote[k] for k in ("choice", "property_id", "reason", "evidence_ids")}}
    return [_out(event, T_ROUND2, agent_id, payload, f"{agent_id}:round-2")]


def _out(src: dict[str, Any], topic: str, actor: str, payload: dict[str, Any], suffix: str) -> Outgoing:
    trace_id = src["trace_id"]
    ev = env.build(event_type=topic, trace_id=trace_id, actor_id=actor, entity_type="debate",
                   entity_id=trace_id, payload=payload, idempotency_key=f"{trace_id}:{suffix}")
    return (topic, trace_id, ev)
