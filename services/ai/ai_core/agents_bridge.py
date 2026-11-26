"""Thin bridge to Role E's agent code in services/agents/ (parser, matching, risk, fairness, chair).

Role D never re-implements agent scoring or the decision rule: the Kafka Chair calls the same pure
functions as the offline benchmark (benchmarks/run_debate.py), so the demo and the benchmark agree.

The seed data is loaded once per process. Agents are deterministic, so recomputing a Round 0 ranking
from the same request always gives the same payload; this keeps the agent workers stateless.
"""
from __future__ import annotations

import sys
from functools import lru_cache
from typing import Any

from .config import REPO_ROOT

_AGENTS_DIR = REPO_ROOT / "services" / "agents"
if str(_AGENTS_DIR) not in sys.path:
    sys.path.insert(0, str(_AGENTS_DIR))

import chair as chair_rules  # noqa: E402  (Role E: services/agents/chair.py)
import fairness  # noqa: E402
import matching  # noqa: E402
import risk  # noqa: E402

ANALYSIS_AGENTS = ("matching", "risk", "fairness")


@lru_cache(maxsize=1)
def _seed() -> dict[str, Any]:
    seed_dir = str(REPO_ROOT / "data" / "seed" / "generated")
    props, blocked = matching.load_seed(seed_dir)
    reviews, cancels = risk.load_evidence(seed_dir)
    return {"props": props, "blocked": blocked, "reviews": reviews, "cancels": cancels}


def round0_payload(agent_id: str, request: dict[str, Any]) -> dict[str, Any]:
    """Run one analysis agent on a structured request and return its Round 0 payload."""
    s = _seed()
    if agent_id == "matching":
        return matching.rank(request, s["props"], s["blocked"])
    if agent_id == "risk":
        return risk.rank(request, s["props"], s["blocked"], s["reviews"], s["cancels"])
    if agent_id == "fairness":
        return fairness.rank(request, s["props"], s["blocked"])
    raise ValueError(f"unknown analysis agent {agent_id!r}")


def round2_vote(agent_id: str, own_round0: dict[str, Any], original: str, challenger: str) -> dict[str, Any]:
    """An agent's Round 2 vote, using Role E's vote rule (it compares its own Round 0 scores)."""
    return chair_rules.vote(agent_id, own_round0, original, challenger)


def parse_request(text: str) -> dict[str, Any]:
    """Run the Parser agent (Ollama qwen2.5:3b, exact cache first). Imported lazily: needs Ollama on a miss."""
    import parser as parser_agent  # noqa: E402  (services/agents/parser.py)
    return parser_agent.parse(text)


def property_facts(property_id: str) -> dict[str, Any] | None:
    """Seed listing facts for the Explanation step (read-only)."""
    return next((p for p in _seed()["props"] if p["property_id"] == property_id), None)
