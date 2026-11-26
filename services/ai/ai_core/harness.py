"""Harness checks on agent output (ai.md "Every consumer validates ...", spec 7.5.5).

An invalid Round 0 or Round 2 message is rejected: the Chair records it, treats that agent as missing
for the round, and never lets the bad data reach the decision rule.
"""
from __future__ import annotations

from typing import Any

VOTE_CHOICES = {"keep_original", "switch_to_challenger", "abstain"}


class InvalidAgentOutput(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


def check_round0(agent_id: str, payload: dict[str, Any]) -> None:
    ranked = payload.get("ranked_properties")
    candidates = payload.get("candidate_property_ids")
    if not isinstance(ranked, list) or not isinstance(candidates, list):
        raise InvalidAgentOutput("INVALID_AGENT_OUTPUT", f"{agent_id}: ranked_properties/candidate_property_ids missing")
    if not payload.get("scoring_version"):
        raise InvalidAgentOutput("INVALID_AGENT_OUTPUT", f"{agent_id}: scoring_version missing")
    allowed = set(candidates)
    for i, r in enumerate(ranked):
        pid, score = r.get("property_id"), r.get("score")
        if pid not in allowed:
            raise InvalidAgentOutput("INVALID_AGENT_OUTPUT", f"{agent_id}: ranked_properties[{i}] {pid!r} not a candidate")
        if not isinstance(score, (int, float)) or not 0 <= score <= 1:
            raise InvalidAgentOutput("INVALID_AGENT_OUTPUT", f"{agent_id}: score {score!r} outside 0..1")
    if len(ranked) < min(10, len(candidates)):
        raise InvalidAgentOutput("INVALID_AGENT_OUTPUT", f"{agent_id}: needs at least 10 ranked properties")


def check_vote(agent_id: str, vote: dict[str, Any], original: str, challenger: str) -> None:
    choice = vote.get("choice")
    if choice not in VOTE_CHOICES:
        raise InvalidAgentOutput("INVALID_AGENT_OUTPUT", f"{agent_id}: vote choice {choice!r}")
    if not vote.get("reason"):
        raise InvalidAgentOutput("INVALID_AGENT_OUTPUT", f"{agent_id}: every vote needs a reason")
    if choice != "abstain" and vote.get("property_id") not in (original, challenger):
        raise InvalidAgentOutput("INVALID_AGENT_OUTPUT",
                                 f"{agent_id}: vote.property_id must be the original or the challenger")
