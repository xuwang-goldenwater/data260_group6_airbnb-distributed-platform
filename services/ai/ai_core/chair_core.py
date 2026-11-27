"""The Kafka Chair's state machine (Role D, tasks 2.10 / 3.7 / 4.7 / 4.8).

This module has NO Kafka code. It receives already-decoded events, updates one debate per trace_id,
and returns the events to publish. ai_workers/chair_worker.py does the Kafka I/O around it.
Keeping it pure makes the timeout and concurrency behaviour testable without a broker.

Who decides what:
  * Role E's services/agents/chair.py owns the DECISION RULES (disagreement, Round 1 challenge,
    safety_then_majority, final_ranking). We call those functions; we never re-implement them.
  * This module owns the ORCHESTRATION: per-debate state, which agents we are waiting for, round
    deadlines, harness rejection, duplicates, late messages, persistence and what gets published.

Debate life cycle (phase):
  round0  -> waiting for each expected agent's independent Round 0 opinion (debate.round.0)
  round2  -> challenge sent on debate.round.1; waiting for answers + votes (debate.round.2)
  done    -> ai.results published; human approval comes next (task 4.9)

Rules that the demo checks:
  * Isolation (★ three concurrent debates): every piece of state lives under debates[trace_id].
    Kafka key = trace_id keeps one debate's messages in order; grouping by trace_id is what keeps
    different debates apart (several traces can share a partition).
  * Timeout (★ agent timeout): each round has a deadline. tick() closes a round whose deadline passed,
    records the missing agents, and the result carries a limitation note. Approval is still required.
  * Duplicates: an idempotency_key already in debate["seen"] is ignored (at-least-once redelivery).
  * Late messages: a message for a round that already closed is logged in debate["late"] and never
    changes the decision.
  * Cross-topic ordering: Kafka does not order ai.parsed against debate.round.0, so a Round 0 message
    can arrive before the Chair has seen ai.parsed. It is held in `early` and replayed.
"""
from __future__ import annotations

import time
from typing import Any, Callable

from . import envelope as env
from .agents_bridge import chair_rules, property_facts
from .explanation import Explainer, context, template_explainer
from .config import EXPECTED_AGENTS, T_ROUND1, T_RESULTS
from .harness import InvalidAgentOutput, check_round0, check_vote
from .store import Store

Outgoing = tuple[str, str, dict[str, Any]]      # (topic, kafka key, envelope)


class ChairCore:
    def __init__(self, store: Store, *, round_timeout_s: float = 10.0, clock: Callable[[], float] = time.time,
                 explainer: Explainer = template_explainer):
        self.store = store
        self.explainer = explainer
        self.round_timeout_s = round_timeout_s
        self.clock = clock
        self.debates: dict[str, dict[str, Any]] = {}
        self.early: dict[str, list[dict[str, Any]]] = {}

    # ------------------------------------------------------------------ recovery
    def recover(self) -> int:
        """After a restart, reload every unfinished debate from the store. Deadlines keep running."""
        for d in self.store.open_debates():
            self.debates[d["trace_id"]] = d
        return len(self.debates)

    def unsent(self) -> list[Outgoing]:
        """Events that were saved but maybe never reached Kafka (crash between save and publish).
        The worker republishes them after recover(); consumers drop repeats by idempotency_key."""
        return [tuple(o) for d in self.debates.values() for o in d.get("outbox", [])]

    def mark_sent(self, trace_ids) -> None:
        """Called by the worker once the producer has flushed: those events are safely in Kafka."""
        for t in set(trace_ids):
            d = self.debates.get(t)
            if d and d.get("outbox"):
                d["outbox"] = []
                self._save(d)
            if d and d["phase"] == "done":
                self.debates.pop(t, None)      # finished debates leave memory; they stay in the store

    # ------------------------------------------------------------------ entry points
    def handle(self, event: dict[str, Any]) -> list[Outgoing]:
        etype = event["event_type"]
        if etype == "ai.parsed":
            return self._on_parsed(event)
        if etype == "debate.round.0":
            return self._on_round0(event)
        if etype == "debate.round.2":
            return self._on_round2(event)
        return []   # other AI events are not for the Chair

    def tick(self) -> list[Outgoing]:
        """Close every round whose deadline has passed. Called by the worker about twice a second."""
        out: list[Outgoing] = []
        now = self.clock()
        for d in list(self.debates.values()):
            if d["phase"] in ("round0", "round2") and now >= d["deadline"]:
                waiting = self._waiting_for(d)
                d["missing"][d["phase"]] = sorted(waiting)
                d["log"].append({"at": now, "what": f"{d['phase']} deadline passed", "missing": sorted(waiting)})
                out += self._close_round0(d) if d["phase"] == "round0" else self._finalize(d)
        return out

    # ------------------------------------------------------------------ handlers
    def _on_parsed(self, event):
        trace_id, p = event["trace_id"], event["payload"]
        if trace_id in self.debates or self.store.get_debate(trace_id):
            return []                                   # duplicate ai.parsed
        config = p.get("evaluation_config", "full_debate")
        now = self.clock()
        d = {
            "trace_id": trace_id, "task_id": p.get("task_id", trace_id), "plan_id": p.get("plan_id"),
            "debate_id": "deb-" + trace_id.split("-", 1)[-1], "config": config,
            "expected": list(EXPECTED_AGENTS.get(config, [])), "request": p.get("request"),
            "phase": "round0", "deadline": now + self.round_timeout_s,
            "round0": {}, "round1": None, "voters": [], "votes": {},
            "rejected": [], "missing": {"round0": [], "round2": []}, "late": [], "seen": [], "outbox": [],
            "log": [{"at": now, "what": "debate opened", "config": config}],
            "timings": {"opened_at": now},
            # one plan per trace (ai.md "Plan, Timeout, and Safety Rules"): every step has an owner,
            # a status and timestamps; a dependent step starts only after its prerequisite completes
            "plan": [{"step_id": s, "owner": o, "status": "pending", "started_at": None, "finished_at": None,
                      "note": None}
                     for s, o in (("parse", "parser"), ("retrieve_candidates", "agents"), ("round_0", "chair"),
                                  ("round_1_2", "chair"), ("adjudicate", "chair"), ("explain", "explanation"),
                                  ("human_approval", "guest"))],
        }
        self._step(d, "parse", "completed", note=p.get("parser_cache") or p.get("model"))
        self._step(d, "round_0", "in_progress")
        self.debates[trace_id] = d
        self.store.append_event(trace_id, event)
        if p.get("request") is None or config not in EXPECTED_AGENTS:
            reason = p.get("error") or f"unsupported evaluation_config {config!r}"
            return self._fail(d, f"parse_failed: {reason}")
        self._save(d)
        out: list[Outgoing] = []
        for held in self.early.pop(trace_id, []):       # replay Round 0 messages that arrived first
            out += self.handle(held)
        return out

    def _on_round0(self, event):
        trace_id, p = event["trace_id"], event["payload"]
        d = self.debates.get(trace_id)
        if d is None:
            self.early.setdefault(trace_id, []).append(event)   # ai.parsed not seen yet
            return []
        if self._duplicate(d, event):
            return []
        agent = p.get("agent_id")
        self.store.append_event(trace_id, event)
        if d["phase"] != "round0":
            return self._late(d, agent, 0)
        if agent not in d["expected"] or agent in d["round0"]:
            d["log"].append({"at": self.clock(), "what": "unexpected round0 message ignored", "agent": agent})
            self._save(d)
            return []
        try:
            if p.get("status", "completed") != "completed":
                raise InvalidAgentOutput("AGENT_ERROR", f"{agent}: status {p.get('status')}")
            check_round0(agent, p)
            d["round0"][agent] = p
        except InvalidAgentOutput as exc:
            d["rejected"].append({"round": 0, "agent_id": agent, "code": exc.code, "message": exc.message})
        if not self._waiting_for(d):
            d["timings"]["round0_closed_at"] = self.clock()
            return self._close_round0(d)
        self._save(d)
        return []

    def _on_round2(self, event):
        trace_id, p = event["trace_id"], event["payload"]
        d = self.debates.get(trace_id)
        if d is None or self._duplicate(d, event):
            return []
        agent = p.get("agent_id")
        self.store.append_event(trace_id, event)
        if d["phase"] != "round2":
            return self._late(d, agent, 2)
        if agent not in d["voters"] or agent in d["votes"]:
            self._save(d)
            return []
        r1 = d["round1"]
        vote = p.get("vote") or {}
        try:
            check_vote(agent, vote, r1["original_property_id"], r1["challenger_property_id"])
            d["votes"][agent] = {**vote, "agent_id": agent}
        except InvalidAgentOutput as exc:
            d["rejected"].append({"round": 2, "agent_id": agent, "code": exc.code, "message": exc.message})
        if not self._waiting_for(d):
            d["timings"]["round2_closed_at"] = self.clock()
            return self._finalize(d)
        self._save(d)
        return []

    # ------------------------------------------------------------------ round transitions
    def _close_round0(self, d):
        present = {a: d["round0"][a] for a in d["expected"] if a in d["round0"]}
        self._step(d, "round_0", "completed", note=f"{len(present)}/{len(d['expected'])} agents")
        if "matching" not in present:
            return self._fail(d, "matching_missing: no valid Matching opinion before the deadline")
        try:
            chair_rules.check_candidates(present)
        except chair_rules.CandidateMismatch as exc:
            return self._fail(d, f"candidate_mismatch: {exc}")
        n = len(present["matching"]["candidate_property_ids"])
        self._step(d, "retrieve_candidates", "completed", note=f"{n} candidates from the fixed pool")
        if d["config"] == "single_agent":
            self._step(d, "round_1_2", "skipped", note="single_agent config")
            return self._finalize(d)
        d["disagreement"] = chair_rules.disagreement(present)
        r1 = chair_rules.round1(present, d["config"])
        d["round1"] = r1
        if r1["challenger_property_id"] is None:       # everyone agrees on top-1: no vote needed
            self._step(d, "round_1_2", "skipped", note="agents agree on top-1")
            return self._finalize(d)
        self._step(d, "round_1_2", "in_progress")
        d["phase"] = "round2"
        d["voters"] = sorted(present)
        d["deadline"] = self.clock() + self.round_timeout_s
        d["log"].append({"at": self.clock(), "what": "round1 published", "voters": d["voters"]})
        payload = {**r1, "debate_id": d["debate_id"], "plan_id": d["plan_id"], "task_id": d["task_id"],
                   "request": d["request"], "target_agent_ids": d["voters"],
                   "deadline_ms": int(self.round_timeout_s * 1000)}
        out = self._out(d, T_ROUND1, payload, "chair:round-1")
        self._save(d)
        return [out]

    def _finalize(self, d):
        present = {a: d["round0"][a] for a in d["expected"] if a in d["round0"]}
        r1 = d.get("round1")
        if d["config"] == "single_agent" or r1 is None:
            winner = chair_rules.top_ids(present["matching"], 1)[0]
            adjudication = {"method": "single_agent", "deadlock": False,
                            "decision_rule": chair_rules.DECISION_RULE, "tie_break_applied": None}
            ranking = chair_rules.top_ids(present["matching"])
            votes = []
        else:
            votes = [d["votes"][a] for a in sorted(d["votes"])]
            winner, adjudication = chair_rules.adjudicate(present, r1, votes)
            ranking = chair_rules.final_ranking(present, winner)
        if d["plan"][3]["status"] == "in_progress":
            self._step(d, "round_1_2", "completed", note=f"{len(votes)}/{len(d['voters'])} votes")
        self._step(d, "adjudicate", "completed", note=adjudication["method"])
        dissent = [{"agent_id": v["agent_id"], "reason": v["reason"]}
                   for v in votes if v.get("property_id") not in (winner, None)]
        limitations = self._limitations(d)
        m_scores = {r["property_id"]: r for r in present["matching"]["ranked_properties"]}
        top = m_scores.get(winner, {"property_id": winner})
        alternatives = [{"property_id": pid, "score": m_scores.get(pid, {}).get("score"),
                         "reasons": m_scores.get(pid, {}).get("reasons", [])} for pid in ranking[1:3]]
        flags = [f for f in present.get("risk", {}).get("risk_flags", []) if f.get("property_id") == winner]
        # Explanation step: reads only the decided result; cannot change it
        self._step(d, "explain", "in_progress")
        explanation = self.explainer(context(property_facts(winner), top, alternatives[0] if alternatives else None,
                                             flags, dissent, limitations))
        if explanation.get("source") == "template_fallback":
            limitations = limitations + ["explanation_fallback"]
        self._step(d, "explain", "completed", note=explanation.get("source"))
        d["timings"]["explained_at"] = self.clock()
        result = {
            "trace_id": d["trace_id"], "task_id": d["task_id"], "plan_id": d["plan_id"],
            "debate_id": d["debate_id"], "config": d["config"], "status": "awaiting_approval",
            "recommendation": {"property_id": winner, "score": top.get("score"), "reasons": top.get("reasons", [])},
            "final_ranking": ranking,
            "alternatives": alternatives,
            "disagreement": d.get("disagreement"),
            "round1": r1,
            "votes": votes,
            "vote_summary": {c: sum(v["choice"] == c for v in votes)
                             for c in ("keep_original", "switch_to_challenger", "abstain")},
            "dissent": dissent,
            "adjudication": adjudication,
            "audit_findings": present.get("fairness", {}).get("audit_findings", []),
            # measured values only (spec 13.6) ...
            "evidence": {"scoring_versions": {a: p["scoring_version"] for a, p in present.items()},
                         "score_breakdown": top.get("score_breakdown", {}),
                         "risk_flags": flags,
                         "evidence_ids": top.get("evidence_ids", [])},
            # ... and generated text, kept separate and never used for scoring
            "explanation": explanation,
            "limitations": limitations,
            "approval_required": True,      # always: agents never act on their own
            "allowed_agent_actions": [],    # agents cannot book, message or change availability
            "plan": d["plan"],
            "timings_s": self._timings(d),
        }
        self._step(d, "human_approval", "in_progress")
        return self._publish_result(d, result)

    def _fail(self, d, reason):
        for st in d["plan"]:
            if st["status"] in ("pending", "in_progress"):
                st["status"] = "failed" if st["step_id"] != "human_approval" else "pending"
        result = {
            "trace_id": d["trace_id"], "task_id": d["task_id"], "plan_id": d["plan_id"],
            "debate_id": d["debate_id"], "config": d["config"], "status": "failed",
            "recommendation": None, "final_ranking": [], "limitations": [reason] + self._limitations(d),
            "approval_required": True, "plan": d["plan"], "timings_s": self._timings(d),
        }
        return self._publish_result(d, result)

    def _publish_result(self, d, result):
        d["phase"] = "done"
        d["timings"]["done_at"] = self.clock()
        result["timings_s"] = self._timings(d)
        self.store.save_result(result)       # persist first, then publish (worker commits offsets after)
        out = self._out(d, T_RESULTS, result, "chair:result")
        self._save(d)
        return [out]

    # ------------------------------------------------------------------ helpers
    def _waiting_for(self, d) -> set[str]:
        if d["phase"] == "round0":
            answered = set(d["round0"]) | {r["agent_id"] for r in d["rejected"] if r["round"] == 0}
            return set(d["expected"]) - answered
        if d["phase"] == "round2":
            answered = set(d["votes"]) | {r["agent_id"] for r in d["rejected"] if r["round"] == 2}
            return set(d["voters"]) - answered
        return set()

    def _duplicate(self, d, event) -> bool:
        key = event["idempotency_key"]
        if key in d["seen"]:
            return True
        d["seen"].append(key)
        return False

    def _late(self, d, agent, rnd):
        d["late"].append({"agent_id": agent, "round": rnd, "at": self.clock(), "phase_then": d["phase"]})
        self._save(d)
        return []

    @staticmethod
    def _limitations(d) -> list[str]:
        notes = [f"{a} timed out in round {rnd[-1]}" for rnd in ("round0", "round2") for a in d["missing"][rnd]]
        notes += [f"{r['agent_id']} output rejected in round {r['round']} ({r['code']})" for r in d["rejected"]]
        return notes

    @staticmethod
    def _timings(d) -> dict[str, float]:
        t = d["timings"]
        base = t["opened_at"]
        return {k.replace("_at", ""): round(v - base, 3) for k, v in t.items() if k != "opened_at"}

    def _save(self, d):
        self.store.save_debate(d)

    def _step(self, d, step_id, status, note=None):
        now = self.clock()
        for st in d["plan"]:
            if st["step_id"] == step_id:
                if status == "in_progress" or st["started_at"] is None:
                    st["started_at"] = st["started_at"] or now
                if status in ("completed", "skipped", "failed"):
                    st["finished_at"] = now
                st["status"] = status
                if note is not None:
                    st["note"] = note

    def _out(self, d, topic, payload, idem_suffix) -> Outgoing:
        ev = env.build(event_type=topic, trace_id=d["trace_id"], actor_id="chair",
                       entity_type="debate", entity_id=d["debate_id"], payload=payload,
                       idempotency_key=f"{d['trace_id']}:{idem_suffix}")
        self.store.append_event(d["trace_id"], ev)
        d["outbox"].append([topic, d["trace_id"], ev])     # cleared by mark_sent() after Kafka confirms
        return (topic, d["trace_id"], ev)
