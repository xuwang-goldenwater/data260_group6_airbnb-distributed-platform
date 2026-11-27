"""Tests for the Chair state machine, with no Kafka broker.

Run from the repo root:  python -m pytest services/ai/tests -q
"""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))   # services/ai

from ai_core import agent_logic, envelope as env                          # noqa: E402
from ai_core.agents_bridge import chair_rules, round0_payload             # noqa: E402
from ai_core.chair_core import ChairCore                                  # noqa: E402
from ai_core.config import REPO_ROOT, T_RESULTS, T_ROUND1                 # noqa: E402
from ai_core.store import MemoryStore                                     # noqa: E402

REQUESTS = {r["request_id"]: r for r in json.loads((REPO_ROOT / "benchmarks/requests.json").read_text())["requests"]}
SCHEMA = REPO_ROOT / "shared" / "envelope.schema.json"


class Clock:
    def __init__(self):
        self.t = 1_000.0

    def __call__(self):
        return self.t


def parsed_event(trace_id, request_id, config="full_debate"):
    q = REQUESTS[request_id]["expected_parse"]
    return env.build(event_type="ai.parsed", trace_id=trace_id, actor_id="parser", entity_type="debate",
                     entity_id=trace_id, idempotency_key=f"{trace_id}:parser:parsed",
                     payload={"task_id": trace_id, "plan_id": "plan-" + trace_id, "evaluation_config": config,
                              "request": q, "error": None})


def round0_events(parsed, agents):
    return [agent_logic.round0_step(a, parsed)[0][2] for a in agents]


def drive(chair, events):
    """Feed events to the Chair, and answer any Round 1 it publishes with every target agent's vote."""
    results, queue = [], list(events)
    while queue:
        for topic, key, ev in chair.handle(queue.pop(0)):
            env.validate(ev, SCHEMA, key=key)          # everything we publish passes the shared schema
            if topic == T_ROUND1:
                for agent in ev["payload"]["target_agent_ids"]:
                    queue += [o[2] for o in agent_logic.round2_step(agent, ev)]
            elif topic == T_RESULTS:
                results.append(ev["payload"])
    return results


def offline(request_id, config="full_debate"):
    q = REQUESTS[request_id]["expected_parse"]
    agents = {"full_debate": ["matching", "risk", "fairness"], "debate_without_auditor": ["matching", "risk"]}[config]
    return chair_rules.run({a: round0_payload(a, q) for a in agents}, config)


@pytest.fixture
def chair():
    return ChairCore(MemoryStore(), round_timeout_s=10, clock=Clock())


# ---------------------------------------------------------------- normal debates
@pytest.mark.parametrize("rid", ["R45", "R07", "R35"])      # consensus, majority, safety gate
def test_kafka_chair_matches_offline_benchmark(chair, rid):
    p = parsed_event("tr-" + rid, rid)
    [result] = drive(chair, [p] + round0_events(p, ["matching", "risk", "fairness"]))
    expected = offline(rid)
    assert result["recommendation"]["property_id"] == expected["recommendation"]
    assert result["final_ranking"] == expected["final_ranking"]
    assert result["adjudication"]["method"] == expected["adjudication"]["method"]
    assert result["status"] == "awaiting_approval" and result["approval_required"] is True
    assert len(result["final_ranking"]) == 10 and result["limitations"] == []


def test_single_agent_config_skips_debate(chair):
    p = parsed_event("tr-single", "R07", config="single_agent")
    [result] = drive(chair, [p] + round0_events(p, ["matching"]))
    assert result["adjudication"]["method"] == "single_agent"
    assert result["round1"] is None


# ---------------------------------------------------------------- ★ agent timeout (task 4.7)
def test_missing_agent_times_out_with_partial_result():
    clock = Clock()
    chair = ChairCore(MemoryStore(), round_timeout_s=10, clock=clock)
    p = parsed_event("tr-timeout", "R45")
    assert drive(chair, [p] + round0_events(p, ["matching", "risk"])) == []   # fairness never answers
    clock.t += 9.9
    assert chair.tick() == []                                                 # deadline not reached yet
    clock.t += 0.2
    [(topic, _, ev)] = chair.tick()
    result = ev["payload"]
    assert topic == T_RESULTS
    assert "fairness timed out in round 0" in result["limitations"]
    assert result["status"] == "awaiting_approval" and result["approval_required"] is True


def test_no_matching_before_deadline_fails_safely():
    clock = Clock()
    chair = ChairCore(MemoryStore(), round_timeout_s=5, clock=clock)
    p = parsed_event("tr-nomatch", "R45")
    drive(chair, [p] + round0_events(p, ["risk", "fairness"]))
    clock.t += 6
    [(_, _, ev)] = chair.tick()
    assert ev["payload"]["status"] == "failed"
    assert ev["payload"]["limitations"][0].startswith("matching_missing")


# ---------------------------------------------------------------- ★ three concurrent debates (task 4.8)
def test_three_interleaved_debates_do_not_mix():
    chair = ChairCore(MemoryStore(), round_timeout_s=10, clock=Clock())
    rids = ["R07", "R13", "R35"]
    parsed = [parsed_event(f"tr-{i}", rid) for i, rid in enumerate(rids)]
    r0 = [round0_events(p, ["matching", "risk", "fairness"]) for p in parsed]
    interleaved = parsed + [r0[d][a] for a in range(3) for d in range(3)]   # agent by agent across debates
    results = {r["trace_id"]: r for r in drive(chair, interleaved)}
    assert len(results) == 3
    for i, rid in enumerate(rids):
        assert results[f"tr-{i}"]["final_ranking"] == offline(rid)["final_ranking"]


# ---------------------------------------------------------------- delivery problems
def test_duplicate_round0_is_ignored(chair):
    p = parsed_event("tr-dup", "R45")
    m, r, f = round0_events(p, ["matching", "risk", "fairness"])
    results = drive(chair, [p, m, copy.deepcopy(m), r, f])
    assert len(results) == 1


def test_round0_before_parsed_is_replayed(chair):
    p = parsed_event("tr-early", "R45")
    events = round0_events(p, ["matching", "risk", "fairness"])
    results = drive(chair, events[:2] + [p] + events[2:])     # two opinions arrive before ai.parsed
    assert len(results) == 1 and results[0]["limitations"] == []


def test_late_round0_is_logged_not_counted():
    clock = Clock()
    store = MemoryStore()
    chair = ChairCore(store, round_timeout_s=10, clock=clock)
    p = parsed_event("tr-late", "R45")
    m, r, f = round0_events(p, ["matching", "risk", "fairness"])
    drive(chair, [p, m, r])
    clock.t += 11
    [(_, _, ev)] = chair.tick()
    assert drive(chair, [f]) == []                                   # fairness finally arrives
    assert store.get_debate("tr-late")["late"][0]["agent_id"] == "fairness"
    assert store.get_result("tr-late")["recommendation"] == ev["payload"]["recommendation"]


def test_invalid_agent_output_is_rejected(chair):
    p = parsed_event("tr-bad", "R45")
    m, r, f = round0_events(p, ["matching", "risk", "fairness"])
    f["payload"]["ranked_properties"][0]["score"] = 1.7               # outside 0..1
    [result] = drive(chair, [p, m, r, f])
    assert any("fairness output rejected in round 0" in n for n in result["limitations"])


def test_restart_recovers_open_debate():
    clock, store = Clock(), MemoryStore()
    first = ChairCore(store, round_timeout_s=10, clock=clock)
    p = parsed_event("tr-crash", "R45")
    m, r, f = round0_events(p, ["matching", "risk", "fairness"])
    drive(first, [p, m, r])                                          # Chair crashes here
    second = ChairCore(store, round_timeout_s=10, clock=clock)
    assert second.recover() == 1
    assert drive(second, [copy.deepcopy(r)]) == []                   # redelivered message: deduplicated
    [result] = drive(second, [f])
    assert result["limitations"] == []


# ---------------------------------------------------------------- explanation step (task 4.9)
def test_explanation_is_filled_and_kept_apart_from_evidence(chair):
    p = parsed_event("tr-ex", "R07")
    [result] = drive(chair, [p] + round0_events(p, ["matching", "risk", "fairness"]))
    ex = result["explanation"]
    assert ex["source"] == "template" and ex["generated"] is True
    assert result["recommendation"]["property_id"] in ex["text"]
    assert "explanation" not in result["evidence"] and "score_breakdown" in result["evidence"]
    assert len(result["alternatives"]) == 2 and result["allowed_agent_actions"] == []
    assert [s["step_id"] for s in result["plan"]][-2:] == ["explain", "human_approval"]


def test_slow_llm_falls_back_to_template_and_says_so():
    from ai_core.explanation import ollama_explainer
    slow = ollama_explainer("http://127.0.0.1:9", "qwen2.5:3b", timeout_s=0.2)      # nothing listens there
    core = ChairCore(MemoryStore(), round_timeout_s=10, clock=Clock(), explainer=slow)
    p = parsed_event("tr-fb", "R45")
    [result] = drive(core, [p] + round0_events(p, ["matching", "risk", "fairness"]))
    assert result["explanation"]["source"] == "template_fallback"
    assert "explanation_fallback" in result["limitations"]
    assert result["recommendation"]["property_id"] == offline("R45")["recommendation"]   # decision unchanged


def test_corrupt_agent_output_is_rejected_not_used(chair):
    """What `agent_worker fairness --corrupt` sends: score 1.7 on an unknown property."""
    from ai_workers.agent_worker import corrupt
    p = parsed_event("tr-bad", "R07")
    good = round0_events(p, ["matching", "risk"])
    bad = corrupt(agent_logic.round0_step("fairness", p)[0])[2]
    [result] = drive(chair, [p] + good + [bad])
    assert any(lim.startswith("fairness output rejected in round 0") for lim in result["limitations"])
    assert result["recommendation"]["property_id"] is not None


# ---------------------------------------------------------------- explanation-v2 (Role E, §7.5.4)
def test_private_room_pick_shows_entire_place_alternative(chair):
    """R01: the debate recommends a private room; explanation-v2 adds the entire-place option
    from services/agents/alternatives.py without touching the ranking."""
    p = parsed_event("tr-v2", "R01")
    [result] = drive(chair, [p] + round0_events(p, ["matching", "risk", "fairness"]))
    ex = result["explanation"]
    assert ex["version"] == "explanation-v2"
    assert ex["entire_place_alternative"]["property_id"] == "property-2041"   # same as eval_explanation.py
    assert "property-2041" in ex["text"]
    assert result["recommendation"]["property_id"] == offline("R01")["recommendation"]
    assert result["final_ranking"] == offline("R01")["final_ranking"]


def test_non_room_pick_keeps_v1_text(chair):
    p = parsed_event("tr-v1", "R45")
    [result] = drive(chair, [p] + round0_events(p, ["matching", "risk", "fairness"]))
    assert result["explanation"]["entire_place_alternative"] is None
    assert "entire place" not in result["explanation"]["text"]


def test_llm_text_that_forgets_the_alternative_gets_it_appended(monkeypatch):
    from ai_core import explanation
    ctx = {"entire_place_alternative": {"property_id": "property-2041", "property_type": "apartment",
                                        "nightly_price": 150, "rating": 4.6}}

    class Resp:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return b'{"message": {"content": "A quiet room near downtown."}}'

    monkeypatch.setattr(explanation.urllib.request, "urlopen", lambda req, timeout: Resp())
    out = explanation.ollama_explainer("http://x", "m", 1)(ctx)
    assert out["source"] == "llm" and "property-2041" in out["text"]
