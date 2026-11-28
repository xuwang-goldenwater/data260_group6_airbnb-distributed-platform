"""AI dashboard (section 8, Role E): live MongoDB records + benchmark files, trace filter."""
from __future__ import annotations

from test_api_flow import GUEST, decided_task


def test_dashboard_live_benchmark_and_trace():
    client, store, bus, task_id, result = decided_task("R35")       # R35: agents disagree, risk challenge
    assert client.post(f"/ai/results/{task_id}/approve", headers={**GUEST, "Idempotency-Key": "k1"},
                       json={"decision": "approved"}).status_code == 200

    page = client.get("/ai/dashboard")
    assert page.status_code == 200 and "AI Dashboard" in page.text

    data = client.get("/ai/dashboard/data").json()
    live = data["live"]
    assert live["results"] == 1 and live["approvals"]["approved"] == 1
    assert live["debate"]["debates"] == 1 and live["debate"]["challenges"] == 1
    assert sum(live["debate"]["votes"].values()) == 3
    bench = data["benchmark"]
    assert {q["config"] for q in bench["quality"]} >= {"baseline", "single_agent", "full_debate"}
    assert bench["full_debate"]["debates"] == 50 and "p10_vs_other_labeler" in bench["human_ceiling"]
    assert "trace" not in data

    trace = client.get(f"/ai/dashboard/data?trace_id={task_id}").json()["trace"]
    types = [e["event_type"] for e in trace["events"]]
    assert "debate.round.1" in types and "ai.results" in types
    assert {"matching", "risk", "fairness"} <= {e["agent"] for e in trace["events"]}
    assert trace["events"][0]["offset_ms"] == 0 and trace["recommendation"] == store.get_result(task_id)["recommendation"]["property_id"]
    assert client.get("/ai/dashboard/data?trace_id=nope").json()["trace"] is None
