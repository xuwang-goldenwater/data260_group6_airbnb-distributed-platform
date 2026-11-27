"""Explanation step (task 4.9, ai.md "Plan, Timeout, and Safety Rules").

Runs inside the Chair AFTER adjudication and BEFORE ai.results is published. It only reads the
already-decided result; it cannot change the ranking, the votes or the decision. Its text goes in
`explanation`, kept apart from the measured `evidence` (spec 13.6).

Mode "llm" asks the local Ollama model with a hard deadline (default 5 s). If the call fails or is
too slow, a fixed template built from the evidence is used instead, `source` says
"template_fallback", and the result gets an `explanation_fallback` limitation. A slow explanation
never blocks the debate.
"""
from __future__ import annotations

import json
import urllib.request
from typing import Any, Callable

Explainer = Callable[[dict[str, Any]], dict[str, Any]]


def context(winner_prop: dict[str, Any] | None, winner_entry: dict[str, Any], runner_up: dict[str, Any] | None,
            risk_flags: list[dict[str, Any]], dissent: list[dict[str, Any]], limitations: list[str],
            alternative: dict[str, Any] | None = None) -> dict[str, Any]:
    """The only facts the Explanation step may use."""
    p = winner_prop or {}
    return {
        "property_id": winner_entry.get("property_id"),
        "title": p.get("title"), "city": (p.get("location") or {}).get("city"),
        "nightly_price": p.get("nightly_price"), "rating": p.get("rating"), "review_count": p.get("review_count"),
        "property_type": p.get("property_type"), "amenities": p.get("amenities", []),
        "reasons": winner_entry.get("reasons", []),
        "risk_notes": list(dict.fromkeys(f.get("detail") for f in risk_flags if f.get("detail")))[:3],
        "runner_up": runner_up,
        "dissent": [f"{d['agent_id']} preferred another listing ({d['reason']})" for d in dissent][:2],
        "limitations": limitations,
        # explanation-v2: entire-place option shown next to a private-room pick
        # (services/agents/alternatives.py); None keeps the v1 text unchanged
        "entire_place_alternative": alternative,
    }


def template_text(ctx: dict[str, Any]) -> str:
    parts = [f"We recommend {ctx['property_id']}"]
    facts = []
    if ctx.get("nightly_price") is not None:
        facts.append(f"${ctx['nightly_price']:.0f} per night")
    if ctx.get("property_type"):
        facts.append(f"a {ctx['property_type']} in {ctx.get('city') or 'the requested city'}")
    if ctx.get("review_count"):
        facts.append(f"rated {float(ctx['rating']):.1f} from {ctx['review_count']} reviews")
    elif ctx.get("review_count") == 0:
        facts.append("a new listing with no reviews yet")
    if facts:
        parts.append(": " + ", ".join(facts))
    text = "".join(parts) + "."
    if ctx.get("reasons"):
        text += " Why: " + "; ".join(ctx["reasons"][:3]) + "."
    if ctx.get("risk_notes"):
        text += " Worth checking: " + "; ".join(ctx["risk_notes"]) + "."
    if ctx.get("runner_up"):
        text += f" Next best option: {ctx['runner_up']['property_id']}."
    alt = ctx.get("entire_place_alternative")
    if alt:
        text += " " + alternative_sentence(alt)
    if ctx.get("dissent"):
        text += " Dissent: " + ctx["dissent"][0] + "."
    if ctx.get("limitations"):
        text += " Note: " + "; ".join(ctx["limitations"]) + "."
    return text


def alternative_sentence(alt: dict[str, Any]) -> str:
    rated = f"rated {alt['rating']:.1f}" if alt.get("rating") is not None else "new listing"
    return (f"An entire place at a similar price is also available: {alt['property_id']} "
            f"({alt['property_type']}, ${alt['nightly_price']:.0f}, {rated}).")


def template_explainer(ctx: dict[str, Any]) -> dict[str, Any]:
    return {"text": template_text(ctx), "source": "template", "model": None, "generated": True}


def ollama_explainer(url: str, model: str, timeout_s: float) -> Explainer:
    prompt = ("Write 2-3 short plain-English sentences telling a guest why this rental was recommended. "
              "Use ONLY the facts in the JSON. Do not invent prices, ratings or amenities. "
              "Mention any risk notes or limitations honestly. If entire_place_alternative is not null, "
              "add one sentence offering it as an entire place at a similar price.")

    def explain(ctx: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps({"model": model, "stream": False, "options": {"temperature": 0},
                           "messages": [{"role": "system", "content": prompt},
                                        {"role": "user", "content": json.dumps(ctx)}]}).encode()
        try:
            req = urllib.request.Request(f"{url}/api/chat", data=body, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                text = json.loads(resp.read())["message"]["content"].strip()
            if not text:
                raise ValueError("empty explanation")
            alt = ctx.get("entire_place_alternative")
            if alt and alt["property_id"] not in text:     # explanation-v2 must always show the option
                text += " " + alternative_sentence(alt)
            return {"text": text, "source": "llm", "model": model, "generated": True}
        except Exception as exc:          # timeout, Ollama down, bad JSON: never block the result
            return {"text": template_text(ctx), "source": "template_fallback", "model": None,
                    "generated": True, "fallback_reason": type(exc).__name__}

    return explain
