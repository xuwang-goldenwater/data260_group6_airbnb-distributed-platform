"""Guest Request Parser agent: free-text request -> structured request.

Uses a local Ollama model (default qwen2.5:3b) with a JSON schema, then
validates the output in code. On a validation error it retries once with the
error message; if that fails too it returns an error instead of bad data
(assignment 7.5.1: the harness rejects invalid agent output).

Every real model call is stored in an exact-match cache keyed by
(model, prompt version, text), so re-running the benchmark gives the same
parse for every configuration and needs no model calls.

Usage (from repo root, Ollama running):
    python3 services/agents/parser.py R05        # parse one benchmark request
    python3 services/agents/parser.py --all      # all 50 + per-field accuracy
Env (optional): OLLAMA_URL (default http://localhost:11434), OLLAMA_MODEL
"""
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
MODEL = os.environ.get("OLLAMA_MODEL", "qwen2.5:3b")
PROMPT_VERSION = "parser-prompt-v1"
CACHE_PATH = Path("benchmarks/cache/parser_cache.json")
REQUESTS = Path("benchmarks/requests.json")

CITIES = ["San Jose", "Santa Cruz", "Monterey", "San Francisco"]
AMENITIES = ["kitchen", "parking", "pool", "washer", "wifi"]
TYPES = ["apartment", "house", "condo", "cabin", "room"]
FIELDS = ["city", "check_in", "check_out", "guests", "max_nightly_price",
          "required_amenities", "min_rating", "preferred_property_type"]

# No nulls: 0 = no minimum rating, "none" = no type preference (easier for small models).
SCHEMA = {
    "type": "object",
    "properties": {
        "city": {"type": "string", "enum": CITIES},
        "check_in": {"type": "string"},
        "check_out": {"type": "string"},
        "guests": {"type": "integer"},
        "max_nightly_price": {"type": "integer"},
        "required_amenities": {"type": "array", "items": {"type": "string", "enum": AMENITIES}},
        "min_rating": {"type": "number"},
        "preferred_property_type": {"type": "string", "enum": TYPES + ["none"]},
    },
    "required": FIELDS,
}

SYSTEM_PROMPT = """You extract a vacation-rental search from a guest's message. Reply with JSON only.
Rules:
- city: one of San Jose, Santa Cruz, Monterey, San Francisco.
- check_in, check_out: YYYY-MM-DD exactly as written in the message.
- guests: number of people. "just me" = 1.
- max_nightly_price: the nightly budget in dollars, integer.
- required_amenities: ONLY amenities the guest says they need (from kitchen, parking, pool, washer, wifi). Empty list if none.
- min_rating: the minimum rating if the guest states one, otherwise 0.
- preferred_property_type: the type the guest would like (apartment, house, condo, cabin, room). "private room" = room, "entire apartment" = apartment, "whole house" = house. Use "none" if not mentioned. A preference is NOT a requirement.

Example
Message: "Visiting family in Santa Cruz from 2026-11-26 to 2026-11-28, 3 of us. Max $290 a night, and we need kitchen and washer. A cabin would feel most like home."
JSON: {"city": "Santa Cruz", "check_in": "2026-11-26", "check_out": "2026-11-28", "guests": 3, "max_nightly_price": 290, "required_amenities": ["kitchen", "washer"], "min_rating": 0, "preferred_property_type": "cabin"}"""


# ---------- cache ----------

def cache_key(text):
    return hashlib.sha256(f"{MODEL}|{PROMPT_VERSION}|{text}".encode()).hexdigest()


def load_cache():
    return json.loads(CACHE_PATH.read_text()) if CACHE_PATH.exists() else {}


def save_cache(cache):
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, indent=1, sort_keys=True) + "\n")


# ---------- model call ----------

def call_ollama(messages):
    body = json.dumps({
        "model": MODEL,
        "messages": messages,
        "format": SCHEMA,
        "stream": False,
        "options": {"temperature": 0, "seed": 260},
    }).encode()
    req = urllib.request.Request(f"{OLLAMA_URL}/api/chat", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())["message"]["content"]


# ---------- validation ----------

def normalize(raw):
    """Model JSON -> our request format (0 -> None, 'none' -> None, sorted amenities)."""
    out = {k: raw.get(k) for k in FIELDS}
    out["min_rating"] = out["min_rating"] or None
    if out["preferred_property_type"] == "none":
        out["preferred_property_type"] = None
    if isinstance(out["required_amenities"], list):
        out["required_amenities"] = sorted(set(out["required_amenities"]))
    return out


def validate(q):
    """Return a list of problems; empty list = valid."""
    errs = []
    if q["city"] not in CITIES:
        errs.append(f"city must be one of {CITIES}")
    try:
        ci, co = date.fromisoformat(q["check_in"]), date.fromisoformat(q["check_out"])
        if co <= ci:
            errs.append("check_out must be after check_in")
    except (TypeError, ValueError):
        errs.append("check_in and check_out must be YYYY-MM-DD")
    if not isinstance(q["guests"], int) or not 1 <= q["guests"] <= 16:
        errs.append("guests must be an integer from 1 to 16")
    if not isinstance(q["max_nightly_price"], (int, float)) or q["max_nightly_price"] <= 0:
        errs.append("max_nightly_price must be a positive number")
    if not isinstance(q["required_amenities"], list) or not set(q["required_amenities"]) <= set(AMENITIES):
        errs.append(f"required_amenities must be a list from {AMENITIES}")
    if q["min_rating"] is not None and not 0 < q["min_rating"] <= 5:
        errs.append("min_rating must be between 0 and 5")
    if q["preferred_property_type"] not in TYPES + [None]:
        errs.append(f"preferred_property_type must be one of {TYPES} or none")
    return errs


# ---------- agent ----------

def parse(text, cache=None):
    """Return {"request": dict | None, "error": str | None, meta...}."""
    cache = load_cache() if cache is None else cache
    key = cache_key(text)
    t0 = time.perf_counter()
    if key in cache:
        q = cache[key]["request"]
        return {"request": q, "error": None, "model": MODEL, "prompt_version": PROMPT_VERSION,
                "llm_cache_hit": True, "attempts": 0,
                "latency_ms": round((time.perf_counter() - t0) * 1000, 2)}

    messages = [{"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Message: {text}"}]
    error = None
    for attempt in (1, 2):                       # one retry with the validation errors
        try:
            content = call_ollama(messages)
            q = normalize(json.loads(content))
            problems = validate(q)
        except urllib.error.URLError as e:
            return {"request": None, "error": f"ollama unreachable: {e}", "model": MODEL,
                    "prompt_version": PROMPT_VERSION, "llm_cache_hit": False, "attempts": attempt,
                    "latency_ms": round((time.perf_counter() - t0) * 1000, 2)}
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            content, q, problems = str(e), None, [f"output was not valid JSON: {e}"]
        if not problems:
            cache[key] = {"text": text, "request": q, "model": MODEL, "prompt_version": PROMPT_VERSION}
            save_cache(cache)
            return {"request": q, "error": None, "model": MODEL, "prompt_version": PROMPT_VERSION,
                    "llm_cache_hit": False, "attempts": attempt,
                    "latency_ms": round((time.perf_counter() - t0) * 1000, 2)}
        error = "; ".join(problems)
        messages += [{"role": "assistant", "content": content},
                     {"role": "user", "content": f"Invalid: {error}. Return corrected JSON."}]
    return {"request": None, "error": f"INVALID_AGENT_OUTPUT: {error}", "model": MODEL,
            "prompt_version": PROMPT_VERSION, "llm_cache_hit": False, "attempts": 2,
            "latency_ms": round((time.perf_counter() - t0) * 1000, 2)}


# ---------- CLI ----------

def main():
    reqs = json.loads(REQUESTS.read_text())["requests"]
    if len(sys.argv) > 1 and sys.argv[1] != "--all":
        reqs = [r for r in reqs if r["request_id"] == sys.argv[1]]
    cache = load_cache()
    correct = {k: 0 for k in FIELDS}
    exact = failed = 0
    for r in reqs:
        out = parse(r["request_text"], cache)
        if out["error"]:
            failed += 1
            print(f"{r['request_id']}: ERROR {out['error']}")
            continue
        wrong = [k for k in FIELDS if out["request"][k] != r["expected_parse"][k]]
        for k in FIELDS:
            correct[k] += k not in wrong
        exact += not wrong
        tag = "cache" if out["llm_cache_hit"] else f"{out['latency_ms'] / 1000:.1f}s, try {out['attempts']}"
        detail = ", ".join(f"{k}: got {out['request'][k]!r} want {r['expected_parse'][k]!r}" for k in wrong)
        print(f"{r['request_id']} ({tag}): {'OK' if not wrong else 'WRONG ' + detail}")
    n = len(reqs)
    print(f"\n{exact}/{n} fully correct, {failed} failed")
    if n > 1:
        for k in FIELDS:
            print(f"  {k:>24}: {correct[k]}/{n}")


if __name__ == "__main__":
    main()
