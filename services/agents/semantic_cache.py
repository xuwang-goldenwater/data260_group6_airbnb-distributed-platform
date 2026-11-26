"""Semantic cache for the Parser: reuse a parse when a new request means the
same thing as a cached one.

A cached parse is reused only if BOTH gates pass on the same cached entry:
  1. hard-constraint gate: the two texts contain exactly the same "hard
     tokens" - numbers, dates, city names, property types, and each amenity
     with its role (required, or optional when its sentence says "would be
     nice", "not required", "optional", ...)
  2. semantic gate: among the entries that pass gate 1, the most similar one
     has cosine similarity (Ollama nomic-embed-text) >= THRESHOLD
So a paraphrase ("kitchen is a must" vs "we need a kitchen") can hit, but a
request with a different budget, date, city, amenity or amenity role never
does. Threshold 0.90 was chosen with benchmarks/test_semantic_cache.py.

Usage (from repo root, Ollama running with nomic-embed-text pulled):
    python3 services/agents/semantic_cache.py "text A" "text B"
"""
import json
import math
import os
import re
import sys
import urllib.request
from pathlib import Path

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
EMBED_MODEL = os.environ.get("OLLAMA_EMBED_MODEL", "nomic-embed-text")
THRESHOLD = 0.90
CACHE_PATH = Path("benchmarks/cache/semantic_cache.json")

CITIES = ["san jose", "santa cruz", "monterey", "san francisco"]
AMENITIES = ["kitchen", "parking", "pool", "washer", "wifi"]
# phrase -> canonical property type (longest phrases first)
TYPE_WORDS = {"private room": "room", "entire apartment": "apartment", "whole house": "house",
              "apartment": "apartment", "house": "house", "condo": "condo", "cabin": "cabin",
              "room": "room"}
NUMBER_WORDS = {"just me": "1", "alone": "1", "solo": "1"}
OPTIONAL_CUES = ["would be nice", "nice to have", "not required", "aren't required",
                 "isn't required", "optional", "if possible", "not a must", "bonus",
                 "don't need", "do not need", "not necessary"]


def hard_tokens(text):
    """Sorted tuple of the constraint-bearing tokens in a request."""
    t = text.lower()
    tokens = re.findall(r"\d{4}-\d{2}-\d{2}", t)
    rest = re.sub(r"\d{4}-\d{2}-\d{2}", " ", t)
    tokens += [str(float(n)).rstrip("0").rstrip(".") for n in re.findall(r"\d+(?:\.\d+)?", rest)]
    tokens += [f"city:{c}" for c in CITIES if c in t]
    for sentence in re.split(r"[.!?;]", t):
        role = "optional" if any(c in sentence for c in OPTIONAL_CUES) else "required"
        tokens += [f"amenity:{a}:{role}" for a in AMENITIES if re.search(rf"\b{a}\b", sentence)]
    for phrase, canon in TYPE_WORDS.items():
        if re.search(rf"\b{phrase}\b", t):
            tokens.append(f"type:{canon}")
            t = t.replace(phrase, " ")          # "private room" should not also count as "room"
    tokens += [NUMBER_WORDS[p] for p in NUMBER_WORDS if p in t]
    return tuple(sorted(tokens))


def embed(text):
    body = json.dumps({"model": EMBED_MODEL, "input": f"search_query: {text}"}).encode()
    req = urllib.request.Request(f"{OLLAMA_URL}/api/embed", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())["embeddings"][0]


def cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    return dot / (math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)))


class SemanticCache:
    def __init__(self, path=CACHE_PATH, threshold=THRESHOLD):
        self.path, self.threshold = Path(path), threshold
        self.entries = json.loads(self.path.read_text()) if self.path.exists() else []

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.entries) + "\n")

    def lookup(self, text, vector=None):
        """Return (cached request or None, diagnostics)."""
        vector = vector or embed(text)
        tokens = list(hard_tokens(text))
        scored = [(cosine(vector, e["embedding"]), e) for e in self.entries if e["model"] == EMBED_MODEL]
        gated = [(s, e) for s, e in scored if e["tokens"] == tokens]       # gate 1 first
        pool = gated or scored                                             # diagnostics if nothing passes
        best_sim, best = max(pool, key=lambda x: x[0]) if pool else (-1.0, None)
        diag = {"similarity": round(best_sim, 4) if best else None,
                "matched_text": best["text"] if best else None,
                "hard_gate": bool(gated),
                "semantic_gate": best is not None and best_sim >= self.threshold}
        hit = diag["hard_gate"] and diag["semantic_gate"]
        return (best["request"] if hit else None), dict(diag, hit=hit, vector=vector)

    def add(self, text, request, vector=None):
        self.entries.append({"text": text, "tokens": list(hard_tokens(text)),
                             "embedding": vector or embed(text), "request": request,
                             "model": EMBED_MODEL})


if __name__ == "__main__":
    a, b = sys.argv[1], sys.argv[2]
    ta, tb = hard_tokens(a), hard_tokens(b)
    sim = cosine(embed(a), embed(b))
    print(f"similarity      {sim:.4f}  (threshold {THRESHOLD}) -> semantic gate {'PASS' if sim >= THRESHOLD else 'FAIL'}")
    print(f"hard tokens A   {ta}")
    print(f"hard tokens B   {tb}")
    print(f"hard gate       {'PASS' if ta == tb else 'FAIL'}")
    print(f"cache hit       {'YES' if sim >= THRESHOLD and ta == tb else 'NO'}")
