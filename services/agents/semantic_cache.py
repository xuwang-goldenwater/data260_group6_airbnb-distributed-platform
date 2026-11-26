"""Semantic cache for the Parser: reuse a parse when a new request means the
same thing as a cached one.

A cached parse is reused only if BOTH gates pass:
  1. semantic gate: cosine similarity of the two request embeddings
     (Ollama nomic-embed-text) >= THRESHOLD
  2. hard-constraint gate: the two texts contain exactly the same "hard
     tokens" - numbers, dates, city names, amenity names, property types
So a paraphrase ("kitchen is a must" vs "we need a kitchen") can hit, but a
request with a different budget, date, city or amenity never does.

Known limit: two texts with the same hard tokens in different roles
("need kitchen, love a pool" vs "need pool, love a kitchen") pass gate 2;
only gate 1 can separate them.

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
THRESHOLD = 0.92
CACHE_PATH = Path("benchmarks/cache/semantic_cache.json")

CITIES = ["san jose", "santa cruz", "monterey", "san francisco"]
AMENITIES = ["kitchen", "parking", "pool", "washer", "wifi"]
# phrase -> canonical property type (longest phrases first)
TYPE_WORDS = {"private room": "room", "entire apartment": "apartment", "whole house": "house",
              "apartment": "apartment", "house": "house", "condo": "condo", "cabin": "cabin",
              "room": "room"}
NUMBER_WORDS = {"just me": "1", "alone": "1", "solo": "1"}


def hard_tokens(text):
    """Sorted tuple of the constraint-bearing tokens in a request."""
    t = text.lower()
    tokens = re.findall(r"\d{4}-\d{2}-\d{2}", t)
    rest = re.sub(r"\d{4}-\d{2}-\d{2}", " ", t)
    tokens += [str(float(n)).rstrip("0").rstrip(".") for n in re.findall(r"\d+(?:\.\d+)?", rest)]
    tokens += [f"city:{c}" for c in CITIES if c in t]
    tokens += [f"amenity:{a}" for a in AMENITIES if re.search(rf"\b{a}\b", t)]
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
        tokens = hard_tokens(text)
        best, best_sim = None, -1.0
        for e in self.entries:
            if e["model"] != EMBED_MODEL:
                continue
            sim = cosine(vector, e["embedding"])
            if sim > best_sim:
                best, best_sim = e, sim
        diag = {"similarity": round(best_sim, 4) if best else None,
                "matched_text": best["text"] if best else None,
                "semantic_gate": best is not None and best_sim >= self.threshold,
                "hard_gate": best is not None and tuple(best["tokens"]) == tokens}
        hit = diag["semantic_gate"] and diag["hard_gate"]
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
