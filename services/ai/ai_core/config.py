"""Settings for the AI service (Role D). Everything can be overridden with environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# repo root = services/ai/ai_core/config.py -> parents[3]
REPO_ROOT = Path(__file__).resolve().parents[3]

# Kafka topics owned or used by the AI flow (docs/api/00_conventions.md topic registry)
T_REQUESTS = "ai.requests"
T_PARSED = "ai.parsed"
T_ROUND0 = "debate.round.0"
T_ROUND1 = "debate.round.1"
T_ROUND2 = "debate.round.2"
T_RESULTS = "ai.results"
T_APPROVALS = "ai.approvals"
T_DLQ = "platform.dlq"

# evaluation_config -> agents the Chair waits for (docs/api/ai.md "expected agents")
EXPECTED_AGENTS = {
    "single_agent": ["matching"],
    "debate_without_auditor": ["matching", "risk"],
    "full_debate": ["matching", "risk", "fairness"],
}
ONLINE_CONFIGS = tuple(EXPECTED_AGENTS)   # "baseline" runs offline in benchmarks/, not through Kafka


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    mongo_uri: str = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    mongo_db: str = os.environ.get("MONGO_DB", "airbnb_ai")
    # per-round deadline in seconds (ai.md: deadline_ms 10000 by default)
    round_timeout_s: float = float(os.environ.get("AI_ROUND_TIMEOUT_S", "10"))
    # Explanation step: "llm" (Ollama, falls back to a template) or "template"
    explain_mode: str = os.environ.get("AI_EXPLAIN_MODE", "llm")
    explain_timeout_s: float = float(os.environ.get("AI_EXPLAIN_TIMEOUT_S", "5"))
    ollama_url: str = os.environ.get("OLLAMA_URL", "http://localhost:11434")
    explain_model: str = os.environ.get("AI_EXPLAIN_MODEL", "qwen2.5:3b")
    # JWT (same variables as the Booking service, docs/api/00_conventions.md)
    jwt_secret: str = os.environ.get("JWT_SECRET", "")
    jwt_algorithm: str = os.environ.get("JWT_ALGORITHM", "HS256")
    jwt_issuer: str = os.environ.get("JWT_ISSUER", "")
    envelope_schema: Path = REPO_ROOT / "shared" / "envelope.schema.json"
    seed_dir: Path = REPO_ROOT / "data" / "seed" / "generated"


def settings() -> Settings:
    return Settings()
