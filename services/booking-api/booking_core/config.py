"""Environment-driven settings shared by the Booking API, worker and scripts.

All configuration comes from environment variables. For local runs the optional file
``services/booking-api/.env`` (never committed; copy ``.env.example``) is loaded first;
real environment variables always win over the file.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

SERVICE_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]


def load_env_file(path: str | os.PathLike | None = None) -> None:
    env_path = Path(path or os.environ.get("BOOKING_ENV_FILE", SERVICE_DIR / ".env"))
    if not env_path.is_file():
        return
    for raw in env_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _str(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _int(name: str, default: int) -> int:
    value = os.environ.get(name, "").strip()
    return int(value) if value else default


def _float(name: str, default: float) -> float:
    value = os.environ.get(name, "").strip()
    return float(value) if value else default


def _bool(name: str, default: bool) -> bool:
    value = os.environ.get(name, "").strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    # MySQL
    mysql_host: str
    mysql_port: int
    mysql_user: str
    mysql_password: str
    mysql_database: str
    mysql_pool_size: int
    # Kafka
    kafka_bootstrap: str
    # Auth
    jwt_secret: str
    jwt_algorithm: str
    jwt_issuer: str
    service_token: str
    dev_token_endpoint: bool
    # Cross-service clients ("seed" fallback reads data/seed/generated/*.json)
    property_source: str
    property_service_url: str
    user_service_url: str
    analytics_service_url: str
    http_timeout_s: float
    seed_dir: Path
    envelope_schema_path: Path
    # API behaviour flags
    precheck_enabled: bool
    rate_limit_per_minute: int
    test_spread_partitions: bool
    outbox_interval_s: float
    outbox_min_age_s: float
    completion_interval_s: float
    ws_enabled: bool
    ws_consumer_group: str
    # Worker
    worker_id: str
    worker_group: str
    worker_fault: str
    worker_transient_faults: int

    @classmethod
    def from_env(cls) -> "Settings":
        load_env_file()
        return cls(
            mysql_host=_str("MYSQL_HOST", "127.0.0.1"),
            mysql_port=_int("MYSQL_PORT", 3306),
            mysql_user=_str("MYSQL_USER", "airbnb"),
            mysql_password=_str("MYSQL_PASSWORD", "airbnb"),
            mysql_database=_str("MYSQL_DATABASE", "airbnb"),
            mysql_pool_size=_int("MYSQL_POOL_SIZE", 32),
            kafka_bootstrap=_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            jwt_secret=_str("JWT_SECRET"),
            jwt_algorithm=_str("JWT_ALGORITHM", "HS256"),
            jwt_issuer=_str("JWT_ISSUER"),
            service_token=_str("BOOKING_SERVICE_TOKEN"),
            dev_token_endpoint=_bool("BOOKING_DEV_TOKEN_ENDPOINT", False),
            property_source=_str("PROPERTY_SOURCE", "seed").lower(),
            property_service_url=_str("PROPERTY_SERVICE_URL").rstrip("/"),
            user_service_url=_str("USER_SERVICE_URL").rstrip("/"),
            analytics_service_url=_str("ANALYTICS_SERVICE_URL").rstrip("/"),
            http_timeout_s=_float("HTTP_TIMEOUT_S", 2.0),
            seed_dir=Path(_str("SEED_DIR") or REPO_ROOT / "data/seed/generated"),
            envelope_schema_path=Path(_str("ENVELOPE_SCHEMA") or REPO_ROOT / "shared/envelope.schema.json"),
            precheck_enabled=_bool("BOOKING_PRECHECK_ENABLED", True),
            rate_limit_per_minute=_int("BOOKING_RATE_LIMIT_PER_MINUTE", 120),
            test_spread_partitions=_bool("BOOKING_TEST_SPREAD_PARTITIONS", False),
            outbox_interval_s=_float("BOOKING_OUTBOX_INTERVAL_S", 2.0),
            outbox_min_age_s=_float("BOOKING_OUTBOX_MIN_AGE_S", 5.0),
            completion_interval_s=_float("BOOKING_COMPLETION_INTERVAL_S", 900.0),
            ws_enabled=_bool("BOOKING_WS_ENABLED", True),
            ws_consumer_group=_str("BOOKING_WS_CONSUMER_GROUP", "booking-status"),
            worker_id=_str("WORKER_ID", "worker-1"),
            worker_group=_str("WORKER_GROUP", "booking-worker"),
            worker_fault=_str("WORKER_FAULT").lower(),
            worker_transient_faults=_int("WORKER_TRANSIENT_FAULTS", 0),
        )
