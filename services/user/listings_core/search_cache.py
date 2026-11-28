"""Cache listing search data only. Never cache Booking availability or final pages."""
from collections import Counter
from copy import deepcopy
import hashlib
import json
import threading
import time
from typing import Annotated, Literal
import uuid

from pydantic import Field, TypeAdapter, ValidationError
from redis import Redis
from redis.exceptions import RedisError

from .models import ID
from .write_models import CreateProperty, Version


class CachedProperty(CreateProperty):
    property_id: ID
    host_id: ID
    availability: Literal["available", "partially_available", "unavailable"]
    status: Literal["active"]
    rating: Annotated[float, Field(ge=0, le=5, allow_inf_nan=False, strict=True)]
    review_count: Annotated[int, Field(ge=0, strict=True)]
    version: Version
    created_datetime: str
    updated_datetime: str


ROWS = TypeAdapter(list[CachedProperty])


def revision(repository):
    # Both Property writes and rating projection already increment version in
    # the SAME transaction as the listing change. No new schema is required.
    with repository.connection() as conn, conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS n,COALESCE(SUM(version),0) AS v FROM properties")
        row = cursor.fetchone()
    return str(row["n"]) + ":" + str(row["v"])


def make_client(settings):
    return Redis.from_url(settings.redis_url, decode_responses=True,
                          socket_connect_timeout=.25, socket_timeout=.25,
                          retry_on_timeout=False)


def prefix(settings):
    scope = json.dumps([settings.mysql_host, settings.mysql_port, settings.mysql_database], separators=(",", ":"))
    return "property-search:v1:" + hashlib.sha256(scope.encode()).hexdigest()[:24] + ":"


def normalized(body):
    data = body.model_dump(mode="json")
    for field in ("required_amenities", "property_types"):
        data[field] = sorted(data[field])
    for field in ("minimum_nightly_price", "maximum_nightly_price", "minimum_rating"):
        if data[field] is not None:
            data[field] = float(data[field])
    # Preserve location case: MySQL's collation handles it; no Python-only
    # normalizer should merge SQL-distinct requests (accents, postal strings).
    return data


class SearchCache:
    def __init__(self, settings, *, client=None):
        self.settings, self.enabled = settings, settings.cache_enabled
        self.redis = (client if client is not None else make_client(settings)) if self.enabled else None
        self.prefix = prefix(settings)
        self.lock, self.counts = threading.Lock(), Counter()
        self.ms_sum, self.calls = 0.0, 0

    def token(self):
        key = self.prefix + "generation"
        token = self.redis.get(key)
        if token is None:
            self.redis.set(key, uuid.uuid4().hex, nx=True)
            token = self.redis.get(key)
        if not isinstance(token, str) or len(token) != 32 or any(c not in "0123456789abcdef" for c in token):
            raise ValueError("Invalid cache generation")
        return token

    def key(self, body, source_revision, token):
        manifest = json.loads((self.settings.seed_dir / "manifest.json").read_text())
        identity = [manifest["dataset_version"], manifest["sha256"]["properties.json"]]
        raw = json.dumps([identity, normalized(body), source_revision, token], sort_keys=True, separators=(",", ":"), allow_nan=False)
        return self.prefix + "rows:" + hashlib.sha256(raw.encode()).hexdigest()

    def load(self, repository, body):
        started = time.perf_counter()
        status = "BYPASS"
        try:
            if not self.enabled:
                return repository.search(body), status
            try:
                before, token = revision(repository), self.token()
                key = self.key(body, before, token)
                blob = self.redis.get(key)
                if blob is not None:
                    envelope = json.loads(blob)
                    if not isinstance(envelope, dict) or envelope.get("key") != key:
                        raise ValueError("Cache identity mismatch")
                    rows = [p.model_dump(mode="json") for p in ROWS.validate_python(envelope["rows"])]
                    if len({p["property_id"] for p in rows}) != len(rows):
                        raise ValueError("Duplicate cached property")
                    status = "HIT"
                else:
                    rows, status = repository.search(body), "MISS"
                # Reject cached snapshots crossing a concurrent committed
                # listing/rating write or event invalidation. New fills retain
                # their old key, so delayed fills cannot resurrect a new epoch.
                if revision(repository) != before or self.redis.get(self.prefix + "generation") != token:
                    status = "RACE"
                    return repository.search(body), status
                if status == "MISS":
                    self.redis.set(key, json.dumps({"key": key, "rows": rows}, separators=(",", ":"), allow_nan=False), ex=self.settings.cache_ttl_s)
                return deepcopy(rows), status
            except (RedisError, ValueError, KeyError, TypeError, OSError, ValidationError):
                status = "FALLBACK"
                return repository.search(body), status
        finally:
            with self.lock:
                self.counts[status.lower()] += 1
                self.calls += 1
                self.ms_sum += (time.perf_counter() - started) * 1000

    def stats(self):
        with self.lock:
            result = {"enabled": self.enabled, "calls": self.calls, "outcomes": dict(self.counts),
                      "listing_lookup_ms_mean": self.ms_sum / self.calls if self.calls else 0.0,
                      "ttl_seconds": self.settings.cache_ttl_s}
        result["redis_available"], result["event_invalidations"] = False, None
        if self.redis is not None:
            try:
                result["redis_available"] = bool(self.redis.ping())
                result["event_invalidations"] = int(self.redis.get(self.prefix + "invalidations") or 0)
            except (RedisError, ValueError):
                pass
        return result
