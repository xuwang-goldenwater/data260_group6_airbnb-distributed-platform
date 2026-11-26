"""Cross-service clients with fallbacks for teammates' services that are not deployed yet.

PropertyClient: PROPERTY_SOURCE=http → POST {PROPERTY_SERVICE_URL}/properties/get (Role A);
                PROPERTY_SOURCE=seed → data/seed/generated/properties.json.
UserClient:      USER_SERVICE_URL → POST /guests/get, else / on failure seed guests.json.
AnalyticsClient: ANALYTICS_SERVICE_URL → GET /analytics/host/{host_id}; unavailable → None (views/saves null).
"""
from __future__ import annotations

import json
import logging
import time
from functools import cached_property
from pathlib import Path
from typing import Any, Protocol

import httpx
import jwt

from .config import Settings
from .errors import DependencyUnavailable

log = logging.getLogger("booking.clients")

PROPERTY_FIELDS = ("property_id", "host_id", "status", "capacity", "nightly_price", "cleaning_fee", "rating",
                   "review_count")


def service_jwt(settings: Settings, service: str = "booking-service", ttl_s: int = 300) -> str:
    now = int(time.time())
    claims = {"sub": service, "role": "service", "iat": now, "exp": now + ttl_s}
    if settings.jwt_issuer:
        claims["iss"] = settings.jwt_issuer
    return jwt.encode(claims, settings.jwt_secret, algorithm=settings.jwt_algorithm)


class _Seed:
    def __init__(self, seed_dir: Path):
        self._dir = seed_dir

    @cached_property
    def properties(self) -> dict[str, dict[str, Any]]:
        rows = json.loads((self._dir / "properties.json").read_text())
        return {p["property_id"]: {k: p.get(k) for k in PROPERTY_FIELDS} for p in rows}

    @cached_property
    def guests(self) -> dict[str, dict[str, Any]]:
        return {g["guest_id"]: g for g in json.loads((self._dir / "guests.json").read_text())}

    @cached_property
    def hosts(self) -> set[str]:
        return {h["host_id"] for h in json.loads((self._dir / "hosts.json").read_text())}


class PropertyClient(Protocol):
    source: str
    host_list_source: str

    def get(self, property_id: str) -> dict[str, Any] | None: ...

    def list_by_host(self, host_id: str) -> list[dict[str, Any]]: ...


class SeedPropertyClient:
    source = "seed"
    host_list_source = "seed"

    def __init__(self, settings: Settings):
        self._seed = _Seed(settings.seed_dir)

    def get(self, property_id: str) -> dict[str, Any] | None:
        p = self._seed.properties.get(property_id)
        return dict(p) if p else None

    def list_by_host(self, host_id: str) -> list[dict[str, Any]]:
        return [dict(p) for p in self._seed.properties.values() if p["host_id"] == host_id]


class HttpPropertyClient:
    source = "property-service"
    host_list_source = "seed"   # no Role A "properties by host" endpoint yet

    def __init__(self, settings: Settings):
        if not settings.property_service_url:
            raise ValueError("PROPERTY_SOURCE=http requires PROPERTY_SERVICE_URL")
        self._s = settings
        self._http = httpx.Client(base_url=settings.property_service_url, timeout=settings.http_timeout_s)
        self._seed = SeedPropertyClient(settings)

    def get(self, property_id: str) -> dict[str, Any] | None:
        try:
            r = self._http.post("/properties/get", json={"property_id": property_id},
                                headers={"Authorization": f"Bearer {service_jwt(self._s)}"})
        except httpx.HTTPError as exc:
            raise DependencyUnavailable("property-service", exc) from exc
        if r.status_code == 404:
            return None
        if r.status_code >= 500 or r.status_code in (401, 403):
            raise DependencyUnavailable("property-service", RuntimeError(f"HTTP {r.status_code}"))
        r.raise_for_status()
        body = r.json()
        return {k: body.get(k) for k in PROPERTY_FIELDS}

    def list_by_host(self, host_id: str) -> list[dict[str, Any]]:
        # Role A has no "properties by host" endpoint yet; the fixed 200-property pool is used instead.
        return self._seed.list_by_host(host_id)


def make_property_client(settings: Settings) -> PropertyClient:
    if settings.property_source == "http":
        return HttpPropertyClient(settings)
    return SeedPropertyClient(settings)


class UserClient:
    def __init__(self, settings: Settings):
        self._s = settings
        self._seed = _Seed(settings.seed_dir)
        self._http = (httpx.Client(base_url=settings.user_service_url, timeout=settings.http_timeout_s)
                      if settings.user_service_url else None)

    def saved_properties_count(self, guest_id: str) -> tuple[int | None, str]:
        if self._http is not None:
            try:
                r = self._http.post("/guests/get", json={"guest_id": guest_id},
                                    headers={"Authorization": f"Bearer {service_jwt(self._s)}"})
                if r.status_code == 200:
                    return int(r.json().get("saved_properties_count", 0)), "user-service"
            except (httpx.HTTPError, ValueError) as exc:
                log.warning("user-service unavailable, using seed fallback: %s", exc)
        g = self._seed.guests.get(guest_id)
        return (g.get("saved_properties_count", 0) if g else None), "seed"

    def is_host(self, subject: str) -> bool:
        return subject in self._seed.hosts


class AnalyticsClient:
    def __init__(self, settings: Settings):
        self._s = settings
        self._http = (httpx.Client(base_url=settings.analytics_service_url, timeout=settings.http_timeout_s)
                      if settings.analytics_service_url else None)

    def host_funnel(self, host_id: str, days: int, property_id: str | None) -> dict[str, int] | None:
        if self._http is None:
            return None
        params: dict[str, Any] = {"days": days}
        if property_id:
            params["property_id"] = property_id
        try:
            r = self._http.get(f"/analytics/host/{host_id}", params=params,
                               headers={"Authorization": f"Bearer {service_jwt(self._s)}"})
            if r.status_code != 200:
                return None
            body = r.json()
            src = body.get("funnel", body)
            return {"views": src.get("views"), "saves": src.get("saves")}
        except (httpx.HTTPError, ValueError) as exc:
            log.warning("analytics unavailable: %s", exc)
            return None
