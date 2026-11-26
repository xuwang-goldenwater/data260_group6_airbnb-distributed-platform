"""DEV ONLY: mint a test JWT for a seed guest/host (or a service) signed with JWT_SECRET from the env/.env.

Role A will own the real issuer; this exists so Role B can be demoed before the User service lands.
Usage: dev_token.py guest-1001 [--role guest|host|service] [--ttl 43200]
"""
from __future__ import annotations

import argparse
import time

import _bootstrap  # noqa: F401
import jwt
from booking_core.config import Settings


def mint(settings: Settings, sub: str, role: str, ttl: int = 12 * 3600) -> str:
    now = int(time.time())
    claims = {"sub": sub, "role": role, "iat": now, "exp": now + ttl}
    if settings.jwt_issuer:
        claims["iss"] = settings.jwt_issuer
    return jwt.encode(claims, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("sub")
    ap.add_argument("--role", choices=["guest", "host", "service"], default=None)
    ap.add_argument("--ttl", type=int, default=12 * 3600)
    a = ap.parse_args()
    role = a.role or ("host" if a.sub.startswith("host-") else "guest")
    settings = Settings.from_env()
    if not settings.jwt_secret:
        raise SystemExit("JWT_SECRET is not set (services/booking-api/.env)")
    print(mint(settings, a.sub, role, a.ttl))


if __name__ == "__main__":
    main()
