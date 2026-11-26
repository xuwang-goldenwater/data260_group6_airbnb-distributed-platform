"""JWT check for the AI API (docs/api/00_conventions.md "Authentication and identity").

Identity comes ONLY from a validated token: `sub` is the user id and `role` is one of
guest | host | admin | service | agent. A guest_id or reviewer_id in a request body is never trusted.

Same variables as the Booking service: JWT_SECRET, JWT_ALGORITHM (HS256), optional JWT_ISSUER.
No secret configured -> 503 (the service refuses to run unauthenticated instead of letting everyone in).
"""
from __future__ import annotations

import time
from dataclasses import dataclass

import jwt

from .config import Settings

HUMAN_ROLES = {"guest", "host", "admin"}      # may approve / edit / reject
MACHINE_ROLES = {"service", "agent"}          # never may (ai.md: an agent gets 403)


class AuthError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


@dataclass(frozen=True)
class Principal:
    sub: str
    role: str

    @property
    def is_human(self) -> bool:
        return self.role in HUMAN_ROLES

    def can_see(self, owner_id: str) -> bool:
        return self.role in ("admin", "service") or self.sub == owner_id


def verify(token: str | None, s: Settings) -> Principal:
    if not s.jwt_secret:
        raise AuthError(503, "AUTH_NOT_CONFIGURED", "JWT_SECRET is not set on the AI service")
    if not token:
        raise AuthError(401, "UNAUTHENTICATED", "missing bearer token")
    try:
        opts = {"require": ["sub", "exp"]}
        claims = jwt.decode(token, s.jwt_secret, algorithms=[s.jwt_algorithm], options=opts,
                            issuer=s.jwt_issuer or None)
    except jwt.PyJWTError as exc:
        raise AuthError(401, "UNAUTHENTICATED", f"invalid token: {type(exc).__name__}") from exc
    role = claims.get("role", "guest")
    if role not in HUMAN_ROLES | MACHINE_ROLES:
        raise AuthError(401, "UNAUTHENTICATED", f"unknown role {role!r}")
    return Principal(sub=str(claims["sub"]), role=role)


def bearer(header: str | None) -> str | None:
    if header and header.lower().startswith("bearer "):
        return header[7:].strip()
    return None


def make_token(sub: str, role: str, s: Settings, ttl_s: int = 3600) -> str:
    """For local demos and tests only (scripts/dev_token.py). Real tokens come from the platform login."""
    claims = {"sub": sub, "role": role, "iat": int(time.time()), "exp": int(time.time()) + ttl_s}
    if s.jwt_issuer:
        claims["iss"] = s.jwt_issuer
    return jwt.encode(claims, s.jwt_secret, algorithm=s.jwt_algorithm)
