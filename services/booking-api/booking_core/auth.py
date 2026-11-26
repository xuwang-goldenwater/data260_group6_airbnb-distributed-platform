"""JWT verification per docs/api/00_conventions.md: identity = validated `sub`; never trust body IDs.

Claims used: sub (guest-…/host-…/service name), role (guest|host|service), exp; iss checked when JWT_ISSUER set.
Role A will own the real issuer; until then scripts/dev_token.py mints dev tokens with the same secret.
"""
from __future__ import annotations

import hmac
from dataclasses import dataclass

import jwt

from .config import Settings
from .errors import ApiError


@dataclass(frozen=True)
class Actor:
    sub: str
    role: str

    @property
    def is_host(self) -> bool:
        return self.role == "host"


def _unauthorized(msg: str) -> ApiError:
    return ApiError(401, "UNAUTHORIZED", msg, headers={"WWW-Authenticate": "Bearer"})


def decode_token(settings: Settings, token: str) -> Actor:
    if not settings.jwt_secret:
        raise ApiError(503, "DEPENDENCY_UNAVAILABLE", "JWT verification is not configured.", retryable=True)
    options = {"require": ["sub", "exp"]}
    try:
        claims = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm],
                            issuer=settings.jwt_issuer or None, options=options)
    except jwt.ExpiredSignatureError:
        raise _unauthorized("Token expired.") from None
    except jwt.PyJWTError:
        raise _unauthorized("Invalid token.") from None
    sub = str(claims["sub"])
    role = claims.get("role") or ("host" if sub.startswith("host-") else "guest")
    if role not in ("guest", "host", "service", "admin"):
        raise _unauthorized("Unknown role.")
    return Actor(sub=sub, role=role)


def bearer(authorization: str | None) -> str | None:
    if not authorization:
        return None
    scheme, _, token = authorization.partition(" ")
    return token.strip() if scheme.lower() == "bearer" and token.strip() else None


def require_user(settings: Settings, authorization: str | None) -> Actor:
    token = bearer(authorization)
    if token is None:
        raise _unauthorized("Missing bearer token.")
    actor = decode_token(settings, token)
    if actor.role == "service":
        raise ApiError(403, "FORBIDDEN", "Service tokens cannot call user endpoints.")
    return actor


def optional_user(settings: Settings, authorization: str | None) -> Actor | None:
    token = bearer(authorization)
    return decode_token(settings, token) if token else None


def require_service(settings: Settings, authorization: str | None, service_token: str | None) -> str:
    if settings.service_token and service_token and hmac.compare_digest(service_token, settings.service_token):
        return "service-token"
    token = bearer(authorization)
    if token is None:
        raise _unauthorized("Service authentication required.")
    actor = decode_token(settings, token)
    if actor.role != "service":
        raise ApiError(403, "FORBIDDEN", "Internal endpoints require service authentication.")
    return actor.sub
