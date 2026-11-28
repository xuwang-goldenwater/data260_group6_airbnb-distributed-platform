from dataclasses import dataclass
import logging
import re
import time
import uuid

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException
import jwt

log = logging.getLogger("listings")


class ApiError(Exception):
    def __init__(self, status, code, message, *, details=None):
        super().__init__(message)
        self.status, self.code, self.message, self.details = status, code, message, details


def unavailable(name):
    return ApiError(503, "DEPENDENCY_UNAVAILABLE", f"{name} is unavailable.")


@dataclass(frozen=True)
class Actor:
    sub: str
    role: str


def authenticate(request, settings, *, optional=False, service_only=False):
    header = request.headers.get("authorization")
    if header is None and optional:
        return None
    if not header or not header.lower().startswith("bearer ") or not header[7:].strip():
        raise ApiError(401, "UNAUTHORIZED", "A bearer token is required.")
    if not settings.jwt_secret:
        raise unavailable("JWT verification configuration")
    try:
        claims = jwt.decode(header[7:].strip(), settings.jwt_secret, algorithms=["HS256"],
                            issuer=settings.jwt_issuer or None,
                            options={"require": ["sub", "exp", "role"]})
        if not isinstance(claims["sub"], str) or not claims["sub"].strip():
            raise ValueError("Empty subject")
        if claims["role"] not in {"guest", "host", "admin", "service", "agent"}:
            raise ValueError("Unknown role")
    except (jwt.PyJWTError, ValueError, TypeError):
        raise ApiError(401, "UNAUTHORIZED", "Invalid or expired bearer token.") from None
    actor = Actor(claims["sub"], claims["role"])
    if service_only and actor.role != "service":
        raise ApiError(403, "FORBIDDEN", "A service token is required.")
    return actor


def require_profile_access(actor, profile_id, profile_role):
    if actor.role in {"admin", "service"}:
        return
    if actor.role != profile_role or actor.sub != profile_id:
        raise ApiError(403, "NOT_PROFILE_OWNER", "You may only read your own complete profile.")


def service_token(settings):
    if not settings.jwt_secret:
        raise unavailable("JWT verification configuration")
    claims = {"sub": "property-service", "role": "service", "iat": int(time.time()),
              "exp": int(time.time()) + 300}
    if settings.jwt_issuer:
        claims["iss"] = settings.jwt_issuer
    return jwt.encode(claims, settings.jwt_secret, algorithm="HS256")


def make_app(title):
    app = FastAPI(title=title, version="0.1.0")

    @app.middleware("http")
    async def trace(request: Request, call_next):
        supplied = request.headers.get("x-trace-id", "")
        request.state.trace_id = supplied if re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", supplied) else "tr-" + uuid.uuid4().hex
        response = await call_next(request)
        response.headers["X-Trace-Id"] = request.state.trace_id
        return response

    def error(request, status, code, message, details=None):
        payload = {"code": code, "message": message,
                   "trace_id": getattr(request.state, "trace_id", "tr-" + uuid.uuid4().hex),
                   "retryable": status == 503}
        if details is not None:
            payload["details"] = details
        headers = {"WWW-Authenticate": "Bearer"} if status == 401 else None
        return JSONResponse({"error": payload}, status_code=status, headers=headers)

    @app.exception_handler(ApiError)
    async def api_error(request, exc):
        return error(request, exc.status, exc.code, exc.message, exc.details)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        details = [{"field": ".".join(map(str, e["loc"])), "message": e["msg"]} for e in exc.errors()]
        return error(request, 400, "VALIDATION_ERROR", "Invalid request fields.", details)

    @app.exception_handler(HTTPException)
    async def http_error(request, exc):
        return error(request, exc.status_code, "HTTP_ERROR", str(exc.detail))

    @app.exception_handler(Exception)
    async def unexpected(request, exc):
        log.exception("Unexpected error trace_id=%s", getattr(request.state, "trace_id", "unknown"))
        return error(request, 500, "INTERNAL_ERROR", "Unexpected service error.")

    return app
