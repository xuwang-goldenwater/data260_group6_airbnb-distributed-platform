"""Common error body (docs/api/00_conventions.md) as an exception."""
from __future__ import annotations

from typing import Any


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, *, retryable: bool = False,
                 details: dict[str, Any] | None = None, headers: dict[str, str] | None = None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.retryable = retryable
        self.details = details
        self.headers = headers

    def body(self, trace_id: str) -> dict[str, Any]:
        err: dict[str, Any] = {"code": self.code, "message": self.message,
                               "trace_id": trace_id, "retryable": self.retryable}
        if self.details:
            err["details"] = self.details
        return {"error": err}


def validation(message: str, **details: Any) -> ApiError:
    return ApiError(400, "VALIDATION_ERROR", message, details=details or None)


def dependency_unavailable(what: str) -> ApiError:
    return ApiError(503, "DEPENDENCY_UNAVAILABLE", f"{what} is unavailable.", retryable=True,
                    details={"dependency": what})


class DependencyUnavailable(Exception):
    """Raised by clients/DB pool when a dependency cannot be reached."""

    def __init__(self, dependency: str, cause: Exception | None = None):
        super().__init__(f"{dependency} unavailable: {cause}")
        self.dependency = dependency
