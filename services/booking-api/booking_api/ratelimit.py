"""In-process sliding-window limiter for /bookings/create (429 RATE_LIMITED). 0 disables it."""
from __future__ import annotations

import threading
import time
from collections import defaultdict, deque

from booking_core.errors import ApiError


class RateLimiter:
    def __init__(self, per_minute: int):
        self.per_minute = per_minute
        self._hits: dict[str, deque] = defaultdict(deque)
        self._lock = threading.Lock()

    def check(self, subject: str) -> None:
        if self.per_minute <= 0:
            return
        now = time.monotonic()
        with self._lock:
            q = self._hits[subject]
            while q and now - q[0] > 60:
                q.popleft()
            if len(q) >= self.per_minute:
                retry = max(1, int(60 - (now - q[0])) + 1)
                raise ApiError(429, "RATE_LIMITED", "Too many booking requests; retry later.", retryable=True,
                               details={"retry_after_seconds": retry}, headers={"Retry-After": str(retry)})
            q.append(now)
