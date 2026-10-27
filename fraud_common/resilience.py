from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")


class CircuitOpenError(RuntimeError):
    """Circuit breaker đang mở: không gọi dịch vụ phía sau."""


class CircuitBreaker:
    """closed -> (n lỗi liên tiếp) -> open -> (sau reset_after_s) -> half-open -> closed/open."""

    def __init__(self, failure_threshold: int = 5, reset_after_s: float = 30.0) -> None:
        self.failure_threshold = failure_threshold
        self.reset_after_s = reset_after_s
        self._failures = 0
        self._opened_at: float | None = None

    @property
    def is_open(self) -> bool:
        return self._opened_at is not None and (time.monotonic() - self._opened_at) < self.reset_after_s

    async def call(self, fn: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
        if self.is_open:
            raise CircuitOpenError("circuit breaker is open")
        half_open = self._opened_at is not None
        try:
            result = await fn(*args, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._failures += 1
            if half_open or self._failures >= self.failure_threshold:
                self._opened_at = time.monotonic()
            raise
        self._failures = 0
        self._opened_at = None
        return result
