"""Retry timing: exponential backoff with jitter, or the server's Retry-After when given."""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass

MAX_RETRY_AFTER = 900.0  # never trust a Retry-After longer than 15 minutes


def half_jitter(delay: float) -> float:
    """Randomise a delay into [delay / 2, delay] so parallel workers do not retry in step."""
    return random.uniform(delay / 2, delay)


@dataclass(frozen=True)
class RetryPolicy:
    """Up to `max_attempts` attempts; the base delay doubles after each failure (5s, 10s, ...)."""

    max_attempts: int = 6
    initial_delay: float = 5.0
    max_delay: float = 120.0
    sleep: Callable[[float], None] = time.sleep
    jitter: Callable[[float], float] = half_jitter

    def delay(self, attempt: int, retry_after: float | None = None) -> float:
        """Seconds to wait after failed attempt number `attempt` (1-based)."""
        if retry_after is not None:
            return min(retry_after, MAX_RETRY_AFTER)
        return self.jitter(min(self.max_delay, self.initial_delay * 2 ** (attempt - 1)))
