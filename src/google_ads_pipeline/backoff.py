"""Retries with exponential backoff and jitter."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

T = TypeVar("T")

log = logging.getLogger(__name__)


def half_jitter(delay: float) -> float:
    """Randomise a delay into [delay / 2, delay] so parallel workers do not retry in step."""
    return random.uniform(delay / 2, delay)


@dataclass(frozen=True)
class RetryPolicy:
    """Retry a call while the error is transient, up to `max_attempts` attempts in total.

    The base delay doubles after every failure (5s, 10s, 20s, 40s, capped at `max_delay`),
    which is the backoff schedule the Google Ads API documentation suggests.
    """

    max_attempts: int = 5
    initial_delay: float = 5.0
    max_delay: float = 60.0
    sleep: Callable[[float], None] = time.sleep
    jitter: Callable[[float], float] = half_jitter

    def delay(self, attempt: int) -> float:
        """Base delay after failed attempt number `attempt` (1-based), before jitter."""
        return min(self.max_delay, self.initial_delay * 2 ** (attempt - 1))

    def call(
        self,
        fn: Callable[[], T],
        is_transient: Callable[[BaseException], bool],
        description: str = "request",
    ) -> T:
        attempt = 1
        while True:
            try:
                return fn()
            except Exception as exc:
                if attempt >= self.max_attempts or not is_transient(exc):
                    raise
                delay = self.jitter(self.delay(attempt))
                log.warning(
                    "%s failed on attempt %d/%d (%s: %s); retrying in %.1fs",
                    description,
                    attempt,
                    self.max_attempts,
                    type(exc).__name__,
                    exc,
                    delay,
                )
                self.sleep(delay)
                attempt += 1
