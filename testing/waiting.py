"""Shared poll-until-observable helper for the harnesses."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


class WaitTimeout(RuntimeError):
    """A polled condition never became observable. Callers decide what it means."""


def await_until(
    probe: Callable[[], T | None],
    *,
    budget: float,
    describe: str,
    interval: float = 1.0,
) -> T:
    """Poll until `probe` returns a value. `None` means "not yet", never "false"."""
    deadline = time.monotonic() + budget
    while True:
        value = probe()
        if value is not None:
            return value
        if time.monotonic() >= deadline:
            raise WaitTimeout(f"{describe} within {budget:g}s")
        time.sleep(interval)
