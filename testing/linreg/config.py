"""Workload configuration and its defaults.

Separate from the entrypoint so the workload modules can import it without a
cycle, and so nothing is tempted to reach back for a module global.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

DEFAULT_NUM_KEYS: Final[int] = 3
"""Independent register keys. Each is checked separately."""

DEFAULT_NUM_WORKERS: Final[int] = 2
"""Concurrent client threads."""

DEFAULT_WORKLOAD_DURATION_SECONDS: Final[float] = 6.0
"""Total wall-clock time the workload runs."""

DEFAULT_KILL_LEADER_AFTER_SECONDS: Final[float] = 2.0
"""When (relative to workload start) to inject the failover."""

GATEWAY_RETRY_BACKOFF_BASE: Final[float] = 0.05
"""Per-consecutive-failure backoff after an indeterminate op, in seconds."""

GATEWAY_RETRY_BACKOFF_MAX: Final[float] = 0.5
"""Cap on that backoff, so a fully-down cluster neither spins nor sleeps past
its own recovery."""

CHAOS_STORM_DURATION: Final[float] = 25.0
"""Window the `chaos_storm` attack spreads its 3-5 faults across."""

PSQL_TIMEOUT_SECONDS: Final[int] = 4


@dataclass(frozen=True)
class WorkloadConfig:
    """Run configuration, resolved once from the CLI and passed explicitly.

    Explicit rather than module-global because `global` rebinds a name only in
    the module that defines it. A worker or checker living in another module
    would keep reading the defaults and silently ignore every CLI flag, so the
    harness would report on a workload nobody asked for.
    """

    workers: int = DEFAULT_NUM_WORKERS
    keys: int = DEFAULT_NUM_KEYS
    duration_s: float = DEFAULT_WORKLOAD_DURATION_SECONDS
    fault_at: float = DEFAULT_KILL_LEADER_AFTER_SECONDS
